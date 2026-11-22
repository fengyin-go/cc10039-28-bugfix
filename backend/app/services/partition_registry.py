"""分区读取台账：回填任务和保留策略共同维护的“分区实际可读位置”。

回填失败后检查点不能代表数据源还一定可读：同一分区可能刚到保留期限，
被保留策略归档或清理。这个台账用一把可重入锁把“读取位置变更”和
“回填重新读取”串起来，保证同一天的分区不会被清理和续做同时改写，
并把每次到期处理的结果登记成保留回执，供回填恢复反查剩余数据源。
"""
from __future__ import annotations

import copy
import datetime
import re
import threading
from typing import Any

_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")


def extract_partitions(time_range: str) -> list[str]:
    """从回填时间范围中解析出按天划分的分区清单。

    支持 ``2026-09-01``、``2026-09-01~2026-09-03``、
    ``2026-09-01 至 2026-09-03`` 等写法；解析不到日期时把整段文本
    当成一个分区标识，避免出现“没有分区可续做”的假空清单。
    """
    text = str(time_range or "").strip()
    dates = _DATE_RE.findall(text)
    if not dates:
        return [text] if text else []

    unique = sorted(set(dates))
    if len(unique) == 1:
        return unique

    try:
        start = datetime.date.fromisoformat(unique[0])
        end = datetime.date.fromisoformat(unique[-1])
    except ValueError:
        return unique
    if end < start:
        start, end = end, start

    partitions: list[str] = []
    current = start
    # 上限一年，避免脏数据把内存打爆。
    while current <= end and len(partitions) < 366:
        partitions.append(current.isoformat())
        current += datetime.timedelta(days=1)
    return partitions


def hot_location(table_name: str, partition: str) -> str:
    return f"hot://lake/{table_name}/dt={partition}"


def archive_location(table_name: str, partition: str) -> str:
    return f"archive://cold/{table_name}/dt={partition}"


def _now() -> str:
    return datetime.datetime.now().isoformat()


class PartitionRegistry:
    """分区读取位置台账（线程安全）。

    记录里同时保留 ``read_location`` 和 ``location_kind``，归档不会删除
    数据，只把可读位置从热区切到归档区；物理清理才会把分区标记为
    ``missing``。回填续做只认台账里的实际读取位置。
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        # (湖表, 分区) → 分区记录
        self._records: dict[tuple[str, str], dict[str, Any]] = {}
        self._receipts: list[dict[str, Any]] = []
        self._conflicts: list[dict[str, Any]] = []

    # ---- 生命周期 ----

    def reset(self) -> None:
        """清空台账，测试和本地重置使用。"""
        with self._lock:
            self._records.clear()
            self._receipts.clear()
            self._conflicts.clear()

    # ---- 读取 ----

    def ensure(
        self, table_name: str, partition: str, task_id: int | None = None,
    ) -> dict[str, Any]:
        """确保分区记录存在，返回记录副本。"""
        with self._lock:
            record = self._ensure_locked(table_name, partition, task_id)
            return copy.deepcopy(record)

    def get(self, table_name: str, partition: str) -> dict[str, Any] | None:
        with self._lock:
            record = self._records.get((table_name, partition))
            return copy.deepcopy(record) if record is not None else None

    def peek(self, table_name: str, partition: str) -> dict[str, Any] | None:
        """只读查看分区记录，不创建记录、不改变任何位置。"""
        return self.get(table_name, partition)

    def describe(self, table_name: str, partition: str) -> dict[str, Any]:
        """只读描述分区当前位置；无记录时按热区位置给出预览，不写台账。"""
        with self._lock:
            record = self._records.get((table_name, partition))
            if record is not None:
                return copy.deepcopy(record)
            location = hot_location(table_name, partition)
            return {
                "table_name": table_name,
                "湖表": table_name,
                "partition": partition,
                "分区": partition,
                "read_location": location,
                "读取位置": location,
                "original_location": location,
                "location_kind": "hot",
                "status": "hot",
                "readable": True,
                "backfill_task_id": None,
                "backfilled_count": 0,
                "done": False,
                "settled": False,
                "reader": None,
                "pinned_by": None,
                "retention_id": None,
                "version": 0,
            }

    def records_for_table(self, table_name: str) -> list[dict[str, Any]]:
        with self._lock:
            return [
                copy.deepcopy(record)
                for (table, _), record in self._records.items()
                if table == table_name
            ]

    def records_for_task(self, task_id: int) -> list[dict[str, Any]]:
        with self._lock:
            return [
                copy.deepcopy(record)
                for record in self._records.values()
                if record.get("backfill_task_id") == task_id
            ]

    def receipts_for(
        self, table_name: str | None = None, retention_id: int | None = None,
    ) -> list[dict[str, Any]]:
        with self._lock:
            receipts = self._receipts
            if table_name is not None:
                receipts = [r for r in receipts if r.get("table_name") == table_name]
            if retention_id is not None:
                receipts = [r for r in receipts if r.get("retention_id") == retention_id]
            return copy.deepcopy(receipts)

    def conflicts_for(
        self, table_name: str | None = None, retention_id: int | None = None,
    ) -> list[dict[str, Any]]:
        with self._lock:
            conflicts = self._conflicts
            if table_name is not None:
                conflicts = [c for c in conflicts if c.get("table_name") == table_name]
            if retention_id is not None:
                conflicts = [c for c in conflicts if c.get("retention_id") == retention_id]
            return copy.deepcopy(conflicts)

    # ---- 回填侧操作 ----

    def resolve(
        self, table_name: str, partition: str, task_id: int | None = None,
    ) -> tuple[dict[str, Any], str]:
        """解析分区当前可读位置，返回 (记录, 来源)。

        来源为 ``hot`` / ``retention_archive`` / ``missing``。整个解析
        在锁内完成，避免读到保留策略改到一半的位置；没有记录时不创建。
        """
        with self._lock:
            record = self.describe(table_name, partition)
            if record.get("status") == "missing" or not record.get("readable", True):
                return record, "missing"
            if record.get("location_kind") == "archive":
                return record, "retention_archive"
            return record, "hot"

    def set_count(
        self,
        table_name: str,
        partition: str,
        count: int,
        *,
        task_id: int | None = None,
        done: bool = True,
    ) -> dict[str, Any]:
        """登记某个分区已补条数；完成的分区释放恢复占位。"""
        with self._lock:
            record = self._ensure_locked(table_name, partition, task_id)
            record["backfilled_count"] = max(0, int(count))
            record["done"] = bool(done)
            record["settled"] = bool(done)
            if task_id is not None:
                record["backfill_task_id"] = task_id
            record["version"] = int(record.get("version", 0)) + 1
            record["updated_at"] = _now()
            if done and record.get("pinned_by") == task_id:
                record["pinned_by"] = None
            return copy.deepcopy(record)

    def read_partition(
        self,
        table_name: str,
        partition: str,
        task_id: int,
        count: int,
    ) -> dict[str, Any]:
        """在恢复窗口内读取一个分区。

        整个读取在锁内完成：先占位（``reader``），再确认可读位置，读取后
        直接登记已补条数并释放占位。这样保留策略不会在读取中途改写位置。
        """
        with self._lock:
            record = self._ensure_locked(table_name, partition, task_id)
            if record.get("reader") is not None:
                return {
                    "ok": False,
                    "reason": "reader_busy",
                    "reader": record.get("reader"),
                }
            if record.get("status") == "missing" or not record.get("readable", True):
                return {"ok": False, "reason": "missing"}

            record["reader"] = task_id
            try:
                source = (
                    "retention_archive"
                    if record.get("location_kind") == "archive"
                    else "hot"
                )
                location = record.get("read_location")
                record["backfilled_count"] = int(count)
                record["done"] = True
                record["settled"] = True
                record["backfill_task_id"] = task_id
                record["last_pin"] = {
                    "task_id": task_id,
                    "source": source,
                    "location": location,
                    "read_at": _now(),
                }
                record["version"] = int(record.get("version", 0)) + 1
                record["updated_at"] = _now()
                if record.get("pinned_by") == task_id:
                    record["pinned_by"] = None
                return {
                    "ok": True,
                    "source": source,
                    "location": location,
                    "count": int(count),
                    "record": copy.deepcopy(record),
                }
            finally:
                record["reader"] = None

    def finalize(self, table_name: str, partition: str, task_id: int) -> None:
        """续做窗口自然结束后清除该分区上的任务标记（读取结果已留在台账里）。"""
        with self._lock:
            record = self._records.get((table_name, partition))
            if record is None:
                return
            if record.get("pinned_by") == task_id:
                record["pinned_by"] = None

    # 兼容旧调用名：读取方在使用完毕后归还占位。
    release_reader = finalize

    def pin_task(self, table_name: str, partitions: list[str], task_id: int) -> None:
        """标记恢复窗口：物理删除要为回填让路，归档仍可切换可读位置。"""
        with self._lock:
            for partition in partitions:
                record = self._ensure_locked(table_name, partition, task_id)
                record["pinned_by"] = task_id

    def unpin_task(self, table_name: str, task_id: int) -> None:
        with self._lock:
            for record in self._records.values():
                if record.get("table_name") == table_name and record.get("pinned_by") == task_id:
                    record["pinned_by"] = None

    # ---- 保留侧操作 ----

    def archive(
        self, table_name: str, partition: str, retention_id: int,
    ) -> dict[str, Any]:
        """把分区切到归档区并登记保留回执；读取中的分区先不动。"""
        with self._lock:
            record = self._ensure_locked(table_name, partition)
            if record.get("status") == "missing" or not record.get("readable", True):
                return {"ok": False, "reason": "missing", "record": copy.deepcopy(record)}
            if record.get("reader"):
                return {
                    "ok": False,
                    "reason": "reader_busy",
                    "record": copy.deepcopy(record),
                    "reader": record.get("reader"),
                }
            if record.get("location_kind") == "archive":
                return {"ok": False, "reason": "already_archived", "record": copy.deepcopy(record)}

            previous = record.get("read_location")
            location = archive_location(table_name, partition)
            record.update({
                "read_location": location,
                "location_kind": "archive",
                "status": "archived",
                "readable": True,
                "retention_id": retention_id,
                "archived_location": location,
                "cleaned_at": _now(),
            })
            record["version"] = int(record.get("version", 0)) + 1
            receipt = self._make_receipt(
                table_name, partition, retention_id, "归档", location,
                previous_location=previous, readable=True, record=record,
            )
            self._receipts.append(receipt)
            return {"ok": True, "reason": "archived", "receipt": copy.deepcopy(receipt),
                    "record": copy.deepcopy(record)}

    def delete(
        self, table_name: str, partition: str, retention_id: int,
    ) -> dict[str, Any]:
        """物理清理分区；恢复窗口内或正在读取的分区延后处理。"""
        with self._lock:
            record = self._ensure_locked(table_name, partition)
            if record.get("status") == "missing" or not record.get("readable", True):
                return {"ok": False, "reason": "missing", "record": copy.deepcopy(record)}
            if record.get("reader"):
                return {
                    "ok": False,
                    "reason": "reader_busy",
                    "record": copy.deepcopy(record),
                    "reader": record.get("reader"),
                }
            if record.get("pinned_by") is not None:
                return {
                    "ok": False,
                    "reason": "recovering",
                    "record": copy.deepcopy(record),
                    "pinned_by": record.get("pinned_by"),
                }

            previous = record.get("read_location")
            record.update({
                "read_location": None,
                "location_kind": "removed",
                "status": "missing",
                "readable": False,
                "retention_id": retention_id,
                "cleaned_at": _now(),
            })
            record["version"] = int(record.get("version", 0)) + 1
            receipt = self._make_receipt(
                table_name, partition, retention_id, "删除", None,
                previous_location=previous, readable=False, record=record,
            )
            self._receipts.append(receipt)
            return {"ok": True, "reason": "deleted", "receipt": copy.deepcopy(receipt),
                    "record": copy.deepcopy(record)}

    def record_conflict(
        self,
        table_name: str,
        partition: str,
        retention_id: int,
        reason: str,
        detail: str,
    ) -> dict[str, Any]:
        with self._lock:
            conflict = {
                "retention_id": retention_id,
                "table_name": table_name,
                "湖表": table_name,
                "partition": partition,
                "分区": partition,
                "reason": reason,
                "原因": detail,
                "timestamp": _now(),
                "记录时间": _now(),
            }
            self._conflicts.append(conflict)
            return copy.deepcopy(conflict)

    # ---- 内部 ----

    def _ensure_locked(
        self, table_name: str, partition: str, task_id: int | None = None,
    ) -> dict[str, Any]:
        key = (table_name, partition)
        record = self._records.get(key)
        if record is None:
            location = hot_location(table_name, partition)
            record = {
                "table_name": table_name,
                "湖表": table_name,
                "partition": partition,
                "分区": partition,
                "read_location": location,
                "读取位置": location,
                "original_location": location,
                "location_kind": "hot",
                "status": "hot",
                "readable": True,
                "backfill_task_id": task_id,
                "backfilled_count": 0,
                "done": False,
                "settled": False,
                "reader": None,
                "pinned_by": None,
                "retention_id": None,
                "version": 0,
                "created_at": _now(),
            }
            self._records[key] = record
        elif task_id is not None and record.get("backfill_task_id") is None:
            record["backfill_task_id"] = task_id
        return record

    def _make_receipt(
        self,
        table_name: str,
        partition: str,
        retention_id: int,
        action: str,
        location: str | None,
        *,
        previous_location: str | None,
        readable: bool,
        record: dict[str, Any],
    ) -> dict[str, Any]:
        timestamp = _now()
        return {
            "id": len(self._receipts) + 1,
            "retention_id": retention_id,
            "保留策略": retention_id,
            "table_name": table_name,
            "湖表": table_name,
            "partition": partition,
            "分区": partition,
            "action": action,
            "清理动作": action,
            "read_location": location,
            "读取位置": location,
            "previous_location": previous_location,
            "原读取位置": previous_location,
            "readable": readable,
            "可读": readable,
            "backfilled_count": int(record.get("backfilled_count", 0)),
            "已补条数": int(record.get("backfilled_count", 0)),
            "backfill_job_id": record.get("backfill_task_id"),
            "回填任务": record.get("backfill_task_id"),
            "timestamp": timestamp,
            "记录时间": timestamp,
        }


partition_registry = PartitionRegistry()
