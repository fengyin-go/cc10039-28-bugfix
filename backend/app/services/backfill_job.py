"""数据回填任务领域服务：管理历史数据回填、补数和断点续做。

回填按天划分分区，检查点只记录“已确认”的进度。中断或失败后检查点不会被清掉，
续做动作从已确认的分区之后继续；如果某个分区已被保留策略清理掉，续做会说明
缺少数据源的原因并保留原检查点，而不是把已完成的进度重做一遍。

分区实际读取位置由 ``partition_registry`` 统一维护（线程安全）：保留策略归档 /
删除分区与回填重新读取同一分区时，双方通过台账的占位互斥，不会同时改写位置。
"""
from __future__ import annotations

import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.services.partition_registry import (
    extract_partitions,
    partition_registry,
)
from app.store import store

# 演示数据规模：每个按天分区在示例数据中按固定条数计算，便于展示“已补条数”。
PARTITION_RECORDS = 10

_REASON_TEXT = {
    "missing": "分区已被保留策略物理清理，数据源不可读",
    "reader_busy": "分区正在被其他读取方占用",
    "recovering": "分区处于其他任务的恢复窗口，暂时不可改动",
}


class BackfillJobService(BaseModuleService):
    MODULE = "backfill_job"
    ENTITY = "回填任务"
    OPERATION_TERM = "调度"
    LIST_FIELDS = ["任务名称", "目标湖表", "回填时间范围", "处理记录数", "执行状态"]
    REQUIRED_FIELDS = ["任务名称", "目标湖表", "回填时间范围"]
    STATUS_ORDER = ["待调度", "回填中", "已完成", "回填失败"]
    ACTION_RULES = {"启动回填": "回填中", "取消回填": "回填失败"}
    NEGATIVE_ACTIONS = []

    # 回填检查点：任务 id → 已确认记录数
    _checkpoints: dict[int, int] = {}
    # 续做计划：任务 id → 分区范围、上次续做结果
    _plans: dict[int, dict[str, Any]] = {}

    # ---- 启动 ----

    def start_backfill(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """启动回填任务：检查 Schema 变更状态后启动。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "回填任务不存在"
        if entry.get("status") != "待调度":
            if entry.get("status") == "回填失败":
                return None, "回填任务处于回填失败状态，请使用「续做回填」从检查点恢复"
            return None, f"回填任务状态为{entry.get('status')}，不可启动"

        table_name = entry.get("目标湖表", "")
        time_range = entry.get("回填时间范围", "")

        # 检查同湖表是否有执行中的回填
        active_backfills = [
            bj for bj in store.rows(self.MODULE)
            if bj.get("目标湖表") == table_name
            and bj.get("status") == "回填中"
            and bj.get("id") != entry_id
        ]
        if active_backfills:
            return None, f"湖表 {table_name} 已有回填任务在执行中"

        partitions = extract_partitions(time_range)
        if not partitions:
            return None, "回填时间范围为空，无法划分分区"

        checkpoint = self._checkpoints.get(entry_id, 0)
        if checkpoint <= 0:
            checkpoint = self._numeric_field(entry.get("处理记录数"))
        if checkpoint <= 0:
            # 中断后重新启动时，已完成的分区来自台账，不能把进度清零。
            checkpoint = sum(
                int(item.get("backfilled_count", 0) or 0)
                for item in partition_registry.records_for_task(entry_id)
            )

        self._plans[entry_id] = {
            "task_id": entry_id,
            "table_name": table_name,
            "partitions": partitions,
            "created_at": datetime.datetime.now().isoformat(),
            "last_resume": None,
        }

        entry["status"] = "回填中"
        entry["pending"] = True
        entry["abnormal"] = False
        entry["处理记录数"] = str(checkpoint)
        entry["执行状态"] = "回填中"

        store.log_event("backfill_started", "backfill_job", "backfill_job", {
            "task_id": entry_id,
            "table_name": table_name,
            "time_range": time_range,
            "partitions": partitions,
            "checkpoint": checkpoint,
        })

        return entry, (
            f"回填任务 {entry.get('任务名称')} 已启动，时间范围={time_range}，"
            f"共 {len(partitions)} 个分区，检查点从 {checkpoint} 开始"
        )

    # ---- 检查点 ----

    def checkpoint_backfill(
        self, entry_id: int, processed_count: int,
    ) -> tuple[dict[str, Any] | None, str]:
        """更新回填检查点，支持断点续做。

        检查点按分区顺序落到分区台账，失败后续做时能准确跳过已补完的分区。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "回填任务不存在"
        if entry.get("status") != "回填中":
            return None, f"回填任务状态为{entry.get('status')}，不可保存检查点"

        processed_count = max(0, int(processed_count))
        plan = self._plan_for(entry)
        table_name = entry.get("目标湖表", "")

        for partition, assigned in self._allocate(plan["partitions"], processed_count).items():
            partition_registry.set_count(
                table_name, partition, assigned,
                task_id=entry_id, done=assigned >= PARTITION_RECORDS,
            )

        self._checkpoints[entry_id] = processed_count
        entry["处理记录数"] = str(processed_count)

        store.log_event("backfill_checkpoint", "backfill_job", "backfill_job", {
            "task_id": entry_id,
            "checkpoint": processed_count,
            "confirmed_partitions": self._done_partitions(entry_id),
        })

        return entry, f"回填任务检查点已更新：已处理 {processed_count} 条记录"

    def get_checkpoint(self, entry_id: int) -> int | None:
        """获取回填检查点。"""
        return self._checkpoints.get(entry_id)

    # ---- 完成 / 失败 ----

    def complete_backfill(
        self, entry_id: int, total_records: int,
    ) -> tuple[dict[str, Any] | None, str]:
        """完成回填任务，通知治理报告。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "回填任务不存在"
        if entry.get("status") != "回填中":
            return None, f"回填任务状态为{entry.get('status')}，不可完成"

        table_name = entry.get("目标湖表", "")

        entry["status"] = "已完成"
        entry["pending"] = False
        entry["abnormal"] = False
        entry["处理记录数"] = str(total_records)
        entry["执行状态"] = "已完成"
        entry["回填进度"] = (
            f"{len(self._done_partitions(entry_id))} 个分区已补完，共 {total_records} 条"
        )

        # 清理检查点与恢复窗口
        self._checkpoints.pop(entry_id, None)
        self._plans.pop(entry_id, None)
        partition_registry.unpin_task(table_name, entry_id)

        store.log_event("backfill_completed_to_govern", "backfill_job", "govern_report", {
            "task_id": entry_id,
            "table_name": table_name,
            "total_records": total_records,
        })

        return entry, f"回填任务 {entry.get('任务名称')} 已完成，总计 {total_records} 条记录"

    def fail_backfill(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """回填失败：保留检查点和已完成分区，供续做使用。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "回填任务不存在"
        if entry.get("status") != "回填中":
            return None, f"回填任务状态为{entry.get('status')}，不可标记失败"

        checkpoint = self._confirmed_checkpoint(entry)
        # 失败不清空检查点，也不覆盖已补完分区的条数。
        self._checkpoints[entry_id] = checkpoint

        done_partitions = self._done_partitions(entry_id)

        entry["status"] = "回填失败"
        entry["pending"] = False
        entry["abnormal"] = True
        entry["处理记录数"] = str(checkpoint)
        entry["执行状态"] = "回填失败"
        entry["回填进度"] = (
            f"{len(done_partitions)} 个分区已补完，检查点 {checkpoint} 条"
        )

        # 恢复窗口关闭：不再阻止保留策略处理这些分区。
        partition_registry.unpin_task(entry.get("目标湖表", ""), entry_id)

        store.log_event("backfill_failed", "backfill_job", "retention_mgr", {
            "task_id": entry_id,
            "failed_at_checkpoint": checkpoint,
            "confirmed_partitions": done_partitions,
        })

        return entry, (
            f"回填任务失败，检查点保留在 {checkpoint} 条，"
            f"已完成 {len(done_partitions)} 个分区，可从该位置续做"
        )

    # ---- 续做 ----

    def resume_backfill(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """从失败检查点续做：只读取剩余分区，不重做已完成分区。

        读取时通过分区台账锁定每个分区的可读位置；保留策略此时若在清理同一分区，
        归档会把读取位置切到归档区（仍可读），物理删除则会被拒绝并登记冲突。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "回填任务不存在"
        if entry.get("status") != "回填失败":
            return None, f"回填任务状态为{entry.get('status')}，只有回填失败的任务可以续做"

        table_name = entry.get("目标湖表", "")
        plan = self._plan_for(entry)
        partitions = plan["partitions"]
        if not partitions:
            return None, "回填时间范围无法解析出分区，无法续做"

        # 同湖表不允许两个回填同时续做，避免同一天的分区被两方读取。
        active_backfills = [
            bj for bj in store.rows(self.MODULE)
            if bj.get("目标湖表") == table_name
            and bj.get("status") == "回填中"
            and bj.get("id") != entry_id
        ]
        if active_backfills:
            return None, f"湖表 {table_name} 已有回填任务在执行中，暂不可续做"

        confirmed_before = self._confirmed_checkpoint(entry)
        done_map = {
            item["partition"]: item for item in self._done_partitions(entry_id)
        }
        if not done_map and confirmed_before > 0:
            # 兼容只有全局检查点、没有分区明细的历史失败任务：按分区顺序还原。
            self._reconstruct_done_partitions(table_name, partitions, entry_id, confirmed_before)
            done_map = {
                item["partition"]: item for item in self._done_partitions(entry_id)
            }
        remaining = [p for p in partitions if p not in done_map]
        if not remaining:
            return None, (
                f"回填范围 {len(partitions)} 个分区均已完成（检查点 {confirmed_before} 条），"
                "可直接完成回填或核对后重新调度"
            )

        # 打开恢复窗口：窗口内保留策略的物理删除会为续做让路。
        partition_registry.pin_task(table_name, remaining, entry_id)

        resumed: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        for partition in remaining:
            outcome = partition_registry.read_partition(
                table_name, partition, entry_id, PARTITION_RECORDS,
            )
            if outcome.get("ok"):
                resumed.append({
                    "partition": partition,
                    "分区": partition,
                    "source": outcome.get("source"),
                    "来源": outcome.get("source"),
                    "location": outcome.get("location"),
                    "读取位置": outcome.get("location"),
                    "count": outcome.get("count", PARTITION_RECORDS),
                    "已补条数": outcome.get("count", PARTITION_RECORDS),
                })
            else:
                reason = outcome.get("reason", "missing")
                skipped.append({
                    "partition": partition,
                    "分区": partition,
                    "source": "missing" if reason == "missing" else reason,
                    "来源": "missing" if reason == "missing" else reason,
                    "location": None,
                    "读取位置": None,
                    "reason": _REASON_TEXT.get(reason, reason),
                    "原因": _REASON_TEXT.get(reason, reason),
                })

        confirmed_after = confirmed_before + sum(item["count"] for item in resumed)
        plan["last_resume"] = {
            "resumed": resumed,
            "skipped": skipped,
            "resumed_at": datetime.datetime.now().isoformat(),
        }

        if not resumed:
            reason_text = "；".join(
                f"{item['partition']}：{item['reason']}" for item in skipped
            ) or "剩余分区均不可读"
            store.log_event("backfill_resume_blocked", "backfill_job", "retention_mgr", {
                "task_id": entry_id,
                "checkpoint": confirmed_before,
                "skipped": skipped,
            })
            return None, (
                f"暂无可续做的可读分区：{reason_text}。"
                f"已确认检查点 {confirmed_before} 条已保留，补齐数据源后可再次续做"
            )

        self._checkpoints[entry_id] = confirmed_after
        entry["status"] = "回填中"
        entry["pending"] = True
        entry["abnormal"] = False
        entry["处理记录数"] = str(confirmed_after)
        entry["执行状态"] = "回填中"

        store.log_event("backfill_resumed", "backfill_job", "backfill_job", {
            "task_id": entry_id,
            "table_name": table_name,
            "resumed_partitions": [item["partition"] for item in resumed],
            "skipped_partitions": skipped,
            "checkpoint": confirmed_after,
        })

        note = ""
        if skipped:
            note = "；未能续做：" + "，".join(
                f"{item['partition']}（{item['reason']}）" for item in skipped
            )
        return entry, (
            f"回填任务 {entry.get('任务名称')} 已从检查点 {confirmed_before} 条续做，"
            f"补完 {len(resumed)} 个分区，当前检查点 {confirmed_after} 条" + note
        )

    def resume_plan(self, entry_id: int) -> dict[str, Any] | None:
        """获取可续做分区清单（只读，不改变分区台账）。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None

        table_name = entry.get("目标湖表", "")
        partitions = extract_partitions(entry.get("回填时间范围", ""))
        done_map = {item["partition"]: item for item in self._done_partitions(entry_id)}
        checkpoint = self._confirmed_checkpoint(entry)
        if not done_map and checkpoint > 0:
            # 只有全局检查点、没有分区明细时，按分区顺序推演已完成分区（只读）。
            done_map = {
                partition: {"partition": partition, "count": assigned}
                for partition, assigned in self._allocate(partitions, checkpoint).items()
                if assigned >= PARTITION_RECORDS
            }

        completed: list[dict[str, Any]] = []
        remaining: list[dict[str, Any]] = []
        for partition in partitions:
            record = partition_registry.describe(table_name, partition)
            source = self._source_of(record)
            if partition in done_map:
                completed.append({
                    "partition": partition,
                    "分区": partition,
                    "count": done_map[partition]["count"],
                    "已补条数": done_map[partition]["count"],
                    "backfilled_count": done_map[partition]["count"],
                    "location": record.get("read_location"),
                    "读取位置": record.get("read_location"),
                    "read_location": record.get("read_location"),
                    "source": source,
                    "来源": source,
                    "done": True,
                })
                continue

            remaining.append({
                "partition": partition,
                "分区": partition,
                "count": int(record.get("backfilled_count", 0) or 0),
                "已补条数": int(record.get("backfilled_count", 0) or 0),
                "backfilled_count": int(record.get("backfilled_count", 0) or 0),
                "location": record.get("read_location"),
                "读取位置": record.get("read_location"),
                "read_location": record.get("read_location"),
                "source": source,
                "来源": source,
                "readable": source != "missing",
                "done": False,
            })

        readable_remaining = [item for item in remaining if item["readable"]]
        can_resume = entry.get("status") == "回填失败" and len(readable_remaining) > 0
        if entry.get("status") != "回填失败":
            message = f"回填任务状态为{entry.get('status')}，当前不需要续做"
        elif not remaining:
            message = "回填范围内的分区均已完成，可直接完成回填"
        elif not readable_remaining:
            message = "剩余分区均缺少可读数据源，请先恢复数据或调整回填范围"
        else:
            message = (
                f"可从检查点 {checkpoint} 条续做，剩余 {len(readable_remaining)} 个可读分区"
            )

        return {
            "task_id": entry_id,
            "table_name": table_name,
            "湖表": table_name,
            "status": entry.get("status"),
            "checkpoint": checkpoint,
            "检查点": checkpoint,
            "can_resume": can_resume,
            "可续做": can_resume,
            "message": message,
            "说明": message,
            "completed_partitions": completed,
            "已完成分区": completed,
            "remaining_partitions": remaining,
            "剩余分区": remaining,
            "last_resume": (self._plans.get(entry_id) or {}).get("last_resume"),
        }

    # ---- 动作分派 ----

    def view_entry(self, entry_id: int) -> dict[str, Any] | None:
        """详情视图：在任务行上附带可续做分区清单。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None
        plan = self.resume_plan(entry_id) or {}
        detail = dict(entry)
        detail["可续做"] = plan.get("can_resume", False)
        detail["续做说明"] = plan.get("message", "")
        detail["剩余分区"] = plan.get("remaining_partitions", [])
        detail["已完成分区"] = plan.get("completed_partitions", [])
        detail["检查点"] = plan.get("checkpoint", 0)
        return detail

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action == "启动回填":
            return self.start_backfill(entry_id)
        if action == "续做回填":
            return self.resume_backfill(entry_id)
        if action == "取消回填":
            # 取消属于中断：同样保留检查点，之后可以走续做入口恢复。
            return self.fail_backfill(entry_id)
        return super().run_action(entry_id, action)

    # ---- 内部 ----

    def _plan_for(self, entry: dict[str, Any]) -> dict[str, Any]:
        entry_id = int(entry.get("id", 0))
        plan = self._plans.get(entry_id)
        if plan is None:
            plan = {
                "task_id": entry_id,
                "table_name": entry.get("目标湖表", ""),
                "partitions": extract_partitions(entry.get("回填时间范围", "")),
                "created_at": datetime.datetime.now().isoformat(),
                "last_resume": None,
            }
            self._plans[entry_id] = plan
        return plan

    def _confirmed_checkpoint(self, entry: dict[str, Any]) -> int:
        """已确认检查点：优先用失败时保留的值，其次回落到分区台账。"""
        entry_id = int(entry.get("id", 0))
        if entry_id in self._checkpoints:
            return max(0, int(self._checkpoints[entry_id]))
        recorded = sum(
            int(item.get("backfilled_count", 0) or 0)
            for item in partition_registry.records_for_task(entry_id)
        )
        if recorded:
            return recorded
        return max(0, self._numeric_field(entry.get("处理记录数")))

    def _done_partitions(self, entry_id: int) -> list[dict[str, Any]]:
        return [
            {
                "partition": record["partition"],
                "count": int(record.get("backfilled_count", 0) or 0),
            }
            for record in partition_registry.records_for_task(entry_id)
            if record.get("done") and int(record.get("backfilled_count", 0) or 0) > 0
        ]

    def _reconstruct_done_partitions(
        self,
        table_name: str,
        partitions: list[str],
        entry_id: int,
        checkpoint: int,
    ) -> None:
        for partition, assigned in self._allocate(partitions, checkpoint).items():
            partition_registry.set_count(
                table_name, partition, assigned,
                task_id=entry_id, done=assigned >= PARTITION_RECORDS,
            )

    @staticmethod
    def _allocate(partitions: list[str], checkpoint: int) -> dict[str, int]:
        """按分区顺序把已确认条数摊回到各分区。"""
        allocation: dict[str, int] = {}
        remaining = max(0, int(checkpoint))
        for partition in partitions:
            if remaining <= 0:
                break
            assigned = min(PARTITION_RECORDS, remaining)
            allocation[partition] = assigned
            remaining -= assigned
        return allocation

    @staticmethod
    def _numeric_field(value: Any) -> int:
        text = str(value or "").strip()
        return int(text) if text.isdigit() else 0

    def _source_of(self, record: dict[str, Any]) -> str:
        """把分区台账记录映射成对外的数据来源标识。"""
        if not record:
            return "hot"
        if record.get("status") == "missing" or not record.get("readable", True):
            return "missing"
        if record.get("location_kind") == "archive":
            return "retention_archive"
        return "hot"
