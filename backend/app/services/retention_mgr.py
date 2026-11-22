"""保留策略领域服务：管理过期数据的自动清理、归档和保留策略。

上游接收小文件合并完成事件，下游联动存储成本分析和治理报告。
保留策略是数据生命周期管理的最后一道防线。

到期处理不再只记一条“清理了 3 个分区”的流水：它先关联同湖表失败回填的
可续做分区清单，再把每个分区的实际可读位置登记成保留回执。归档只切换读取
位置（数据仍可读），删除才会让分区不可读；如果分区正处在回填恢复窗口或
正在被读取，删除会延后并登记冲突，不会和数据读取同时改写同一个分区。
"""
from __future__ import annotations

import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.services.partition_registry import partition_registry
from app.store import store


def _parse_days(value: Any) -> int | None:
    text = str(value or "").strip()
    digits = "".join(ch for ch in text if ch.isdigit())
    return int(digits) if digits else None


def _parse_date(value: Any) -> datetime.date | None:
    text = str(value or "").strip()
    try:
        return datetime.date.fromisoformat(text[:10])
    except ValueError:
        return None


class RetentionMgrService(BaseModuleService):
    MODULE = "retention_mgr"
    ENTITY = "保留策略"
    OPERATION_TERM = "归档"
    LIST_FIELDS = ["策略名称", "适用湖表", "保留天数", "清理动作", "下次执行时间"]
    REQUIRED_FIELDS = ["策略名称", "适用湖表", "保留天数"]
    STATUS_ORDER = ["待生效", "已生效", "已暂停"]
    ACTION_RULES = {"启用保留": "已生效", "暂停保留": "已暂停"}
    NEGATIVE_ACTIONS = []

    def enable_retention(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """启用保留策略：验证湖表已注册并设置清理计划。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "保留策略不存在"
        if entry.get("status") != "待生效":
            return None, f"保留策略状态为{entry.get('status')}，不可启用"

        table_name = entry.get("适用湖表", "")
        retention_days = entry.get("保留天数", "")

        # 验证湖表已注册
        lake_tables = store.find_by_field("lake_table", "表名", table_name)
        if not lake_tables:
            return None, f"湖表 {table_name} 不存在，不可启用保留策略"

        entry["status"] = "已生效"
        entry["pending"] = False
        entry["abnormal"] = False

        store.log_event("retention_enabled", "retention_mgr", "lake_table", {
            "retention_id": entry_id,
            "table_name": table_name,
            "retention_days": retention_days,
        })

        return entry, f"保留策略 {entry.get('策略名称')} 已生效，保留天数={retention_days}"

    def pause_retention(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """暂停保留策略：记录当前检查点后暂停。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "保留策略不存在"
        if entry.get("status") != "已生效":
            return None, f"保留策略状态为{entry.get('status')}，不可暂停"

        # 创建检查点
        snapshot_key = store.create_snapshot(self.MODULE)

        entry["status"] = "已暂停"
        entry["pending"] = False
        entry["abnormal"] = False

        store.log_event("retention_paused", "retention_mgr", "retention_mgr", {
            "retention_id": entry_id,
            "snapshot_key": snapshot_key,
        })

        return entry, f"保留策略 {entry.get('策略名称')} 已暂停，检查点={snapshot_key}"

    # ---- 到期处理 ----

    def execute_cleanup(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """执行到期处理：清理或归档过期分区，并登记每个分区的实际读取位置。

        候选分区来自同湖表回填任务的分区清单里尚未补完的部分：保留策略和
        回填恢复对同一批分区的认知保持一致，已完成的分区和条数不会被改动。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "保留策略不存在"
        if entry.get("status") != "已生效":
            return None, f"保留策略状态为{entry.get('status')}，不可执行清理"

        table_name = entry.get("适用湖表", "")
        retention_days = _parse_days(entry.get("保留天数"))
        cleanup_action = self._normalize_action(entry.get("清理动作", "归档"))

        candidates = self._candidate_partitions(entry)
        expired = [
            partition for partition in candidates
            if self._is_expired(partition, retention_days)
        ]
        skipped_unexpired = [p for p in candidates if p not in expired]

        results: list[dict[str, Any]] = []
        for partition in expired:
            if cleanup_action == "delete":
                outcome = partition_registry.delete(table_name, partition, entry_id)
            else:
                outcome = partition_registry.archive(table_name, partition, entry_id)

            if outcome.get("ok"):
                results.append(outcome["receipt"])
                continue

            reason = outcome.get("reason", "unknown")
            if reason == "recovering":
                detail = (
                    f"分区处于回填恢复窗口（任务 {outcome.get('pinned_by')}），"
                    "物理删除延后，保留原读取位置"
                )
                partition_registry.record_conflict(
                    table_name, partition, entry_id, reason, detail,
                )
            elif reason == "reader_busy":
                detail = (
                    f"分区正在被任务 {outcome.get('reader')} 读取，"
                    "本次改动延后，保留原读取位置"
                )
                partition_registry.record_conflict(
                    table_name, partition, entry_id, reason, detail,
                )
            elif reason == "missing":
                detail = "分区已不可读，无需重复处理"
            else:
                detail = "分区已处于目标位置，无需重复处理"
            results.append({
                "table_name": table_name,
                "湖表": table_name,
                "partition": partition,
                "分区": partition,
                "action": "延后" if reason in ("recovering", "reader_busy") else "跳过",
                "清理动作": "延后" if reason in ("recovering", "reader_busy") else "跳过",
                "readable": reason not in ("missing",),
                "可读": reason not in ("missing",),
                "read_location": (outcome.get("record") or {}).get("read_location"),
                "读取位置": (outcome.get("record") or {}).get("read_location"),
                "reason": detail,
                "原因": detail,
            })

        cleaned = [r for r in results if r.get("action") in ("归档", "删除")]
        # 保留策略自身的执行结果（指标和详情都从这里读）。
        entry["已清理分区"] = str(len(cleaned))
        entry["下次清理范围"] = f"{len(expired)} 个到期分区"
        entry["最近清理时间"] = datetime.datetime.now().isoformat()
        entry["上次清理动作"] = cleanup_action
        # 审计与联动信息挂在模块级台账里，不改变行自身的业务状态。
        entry["关联回填任务"] = self._failed_backfill_ids(table_name)
        entry["关联分区"] = list(expired) + skipped_unexpired

        store.log_event("retention_cleanup_executed", "retention_mgr", "govern_report", {
            "retention_id": entry_id,
            "table_name": table_name,
            "retention_days": retention_days,
            "action": cleanup_action,
            "cleaned_partitions": len(cleaned),
            "expired_partitions": expired,
            "skipped_partitions": skipped_unexpired,
            "conflicts": [
                r for r in results if r.get("action") == "延后"
            ],
        })

        # 更新成本报告中的存储释放信息
        cost_reports = store.find_by_field("cost_report", "目标业务域", table_name)
        for cr in cost_reports:
            cr["pending"] = True
        if cost_reports:
            store.log_event("retention_to_cost", "retention_mgr", "cost_report", {
                "retention_id": entry_id,
                "affected_reports": len(cost_reports),
            })

        deferred = [r for r in results if r.get("action") == "延后"]
        message = (
            f"保留策略 {entry.get('策略名称')} 清理完成，操作={cleanup_action}，"
            f"到期分区={len(expired)}，实际处理={len(cleaned)}"
        )
        if deferred:
            message += f"，延后 {len(deferred)} 个分区（正被回填读取或处于恢复窗口）"
        return entry, message

    def retention_detail(self, entry_id: int) -> dict[str, Any] | None:
        """保留策略详情：剩余分区的实际读取位置、已补条数和保留回执。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None

        table_name = entry.get("适用湖表", "")
        resume_partitions = self._linked_partitions(table_name)
        if not resume_partitions:
            # 没有关联回填任务时，按上次清理范围展示分区位置。
            resume_partitions = [
                {
                    "partition": str(partition),
                    "count": 0,
                    "done": False,
                    "source": self._source_from_registry(table_name, str(partition)),
                }
                for partition in entry.get("关联分区", [])
                if str(partition or "").strip()
            ]
        partition_detail: list[dict[str, Any]] = []
        completed_detail: list[dict[str, Any]] = []
        all_detail: list[dict[str, Any]] = []
        for item in resume_partitions:
            partition = item["partition"]
            record = partition_registry.peek(table_name, partition) or {}
            location = record.get("read_location", item.get("location"))
            source = self._source_from_registry(table_name, partition)
            backfilled_count = int(
                record.get("backfilled_count", item.get("count", 0)) or 0
            )
            row = {
                "partition": partition,
                "分区": partition,
                "read_location": location,
                "读取位置": location,
                "source": source,
                "来源": source,
                "readable": source != "missing" and location is not None,
                "可读": source != "missing" and location is not None,
                "backfilled_count": backfilled_count,
                "已补条数": backfilled_count,
                "done": bool(record.get("done", item.get("done"))),
            }
            all_detail.append(row)
            if row["done"]:
                completed_detail.append(row)
            else:
                partition_detail.append(row)

        receipts = partition_registry.receipts_for(table_name=table_name)
        conflicts = partition_registry.conflicts_for(table_name=table_name)
        # 关联回填任务既保留到期处理时的登记，也带上当前仍失败的任务。
        associated = list(dict.fromkeys(
            [int(job_id) for job_id in entry.get("关联回填任务", [])]
            + self._failed_backfill_ids(table_name)
        ))
        detail_summary = {
            "total": len(all_detail),
            "remaining_count": len(partition_detail),
            "completed_partitions": len(completed_detail),
            "readable": sum(1 for p in all_detail if p["readable"]),
            "missing": sum(1 for p in all_detail if not p["readable"]),
            "backfilled_count": sum(p["backfilled_count"] for p in all_detail),
            "remaining_count_sum": sum(
                p["backfilled_count"] for p in partition_detail
            ),
            "completed_count": sum(p["backfilled_count"] for p in completed_detail),
            "receipt_count": len(receipts),
            "conflict_count": len(conflicts),
        }
        # 策略行保持扁平字段，便于列表页直接筛选和导出处方。
        entry["剩余分区"] = partition_detail
        entry["已完成分区"] = completed_detail
        entry["已登记读取位置"] = [
            {"分区": p["partition"], "读取位置": p["read_location"]}
            for p in all_detail
        ]
        entry["剩余分区已补条数"] = detail_summary["remaining_count_sum"]
        entry["已完成分区条数"] = detail_summary["completed_count"]
        entry["保留回执数"] = detail_summary["receipt_count"]
        entry["清理冲突数"] = detail_summary["conflict_count"]

        return {
            "id": entry_id,
            "retention_id": entry_id,
            "status": entry.get("status"),
            "策略名称": entry.get("策略名称"),
            "适用湖表": table_name,
            "table_name": table_name,
            "保留天数": entry.get("保留天数"),
            "清理动作": entry.get("清理动作"),
            "下次执行时间": entry.get("下次执行时间"),
            "已清理分区": entry.get("已清理分区", "0"),
            "下次清理范围": entry.get("下次清理范围", ""),
            "最近清理时间": entry.get("最近清理时间"),
            "上次清理动作": entry.get("上次清理动作"),
            "关联回填任务": associated,
            "associated_backfill_jobs": associated,
            "partitions": all_detail,
            "全部分区": all_detail,
            "剩余分区": partition_detail,
            "已完成分区": completed_detail,
            "已登记读取位置": entry["已登记读取位置"],
            "剩余分区已补条数": detail_summary["remaining_count_sum"],
            "已完成分区条数": detail_summary["completed_count"],
            "保留回执数": detail_summary["receipt_count"],
            "清理冲突数": detail_summary["conflict_count"],
            "receipts": receipts,
            "保留回执": receipts,
            "conflicts": conflicts,
            "冲突": conflicts,
            "summary": detail_summary,
        }

    def view_entry(self, entry_id: int) -> dict[str, Any] | None:
        """详情视图：在策略行上追加关联分区、保留回执和读取位置。"""
        return self.retention_detail(entry_id)

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action == "启用保留":
            return self.enable_retention(entry_id)
        if action == "暂停保留":
            return self.pause_retention(entry_id)
        if action == "执行清理":
            return self.execute_cleanup(entry_id)
        return super().run_action(entry_id, action)

    # ---- 内部 ----

    @staticmethod
    def _normalize_action(value: Any) -> str:
        text = str(value or "").strip()
        if any(token in text for token in ("删除", "清理", "物理")):
            return "delete"
        return "archive"

    @staticmethod
    def _source_from_registry(table_name: str, partition: str) -> str:
        record = partition_registry.peek(table_name, partition)
        if record is None:
            return "hot"
        if record.get("status") == "missing" or not record.get("readable", True):
            return "missing"
        if record.get("location_kind") == "archive":
            return "retention_archive"
        return "hot"

    def _failed_backfills(self, table_name: str) -> list[dict[str, Any]]:
        return [
            job for job in store.rows("backfill_job")
            if job.get("目标湖表") == table_name and job.get("status") == "回填失败"
        ]

    def _linked_backfills(
        self, table_name: str, entry: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """与保留策略关联的回填任务：失败、执行中，以及到期处理时登记过的任务。"""
        recorded = {int(job_id) for job_id in (entry or {}).get("关联回填任务", [])}
        return [
            job for job in store.rows("backfill_job")
            if job.get("目标湖表") == table_name
            and (
                job.get("status") in ("回填失败", "回填中")
                or int(job.get("id", 0)) in recorded
            )
        ]

    def _failed_backfill_ids(self, table_name: str) -> list[int]:
        return [int(job.get("id", 0)) for job in self._failed_backfills(table_name)]

    def _linked_partitions(self, table_name: str) -> list[dict[str, Any]]:
        """关联同湖表回填任务的分区清单（已完成和剩余都返回，带 done 标记）。"""
        from app.services.backfill_job import BackfillJobService

        backfill_service = BackfillJobService()
        items: list[dict[str, Any]] = []
        for job in self._linked_backfills(table_name):
            plan = backfill_service.resume_plan(int(job.get("id", 0)))
            if not plan:
                continue
            items.extend(plan.get("completed_partitions", []))
            items.extend(plan.get("remaining_partitions", []))
        return items

    def _candidate_partitions(self, entry: dict[str, Any]) -> list[str]:
        table_name = entry.get("适用湖表", "")
        # 到期处理只针对未完成的分区：已补完的分区和条数保持不动。
        partitions = [
            item["partition"]
            for item in self._linked_partitions(table_name)
            if not item.get("done")
        ]
        if partitions:
            return partitions

        # 没有失败回填时，退回到该湖表台账里已有的分区。
        recorded = [
            record["partition"]
            for record in partition_registry.records_for_table(table_name)
        ]
        if recorded:
            return sorted(set(recorded))

        # 从未登记过分区时，沿用上次清理范围，避免重复清理同一批分区。
        associated = [
            str(p) for p in entry.get("关联分区", []) if str(p or "").strip()
        ]
        if associated:
            return associated

        next_run = _parse_date(entry.get("下次执行时间"))
        return [next_run.isoformat()] if next_run else []

    @staticmethod
    def _is_expired(partition: str, retention_days: int | None) -> bool:
        if retention_days is None:
            # 保留天数配置不可读时不擅自清理，交给值班员核对。
            return False
        partition_date = _parse_date(partition)
        if partition_date is None:
            return False
        cutoff = datetime.date.today() - datetime.timedelta(days=retention_days)
        return partition_date <= cutoff
