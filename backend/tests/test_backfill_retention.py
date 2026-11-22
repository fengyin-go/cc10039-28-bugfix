"""回填中断恢复与保留策略到期处理的联动测试。

覆盖场景：回填失败后检查点保留、续做入口返回可续做分区清单、
保留策略到期处理登记分区实际读取位置、归档后仍能续读、
删除后说明缺少数据源并保留已确认检查点，以及重试与清理并发时
同一天分区的读取位置不被双方同时改写。
"""
from __future__ import annotations

import datetime
import os
import sys
import threading
import time

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.services.backfill_job import BackfillJobService
from app.services.partition_registry import partition_registry
from app.services.retention_mgr import RetentionMgrService
from app.store import store


def _add_task(task_id: int, table_name: str, time_range: str) -> dict:
    entry = {
        "id": task_id, "status": "待调度", "pending": True, "abnormal": False,
        "任务名称": f"联动回填-{task_id}", "目标湖表": table_name,
        "回填时间范围": time_range, "处理记录数": "0", "执行状态": "待调度",
    }
    store.rows("backfill_job").append(entry)
    return entry


def _add_retention(retention_id: int, table_name: str, action: str, days: str = "7") -> dict:
    entry = {
        "id": retention_id, "status": "已生效", "pending": False, "abnormal": False,
        "策略名称": f"联动保留-{retention_id}", "适用湖表": table_name,
        "保留天数": days, "清理动作": action, "下次执行时间": "2026-09-03",
    }
    store.rows("retention_mgr").append(entry)
    return entry


def _failed_after_first_day(
    task_id: int, table_name: str, days: int = 3, start: str = "2026-09-01",
) -> BackfillJobService:
    """准备一个处理完第一天后失败的任务。"""
    import datetime

    start_date = datetime.date.fromisoformat(start)
    end_date = start_date + datetime.timedelta(days=days - 1)
    _add_task(task_id, table_name, f"{start_date}~{end_date}")
    service = BackfillJobService()
    assert service.start_backfill(task_id)[0] is not None
    assert service.checkpoint_backfill(task_id, 10)[0] is not None
    assert service.fail_backfill(task_id)[0] is not None
    return service


class TestBackfillResume:
    def setup_method(self):
        partition_registry.reset()

    def test_resume_entry_returns_partition_list(self):
        """失败回执保留检查点后，续做入口应返回可续做分区清单而不是报错。"""
        service = _failed_after_first_day(7001, "resume_tbl")

        assert service.get_checkpoint(7001) == 10
        plan = service.resume_plan(7001)
        assert plan is not None
        assert plan["can_resume"] is True
        assert plan["checkpoint"] == 10
        assert [p["partition"] for p in plan["remaining_partitions"]] == [
            "2026-09-02", "2026-09-03",
        ]
        assert plan["remaining_partitions"][0]["source"] == "hot"

    def test_resume_continues_from_confirmed_checkpoint(self):
        """续做不重做已完成分区，从已确认检查点接下去。"""
        service = _failed_after_first_day(7002, "resume_tbl2")

        entry, message = service.resume_backfill(7002)
        assert entry is not None, message
        assert entry["status"] == "回填中"
        assert entry["处理记录数"] == "30"

        plan = service.resume_plan(7002)
        assert plan["remaining_partitions"] == []

        # 再次中断后续做仍从已确认范围接续
        assert service.fail_backfill(7002)[0] is not None
        assert service.get_checkpoint(7002) == 30
        resumed_plan = service.resume_plan(7002)
        assert resumed_plan["can_resume"] is False
        assert resumed_plan["checkpoint"] == 30

    def test_second_interruption_resumes_from_confirmed(self):
        """再次中断后续做仍从已确认范围接下去。"""
        service = _failed_after_first_day(7003, "resume_tbl3")
        assert service.resume_backfill(7003)[0] is not None
        assert service.fail_backfill(7003)[0] is not None
        assert service.get_checkpoint(7003) == 30
        plan = service.resume_plan(7003)
        assert plan["can_resume"] is False
        assert plan["checkpoint"] == 30

    def test_resume_on_non_failed_task_rejected(self):
        _add_task(7004, "resume_tbl4", "2026-09-01")
        service = BackfillJobService()
        result, message = service.resume_backfill(7004)
        assert result is None
        assert "只有回填失败" in message

    def test_resume_without_any_readable_source_keeps_checkpoint(self):
        """剩余分区全部缺少来源时说明原因，并保留已确认检查点。"""
        service = _failed_after_first_day(7005, "resume_tbl5")
        _add_retention(7005, "resume_tbl5", "删除")
        RetentionMgrService().execute_cleanup(7005)

        result, message = service.resume_backfill(7005)
        assert result is None
        assert "数据源不可读" in message
        assert service.get_checkpoint(7005) == 10
        assert store.find("backfill_job", 7005)["status"] == "回填失败"


class TestRetentionResumeLink:
    def setup_method(self):
        partition_registry.reset()

    def test_archive_then_resume_reads_archive_location(self):
        """保留策略归档到期分区后，回填从保留回执找回剩余数据源。"""
        service = _failed_after_first_day(7101, "retention_tbl")
        _add_retention(7101, "retention_tbl", "归档")

        retention = RetentionMgrService()
        entry, message = retention.execute_cleanup(7101)
        assert entry is not None, message
        assert entry["已清理分区"] == "2"

        plan = service.resume_plan(7101)
        archived = plan["remaining_partitions"]
        assert archived[0]["source"] == "retention_archive"
        assert archived[0]["read_location"].startswith("archive://cold/retention_tbl/")

        resume_entry, resume_message = service.resume_backfill(7101)
        assert resume_entry is not None, resume_message
        assert resume_entry["处理记录数"] == "30"

        detail = retention.retention_detail(7101)
        assert detail["summary"]["backfilled_count"] == 30
        assert detail["已完成分区条数"] == 30
        assert detail["剩余分区"] == []
        archived_reads = [
            p for p in detail["已完成分区"] if p["来源"] == "retention_archive"
        ]
        assert len(archived_reads) == 2
        assert all(p["读取位置"].startswith("archive://") for p in archived_reads)

    def test_delete_then_resume_explains_missing_source(self):
        """物理清理后回填无法续读，说明原因并保留已确认检查点。"""
        service = _failed_after_first_day(7102, "retention_tbl2")
        _add_retention(7102, "retention_tbl2", "删除")

        retention = RetentionMgrService()
        retention.execute_cleanup(7102)

        plan = service.resume_plan(7102)
        assert plan["can_resume"] is False
        assert all(p["source"] == "missing" for p in plan["remaining_partitions"])

        result, message = service.resume_backfill(7102)
        assert result is None
        assert "数据源不可读" in message
        assert service.get_checkpoint(7102) == 10

        detail = retention.retention_detail(7102)
        assert detail["summary"]["missing"] == 2
        assert all(p["读取位置"] is None for p in detail["剩余分区"])
        assert detail["保留回执"][0]["action"] == "删除"
        assert detail["保留回执"][0]["可读"] is False

    def test_unexpired_partition_not_cleaned(self):
        """未到保留期限的剩余分区不会被清理。"""
        today = datetime.date.today()
        service = _failed_after_first_day(
            7103, "retention_tbl3",
            start=(today - datetime.timedelta(days=2)).isoformat(),
        )
        _add_retention(7103, "retention_tbl3", "删除", days="7")

        retention = RetentionMgrService()
        retention.execute_cleanup(7103)
        plan = service.resume_plan(7103)
        assert all(p["source"] == "hot" for p in plan["remaining_partitions"])
        # 两天都未到 7 天保留期限，不产生任何清理
        assert retention.retention_detail(7103)["已清理分区"] == "0"

    def test_retention_detail_reports_locations_and_counts(self):
        """保留策略详情可核对剩余分区实际读取位置和已补条数。"""
        service = _failed_after_first_day(7104, "retention_tbl4")
        _add_retention(7104, "retention_tbl4", "归档")
        retention = RetentionMgrService()
        retention.execute_cleanup(7104)

        detail = retention.retention_detail(7104)
        assert detail["关联回填任务"] == [7104]
        assert len(detail["剩余分区"]) == 2
        assert detail["剩余分区"][0]["已补条数"] == 0
        assert detail["剩余分区"][0]["读取位置"].startswith("archive://")
        assert detail["已登记读取位置"][0]["分区"] == "2026-09-01"

        # 续做完成后，剩余分区清空，全部条数在已完成分区里逐项可核对
        service.resume_backfill(7104)
        detail = retention.retention_detail(7104)
        assert detail["剩余分区"] == []
        assert detail["剩余分区已补条数"] == 0
        assert detail["已完成分区条数"] == 30
        assert [p["已补条数"] for p in detail["已完成分区"]] == [10, 10, 10]
        assert detail["已完成分区"][2]["读取位置"].startswith("archive://")
        assert "剩余分区已补条数" in store.find("retention_mgr", 7104)


    def test_partial_missing_partition_resumes_the_rest(self):
        """只有部分剩余分区缺少来源时，其余分区照常续做并说明缺失原因。"""
        today = datetime.date.today()
        # 第一天（-8 天）已完成且未到期；第二天（-7 天）到期被删除；
        # 第三天（-6 天）未到期仍可读。
        service = _failed_after_first_day(
            7105, "retention_tbl5",
            start=(today - datetime.timedelta(days=8)).isoformat(),
        )
        _add_retention(7105, "retention_tbl5", "删除", days="7")
        retention = RetentionMgrService()
        retention.execute_cleanup(7105)

        entry, message = service.resume_backfill(7105)
        assert entry is not None, message
        assert "数据源不可读" in message
        assert entry["处理记录数"] == "20"

        detail = retention.retention_detail(7105)
        # 第一天（已完成）和第三天（新续做）都算已完成，第二天缺失保留在剩余分区
        assert detail["已完成分区条数"] == 20
        assert detail["剩余分区已补条数"] == 0
        assert len(detail["剩余分区"]) == 1
        assert detail["剩余分区"][0]["可读"] is False


class TestResumeCleanupRace:
    def setup_method(self):
        partition_registry.reset()

    def test_retry_and_cleanup_do_not_double_write(self):
        """重试回填和到期清理并发时，同一天分区的读取位置不被双方同时改写。"""
        task_id = 7201
        service = _failed_after_first_day(task_id, "race_tbl")
        _add_retention(task_id, "race_tbl", "删除")
        retention = RetentionMgrService()

        original_read = partition_registry.read_partition
        read_started = threading.Event()

        def slow_read(table_name, partition, job_id, count):
            read_started.set()
            time.sleep(0.02)
            return original_read(table_name, partition, job_id, count)

        partition_registry.read_partition = slow_read
        results: dict[str, str] = {}

        def resume() -> None:
            results["resume"] = service.resume_backfill(task_id)[1]

        def cleanup() -> None:
            read_started.wait(timeout=1)
            results["cleanup"] = retention.execute_cleanup(task_id)[1]

        threads = [threading.Thread(target=resume), threading.Thread(target=cleanup)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        partition_registry.read_partition = original_read

        assert "续做" in results["resume"]
        assert "延后" in results["cleanup"]

        detail = retention.retention_detail(task_id)
        assert detail["summary"]["backfilled_count"] == 30
        assert detail["summary"]["missing"] == 0
        assert detail["summary"]["conflict_count"] == 2
        assert all(p["读取位置"] and p["可读"] for p in detail["全部分区"])


class TestRetentionActionDispatch:
    def setup_method(self):
        partition_registry.reset()

    def test_execute_cleanup_action_exposed(self):
        """清理动作可从动作入口触发。"""
        service = _failed_after_first_day(7301, "dispatch_tbl")
        _add_retention(7301, "dispatch_tbl", "归档")
        retention = RetentionMgrService()
        entry, message = retention.run_action(7301, "执行清理")
        assert entry is not None, message
        assert entry["已清理分区"] == "2"
        assert service.resume_plan(7301)["can_resume"] is True
