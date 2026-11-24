"""测试：失败日志逐行回修 pipeline.

覆盖场景：
1. CSV 解析纠错单导入：按来源 + 源文件行号定位失败行，产出有效回修事件批次；
2. 事件分类只读取有效回修批次：保留原文与旧失败原因，已成功的行不再发出；
3. apply_parse_rules → classify_events 串联，并发下同一回修只生效一次；
4. 原行证据校验：物理位置相同但原文更换时单独拒绝，整批其他有效单仍出结果；
5. 错来源 / 缺必填字段 / 原本成功行 / 行号不存在等拒绝原因；
6. 并发重复导入同一版本只写入一次；
7. 同一原行两版有效纠错：领取时锁定 vs 分类前重新领取，17 分别是错误/认证事件；
8. 分类记录可追溯选用的纠错单与原失败原因，不夹用两份修订字段；
9. 重启恢复：结清回执未到的已完成回修不回流，分类记录不丢；
10. 后续修订与首次正式结果分开核销，各自保留原行依据。
"""
from __future__ import annotations

import sys
import os
import threading

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.core.models import (
    IngestJob, LogEntry, LogSourceConfig, ParseResultStatus, ParseRule,
)
from app.core.parse_correction import (
    ClassificationJournalEntry, CorrectionStatus, ImportBatchResult,
    ParseCorrectionPipeline, RejectReason, SourceLogLine,
    evidence_checksum, make_parse_correction_pipeline,
)
from app.core.pipeline import (
    IngestPipeline, PRESET_LOG_SOURCES, PRESET_PARSE_RULES,
    make_pipeline,
)

APP_SOURCE_ID = "LOG_-0002"
# 与 PARS-0002 结构相同，但 level 分组是可选的：缺 level 的行会以
# FIELD_MISSING 进入失败行台账，而不是 FORMAT_MISMATCH。
APP_RULE = ParseRule(
    rule_id="PARS-0002",
    source_id=APP_SOURCE_ID,
    regex_pattern=(
        r'^(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}\.\d{3})\s+\[([^\]]+)\]\s+'
        r'(?:(INFO|WARN|ERROR|DEBUG)\s+)?(\S+)\s+-\s+(.+)$'
    ),
    field_names=["timestamp", "thread", "level", "logger", "message"],
    time_format="%Y-%m-%d %H:%M:%S.%f",
    priority=10,
    state="生效中",
)
APP_FILE = "/var/log/app/backend.log"
# 来源以自定义格式接入（边采集边类）：格式校验通过后，缺 level 的行会被
# 解析规则判为 FIELD_MISSING，正好是"缺字段失败日志"。
APP_SOURCE_OBJ = LogSourceConfig(
    source_id=APP_SOURCE_ID,
    source_name="BackendAppLog",
    app_label="backend-api",
    log_path=APP_FILE,
    log_format="custom",
    encoding="utf-8",
    collect_method="file_tail",
    access_state="已接入",
)
APP_SOURCE = APP_SOURCE_OBJ

# 第 17 行：app 日志缺 level 字段 → 字段缺失失败行。
FAILED_LINE_17 = (
    "2026-11-15 14:30:17.000 [http-nio-8080-exec-17] com.example.AuthController"
    " - 登录尝试 user=admin"
)
# 原本成功的行，用来验证"已成功的行不再发出"。
SUCCESS_LINE_18 = (
    "2026-11-15 14:30:18.000 [http-nio-8080-exec-18] INFO  com.example.AuthController"
    " - 健康检查通过"
)
# 同位置但原文不同的另一条日志（模拟"别次故障"）。
OTHER_INCIDENT_LINE_17 = (
    "2026-11-15 15:02:17.000 [http-nio-8080-exec-31] com.example.AuthController"
    " - 会话过期 user=ops"
)

ERROR_REVISION = (
    "2026-11-15 14:30:17.000 [http-nio-8080-exec-17] ERROR com.example.AuthController"
    " - 登录失败 user=admin"
)
LOGIN_REVISION = (
    "2026-11-15 14:30:17.000 [http-nio-8080-exec-17] INFO  com.example.AuthController"
    " - LOGIN user=admin"
)

CSV_HEADER = "单号,来源,文件路径,行号,原行内容,修订内容,版本,批准人"


def csv_row(
    ticket_id: str,
    source: str = "LOG_-0002",
    file_path: str = APP_FILE,
    line_number: int | str = 17,
    original: str = FAILED_LINE_17,
    corrected: str = ERROR_REVISION,
    version: str = "1",
    approver: str = "值班员",
) -> str:
    cells = [ticket_id, source, file_path, str(line_number), original, corrected, version, approver]
    return ",".join(_csv_cell(cell) for cell in cells)


def _csv_cell(value: str) -> str:
    if any(char in value for char in ',"\n'):
        return '"' + value.replace('"', '""') + '"'
    return value


def csv_document(*rows: str) -> str:
    return "\n".join([CSV_HEADER, *rows]) + "\n"


def make_ledger() -> ParseCorrectionPipeline:
    """准备一个来源文件：第 17 行字段缺失，第 18 行原本成功。"""

    pipeline = make_pipeline()
    correction = make_parse_correction_pipeline(ingest=pipeline, parse_rules=[APP_RULE])
    entries = pipeline.parse_raw_lines(APP_SOURCE_OBJ, [FAILED_LINE_17, SUCCESS_LINE_18])
    parsed = pipeline.apply_parse_rules(entries, APP_RULE)
    # 行号修正：来源文件第 17/18 行，而不是数组下标 1/2。
    for offset, entry in enumerate(parsed, start=17):
        entry.line_number = offset
    correction.register_log_lines("LOG_-0002", APP_FILE, parsed, rule=APP_RULE)
    return correction


def ledger_line(correction: ParseCorrectionPipeline, line_number: int = 17) -> dict:
    for item in correction.lines():
        if item["line_number"] == line_number:
            return item
    raise AssertionError(f"line {line_number} not registered")


# ---------------------------------------------------------------------------
# 1. 导入与拒绝原因
# ---------------------------------------------------------------------------


class TestCorrectionImport:
    def test_failed_line_17_is_field_missing(self):
        correction = make_ledger()
        line = ledger_line(correction)
        assert line["parse_status"] == ParseResultStatus.FIELD_MISSING.value
        assert "level" in line["failure_reason"]

    def test_successful_line_is_not_a_failure(self):
        correction = make_ledger()
        line = ledger_line(correction, 18)
        assert line["parse_status"] == ParseResultStatus.OK.value
        assert line["failure_reason"] == ""

    def test_import_valid_ticket_writes_once(self):
        correction = make_ledger()
        text = csv_document(csv_row("CRT-0001"))
        result = correction.import_csv(text, imported_by="值班员")
        assert isinstance(result, ImportBatchResult)
        assert result.written == 1
        assert result.rejected == 0
        assert result.rows[0].ok is True
        ticket = result.rows[0].ticket
        assert ticket is not None
        assert ticket.status == CorrectionStatus.APPROVED
        assert ticket.line_number == 17
        assert ticket.original_line == FAILED_LINE_17
        assert ticket.evidence_checksum == ledger_line(correction)["checksum"]

    def test_retransmit_identical_version_is_idempotent(self):
        correction = make_ledger()
        text = csv_document(csv_row("CRT-0001"))
        first = correction.import_csv(text)
        second = correction.import_csv(text)
        assert first.written == 1
        assert second.written == 0
        assert second.duplicates == 1
        assert second.rows[0].duplicate is True
        assert second.rows[0].reason_code == RejectReason.DUPLICATE_VERSION.value
        assert len(correction.tickets()) == 1

    def test_concurrent_duplicate_import_writes_once(self):
        correction = make_ledger()
        text = csv_document(csv_row("CRT-0001"))
        written: list[int] = []
        barrier = threading.Barrier(8)

        def worker() -> None:
            barrier.wait()
            written.append(correction.import_csv(text).written)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert sum(written) == 1
        assert len(correction.tickets()) == 1

    def test_wrong_source_rejected(self):
        correction = make_ledger()
        text = csv_document(csv_row("CRT-0001", source="LOG_-9999"))
        result = correction.import_csv(text)
        assert result.written == 0
        assert result.rejected == 1
        assert result.rows[0].reason_code == RejectReason.UNKNOWN_SOURCE.value
        assert "来源" in result.rows[0].reason

    def test_missing_required_field_rejected(self):
        correction = make_ledger()
        # 缺"原行内容"：直接把该列留空
        text = csv_document(
            f'CRT-0001,LOG_-0002,{APP_FILE},17,,{ERROR_REVISION},1,值班员')
        result = correction.import_csv(text)
        assert result.written == 0
        assert result.rejected == 1
        assert result.rows[0].reason_code == RejectReason.MISSING_FIELD.value
        assert "原行内容" in result.rows[0].reason

    def test_line_not_found_rejected(self):
        correction = make_ledger()
        text = csv_document(csv_row("CRT-0001", line_number=999))
        result = correction.import_csv(text)
        assert result.rejected == 1
        assert result.rows[0].reason_code == RejectReason.LINE_NOT_FOUND.value

    def test_already_successful_line_rejected(self):
        correction = make_ledger()
        text = csv_document(
            csv_row("CRT-0001", line_number=18, original=SUCCESS_LINE_18,
                    corrected=LOGIN_REVISION))
        result = correction.import_csv(text)
        assert result.rejected == 1
        assert result.rows[0].reason_code == RejectReason.LINE_ALREADY_SUCCESSFUL.value

    def test_ambiguous_source_without_file_is_rejected(self):
        correction = make_ledger()
        other_entries = correction.ingest.parse_raw_lines(
            APP_SOURCE_OBJ, [FAILED_LINE_17])
        other_entries = correction.ingest.apply_parse_rules(other_entries, APP_RULE)
        other_entries[0].line_number = 17
        correction.register_log_lines(
            "LOG_-0002", "/var/log/app/backend-rotated.log",
            other_entries, rule=APP_RULE)
        text = csv_document(
            csv_row("CRT-0001", file_path="", original=FAILED_LINE_17))
        result = correction.import_csv(text)
        assert result.rejected == 1
        assert result.rows[0].reason_code == RejectReason.AMBIGUOUS_SOURCE.value


# ---------------------------------------------------------------------------
# 2. 证据校验与整批隔离
# ---------------------------------------------------------------------------


class TestEvidence:
    def test_changed_original_line_rejected(self):
        correction = make_ledger()
        # 同一物理位置，但纠错单携带的是另一次故障的原文
        text = csv_document(
            csv_row("CRT-0001", original=OTHER_INCIDENT_LINE_17))
        result = correction.import_csv(text)
        assert result.written == 0
        assert result.rejected == 1
        assert result.rows[0].reason_code == RejectReason.EVIDENCE_MISMATCH.value
        assert correction.pending_failure_lines()[0]["raw_line"] == FAILED_LINE_17

    def test_other_valid_tickets_in_same_batch_still_succeed(self):
        correction = make_ledger()
        text = csv_document(
            csv_row("CRT-FAKE", original=OTHER_INCIDENT_LINE_17),
            csv_row("CRT-WRONG-SOURCE", source="LOG_-4040"),
            csv_row("CRT-OK", original=FAILED_LINE_17),
        )
        result = correction.import_csv(text)
        assert result.written == 1
        assert result.rejected == 2
        ok_row = next(row for row in result.rows if row.ticket_id == "CRT-OK")
        assert ok_row.ok is True
        assert ok_row.ticket is not None
        assert ok_row.ticket.line_number == 17
        reasons = {
            row.ticket_id: row.reason_code
            for row in result.rows if not row.written
        }
        assert reasons["CRT-FAKE"] == RejectReason.EVIDENCE_MISMATCH.value
        assert reasons["CRT-WRONG-SOURCE"] == RejectReason.UNKNOWN_SOURCE.value

    def test_checksum_changes_with_evidence(self):
        base = evidence_checksum("LOG_-0002", APP_FILE, 17, FAILED_LINE_17)
        assert base == evidence_checksum("LOG_-0002", APP_FILE, 17, FAILED_LINE_17)
        assert base != evidence_checksum("LOG_-0002", APP_FILE, 17, OTHER_INCIDENT_LINE_17)
        assert base != evidence_checksum("LOG_-0002", APP_FILE, 18, FAILED_LINE_17)


# ---------------------------------------------------------------------------
# 3. 有效回修批次 → 分类串联
# ---------------------------------------------------------------------------


class TestReworkBatchClassification:
    def test_batch_contains_only_failed_line(self):
        correction = make_ledger()
        correction.import_csv(csv_document(csv_row("CRT-0001")))
        batch = correction.build_rework_batch()
        assert len(batch) == 1
        entry = batch.entries[0]
        assert entry.line_number == 17
        assert entry.parse_status == ParseResultStatus.OK
        assert entry.parsed_fields["level"] == "ERROR"
        assert entry.parsed_fields["message"] == "登录失败 user=admin"

    def test_classify_consumes_apply_parse_rules_fields(self):
        correction = make_ledger()
        correction.import_csv(csv_document(csv_row("CRT-0001")))
        correction.claim("CRT-0001")
        outcome = correction.classify_claimed()
        assert outcome.classified == 1
        record = outcome.records[0]
        # 分类写出的字段来自 apply_parse_rules 对修订内容的解析结果
        assert record.parsed_fields["level"] == "ERROR"
        assert record.parsed_fields["logger"] == "com.example.AuthController"
        assert record.event_type.type_id == "EVEN-0002"
        assert record.event_type.category_tag == "error"

    def test_record_keeps_original_line_and_old_failure_reason(self):
        correction = make_ledger()
        correction.import_csv(csv_document(csv_row("CRT-0001")))
        correction.claim("CRT-0001")
        record = correction.classify_claimed().records[0]
        assert record.correction["original_line"] == FAILED_LINE_17
        assert record.correction["failure_status"] == ParseResultStatus.FIELD_MISSING.value
        assert "level" in record.correction["failure_reason"]
        assert record.correction["ticket_id"] == "CRT-0001"
        assert record.correction["ticket_key"] == "CRT-0001@1"
        assert record.record_id == "EVEN-CRT-0001-1"

    def test_successful_line_never_classified(self):
        correction = make_ledger()
        correction.import_csv(csv_document(csv_row("CRT-0001")))
        correction.claim("CRT-0001")
        outcome = correction.classify_claimed()
        assert all(rec.original_content != SUCCESS_LINE_18 for rec in outcome.records)
        assert len(outcome.records) == 1

    def test_unparseable_revision_not_classified(self):
        correction = make_ledger()
        correction.import_csv(
            csv_document(csv_row("CRT-0001", corrected="完全不是日志格式的内容")))
        batch = correction.build_rework_batch()
        assert len(batch) == 0
        assert batch.items[0].reason_code == RejectReason.CORRECTED_UNPARSEABLE.value
        correction.claim("CRT-0001")
        outcome = correction.classify_claimed()
        assert outcome.classified == 0
        assert outcome.records == []

    def test_field_patch_ticket_is_supported(self):
        correction = make_ledger()
        # 只补失败字段：level=ERROR，原行内容不动
        csv_text = (
            "单号,来源,文件路径,行号,原行内容,修订字段,版本,批准人\n"
            + ",".join(_csv_cell(cell) for cell in [
                "CRT-PATCH", "LOG_-0002", APP_FILE, "17", FAILED_LINE_17,
                "level=ERROR", "1", "值班员",
            ]) + "\n"
        )
        result = correction.import_csv(csv_text)
        assert result.written == 1
        correction.claim("CRT-PATCH")
        outcome = correction.classify_claimed()
        assert outcome.classified == 1
        record = outcome.records[0]
        assert record.parsed_fields["level"] == "ERROR"
        assert record.event_type.category_tag == "error"
        # 原文仍是失败行的原文，字段补丁没有改原文
        assert record.correction["original_line"] == FAILED_LINE_17
        assert record.correction["corrected_fields"] == {"level": "ERROR"}


# ---------------------------------------------------------------------------
# 4. 同一原行两版有效纠错：冲突与领取策略
# ---------------------------------------------------------------------------


def _claim_pair(correction: ParseCorrectionPipeline, strategy: str, claim_strategy: str):
    """先批准 ERROR 单并领取，再批准 LOGIN 单，随后按策略分类。"""

    correction.import_csv(csv_document(csv_row("CRT-ERR", version="1")))
    first_claim = correction.claim("CRT-ERR", strategy=claim_strategy)
    assert first_claim.ok is True

    correction.import_csv(csv_document(
        csv_row("CRT-AUTH", corrected=LOGIN_REVISION, version="2")))
    second_claim = correction.claim("CRT-AUTH", strategy=claim_strategy)
    return first_claim, second_claim


class TestConflict:
    def test_lock_at_claim_keeps_error_and_rejects_second(self):
        correction = make_ledger()
        first_claim, second_claim = _claim_pair(correction, "lock", "lock_at_claim")
        assert second_claim.ok is False
        assert second_claim.ticket is not None
        assert second_claim.ticket.status == CorrectionStatus.CONFLICT
        assert second_claim.ticket.version == "2"

        outcome = correction.classify_claimed(strategy="lock_at_claim")
        assert outcome.classified == 1
        record = outcome.records[0]
        assert record.event_type.category_tag == "error"
        assert record.correction["ticket_id"] == "CRT-ERR"
        assert record.parsed_fields["level"] == "ERROR"
        # 不能夹用第二份单的字段
        assert "LOGIN" not in record.parsed_fields.get("message", "")
        assert record.correction["other_revisions"] == ["2"]
        assert correction.get_ticket("CRT-AUTH", "2")["status"] == "冲突"

    def test_reclaim_before_classify_switches_to_auth_event(self):
        correction = make_ledger()
        first_claim, second_claim = _claim_pair(correction, "reclaim", "reclaim_before_classify")
        assert first_claim.ok is True
        assert second_claim.ok is True
        assert second_claim.superseded == ["CRT-ERR@1"]
        assert correction.get_ticket("CRT-ERR", "1")["status"] == CorrectionStatus.SUPERSEDED.value

        outcome = correction.classify_claimed(strategy="reclaim_before_classify")
        assert outcome.classified == 1
        record = outcome.records[0]
        assert record.event_type.type_id == "EVEN-0003"
        assert record.event_type.category_tag == "auth"
        assert record.correction["ticket_id"] == "CRT-AUTH"
        assert record.correction["version"] == "2"
        assert record.parsed_fields["level"] == "INFO"
        # 分类写出的字段只能来自 LOGIN 单：message 是 LOGIN，没有 ERROR 级别
        assert record.parsed_fields["message"].startswith("LOGIN")
        assert record.correction["chosen_rule"] == "PARS-0002"

    def test_same_line_first_formal_rework_is_single_type(self):
        for strategy, expected in (
            ("lock_at_claim", "error"),
            ("reclaim_before_classify", "auth"),
        ):
            correction = make_ledger()
            _claim_pair(correction, strategy, strategy)
            outcome = correction.classify_claimed(strategy=strategy)
            categories = [rec.event_type.category_tag for rec in outcome.records]
            assert categories == [expected], (strategy, categories)
            assert len(correction.journal()) == 1

    def test_two_versions_imported_before_classify_conflict_at_claim(self):
        correction = make_ledger()
        result = correction.import_csv(csv_document(
            csv_row("CRT-ERR", version="1"),
            csv_row("CRT-AUTH", corrected=LOGIN_REVISION, version="2"),
        ))
        assert result.written == 2
        claims = correction.claim_batch(strategy="lock_at_claim")
        assert sum(1 for claim in claims if claim.ok) == 1
        assert sum(1 for claim in claims if not claim.ok) == 1
        outcome = correction.classify_claimed(strategy="lock_at_claim")
        assert [rec.event_type.category_tag for rec in outcome.records] == ["error"]

    def test_claim_batch_reclaim_uses_latest_version(self):
        correction = make_ledger()
        correction.import_csv(csv_document(
            csv_row("CRT-ERR", version="1"),
            csv_row("CRT-AUTH", corrected=LOGIN_REVISION, version="2"),
        ))
        claims = correction.claim_batch(strategy="reclaim_before_classify")
        assert all(claim.ok for claim in claims)
        assert claims[0].ticket is not None
        assert claims[0].ticket.ticket_id == "CRT-AUTH"
        outcome = correction.classify_claimed(strategy="reclaim_before_classify")
        assert [rec.event_type.category_tag for rec in outcome.records] == ["auth"]


# ---------------------------------------------------------------------------
# 5. 结清与重启恢复
# ---------------------------------------------------------------------------


def _settled_error_scenario() -> ParseCorrectionPipeline:
    """分类侧已把 17 按首份单确认为错误事件，但导入侧还没收到结清回执。"""

    correction = make_ledger()
    correction.import_csv(csv_document(csv_row("CRT-ERR", version="1")))
    correction.claim("CRT-ERR")
    outcome = correction.classify_claimed()
    assert outcome.records[0].event_type.category_tag == "error"
    assert correction.get_ticket("CRT-ERR", "1")["status"] == CorrectionStatus.CLASSIFIED.value
    return correction


class TestSettlementAndRecovery:
    def test_acknowledge_requires_classification_record(self):
        correction = make_ledger()
        correction.import_csv(csv_document(csv_row("CRT-0001")))
        correction.claim("CRT-0001")
        # 还没分类就要求结清：必须拒绝，不能只把单子改成结清
        premature = correction.acknowledge("CRT-0001")
        assert premature["ok"] is False
        assert premature["reason_code"] == RejectReason.CLASSIFICATION_RECORD_MISSING.value
        assert correction.get_ticket("CRT-0001")["status"] == CorrectionStatus.CLAIMED.value

        correction.classify_claimed()
        settled = correction.acknowledge("CRT-0001")
        assert settled["ok"] is True
        assert settled["event_category"] == "error"
        assert settled["record"]["original_content"] == ERROR_REVISION
        assert settled["record"]["correction"]["original_line"] == FAILED_LINE_17

    def test_acknowledge_is_idempotent(self):
        correction = _settled_error_scenario()
        first = correction.acknowledge("CRT-ERR", version="1")
        second = correction.acknowledge("CRT-ERR", version="1")
        assert first["ok"] is True
        assert second["ok"] is True
        assert second["already_settled"] is True
        assert first["record_id"] == second["record_id"]

    def test_restart_before_settlement_does_not_reemit(self):
        correction = _settled_error_scenario()
        snapshot = correction.snapshot(include_records=False)
        # 重启：分类记录尚未落进导入侧，只能依赖分类日志恢复
        restarted = ParseCorrectionPipeline.from_snapshot(snapshot)
        report = restarted.recover()

        assert report.settled_from_journal == ["CRT-ERR@1"]
        assert report.records_restored == 1
        assert restarted.get_ticket("CRT-ERR", "1")["status"] == CorrectionStatus.SETTLED.value
        # 已完成的首次回修不再作为失败行送出
        assert restarted.pending_failure_lines() == []
        assert report.pending_rework_tickets == []
        assert report.inconsistent == []

        # 分类记录也没有丢，可以用结清回执核销
        ack = restarted.acknowledge("CRT-ERR", version="1")
        assert ack["ok"] is True
        assert ack["already_settled"] is True
        assert ack["record_id"] == "EVEN-CRT-ERR-1"
        assert ack["record"]["event_type"]["category_tag"] == "error"
        assert ack["record"]["correction"]["original_line"] == FAILED_LINE_17

    def test_restart_with_records_avoids_duplicates(self):
        correction = _settled_error_scenario()
        snapshot = correction.snapshot(include_records=True)
        restarted = ParseCorrectionPipeline.from_snapshot(snapshot)
        before = len(restarted.ingest.get_event_store().get("unknown", []))
        report = restarted.recover()
        after = len(restarted.ingest.get_event_store().get("unknown", []))
        assert report.records_restored == 0
        assert before == after
        assert len(restarted.event_records()) == 1

    def test_settled_without_journal_is_inconsistent_not_completed(self):
        correction = _settled_error_scenario()
        snapshot = correction.snapshot(include_records=True)
        snapshot["journal"] = []  # 模拟只把单子改成结清、分类记录丢了
        restarted = ParseCorrectionPipeline.from_snapshot(snapshot)
        # 单子仍标记为已分类/已结清，但无分类日志
        restarted.get_ticket  # noqa: B018 - 可读性占位
        report = restarted.recover()
        assert report.inconsistent, "缺少分类记录必须登记为不一致"
        assert any("缺少分类记录" in item["reason"] for item in report.inconsistent)
        # 原行没有被悄悄置为已解决；失败行仍能看见
        line = ledger_line(restarted)
        assert line["resolved"] is False
        assert report.failed_lines_remaining

    def test_recover_is_idempotent(self):
        correction = _settled_error_scenario()
        snapshot = correction.snapshot(include_records=False)
        restarted = ParseCorrectionPipeline.from_snapshot(snapshot)
        first = restarted.recover()
        second = restarted.recover()
        assert first.records_restored == 1
        assert second.records_restored == 0
        assert len(restarted.event_records()) == 1
        assert restarted.pending_failure_lines() == []

    def test_pending_claim_survives_restart(self):
        correction = make_ledger()
        correction.import_csv(csv_document(csv_row("CRT-0001")))
        correction.claim("CRT-0001")
        snapshot = correction.snapshot(include_records=True)
        restarted = ParseCorrectionPipeline.from_snapshot(snapshot)
        report = restarted.recover()
        assert report.pending_rework == ["CRT-0001@1"]
        outcome = restarted.classify_claimed()
        assert outcome.classified == 1
        assert outcome.records[0].correction["ticket_id"] == "CRT-0001"

    def test_restart_with_second_revision_approved_settles_separately(self):
        """分类侧已按首份单确认错误事件，导入侧未收到结清回执就重启，第二份获批。"""

        correction = _settled_error_scenario()
        restarted = ParseCorrectionPipeline.from_snapshot(
            correction.snapshot(include_records=False))

        # 重启时第二份修订单获批
        second = restarted.import_csv(csv_document(
            csv_row("CRT-AUTH", corrected=LOGIN_REVISION, version="2")))
        assert second.written == 1

        report = restarted.recover()
        # 首次回修从分类日志结清，且不再作为失败行送出
        assert report.settled_from_journal == ["CRT-ERR@1"]
        assert restarted.pending_failure_lines() == []
        assert report.pending_rework == ["CRT-AUTH@2"]

        # 第二份单独核销，与首次正式结果分开
        claim = restarted.claim("CRT-AUTH", version="2")
        assert claim.ok is True
        outcome = restarted.classify_claimed()
        assert outcome.classified == 1
        assert outcome.records[0].event_type.category_tag == "auth"
        assert outcome.records[0].correction["ticket_key"] == "CRT-AUTH@2"
        assert outcome.records[0].correction["original_line"] == FAILED_LINE_17

        assert restarted.get_ticket("CRT-ERR", "1")["record_id"] == "EVEN-CRT-ERR-1"
        assert restarted.get_ticket("CRT-ERR", "1")["event_category"] == "error"
        journal = {entry["ticket_key"]: entry for entry in restarted.journal()}
        assert set(journal) == {"CRT-ERR@1", "CRT-AUTH@2"}

    def test_concurrent_classify_writes_one_record(self):
        correction = make_ledger()
        correction.import_csv(csv_document(csv_row("CRT-0001")))
        correction.claim("CRT-0001")
        barrier = threading.Barrier(6)
        counts: list[int] = []

        def worker() -> None:
            barrier.wait()
            counts.append(correction.classify_claimed().classified)

        threads = [threading.Thread(target=worker) for _ in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert sum(counts) == 1
        assert len(correction.event_records()) == 1
        assert len(correction.journal()) == 1

    def test_replaced_target_line_blocks_write_back(self):
        correction = make_ledger()
        correction.import_csv(csv_document(csv_row("CRT-ERR", version="1")))
        correction.claim("CRT-ERR")
        target_key = correction.tickets()[0]["target_key"]
        # 同一物理位置被新一批日志覆盖（原文已更换）
        correction._lines[target_key].raw_line = OTHER_INCIDENT_LINE_17
        correction._lines[target_key].checksum = evidence_checksum(
            "LOG_-0002", APP_FILE, 17, OTHER_INCIDENT_LINE_17)

        outcome = correction.classify_claimed()
        assert outcome.classified == 0
        assert outcome.items[0].reason_code == RejectReason.EVIDENCE_MISMATCH.value
        assert correction.event_records() == []
        assert correction.get_ticket("CRT-ERR", "1")["status"] == CorrectionStatus.CONFLICT.value
        # 当前日志里的原文是新的那条：别次故障的修订没有被写回
        assert ledger_line(correction)["raw_line"] == OTHER_INCIDENT_LINE_17
        assert all(
            record["correction"].get("original_line") != FAILED_LINE_17
            for record in correction.event_records()
        )

    def test_settled_line_rejects_reused_version(self):
        correction = _settled_error_scenario()
        correction.acknowledge("CRT-ERR", version="1")
        # 同位置再导入一个"新单号但复用版本 1"的修订：必须用新版本号单独核销，
        # 不能借版本 1 覆盖首次结果。
        result = correction.import_csv(csv_document(
            csv_row("CRT-REUSE", corrected=LOGIN_REVISION, version="1")))
        assert result.written == 0
        assert result.rows[0].reason_code == RejectReason.TARGET_ALREADY_RESOLVED.value
        assert correction.get_ticket("CRT-ERR", "1")["event_category"] == "error"

        follow_up = correction.import_csv(csv_document(
            csv_row("CRT-REUSE", corrected=LOGIN_REVISION, version="2")))
        assert follow_up.written == 1
        assert follow_up.rows[0].ticket is not None
        assert follow_up.rows[0].ticket.revision == 2


# ---------------------------------------------------------------------------
# 6. 后续修订与首次正式结果分开核销
# ---------------------------------------------------------------------------


class TestFollowUpRevision:
    def _first_settled_then_second(self) -> ParseCorrectionPipeline:
        correction = make_ledger()
        correction.import_csv(csv_document(csv_row("CRT-ERR", version="1")))
        correction.claim("CRT-ERR")
        correction.classify_claimed()
        correction.acknowledge("CRT-ERR", version="1")
        assert correction.get_ticket("CRT-ERR", "1")["status"] == CorrectionStatus.SETTLED.value
        correction.import_csv(csv_document(
            csv_row("CRT-AUTH", corrected=LOGIN_REVISION, version="2")))
        return correction

    def test_follow_up_revision_settled_separately(self):
        correction = self._first_settled_then_second()
        claims = correction.claim("CRT-AUTH", version="2")
        assert claims.ok is True
        outcome = correction.classify_claimed()
        assert outcome.classified == 1
        second = outcome.records[0]
        assert second.event_type.category_tag == "auth"
        assert second.correction["ticket_id"] == "CRT-AUTH"
        assert second.correction["revision"] == 2

        # 首次正式结果仍然单独可核
        first = correction.get_ticket("CRT-ERR", "1")
        assert first["status"] == CorrectionStatus.SETTLED.value
        assert first["record_id"] == "EVEN-CRT-ERR-1"
        assert first["event_category"] == "error"

        journal = {entry["ticket_key"]: entry for entry in correction.journal()}
        assert journal["CRT-ERR@1"]["original_line"] == FAILED_LINE_17
        assert journal["CRT-AUTH@2"]["original_line"] == FAILED_LINE_17
        assert journal["CRT-ERR@1"]["corrected_line"] == ERROR_REVISION
        assert journal["CRT-AUTH@2"]["corrected_line"] == LOGIN_REVISION

        ack = correction.acknowledge("CRT-AUTH", version="2")
        assert ack["ok"] is True
        assert ack["event_category"] == "auth"

    def test_second_revision_cannot_reuse_first_version(self):
        correction = self._first_settled_then_second()
        # 同一版本号重传：仍是幂等命中首次那张单，不会覆盖首份核销结果
        retransmit = correction.import_csv(csv_document(
            csv_row("CRT-ERR", corrected=LOGIN_REVISION, version="1")))
        assert retransmit.written == 0
        assert retransmit.rows[0].duplicate is True
        assert retransmit.rows[0].ticket is not None
        assert retransmit.rows[0].ticket.ticket_id == "CRT-ERR"
        # 首份单的结清与分类结果不受影响
        assert correction.get_ticket("CRT-ERR", "1")["status"] == CorrectionStatus.SETTLED.value
        assert correction.get_ticket("CRT-ERR", "1")["event_category"] == "error"


# ---------------------------------------------------------------------------
# 7. 端到端交错执行：拒绝、分类、证据、重传记录数
# ---------------------------------------------------------------------------


class TestInterleavedEndToEnd:
    def test_full_interleaved_scenario(self):
        correction = make_ledger()
        ingest_records = correction.event_records()
        assert ingest_records == []

        # 第一步：混入错来源、缺必填字段、原文不符的尝试
        batch = csv_document(
            csv_row("CRT-BAD-SOURCE", source="LOG_-7777"),
            csv_row("CRT-BAD-EVIDENCE", original=OTHER_INCIDENT_LINE_17),
            csv_row("CRT-ERR", version="1"),
            csv_row("CRT-AUTH", corrected=LOGIN_REVISION, version="2"),
        )
        result = correction.import_csv(batch, imported_by="值班员")
        assert result.written == 2
        assert result.rejected == 2
        rejections = {row.ticket_id: row.reason_code for row in result.rows if not row.written}
        assert rejections == {
            "CRT-BAD-SOURCE": RejectReason.UNKNOWN_SOURCE.value,
            "CRT-BAD-EVIDENCE": RejectReason.EVIDENCE_MISMATCH.value,
        }

        # 第二步：领取首份单（ERROR），分类尚未提交前第二份（LOGIN）获批
        first = correction.claim("CRT-ERR", version="1", strategy="lock_at_claim")
        assert first.ok is True
        second = correction.claim("CRT-AUTH", version="2", strategy="lock_at_claim")
        assert second.ok is False
        assert second.ticket is not None
        assert second.ticket.status == CorrectionStatus.CONFLICT

        # 第三步：分类写出错误事件，依据是首份单与原失败行
        classified = correction.classify_claimed(strategy="lock_at_claim")
        assert classified.classified == 1
        record = classified.records[0]
        assert record.event_type.type_id == "EVEN-0002"
        assert record.event_type.category_tag == "error"
        assert record.correction["ticket_key"] == "CRT-ERR@1"
        assert record.correction["original_line"] == FAILED_LINE_17
        assert record.correction["failure_status"] == ParseResultStatus.FIELD_MISSING.value
        assert record.parsed_fields["level"] == "ERROR"
        assert "LOGIN" not in record.parsed_fields["message"]

        # 第四步：重传整批（含被拒的尝试和两版合法单）→ 记录数不增长
        before_tickets = len(correction.tickets())
        before_records = len(correction.event_records())
        retransmit = correction.import_csv(batch, imported_by="值班员")
        assert retransmit.written == 0
        assert retransmit.duplicates == 2
        assert retransmit.rejected == 2
        assert len(correction.tickets()) == before_tickets
        assert len(correction.event_records()) == before_records == 1

    def test_reclaim_variant_produces_auth_event(self):
        correction = make_ledger()
        batch = csv_document(
            csv_row("CRT-BAD-SOURCE", source="LOG_-7777"),
            csv_row("CRT-BAD-EVIDENCE", original=OTHER_INCIDENT_LINE_17),
            csv_row("CRT-ERR", version="1"),
            csv_row("CRT-AUTH", corrected=LOGIN_REVISION, version="2"),
        )
        correction.import_csv(batch)
        # 先锁首份单
        assert correction.claim("CRT-ERR", strategy="lock_at_claim").ok is True
        # 分类前重新领取第二份单
        second = correction.claim("CRT-AUTH", strategy="reclaim_before_classify")
        assert second.ok is True
        assert second.superseded == ["CRT-ERR@1"]

        classified = correction.classify_claimed(strategy="reclaim_before_classify")
        assert classified.classified == 1
        record = classified.records[0]
        assert record.event_type.type_id == "EVEN-0003"
        assert record.event_type.category_tag == "auth"
        assert record.correction["ticket_key"] == "CRT-AUTH@2"
        assert record.parsed_fields["level"] == "INFO"
        assert record.parsed_fields["message"].startswith("LOGIN")
        assert "ERROR" not in record.parsed_fields.values()

    def test_bulk_import_endpoint_shape(self):
        correction = make_ledger()
        result = correction.import_csv(csv_document(
            csv_row("CRT-ERR", version="1"),
            csv_row("CRT-BAD-SOURCE", source="LOG_-7777"),
        ))
        payload = result.to_dict()
        assert payload["batch_id"]
        assert payload["total"] == 2
        assert payload["written"] == 1
        assert payload["rejected"] == 1
        assert payload["rows"][0]["ticket"]["ticket_key"] == "CRT-ERR@1"
        assert payload["rows"][1]["reason_code"] == RejectReason.UNKNOWN_SOURCE.value

    def test_conflicted_ticket_cannot_be_settled(self):
        correction = make_ledger()
        _claim_pair(correction, "lock", "lock_at_claim")
        correction.classify_claimed(strategy="lock_at_claim")
        ack = correction.acknowledge("CRT-AUTH", version="2")
        assert ack["ok"] is False
        assert ack["reason_code"] == RejectReason.TICKET_STATE_INVALID.value
        assert ack["ticket"]["status"] == CorrectionStatus.CONFLICT.value


# ---------------------------------------------------------------------------
# 运行入口（确定性测试，不依赖 pytest）
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import traceback

    tests = {
        "import_failed_line_17_field_missing": TestCorrectionImport().test_failed_line_17_is_field_missing,
        "import_successful_line_not_failure": TestCorrectionImport().test_successful_line_is_not_a_failure,
        "import_valid_ticket_writes_once": TestCorrectionImport().test_import_valid_ticket_writes_once,
        "import_retransmit_idempotent": TestCorrectionImport().test_retransmit_identical_version_is_idempotent,
        "import_concurrent_duplicate": TestCorrectionImport().test_concurrent_duplicate_import_writes_once,
        "import_wrong_source": TestCorrectionImport().test_wrong_source_rejected,
        "import_missing_required_field": TestCorrectionImport().test_missing_required_field_rejected,
        "import_line_not_found": TestCorrectionImport().test_line_not_found_rejected,
        "import_already_successful_line": TestCorrectionImport().test_already_successful_line_rejected,
        "import_ambiguous_source": TestCorrectionImport().test_ambiguous_source_without_file_is_rejected,
        "evidence_changed_line": TestEvidence().test_changed_original_line_rejected,
        "evidence_batch_isolation": TestEvidence().test_other_valid_tickets_in_same_batch_still_succeed,
        "evidence_checksum": TestEvidence().test_checksum_changes_with_evidence,
        "batch_only_failed_line": TestReworkBatchClassification().test_batch_contains_only_failed_line,
        "batch_classify_fields": TestReworkBatchClassification().test_classify_consumes_apply_parse_rules_fields,
        "batch_record_evidence": TestReworkBatchClassification().test_record_keeps_original_line_and_old_failure_reason,
        "batch_successful_not_classified": TestReworkBatchClassification().test_successful_line_never_classified,
        "batch_unparseable": TestReworkBatchClassification().test_unparseable_revision_not_classified,
        "batch_field_patch": TestReworkBatchClassification().test_field_patch_ticket_is_supported,
        "conflict_lock_at_claim": TestConflict().test_lock_at_claim_keeps_error_and_rejects_second,
        "conflict_reclaim_before_classify": TestConflict().test_reclaim_before_classify_switches_to_auth_event,
        "conflict_single_type": TestConflict().test_same_line_first_formal_rework_is_single_type,
        "conflict_two_versions_claim": TestConflict().test_two_versions_imported_before_classify_conflict_at_claim,
        "conflict_claim_batch_reclaim": TestConflict().test_claim_batch_reclaim_uses_latest_version,
        "settle_requires_record": TestSettlementAndRecovery().test_acknowledge_requires_classification_record,
        "settle_idempotent": TestSettlementAndRecovery().test_acknowledge_is_idempotent,
        "recover_before_settlement": TestSettlementAndRecovery().test_restart_before_settlement_does_not_reemit,
        "recover_with_records": TestSettlementAndRecovery().test_restart_with_records_avoids_duplicates,
        "recover_inconsistent": TestSettlementAndRecovery().test_settled_without_journal_is_inconsistent_not_completed,
        "recover_idempotent": TestSettlementAndRecovery().test_recover_is_idempotent,
        "recover_pending_claim": TestSettlementAndRecovery().test_pending_claim_survives_restart,
        "recover_second_revision": TestSettlementAndRecovery().test_restart_with_second_revision_approved_settles_separately,
        "recover_concurrent_classify": TestSettlementAndRecovery().test_concurrent_classify_writes_one_record,
        "recover_replaced_target": TestSettlementAndRecovery().test_replaced_target_line_blocks_write_back,
        "followup_reused_version_rejected": TestSettlementAndRecovery().test_settled_line_rejects_reused_version,
        "followup_settled_separately": TestFollowUpRevision().test_follow_up_revision_settled_separately,
        "followup_no_version_reuse": TestFollowUpRevision().test_second_revision_cannot_reuse_first_version,
        "e2e_interleaved": TestInterleavedEndToEnd().test_full_interleaved_scenario,
        "e2e_reclaim_variant": TestInterleavedEndToEnd().test_reclaim_variant_produces_auth_event,
        "e2e_payload": TestInterleavedEndToEnd().test_bulk_import_endpoint_shape,
        "e2e_conflict_ack": TestInterleavedEndToEnd().test_conflicted_ticket_cannot_be_settled,
    }

    passed = 0
    failed = 0
    for name, test_func in tests.items():
        try:
            test_func()
            passed += 1
            print(f"  PASS  test_parse_correction: {name}")
        except Exception:
            failed += 1
            print(f"  FAIL  test_parse_correction: {name}")
            traceback.print_exc()

    print(f"\ntest_parse_correction: {passed} passed, {failed} failed, {len(tests)} total")
    sys.exit(1 if failed else 0)
