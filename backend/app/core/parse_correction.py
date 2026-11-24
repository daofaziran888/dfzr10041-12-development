"""失败日志逐行回修：CSV 解析纠错单导入 → 有效回修事件批次 → 事件分类。

背景
----
采集链路里 ``apply_parse_rules`` 会把解析失败的行（格式不匹配、字段缺失）
留在 ``LogEntry`` 上，但这些行没有后续的修复入口。本模块补上"逐行回修"：

1. ``import_csv`` 导入 CSV 纠错单，按 ``来源 + 源文件行号`` 定位之前失败的行；
2. 纠错单必须携带可核对的原行内容，位置相同但原文已更换时对该单单独拒绝；
3. 只有通过校验的纠错单会进入有效回修事件批次，事件分类只消费该批次；
4. ``apply_parse_rules`` 重新解析修订内容得到有效字段，交给
   ``IngestPipeline.classify_events`` 生成事件记录；
5. 领取（claim）时锁定同一物理行，保证同一回修只生效一次；分类前重新领取
   则会让后批准的修订成为唯一生效版本，两版字段绝不混用；
6. 结清（acknowledge）以分类日志为准：分类记录与结清状态一起落盘，
   重启恢复时既不会把已完成的回修重新当失败行送出，也不会丢分类记录；
7. 同一版本的纠错单重复（含并发）导入只写入一次；后续修订与首次正式结果
   分开核销，各自保留原行依据。
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import threading
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Iterable

from app.core.models import (
    EventRecord, EventTypeDef, LogEntry, LogSourceConfig,
    ParseResultStatus, ParseRule,
)
from app.core.pipeline import IngestPipeline, PRESET_PARSE_RULES

# ---------------------------------------------------------------------------
# 状态与拒绝原因
# ---------------------------------------------------------------------------


class CorrectionStatus(str, Enum):
    """纠错单生命周期。"""

    APPROVED = "已批准"
    CLAIMED = "已领取"
    CLASSIFIED = "已分类"
    SETTLED = "已结清"
    SUPERSEDED = "已替代"
    CONFLICT = "冲突"
    REJECTED = "已拒绝"
    PARSE_FAILED = "回修解析失败"


class RejectReason(str, Enum):
    """纠错单被拒绝的稳定原因码（message 里同时给中文说明）。"""

    MISSING_FIELD = "missing_required_field"
    UNKNOWN_SOURCE = "source_not_found"
    AMBIGUOUS_SOURCE = "ambiguous_source"
    LINE_NOT_FOUND = "line_not_found"
    LINE_ALREADY_SUCCESSFUL = "line_already_successful"
    EVIDENCE_MISMATCH = "original_line_mismatch"
    DUPLICATE_VERSION = "duplicate_version"
    TARGET_ALREADY_RESOLVED = "target_already_resolved"
    CORRECTED_UNPARSEABLE = "corrected_line_unparseable"
    CLASSIFICATION_RECORD_MISSING = "classification_record_missing"
    TICKET_NOT_FOUND = "ticket_not_found"
    TICKET_STATE_INVALID = "ticket_state_invalid"


_FAILURE_TEXT = {
    ParseResultStatus.FORMAT_MISMATCH: "格式不匹配",
    ParseResultStatus.FIELD_MISSING: "字段缺失",
    ParseResultStatus.ENCODING_ERROR: "编码异常",
    ParseResultStatus.LINE_EMPTY: "空行",
}

_HEADER_ALIASES: dict[str, tuple[str, ...]] = {
    "ticket_id": ("单号", "纠错单号", "修订单号", "ticket_id", "correction_id", "ticket"),
    "source_id": ("来源", "日志来源", "来源编号", "source_id", "source"),
    "file_path": ("文件路径", "源文件", "文件", "file_path", "file"),
    "line_number": ("行号", "源文件行号", "行", "line_number", "line"),
    "original_line": ("原行内容", "原文", "原行", "original_line", "original"),
    "corrected_line": ("修订内容", "修正内容", "修订行", "corrected_line", "replacement"),
    "corrected_fields": ("修订字段", "字段修订", "corrected_fields", "fields"),
    "version": ("版本", "修订版本", "version"),
    "approved_by": ("批准人", "审批人", "approved_by"),
}


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _clean(value: Any) -> str:
    return "" if value is None else str(value)


def _normalize_line(value: str) -> str:
    return value.rstrip("\r\n")


def evidence_checksum(source_id: str, file_path: str, line_number: int, raw_line: str) -> str:
    """原行证据指纹：来源、文件、行号和原文共同决定，任一项变化都会失配。"""

    raw = f"{source_id}|{file_path}|{line_number}|{_normalize_line(raw_line)}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# 失败行台账
# ---------------------------------------------------------------------------


@dataclass
class SourceLogLine:
    """来源文件里的一行：既登记失败行，也登记原本成功的行。

    成功行也要登记，这样纠错单指向一个已成功的行时能给出明确拒绝原因
    （"该行原本已解析成功"），而不是笼统的"找不到行"。
    """

    source_id: str
    file_path: str
    line_number: int
    raw_line: str
    parse_status: ParseResultStatus
    failure_reason: str = ""
    parsed_fields: dict[str, str] | None = None
    checksum: str = ""
    resolved: bool = False
    resolved_by_ticket: str = ""
    resolved_ticket_key: str = ""
    resolved_record_id: str = ""

    def __post_init__(self) -> None:
        if not self.checksum:
            self.checksum = evidence_checksum(
                self.source_id, self.file_path, self.line_number, self.raw_line)

    @property
    def target_key(self) -> str:
        return f"{self.source_id}|{self.file_path}|{self.line_number}"

    @property
    def is_failure(self) -> bool:
        return self.parse_status != ParseResultStatus.OK

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_id": self.source_id,
            "file_path": self.file_path,
            "line_number": self.line_number,
            "raw_line": self.raw_line,
            "parse_status": self.parse_status.value,
            "failure_reason": self.failure_reason,
            "checksum": self.checksum,
            "resolved": self.resolved,
            "resolved_by_ticket": self.resolved_by_ticket,
            "resolved_record_id": self.resolved_record_id,
        }


# ---------------------------------------------------------------------------
# 纠错单
# ---------------------------------------------------------------------------


@dataclass
class ParseCorrectionTicket:
    """CSV 导入的一张解析纠错单。"""

    ticket_id: str
    source_id: str
    file_path: str
    line_number: int
    original_line: str
    corrected_line: str = ""
    corrected_fields: dict[str, str] = field(default_factory=dict)
    version: str = "1"
    approved_by: str = ""
    status: CorrectionStatus = CorrectionStatus.APPROVED
    evidence_checksum: str = ""
    target_key: str = ""
    claim_batch: str = ""
    claimed_at: str = ""
    classified_at: str = ""
    settled_at: str = ""
    record_id: str = ""
    event_type_id: str = ""
    event_category: str = ""
    revision: int = 1
    import_seq: int = 0
    note: str = ""

    @property
    def key(self) -> str:
        return f"{self.ticket_id}@{self.version}"

    @property
    def mode(self) -> str:
        return "corrected_line" if self.corrected_line else "corrected_fields"

    def to_dict(self) -> dict[str, Any]:
        return {
            "ticket_id": self.ticket_id,
            "version": self.version,
            "ticket_key": self.key,
            "source_id": self.source_id,
            "file_path": self.file_path,
            "line_number": self.line_number,
            "original_line": self.original_line,
            "corrected_line": self.corrected_line,
            "corrected_fields": dict(self.corrected_fields),
            "approved_by": self.approved_by,
            "status": self.status.value,
            "evidence_checksum": self.evidence_checksum,
            "target_key": self.target_key,
            "claim_batch": self.claim_batch,
            "claimed_at": self.claimed_at,
            "classified_at": self.classified_at,
            "settled_at": self.settled_at,
            "record_id": self.record_id,
            "event_type_id": self.event_type_id,
            "event_category": self.event_category,
            "revision": self.revision,
            "mode": self.mode,
            "note": self.note,
        }


@dataclass
class ImportRowResult:
    """CSV 单行导入结果；整批里一行被拒不影响其他行。"""

    row_number: int
    ticket_id: str
    ok: bool
    written: bool = False
    duplicate: bool = False
    reason_code: str = ""
    reason: str = ""
    ticket: ParseCorrectionTicket | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "row_number": self.row_number,
            "ticket_id": self.ticket_id,
            "ok": self.ok,
            "written": self.written,
            "duplicate": self.duplicate,
            "reason_code": self.reason_code,
            "reason": self.reason,
            "ticket": self.ticket.to_dict() if self.ticket else None,
        }


@dataclass
class ImportBatchResult:
    """一次 CSV 导入的整批结果。"""

    batch_id: str
    rows: list[ImportRowResult] = field(default_factory=list)
    written: int = 0
    rejected: int = 0
    duplicates: int = 0

    @property
    def tickets(self) -> list[ParseCorrectionTicket]:
        return [row.ticket for row in self.rows if row.ticket is not None]

    @property
    def valid_tickets(self) -> list[ParseCorrectionTicket]:
        return [t for t in self.tickets if t.status == CorrectionStatus.APPROVED]

    def to_dict(self) -> dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "written": self.written,
            "rejected": self.rejected,
            "duplicates": self.duplicates,
            "total": len(self.rows),
            "rows": [row.to_dict() for row in self.rows],
        }


@dataclass
class ClaimResult:
    ok: bool
    message: str
    ticket: ParseCorrectionTicket | None = None
    superseded: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "message": self.message,
            "ticket": self.ticket.to_dict() if self.ticket else None,
            "superseded": list(self.superseded),
        }


@dataclass
class ClassifyItemResult:
    ticket_id: str
    version: str
    ok: bool
    status: str
    event_type_id: str = ""
    event_category: str = ""
    record_id: str = ""
    reason_code: str = ""
    reason: str = ""
    skipped: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "ticket_id": self.ticket_id,
            "version": self.version,
            "ok": self.ok,
            "status": self.status,
            "event_type_id": self.event_type_id,
            "event_category": self.event_category,
            "record_id": self.record_id,
            "reason_code": self.reason_code,
            "reason": self.reason,
            "skipped": self.skipped,
        }


@dataclass
class ClassifyBatchResult:
    items: list[ClassifyItemResult] = field(default_factory=list)
    records: list[EventRecord] = field(default_factory=list)

    @property
    def classified(self) -> int:
        return sum(1 for item in self.items if item.ok and not item.skipped)

    def to_dict(self) -> dict[str, Any]:
        return {
            "classified": self.classified,
            "records": [record_payload(record) for record in self.records],
            "items": [item.to_dict() for item in self.items],
        }


@dataclass
class ReworkBatch:
    """有效回修事件批次：只有这些条目会被事件分类消费。"""

    batch_id: str
    entries: list[LogEntry] = field(default_factory=list)
    items: list[ClassifyItemResult] = field(default_factory=list)

    def __len__(self) -> int:
        return len(self.entries)

    def to_dict(self) -> dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "count": len(self.entries),
            "items": [item.to_dict() for item in self.items],
        }


@dataclass
class ClassificationJournalEntry:
    """分类日志：结清和重启恢复都以它为准，避免只改单状态而丢记录。"""

    ticket_key: str
    ticket_id: str
    version: str
    target_key: str
    record_id: str
    record: dict[str, Any]
    event_type_id: str
    event_category: str
    original_line: str
    original_checksum: str
    failure_reason: str
    corrected_line: str
    chosen_rule: str
    revision: int
    claim_batch: str
    classified_at: str = field(default_factory=_now)
    settled: bool = False
    settled_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "ticket_key": self.ticket_key,
            "ticket_id": self.ticket_id,
            "version": self.version,
            "target_key": self.target_key,
            "record_id": self.record_id,
            "record": dict(self.record),
            "event_type_id": self.event_type_id,
            "event_category": self.event_category,
            "original_line": self.original_line,
            "original_checksum": self.original_checksum,
            "failure_reason": self.failure_reason,
            "corrected_line": self.corrected_line,
            "chosen_rule": self.chosen_rule,
            "revision": self.revision,
            "claim_batch": self.claim_batch,
            "classified_at": self.classified_at,
            "settled": self.settled,
            "settled_at": self.settled_at,
        }


@dataclass
class RecoveryReport:
    """重启恢复结果。"""

    settled_from_journal: list[str] = field(default_factory=list)
    pending_rework: list[str] = field(default_factory=list)
    inconsistent: list[dict[str, str]] = field(default_factory=list)
    records_restored: int = 0
    completed_not_reemitted: int = 0
    duplicate_records_avoided: int = 0
    failed_lines_remaining: list[dict[str, Any]] = field(default_factory=list)

    @property
    def pending_rework_tickets(self) -> list[str]:
        return list(self.pending_rework)

    def to_dict(self) -> dict[str, Any]:
        return {
            "settled_from_journal": list(self.settled_from_journal),
            "pending_rework": list(self.pending_rework),
            "inconsistent": list(self.inconsistent),
            "records_restored": self.records_restored,
            "completed_not_reemitted": self.completed_not_reemitted,
            "duplicate_records_avoided": self.duplicate_records_avoided,
            "failed_lines_remaining": list(self.failed_lines_remaining),
        }


def record_payload(record: EventRecord) -> dict[str, Any]:
    """把 EventRecord 转成可回放的结构（恢复时用它还原分类记录）。"""

    return {
        "record_id": record.record_id,
        "event_type": {
            "type_id": record.event_type.type_id,
            "type_name": record.event_type.type_name,
            "category_tag": record.event_type.category_tag,
            "severity": record.event_type.severity,
        },
        "original_content": record.original_content,
        "parsed_fields": dict(record.parsed_fields),
        "masked_fields": dict(record.masked_fields),
        "enriched_fields": dict(record.enriched_fields),
        "event_time": record.event_time.isoformat(),
        "source_host": record.source_host,
        "store_partition": record.store_partition,
        "index_state": record.index_state,
        "correction": dict(record.correction),
    }


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------


class ParseCorrectionPipeline:
    """失败行回修导入、领取、分类、结清与恢复。"""

    def __init__(
        self,
        *,
        ingest: IngestPipeline | None = None,
        parse_rules: list[ParseRule] | None = None,
        clock: Any | None = None,
    ) -> None:
        self.ingest = ingest or IngestPipeline()
        self.parse_rules = list(parse_rules if parse_rules is not None else PRESET_PARSE_RULES)
        self._clock = clock or _now
        self._lock = threading.RLock()

        self._lines: dict[str, SourceLogLine] = {}
        self._source_files: dict[str, set[str]] = {}
        self._tickets: dict[str, ParseCorrectionTicket] = {}
        self._version_index: dict[tuple[str, str], ParseCorrectionTicket] = {}
        self._target_locks: dict[str, str] = {}
        self._journal: dict[str, ClassificationJournalEntry] = {}
        self._records: dict[str, EventRecord] = {}
        self._import_seq = 0
        self._batch_seq = 0

    # ------------------------------------------------------------------
    # 台账登记
    # ------------------------------------------------------------------

    def register_log_lines(
        self, source_id: str, file_path: str, entries: Iterable[LogEntry],
        *, rule: ParseRule | None = None,
    ) -> list[SourceLogLine]:
        """登记来源文件里的行（成功行也登记，用于区分"不是失败行"和"找不到行"）。"""

        registered: list[SourceLogLine] = []
        with self._lock:
            self._source_files.setdefault(source_id, set()).add(file_path)
            for entry in entries:
                status = entry.parse_status
                reason = ""
                if status != ParseResultStatus.OK:
                    reason = _FAILURE_TEXT.get(status, status.value)
                    if status == ParseResultStatus.FIELD_MISSING and rule is not None:
                        fields = entry.parsed_fields or {}
                        missing = [
                            name for name in rule.field_names
                            if not str(fields.get(name) or "").strip()
                        ]
                        if missing:
                            reason = f"字段缺失：{'、'.join(missing)}"
                resolved = SourceLogLine(
                    source_id=source_id,
                    file_path=file_path,
                    line_number=int(entry.line_number),
                    raw_line=entry.raw_line,
                    parse_status=status,
                    failure_reason=reason,
                    parsed_fields=dict(entry.parsed_fields) if entry.parsed_fields else None,
                    # 台账里的校验值统一用"来源+文件+行号+原文"的证据指纹，
                    # 领取和分类前可以据此重新核对原行是否被替换。
                    checksum=evidence_checksum(
                        source_id, file_path, int(entry.line_number), entry.raw_line),
                )
                self._lines[resolved.target_key] = resolved
                registered.append(resolved)
        return registered

    def register_source_batch(
        self,
        pipeline: IngestPipeline,
        source: LogSourceConfig,
        file_path: str,
        raw_lines: list[str],
        rule: ParseRule,
        *,
        start_line: int = 1,
    ) -> list[LogEntry]:
        """便捷入口：解析一批原始行并把结果登记到失败行台账。

        ``start_line`` 是这批第一行在源文件里的真实行号（断点续采时不为 1）。
        """

        entries = pipeline.parse_raw_lines(source, raw_lines)
        entries = pipeline.apply_parse_rules(entries, rule)
        if start_line != 1:
            for entry in entries:
                entry.line_number = start_line + entry.line_number - 1
        self.register_log_lines(source.source_id, file_path, entries, rule=rule)
        return entries

    def lines(self) -> list[dict[str, Any]]:
        with self._lock:
            return [line.to_dict() for line in self._lines.values()]

    def pending_failure_lines(self) -> list[dict[str, Any]]:
        """仍未被任何已分类回修覆盖的失败行；已完成的回修不会回流到这里。"""

        with self._lock:
            pending = []
            for line in self._lines.values():
                if not line.is_failure:
                    continue
                if line.resolved:
                    continue
                if any(j.target_key == line.target_key for j in self._journal.values()):
                    continue
                pending.append(line.to_dict())
            return pending

    # ------------------------------------------------------------------
    # CSV 导入
    # ------------------------------------------------------------------

    def import_csv(
        self,
        csv_text: str,
        *,
        imported_by: str = "",
        batch_id: str | None = None,
    ) -> ImportBatchResult:
        """导入 CSV 纠错单。

        - 缺必填字段、来源对不上、行号找不到、原行证据不符的尝试逐行拒绝；
        - 同一版本（单号 + 版本）只写入一次，并发重复导入不会多写；
        - 一行被拒不影响同批其他合法纠错单。
        """

        with self._lock:
            self._batch_seq += 1
            current_batch = batch_id or f"CRBT-{self._batch_seq:04d}"
            result = ImportBatchResult(batch_id=current_batch)
            rows = list(self._iter_csv_rows(csv_text))
            for row_number, raw_row in rows:
                row = self._canonical_row(raw_row)
                outcome = self._import_row(row, row_number, imported_by)
                result.rows.append(outcome)
                if outcome.written:
                    result.written += 1
                elif outcome.duplicate:
                    result.duplicates += 1
                elif not outcome.ok:
                    result.rejected += 1
            return result

    def _iter_csv_rows(self, csv_text: str) -> Iterable[tuple[int, dict[str, Any]]]:
        text = _clean(csv_text)
        if text.startswith("﻿"):
            text = text.lstrip("﻿")
        reader = csv.DictReader(io.StringIO(text))
        for index, row in enumerate(reader, start=2):  # 第 1 行是表头
            yield index, dict(row)

    def _canonical_row(self, raw_row: dict[str, Any]) -> dict[str, str]:
        normalized = {
            _clean(name).strip().casefold(): _clean(value)
            for name, value in raw_row.items()
            if name is not None
        }
        row: dict[str, str] = {}
        for canonical, aliases in _HEADER_ALIASES.items():
            for alias in aliases:
                value = normalized.get(alias.casefold())
                if value is not None and value.strip() != "":
                    row[canonical] = value.strip()
                    break
        # 修订字段可能含中文逗号/分号，先保留原文，后面再解析
        return row

    def _import_row(
        self, row: dict[str, str], row_number: int, imported_by: str,
    ) -> ImportRowResult:
        ticket_id = row.get("ticket_id", "")
        missing: list[str] = []
        display = {
            "ticket_id": "单号",
            "source_id": "来源",
            "line_number": "行号",
            "original_line": "原行内容",
        }
        for field_name, label in display.items():
            if not row.get(field_name, "").strip():
                missing.append(label)
        if not row.get("corrected_line", "").strip() and not row.get("corrected_fields", "").strip():
            missing.append("修订内容或修订字段")
        if missing:
            return ImportRowResult(
                row_number=row_number,
                ticket_id=ticket_id,
                ok=False,
                reason_code=RejectReason.MISSING_FIELD.value,
                reason=f"缺少必填字段：{'、'.join(missing)}",
            )

        try:
            line_number = int(str(row["line_number"]).strip())
        except (TypeError, ValueError):
            return ImportRowResult(
                row_number=row_number,
                ticket_id=ticket_id,
                ok=False,
                reason_code=RejectReason.MISSING_FIELD.value,
                reason=f"缺少必填字段：行号（无法解析为整数：{row.get('line_number')}）",
            )
        if line_number <= 0:
            return ImportRowResult(
                row_number=row_number,
                ticket_id=ticket_id,
                ok=False,
                reason_code=RejectReason.MISSING_FIELD.value,
                reason=f"缺少必填字段：行号（必须为正整数：{line_number}）",
            )

        source_id = row["source_id"].strip()
        file_path = row.get("file_path", "").strip()
        version = row.get("version", "").strip() or "1"

        if source_id not in self._source_files:
            return ImportRowResult(
                row_number=row_number,
                ticket_id=ticket_id,
                ok=False,
                reason_code=RejectReason.UNKNOWN_SOURCE.value,
                reason=f"来源 {source_id} 无法对回已登记的日志",
            )

        located, locate_error = self._locate_line(source_id, file_path, line_number)
        if located is None:
            return ImportRowResult(
                row_number=row_number,
                ticket_id=ticket_id,
                ok=False,
                reason_code=locate_error[0],
                reason=locate_error[1],
            )

        original_line = _normalize_line(row["original_line"])
        if original_line != _normalize_line(located.raw_line):
            return ImportRowResult(
                row_number=row_number,
                ticket_id=ticket_id,
                ok=False,
                reason_code=RejectReason.EVIDENCE_MISMATCH.value,
                reason=(
                    "原行内容与当前日志不一致：物理位置相同但原文已更换，"
                    "拒绝把别次故障的修订写回当前日志"
                ),
            )

        if not located.is_failure:
            return ImportRowResult(
                row_number=row_number,
                ticket_id=ticket_id,
                ok=False,
                reason_code=RejectReason.LINE_ALREADY_SUCCESSFUL.value,
                reason="该行原本已解析成功，不再作为失败行回修",
            )

        # 同一张单的同一版本只写入一次：并发/重传导入都走这一判定。
        duplicate = self._version_index.get((ticket_id, version))
        if duplicate is not None:
            return ImportRowResult(
                row_number=row_number,
                ticket_id=ticket_id,
                ok=True,
                duplicate=True,
                reason_code=RejectReason.DUPLICATE_VERSION.value,
                reason=f"纠错单 {ticket_id} 的版本 {version} 已导入，重传不重复写入",
                ticket=duplicate,
            )

        # 该行已有正式回修结果：另一张单想复用已生效的版本号，必须单独核销，
        # 不能借这个版本号把首次结果覆盖掉。
        settled_tickets = [
            ticket for ticket in self._tickets.values()
            if ticket.target_key == located.target_key
            and ticket.status in (CorrectionStatus.CLASSIFIED, CorrectionStatus.SETTLED)
        ]
        if settled_tickets and version in {ticket.version for ticket in settled_tickets}:
            first = next(
                ticket for ticket in settled_tickets if ticket.version == version)
            return ImportRowResult(
                row_number=row_number,
                ticket_id=ticket_id,
                ok=False,
                reason_code=RejectReason.TARGET_ALREADY_RESOLVED.value,
                reason=(
                    f"该行首次正式回修已按版本 {version} 生效"
                    + (f"（记录 {first.record_id}）" if first.record_id else "")
                    + "，后续修订需使用新的版本号单独核销"
                ),
                ticket=first,
            )

        # 解析"修订字段"（支持 JSON 或 k=v;k2=v2）
        corrected_fields: dict[str, str] = {}
        raw_fields = row.get("corrected_fields", "").strip()
        if raw_fields:
            corrected_fields, fields_error = self._parse_fields(raw_fields)
            if fields_error:
                return ImportRowResult(
                    row_number=row_number,
                    ticket_id=ticket_id,
                    ok=False,
                    reason_code=RejectReason.MISSING_FIELD.value,
                    reason=f"缺少必填字段：修订字段（{fields_error}）",
                )

        self._import_seq += 1
        revision = 1 + sum(
            1 for ticket in self._tickets.values()
            if ticket.target_key == located.target_key
            and ticket.status in (CorrectionStatus.CLASSIFIED, CorrectionStatus.SETTLED)
        )
        ticket = ParseCorrectionTicket(
            ticket_id=ticket_id,
            source_id=source_id,
            file_path=located.file_path,
            line_number=line_number,
            original_line=located.raw_line,
            corrected_line=_normalize_line(row.get("corrected_line", "")),
            corrected_fields=corrected_fields,
            version=version,
            approved_by=row.get("approved_by", "").strip() or imported_by,
            evidence_checksum=located.checksum,
            target_key=located.target_key,
            revision=revision,
            import_seq=self._import_seq,
        )
        self._tickets[ticket.key] = ticket
        self._version_index[(ticket_id, version)] = ticket
        return ImportRowResult(
            row_number=row_number,
            ticket_id=ticket_id,
            ok=True,
            written=True,
            ticket=ticket,
            reason=f"已导入纠错单 {ticket_id}（版本 {version}）",
        )

    def _parse_fields(self, raw: str) -> tuple[dict[str, str], str]:
        text = raw.strip()
        if text.startswith("{"):
            try:
                payload = json.loads(text)
            except (TypeError, ValueError) as exc:
                return {}, f"JSON 解析失败：{exc}"
            if not isinstance(payload, dict):
                return {}, "修订字段必须是对象"
            return {str(key): _clean(value) for key, value in payload.items()}, ""
        fields: dict[str, str] = {}
        for chunk in text.replace("；", ";").replace("，", ";").split(";"):
            chunk = chunk.strip()
            if not chunk:
                continue
            if "=" not in chunk:
                return {}, f"片段 {chunk!r} 不是 key=value"
            key, value = chunk.split("=", 1)
            key, value = key.strip(), value.strip()
            if not key:
                return {}, "字段名为空"
            fields[key] = value
        if not fields:
            return {}, "没有可用字段"
        return fields, ""

    def _locate_line(
        self, source_id: str, file_path: str, line_number: int,
    ) -> tuple[SourceLogLine | None, tuple[str, str]]:
        if file_path:
            line = self._lines.get(f"{source_id}|{file_path}|{line_number}")
            if line is None:
                return None, (
                    RejectReason.LINE_NOT_FOUND.value,
                    f"来源 {source_id} 的 {file_path} 第 {line_number} 行不在失败行台账中",
                )
            return line, ("", "")

        candidates = [
            line for line in self._lines.values()
            if line.source_id == source_id and line.line_number == line_number
        ]
        if not candidates:
            return None, (
                RejectReason.LINE_NOT_FOUND.value,
                f"来源 {source_id} 第 {line_number} 行不在失败行台账中",
            )
        if len(candidates) > 1:
            return None, (
                RejectReason.AMBIGUOUS_SOURCE.value,
                f"来源 {source_id} 第 {line_number} 行命中多个文件，请在纠错单中写明文件路径",
            )
        return candidates[0], ("", "")

    # ------------------------------------------------------------------
    # 有效回修事件批次
    # ------------------------------------------------------------------

    def build_rework_batch(
        self, ticket_keys: Iterable[str] | None = None,
    ) -> ReworkBatch:
        """把已批准的纠错单解析成有效回修事件批次。

        只有 ``apply_parse_rules`` 能重新解析成功的单子才进入批次；解析仍失败的
        单子被拒绝，不会进入事件分类。
        """

        with self._lock:
            if ticket_keys is None:
                tickets = [
                    ticket for ticket in sorted(self._tickets.values(), key=lambda t: t.import_seq)
                    if ticket.status in (CorrectionStatus.APPROVED, CorrectionStatus.CLAIMED)
                ]
            else:
                tickets = [self._tickets[key] for key in ticket_keys if key in self._tickets]

            batch = ReworkBatch(batch_id=f"CRBT-{self._batch_seq:04d}")
            for ticket in tickets:
                entry, item = self._prepare_entry(ticket)
                batch.items.append(item)
                if entry is not None:
                    batch.entries.append(entry)
            return batch

    def _prepare_entry(
        self, ticket: ParseCorrectionTicket,
    ) -> tuple[LogEntry | None, ClassifyItemResult]:
        line = self._lines.get(ticket.target_key)
        if line is None:
            return None, ClassifyItemResult(
                ticket_id=ticket.ticket_id, version=ticket.version, ok=False,
                status=ticket.status.value, reason_code=RejectReason.LINE_NOT_FOUND.value,
                reason="纠错单对应的日志行已不在台账中",
            )
        # 领取与分类前再核对一次原行证据：物理位置相同但原文已更换时，
        # 这张单已经失去依据，不能再写回当前日志。
        if evidence_checksum(
            line.source_id, line.file_path, line.line_number, line.raw_line,
        ) != ticket.evidence_checksum:
            return None, ClassifyItemResult(
                ticket_id=ticket.ticket_id, version=ticket.version, ok=False,
                status=ticket.status.value,
                reason_code=RejectReason.EVIDENCE_MISMATCH.value,
                reason="当前日志的同位置原文已更换，纠错单的原行依据失效，拒绝回修",
            )
        if not line.is_failure:
            return None, ClassifyItemResult(
                ticket_id=ticket.ticket_id, version=ticket.version, ok=False,
                status=ticket.status.value,
                reason_code=RejectReason.LINE_ALREADY_SUCCESSFUL.value,
                reason="该行原本已解析成功，不进入回修批次",
            )

        if ticket.corrected_line:
            # 修订内容按来源解析规则重新解析；优先使用该来源自己的规则。
            entry = LogEntry(
                raw_line=ticket.corrected_line,
                line_number=ticket.line_number,
                source_id=ticket.source_id,
                parse_status=ParseResultStatus.OK,
            )
            chosen_rule, _ = self._select_rule(ticket.source_id, ticket.corrected_line)
            candidate_rules = [chosen_rule] if chosen_rule is not None else []
            preferred = [
                rule for rule in self.parse_rules
                if rule.source_id == ticket.source_id and rule not in candidate_rules
            ]
            remainder = [
                rule for rule in self.parse_rules
                if rule.source_id != ticket.source_id and rule not in candidate_rules
            ]
            chosen_fields: dict[str, str] | None = None
            chosen_rule_id = ""
            last_status: ParseResultStatus | None = None
            for rule in candidate_rules + preferred + remainder:
                parsed = self.ingest.apply_parse_rules(
                    [LogEntry(
                        raw_line=ticket.corrected_line,
                        line_number=ticket.line_number,
                        source_id=ticket.source_id,
                        parse_status=ParseResultStatus.OK,
                    )],
                    rule,
                )[0]
                last_status = parsed.parse_status
                if parsed.parse_status == ParseResultStatus.OK:
                    chosen_fields = dict(parsed.parsed_fields or {})
                    chosen_rule_id = rule.rule_id
                    entry = parsed
                    break
            if chosen_fields is None:
                ticket.status = CorrectionStatus.PARSE_FAILED
                ticket.note = f"修订内容无法解析（{last_status.value if last_status else '无规则'}）"
                return None, ClassifyItemResult(
                    ticket_id=ticket.ticket_id, version=ticket.version, ok=False,
                    status=ticket.status.value,
                    reason_code=RejectReason.CORRECTED_UNPARSEABLE.value,
                    reason=f"修订内容无法按解析规则得到有效字段：{ticket.note}",
                )
            entry.parsed_fields = chosen_fields
            entry.parse_status = ParseResultStatus.OK
            rule_id = chosen_rule_id
        else:
            # 字段补丁：在原行的失败字段基础上补齐，补丁只来自当前这一张单。
            rule = next(
                (item for item in self.parse_rules if item.source_id == ticket.source_id),
                None,
            )
            expected = list(rule.field_names) if rule is not None else []
            base = dict(line.parsed_fields or {})
            fields = {**base, **ticket.corrected_fields}
            missing = [
                name for name in expected
                if not str(fields.get(name) or "").strip()
            ]
            if missing:
                ticket.status = CorrectionStatus.PARSE_FAILED
                ticket.note = f"字段仍缺失：{'、'.join(missing)}"
                return None, ClassifyItemResult(
                    ticket_id=ticket.ticket_id, version=ticket.version, ok=False,
                    status=ticket.status.value,
                    reason_code=RejectReason.CORRECTED_UNPARSEABLE.value,
                    reason=f"修订字段仍无法组成有效字段：{ticket.note}",
                )
            entry = LogEntry(
                raw_line=line.raw_line,
                line_number=line.line_number,
                source_id=line.source_id,
                parsed_fields=fields,
                parse_status=ParseResultStatus.OK,
            )
            entry.checksum = entry.compute_checksum()
            rule_id = rule.rule_id if rule is not None else ""

        entry.correction = {
            "ticket_id": ticket.ticket_id,
            "ticket_key": ticket.key,
            "version": ticket.version,
            "source_id": ticket.source_id,
            "file_path": ticket.file_path,
            "line_number": ticket.line_number,
            "original_line": ticket.original_line,
            "original_checksum": ticket.evidence_checksum,
            "failure_reason": line.failure_reason,
            "failure_status": line.parse_status.value,
            "corrected_line": ticket.corrected_line,
            "corrected_fields": dict(ticket.corrected_fields),
            "chosen_rule": rule_id,
            "mode": ticket.mode,
            "revision": ticket.revision,
            "claim_batch": ticket.claim_batch,
            "other_revisions": sorted(
                other.version for other in self._tickets.values()
                if other.target_key == ticket.target_key and other.key != ticket.key
            ),
        }
        return entry, ClassifyItemResult(
            ticket_id=ticket.ticket_id, version=ticket.version, ok=True,
            status=ticket.status.value,
        )

    def _select_rule(self, source_id: str, raw_line: str) -> tuple[ParseRule | None, dict[str, str] | None]:
        ordered = [rule for rule in self.parse_rules if rule.source_id == source_id]
        ordered += [rule for rule in self.parse_rules if rule.source_id != source_id]
        for rule in ordered:
            status, fields = rule.parse(raw_line)
            if status == ParseResultStatus.OK:
                return rule, fields
        return None, None

    # ------------------------------------------------------------------
    # 领取与并发
    # ------------------------------------------------------------------

    def claim(
        self,
        ticket_id: str,
        *,
        version: str | None = None,
        strategy: str = "lock_at_claim",
        batch_id: str = "",
    ) -> ClaimResult:
        """领取一张纠错单。同一物理行在领取时被锁定。"""

        with self._lock:
            ticket = self._find_ticket(ticket_id, version)
            if ticket is None:
                return ClaimResult(False, f"纠错单 {ticket_id} 不存在")
            if ticket.status == CorrectionStatus.CLAIMED:
                return ClaimResult(True, f"纠错单 {ticket_id} 已被领取", ticket)
            if ticket.status in (
                CorrectionStatus.SETTLED, CorrectionStatus.CLASSIFIED,
                CorrectionStatus.SUPERSEDED, CorrectionStatus.CONFLICT,
                CorrectionStatus.REJECTED, CorrectionStatus.PARSE_FAILED,
            ):
                return ClaimResult(
                    False, f"纠错单 {ticket_id} 当前状态为 {ticket.status.value}，不能领取", ticket)

            lock_owner = self._target_locks.get(ticket.target_key)
            superseded: list[str] = []
            if lock_owner and lock_owner != ticket.key:
                previous = self._tickets.get(lock_owner)
                # 只有在"分类前"且旧单尚未写出分类记录时，才允许被最新版替代；
                # 旧单一旦已分类，它就是该原行的首次正式结果，后续修订只能单独核销。
                if (
                    strategy == "reclaim_before_classify"
                    and previous is not None
                    and previous.status == CorrectionStatus.CLAIMED
                ):
                    previous.status = CorrectionStatus.SUPERSEDED
                    previous.note = f"分类前重新领取，被 {ticket.key} 替代"
                    superseded.append(previous.key)
                    self._target_locks.pop(ticket.target_key, None)
                else:
                    ticket.status = CorrectionStatus.CONFLICT
                    ticket.note = f"目标行已被纠错单 {lock_owner} 领取"
                    if previous is not None and previous.status in (
                        CorrectionStatus.CLASSIFIED, CorrectionStatus.SETTLED,
                    ):
                        decided = "首次正式回修已确认一种事件类型"
                    else:
                        decided = "同一原行首次正式回修只确认一种类型"
                    return ClaimResult(
                        False,
                        f"目标行已被纠错单 {lock_owner} 领取，{decided}；"
                        "后续修订请在首次结果结清后单独核销",
                        ticket,
                        superseded,
                    )

            ticket.status = CorrectionStatus.CLAIMED
            ticket.claimed_at = self._clock()
            ticket.claim_batch = batch_id or ticket.claim_batch or f"CRBT-{self._batch_seq:04d}"
            self._target_locks[ticket.target_key] = ticket.key
            return ClaimResult(True, f"纠错单 {ticket_id} 已领取", ticket, superseded)

    def claim_batch(
        self,
        *,
        limit: int | None = None,
        strategy: str = "lock_at_claim",
        batch_id: str = "",
    ) -> list[ClaimResult]:
        """按目标行分组领取；每行只领取一张，其余留待冲突/替代判定。"""

        with self._lock:
            approved = sorted(
                (ticket for ticket in self._tickets.values()
                 if ticket.status == CorrectionStatus.APPROVED),
                key=lambda t: t.import_seq,
            )
            grouped: dict[str, list[ParseCorrectionTicket]] = {}
            order: list[str] = []
            for ticket in approved:
                if ticket.target_key not in grouped:
                    grouped[ticket.target_key] = []
                    order.append(ticket.target_key)
                grouped[ticket.target_key].append(ticket)

            results: list[ClaimResult] = []
            for target in order:
                if limit is not None and len(results) >= limit:
                    break
                candidates = sorted(grouped[target], key=lambda t: t.import_seq)
                if strategy == "reclaim_before_classify":
                    # 分类前重新领取：同目标行只保留最新一版生效。
                    chosen = candidates[-1]
                    results.append(self.claim(
                        chosen.ticket_id, version=chosen.version,
                        strategy="reclaim_before_classify", batch_id=batch_id))
                else:
                    # 领取时锁定：首张成功，其余同目标行的单子明确记为冲突。
                    for candidate in candidates:
                        if limit is not None and len(results) >= limit:
                            break
                        results.append(self.claim(
                            candidate.ticket_id, version=candidate.version,
                            strategy="lock_at_claim", batch_id=batch_id))
            return results

    def _find_ticket(self, ticket_id: str, version: str | None = None) -> ParseCorrectionTicket | None:
        if version is not None:
            return self._tickets.get(f"{ticket_id}@{version}")
        candidates = [
            ticket for ticket in self._tickets.values()
            if ticket.ticket_id == ticket_id
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda t: (t.import_seq, t.version))

    # ------------------------------------------------------------------
    # 分类与结清
    # ------------------------------------------------------------------

    def classify_claimed(
        self,
        *,
        strategy: str = "lock_at_claim",
        batch_id: str = "",
    ) -> ClassifyBatchResult:
        """对已领取的纠错单执行失败行重分类。

        只处理已领取且尚未产生分类日志的单子；同一物理行只会写出一条正式记录。
        """

        with self._lock:
            if strategy == "reclaim_before_classify":
                self._reclaim_latest()

            result = ClassifyBatchResult()
            claimed = sorted(
                (ticket for ticket in self._tickets.values()
                 if ticket.status == CorrectionStatus.CLAIMED),
                key=lambda t: t.import_seq,
            )
            if batch_id:
                for ticket in claimed:
                    ticket.claim_batch = batch_id

            for ticket in claimed:
                if ticket.key in self._journal:
                    item = ClassifyItemResult(
                        ticket_id=ticket.ticket_id, version=ticket.version, ok=True,
                        status=ticket.status.value, record_id=ticket.record_id,
                        event_type_id=ticket.event_type_id,
                        event_category=ticket.event_category, skipped=True,
                        reason="该纠错单已分类，不重复写出",
                    )
                    result.items.append(item)
                    continue
                item, record = self._classify_ticket(ticket)
                result.items.append(item)
                if record is not None:
                    result.records.append(record)
            return result

    def _reclaim_latest(self) -> None:
        """分类前重新领取：同目标行若已有更新版本获批，则替代旧领取。

        只替代"已领取、尚未分类"的旧单；已分类的单是首次正式结果，不能被覆盖。
        """

        for target, owner in list(self._target_locks.items()):
            current = self._tickets.get(owner)
            if current is None or current.status != CorrectionStatus.CLAIMED:
                continue
            candidates = [
                ticket for ticket in self._tickets.values()
                if ticket.target_key == target
                and ticket.status == CorrectionStatus.APPROVED
                and ticket.import_seq > current.import_seq
            ]
            if not candidates:
                continue
            latest = max(candidates, key=lambda t: t.import_seq)
            current.status = CorrectionStatus.SUPERSEDED
            current.note = f"分类前重新领取，被 {latest.key} 替代"
            self._target_locks.pop(target, None)
            latest.status = CorrectionStatus.CLAIMED
            latest.claimed_at = self._clock()
            latest.claim_batch = current.claim_batch or latest.claim_batch
            self._target_locks[target] = latest.key

    def _classify_ticket(
        self, ticket: ParseCorrectionTicket,
    ) -> tuple[ClassifyItemResult, EventRecord | None]:
        entry, item = self._prepare_entry(ticket)
        if entry is None:
            if item.reason_code in (
                RejectReason.EVIDENCE_MISMATCH.value,
                RejectReason.LINE_ALREADY_SUCCESSFUL.value,
                RejectReason.LINE_NOT_FOUND.value,
            ):
                # 依据已经失效：这张单不能再留"已领取"反复送分类。
                ticket.status = CorrectionStatus.CONFLICT
                ticket.note = item.reason
            return item, None

        # 分类入口只接收"有效回修事件批次"；一条单子也走同样的批次结构，
        # 保证 apply_parse_rules 得到的字段由 classify_events 消费。
        entry.correction["ticket_id"] = ticket.ticket_id
        entry.correction["ticket_key"] = ticket.key
        batch = ReworkBatch(batch_id=ticket.claim_batch or "CRBT-0001", entries=[entry])
        records = self.ingest.classify_rework_batch(batch)
        if not records:
            ticket.status = CorrectionStatus.PARSE_FAILED
            ticket.note = "分类入口没有消费该回修条目"
            return ClassifyItemResult(
                ticket_id=ticket.ticket_id, version=ticket.version, ok=False,
                status=ticket.status.value,
                reason_code=RejectReason.CORRECTED_UNPARSEABLE.value,
                reason="分类入口没有消费该回修条目",
            ), None

        record = records[0]
        record.record_id = f"EVEN-{ticket.ticket_id}-{ticket.version}"
        record.correction = dict(entry.correction)
        self._records[record.record_id] = record

        line = self._lines.get(ticket.target_key)
        if line is not None:
            line.resolved = True
            line.resolved_by_ticket = ticket.ticket_id
            line.resolved_ticket_key = ticket.key
            line.resolved_record_id = record.record_id

        ticket.status = CorrectionStatus.CLASSIFIED
        ticket.classified_at = self._clock()
        ticket.record_id = record.record_id
        ticket.event_type_id = record.event_type.type_id
        ticket.event_category = record.event_type.category_tag

        journal = ClassificationJournalEntry(
            ticket_key=ticket.key,
            ticket_id=ticket.ticket_id,
            version=ticket.version,
            target_key=ticket.target_key,
            record_id=record.record_id,
            record=record_payload(record),
            event_type_id=record.event_type.type_id,
            event_category=record.event_type.category_tag,
            original_line=ticket.original_line,
            original_checksum=ticket.evidence_checksum,
            failure_reason=line.failure_reason if line is not None else "",
            corrected_line=ticket.corrected_line,
            chosen_rule=str(entry.correction.get("chosen_rule", "")),
            revision=ticket.revision,
            claim_batch=ticket.claim_batch,
        )
        self._journal[ticket.key] = journal
        return ClassifyItemResult(
            ticket_id=ticket.ticket_id, version=ticket.version, ok=True,
            status=ticket.status.value, event_type_id=ticket.event_type_id,
            event_category=ticket.event_category, record_id=ticket.record_id,
            reason="已按该纠错单写出事件记录",
        ), record

    def acknowledge(self, ticket_id: str, *, version: str | None = None) -> dict[str, Any]:
        """导入侧收到分类侧结清回执：分类记录与结清状态一起确认。

        没有分类日志时拒绝结清，避免"只把纠错单改成结清却丢掉分类记录"。
        """

        with self._lock:
            ticket = self._find_ticket(ticket_id, version)
            if ticket is None:
                return {
                    "ok": False,
                    "reason_code": RejectReason.TICKET_NOT_FOUND.value,
                    "message": f"纠错单 {ticket_id} 不存在",
                }
            if ticket.status in (
                CorrectionStatus.CONFLICT, CorrectionStatus.SUPERSEDED,
                CorrectionStatus.REJECTED,
            ):
                return {
                    "ok": False,
                    "reason_code": RejectReason.TICKET_STATE_INVALID.value,
                    "message": (
                        f"纠错单 {ticket_id} 当前状态为 {ticket.status.value}，"
                        "没有作为正式回修生效，不能结清"
                    ),
                    "ticket": ticket.to_dict(),
                }
            journal = self._journal.get(ticket.key)
            if journal is None:
                return {
                    "ok": False,
                    "reason_code": RejectReason.CLASSIFICATION_RECORD_MISSING.value,
                    "message": "没有找到该纠错单的分类记录，不能只结清纠错单",
                    "ticket": ticket.to_dict(),
                }
            if ticket.status == CorrectionStatus.SETTLED:
                return {
                    "ok": True,
                    "already_settled": True,
                    "message": f"纠错单 {ticket_id} 已结清",
                    "record_id": journal.record_id,
                    "event_type_id": journal.event_type_id,
                    "event_category": journal.event_category,
                    "original_line": journal.original_line,
                    "record": journal.record,
                    "ticket": ticket.to_dict(),
                }

            ticket.status = CorrectionStatus.SETTLED
            ticket.settled_at = self._clock()
            journal.settled = True
            journal.settled_at = ticket.settled_at
            line = self._lines.get(ticket.target_key)
            if line is not None:
                line.resolved = True
                line.resolved_by_ticket = ticket.ticket_id
                line.resolved_ticket_key = ticket.key
                line.resolved_record_id = journal.record_id
            if self._target_locks.get(ticket.target_key) == ticket.key:
                self._target_locks.pop(ticket.target_key, None)
            return {
                "ok": True,
                "already_settled": False,
                "message": f"纠错单 {ticket_id} 已结清",
                "record_id": journal.record_id,
                "event_type_id": journal.event_type_id,
                "event_category": journal.event_category,
                "original_line": journal.original_line,
                "record": journal.record,
                "ticket": ticket.to_dict(),
            }

    # ------------------------------------------------------------------
    # 重启恢复
    # ------------------------------------------------------------------

    def recover(self) -> RecoveryReport:
        """模拟导入侧重启后的恢复。

        - 有分类日志、但还没收到结清回执的单子：从分类日志恢复结清与分类记录，
          不再作为失败行送出；
        - 已领取但没有分类日志的单子：进入待回修批次，等待分类；
        - 状态是"已结清"却没有分类日志的单子：记为不一致，原行不置为已解决，
          不会悄悄丢掉记录；
        - 同一分类记录只恢复一次，重复恢复不会多写事件。
        """

        with self._lock:
            report = RecoveryReport()
            restored_ids: set[str] = set()

            for journal in list(self._journal.values()):
                ticket = self._tickets.get(journal.ticket_key)
                if ticket is None:
                    report.inconsistent.append({
                        "ticket_key": journal.ticket_key,
                        "reason": "分类日志对应的纠错单不存在",
                    })
                    continue
                if journal.record_id in restored_ids:
                    report.duplicate_records_avoided += 1
                else:
                    restored = self._restore_record(journal)
                    if restored:
                        report.records_restored += 1
                    restored_ids.add(journal.record_id)

                line = self._lines.get(journal.target_key)
                if line is not None:
                    line.resolved = True
                    line.resolved_by_ticket = ticket.ticket_id
                    line.resolved_ticket_key = ticket.key
                    line.resolved_record_id = journal.record_id

                if ticket.status != CorrectionStatus.SETTLED:
                    ticket.status = CorrectionStatus.SETTLED
                    ticket.settled_at = ticket.settled_at or self._clock()
                    ticket.record_id = journal.record_id
                    ticket.event_type_id = journal.event_type_id
                    ticket.event_category = journal.event_category
                    report.settled_from_journal.append(ticket.key)
                journal.settled = True
                journal.settled_at = journal.settled_at or ticket.settled_at
                if self._target_locks.get(ticket.target_key) == ticket.key:
                    self._target_locks.pop(ticket.target_key, None)
                report.completed_not_reemitted += 1

            # 已结清/已分类但完全没有分类日志：绝不能当成已完成，也不能丢原行。
            for ticket in self._tickets.values():
                if ticket.key in self._journal:
                    continue
                if ticket.status in (CorrectionStatus.SETTLED, CorrectionStatus.CLASSIFIED):
                    report.inconsistent.append({
                        "ticket_key": ticket.key,
                        "reason": "纠错单状态已完成但缺少分类记录，原行保持未解决",
                    })
                    ticket.status = CorrectionStatus.CLASSIFIED
                    ticket.note = "恢复时缺少分类记录，需重新分类"
                    line = self._lines.get(ticket.target_key)
                    if line is not None:
                        line.resolved = False
                        line.resolved_record_id = ""
                    continue
                if ticket.status in (CorrectionStatus.CLAIMED, CorrectionStatus.APPROVED):
                    # 该单自己还没有分类记录：即使同一行已被首次回修补上，
                    # 后续修订也仍待核销，不会因为原行已解决就被吞掉。
                    report.pending_rework.append(ticket.key)
                elif ticket.status == CorrectionStatus.PARSE_FAILED:
                    # 修订内容本身不可解析，不属于可再送分类的有效批次。
                    continue

            report.failed_lines_remaining = self.pending_failure_lines()
            return report

    def _restore_record(self, journal: ClassificationJournalEntry) -> bool:
        if journal.record_id in self._records:
            return False
        record = _record_from_payload(journal.record)
        if record is None:
            return False
        self._records[record.record_id] = record
        self.ingest.store_events([record])
        return True

    # ------------------------------------------------------------------
    # 快照：模拟导入侧重启后的持久化边界
    # ------------------------------------------------------------------

    def snapshot(self, *, include_records: bool = True) -> dict[str, Any]:
        """导出可持久化状态。

        ``include_records=False`` 模拟"结清回执还没到、分类记录还没落进导入侧"
        的重启现场：恢复只能依赖分类日志重建记录。
        """

        with self._lock:
            return {
                "version": 1,
                "import_seq": self._import_seq,
                "batch_seq": self._batch_seq,
                "lines": [line.to_dict() for line in self._lines.values()],
                "tickets": [ticket.to_dict() for ticket in self._tickets.values()],
                "journal": [entry.to_dict() for entry in self._journal.values()],
                "target_locks": dict(self._target_locks),
                "records": (
                    [record_payload(record) for record in self._records.values()]
                    if include_records else []
                ),
            }

    @classmethod
    def from_snapshot(
        cls, payload: dict[str, Any], *,
        ingest: IngestPipeline | None = None,
        parse_rules: list[ParseRule] | None = None,
    ) -> "ParseCorrectionPipeline":
        pipeline = cls(ingest=ingest, parse_rules=parse_rules)
        pipeline.restore_snapshot(payload)
        return pipeline

    def restore_snapshot(self, payload: dict[str, Any]) -> None:
        with self._lock:
            self._import_seq = int(payload.get("import_seq", 0))
            self._batch_seq = int(payload.get("batch_seq", 0))

            self._lines.clear()
            self._source_files.clear()
            for raw in payload.get("lines", []):
                line = SourceLogLine(
                    source_id=raw["source_id"],
                    file_path=raw["file_path"],
                    line_number=int(raw["line_number"]),
                    raw_line=raw["raw_line"],
                    parse_status=ParseResultStatus(raw["parse_status"]),
                    failure_reason=raw.get("failure_reason", ""),
                    checksum=raw.get("checksum", ""),
                    resolved=bool(raw.get("resolved")),
                    resolved_by_ticket=raw.get("resolved_by_ticket", ""),
                    resolved_ticket_key=raw.get("resolved_ticket_key", ""),
                    resolved_record_id=raw.get("resolved_record_id", ""),
                )
                self._lines[line.target_key] = line
                self._source_files.setdefault(line.source_id, set()).add(line.file_path)

            self._tickets.clear()
            self._version_index.clear()
            for raw in payload.get("tickets", []):
                ticket = ParseCorrectionTicket(
                    ticket_id=raw["ticket_id"],
                    source_id=raw["source_id"],
                    file_path=raw.get("file_path", ""),
                    line_number=int(raw["line_number"]),
                    original_line=raw.get("original_line", ""),
                    corrected_line=raw.get("corrected_line", ""),
                    corrected_fields=dict(raw.get("corrected_fields") or {}),
                    version=str(raw.get("version", "1")),
                    approved_by=raw.get("approved_by", ""),
                    status=CorrectionStatus(raw.get("status", CorrectionStatus.APPROVED.value)),
                    evidence_checksum=raw.get("evidence_checksum", ""),
                    target_key=raw.get("target_key", ""),
                    claim_batch=raw.get("claim_batch", ""),
                    claimed_at=raw.get("claimed_at", ""),
                    classified_at=raw.get("classified_at", ""),
                    settled_at=raw.get("settled_at", ""),
                    record_id=raw.get("record_id", ""),
                    event_type_id=raw.get("event_type_id", ""),
                    event_category=raw.get("event_category", ""),
                    revision=int(raw.get("revision", 1)),
                    import_seq=int(raw.get("import_seq", 0)),
                    note=raw.get("note", ""),
                )
                self._tickets[ticket.key] = ticket
                self._version_index[(ticket.ticket_id, ticket.version)] = ticket

            self._journal.clear()
            for raw in payload.get("journal", []):
                entry = ClassificationJournalEntry(
                    ticket_key=raw["ticket_key"],
                    ticket_id=raw["ticket_id"],
                    version=raw["version"],
                    target_key=raw["target_key"],
                    record_id=raw["record_id"],
                    record=dict(raw.get("record") or {}),
                    event_type_id=raw.get("event_type_id", ""),
                    event_category=raw.get("event_category", ""),
                    original_line=raw.get("original_line", ""),
                    original_checksum=raw.get("original_checksum", ""),
                    failure_reason=raw.get("failure_reason", ""),
                    corrected_line=raw.get("corrected_line", ""),
                    chosen_rule=raw.get("chosen_rule", ""),
                    revision=int(raw.get("revision", 1)),
                    claim_batch=raw.get("claim_batch", ""),
                    classified_at=raw.get("classified_at", ""),
                    settled=bool(raw.get("settled")),
                    settled_at=raw.get("settled_at", ""),
                )
                self._journal[entry.ticket_key] = entry

            self._target_locks = dict(payload.get("target_locks") or {})

            self._records.clear()
            for raw in payload.get("records", []):
                record = _record_from_payload(raw)
                if record is not None:
                    self._records[record.record_id] = record
                    self.ingest.store_events([record])

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------

    def tickets(self) -> list[dict[str, Any]]:
        with self._lock:
            return [
                ticket.to_dict()
                for ticket in sorted(self._tickets.values(), key=lambda t: t.import_seq)
            ]

    def journal(self) -> list[dict[str, Any]]:
        with self._lock:
            return [
                entry.to_dict()
                for entry in sorted(self._journal.values(), key=lambda e: e.classified_at)
            ]

    def event_records(self) -> list[dict[str, Any]]:
        with self._lock:
            return [record_payload(record) for record in self._records.values()]

    def get_ticket(self, ticket_id: str, version: str | None = None) -> dict[str, Any] | None:
        with self._lock:
            ticket = self._find_ticket(ticket_id, version)
            return ticket.to_dict() if ticket else None

    def state(self) -> dict[str, Any]:
        with self._lock:
            return {
                "lines": self.lines(),
                "pending_failure_lines": self.pending_failure_lines(),
                "tickets": self.tickets(),
                "journal": self.journal(),
                "records": self.event_records(),
                "target_locks": dict(self._target_locks),
            }


def EventTypeDefFactory(payload: dict[str, Any]) -> EventTypeDef:
    """从分类日志还原事件类型定义。"""

    return EventTypeDef(
        type_id=str(payload.get("type_id", "")),
        type_name=str(payload.get("type_name", "")),
        category_tag=str(payload.get("category_tag", "access")),
        severity=str(payload.get("severity", "INFO")),
    )


def _record_from_payload(payload: dict[str, Any]) -> EventRecord | None:
    """从分类日志还原一条事件记录；字段不全时返回 None。"""

    try:
        return EventRecord(
            record_id=str(payload["record_id"]),
            event_type=EventTypeDefFactory(payload["event_type"]),
            original_content=str(payload["original_content"]),
            parsed_fields=dict(payload.get("parsed_fields") or {}),
            masked_fields=dict(payload.get("masked_fields") or {}),
            enriched_fields=dict(payload.get("enriched_fields") or {}),
            event_time=datetime.fromisoformat(payload["event_time"]),
            source_host=payload.get("source_host", "unknown"),
            store_partition=payload.get("store_partition", ""),
            index_state=payload.get("index_state", "已写入"),
            correction=dict(payload.get("correction") or {}),
        )
    except (KeyError, TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# 工厂与别名
# ---------------------------------------------------------------------------

CorrectionPipeline = ParseCorrectionPipeline
CorrectionImportPipeline = ParseCorrectionPipeline


def make_parse_correction_pipeline(
    *, ingest: IngestPipeline | None = None,
    parse_rules: list[ParseRule] | None = None,
) -> ParseCorrectionPipeline:
    return ParseCorrectionPipeline(ingest=ingest, parse_rules=parse_rules)


make_correction_pipeline = make_parse_correction_pipeline


__all__ = [
    "ParseCorrectionPipeline",
    "CorrectionPipeline",
    "CorrectionImportPipeline",
    "make_parse_correction_pipeline",
    "make_correction_pipeline",
    "CorrectionStatus",
    "RejectReason",
    "ParseCorrectionTicket",
    "SourceLogLine",
    "ImportBatchResult",
    "ImportRowResult",
    "ClaimResult",
    "ClassifyBatchResult",
    "ClassifyItemResult",
    "ReworkBatch",
    "ClassificationJournalEntry",
    "RecoveryReport",
    "evidence_checksum",
    "record_payload",
]
