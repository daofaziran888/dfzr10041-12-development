"""解析纠错单导入与失败行回修接口。

路由前缀放在 ``/api/correction``，必须在通用资源路由之前注册，否则
``/api/{module}`` 会抢先匹配。

接口一览
--------
- ``POST /api/correction/sources/ingest``   登记来源文件行（成功行+失败行）
- ``GET  /api/correction/lines``            查看失败行台账
- ``POST /api/correction/import``           CSV 纠错单导入（JSON/纯文本/文件上传）
- ``GET  /api/correction/template.csv``     下载导入模板
- ``GET  /api/correction/tickets``          查看纠错单
- ``POST /api/correction/tickets/{id}/claim``     领取（锁定原行）
- ``POST /api/correction/classify``         对已领取单子重做失败行分类
- ``POST /api/correction/tickets/{id}/ack`` 结清（需分类记录）
- ``POST /api/correction/recover``          模拟重启恢复
- ``GET  /api/correction/records``          查看回修写出的事件记录
"""
from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import PlainTextResponse

from app.core.parse_correction import ParseCorrectionPipeline
from app.core.pipeline import PRESET_LOG_SOURCES, PRESET_PARSE_RULES, make_pipeline
from app.store import store

router = APIRouter(prefix="/api/correction", tags=["解析纠错与失败行回修"])

_ingest_pipeline = make_pipeline()
service = ParseCorrectionPipeline(
    ingest=_ingest_pipeline,
    parse_rules=list(PRESET_PARSE_RULES),
)

CSV_TEMPLATE = (
    "单号,来源,文件路径,行号,原行内容,修订内容,修订字段,版本,批准人\n"
    "CRT-0001,LOG_-0002,/var/log/app/backend.log,17,原始失败行内容,"
    "修订后的完整日志行,,1,值班员\n"
)


def _source(source_id: str):
    for source in PRESET_LOG_SOURCES.values():
        if source.source_id == source_id:
            return source
    return None


def _rule_for(source_id: str, rule_id: str | None = None):
    for rule in PRESET_PARSE_RULES:
        if rule.source_id == source_id and (rule_id is None or rule.rule_id == rule_id):
            return rule
    return None


@router.post("/sources/ingest")
async def ingest_source(request: Request) -> dict[str, Any]:
    """登记一个来源文件的原始行，供纠错单按来源+行号定位。"""

    payload = await request.json()
    source_id = str(payload.get("source_id", "")).strip()
    file_path = str(payload.get("file_path", "")).strip()
    raw_lines = payload.get("raw_lines") or []
    source = _source(source_id)
    if source is None:
        raise HTTPException(status_code=404, detail=f"未登记的日志来源：{source_id}")
    if not file_path:
        raise HTTPException(status_code=400, detail="缺少必填字段：file_path")
    if not isinstance(raw_lines, list) or not raw_lines:
        raise HTTPException(status_code=400, detail="缺少必填字段：raw_lines")
    rule = _rule_for(source_id, payload.get("rule_id"))
    if rule is None:
        raise HTTPException(status_code=404, detail=f"来源 {source_id} 没有可用解析规则")

    entries = service.register_source_batch(
        service.ingest, source, file_path, [str(line) for line in raw_lines], rule,
        start_line=int(payload.get("start_line") or 1))
    failures = [
        entry for entry in entries
        if entry.parse_status.value != "ok"
    ]
    return {
        "ok": True,
        "message": f"已登记 {len(entries)} 行，其中失败行 {len(failures)} 行",
        "count": len(entries),
        "failed": len(failures),
        "lines": service.lines(),
    }


@router.get("/lines")
def list_lines() -> dict[str, Any]:
    return {
        "items": service.lines(),
        "pending": service.pending_failure_lines(),
        "total": len(service.lines()),
    }


@router.get("/template.csv", response_class=PlainTextResponse)
def download_template() -> PlainTextResponse:
    return PlainTextResponse(
        CSV_TEMPLATE,
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="parse_correction_template.csv"'},
    )


async def _read_csv(request: Request) -> tuple[str, str]:
    """支持三种导入形态：JSON、纯文本 CSV、multipart 文件上传。"""

    content_type = request.headers.get("content-type", "")
    imported_by = ""
    if "multipart/form-data" in content_type:
        form = await request.form()
        imported_by = str(form.get("imported_by") or "")
        upload = form.get("file")
        if upload is None:
            raise HTTPException(status_code=400, detail="缺少必填字段：file（CSV 文件）")
        data = await upload.read()  # type: ignore[union-attr]
        return _decode(data), imported_by
    raw = await request.body()
    if "application/json" in content_type:
        payload = json.loads(raw or b"{}")
        imported_by = str(payload.get("imported_by") or "")
        text = payload.get("csv") or payload.get("content") or ""
        if not text:
            raise HTTPException(status_code=400, detail="缺少必填字段：csv")
        return str(text), imported_by
    return _decode(raw), imported_by


def _decode(raw: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-8", "gb18030"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    raise HTTPException(status_code=400, detail="CSV 文件编码无法识别，请使用 UTF-8")


@router.post("/import")
async def import_csv(request: Request) -> dict[str, Any]:
    """导入 CSV 解析纠错单；逐行给出写入/拒绝原因，单行被拒不整批回滚。"""

    text, imported_by = await _read_csv(request)
    result = service.import_csv(text, imported_by=imported_by)
    payload = result.to_dict()
    payload["ok"] = True
    payload["message"] = (
        f"导入完成：写入 {result.written} 条，重复 {result.duplicates} 条，"
        f"拒绝 {result.rejected} 条"
    )
    return payload


@router.get("/tickets")
def list_tickets() -> dict[str, Any]:
    items = service.tickets()
    return {"items": items, "total": len(items)}


@router.post("/tickets/{ticket_id}/claim")
async def claim_ticket(ticket_id: str, request: Request) -> dict[str, Any]:
    payload = await _maybe_json(request)
    strategy = str(payload.get("strategy") or "lock_at_claim")
    if strategy not in ("lock_at_claim", "reclaim_before_classify"):
        raise HTTPException(
            status_code=400,
            detail="strategy 只能是 lock_at_claim 或 reclaim_before_classify",
        )
    result = service.claim(
        ticket_id,
        version=payload.get("version"),
        strategy=strategy,
        batch_id=str(payload.get("batch_id") or ""),
    )
    payload_out = result.to_dict()
    if not result.ok and result.ticket is not None:
        # 冲突也是业务结论，返回 200 让调用方读取状态码和原因。
        payload_out["ok"] = False
    return payload_out


@router.post("/claim")
async def claim_all(request: Request) -> dict[str, Any]:
    payload = await _maybe_json(request)
    strategy = str(payload.get("strategy") or "lock_at_claim")
    if strategy not in ("lock_at_claim", "reclaim_before_classify"):
        raise HTTPException(
            status_code=400,
            detail="strategy 只能是 lock_at_claim 或 reclaim_before_classify",
        )
    results = service.claim_batch(
        limit=payload.get("limit"),
        strategy=strategy,
        batch_id=str(payload.get("batch_id") or ""),
    )
    return {
        "ok": any(item.ok for item in results),
        "items": [item.to_dict() for item in results],
    }


@router.post("/classify")
async def classify(request: Request) -> dict[str, Any]:
    payload = await _maybe_json(request)
    strategy = str(payload.get("strategy") or "lock_at_claim")
    if strategy not in ("lock_at_claim", "reclaim_before_classify"):
        raise HTTPException(
            status_code=400,
            detail="strategy 只能是 lock_at_claim 或 reclaim_before_classify",
        )
    outcome = service.classify_claimed(
        strategy=strategy, batch_id=str(payload.get("batch_id") or ""))
    for record in outcome.records:
        _project_to_event_store(record)
    payload_out = outcome.to_dict()
    payload_out["ok"] = True
    payload_out["message"] = f"已按有效回修批次写出 {outcome.classified} 条事件记录"
    return payload_out


@router.post("/tickets/{ticket_id}/ack")
async def acknowledge(ticket_id: str, request: Request) -> dict[str, Any]:
    payload = await _maybe_json(request)
    result = service.acknowledge(ticket_id, version=payload.get("version"))
    if not result.get("ok"):
        # 没有分类记录时拒绝结清：这是业务拒绝，不是 404。
        return result
    return result


@router.post("/recover")
def recover() -> dict[str, Any]:
    report = service.recover()
    payload = report.to_dict()
    payload["ok"] = True
    payload["message"] = (
        f"恢复完成：结清 {len(report.settled_from_journal)} 条，"
        f"待回修 {len(report.pending_rework)} 条，"
        f"不一致 {len(report.inconsistent)} 条"
    )
    return payload


@router.get("/records")
def list_records() -> dict[str, Any]:
    items = service.event_records()
    return {"items": items, "total": len(items)}


@router.get("/state")
def state() -> dict[str, Any]:
    return service.state()


async def _maybe_json(request: Request) -> dict[str, Any]:
    raw = await request.body()
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except (TypeError, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _project_to_event_store(record: Any) -> None:
    """把回修写出的记录同步到 event_store 模块，便于工作台直接看到。"""

    rows = store.rows("event_store")
    record_id = record.record_id
    if any(row.get("记录编号") == record_id for row in rows):
        return
    rows.append({
        "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
        "status": "已写入",
        "pending": True,
        "abnormal": False,
        "记录编号": record_id,
        "事件类型": record.event_type.type_name,
        "原始内容": record.correction.get("original_line") or record.original_content,
        "解析字段": json.dumps(record.parsed_fields, ensure_ascii=False),
        "发生时间": record.event_time.isoformat(timespec="seconds"),
        "来源主机": record.source_host,
        "存储分区": record.store_partition,
        "索引状态": record.index_state,
    })
