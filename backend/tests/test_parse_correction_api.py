"""测试：解析纠错单 HTTP 接口（真实 uvicorn 进程 + 标准库请求）。

覆盖：
- 来源文件登记与失败行台账；
- CSV 纠错单导入（text/csv 与 JSON 两种提交方式）；
- 拒绝原因（错来源 / 缺必填字段 / 原文不一致 / 原本成功行）；
- 领取锁定 vs 分类前重新领取；
- 分类写出的事件类型、记录可追溯字段；
- 结清需要分类记录；
- 重传后记录数不变。

运行：``python tests/test_parse_correction_api.py``（需要本机空闲端口）。
"""
from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

FAIL17 = (
    "2026-11-15 14:30:17.000 [http-nio-8080-exec-17] com.example.AuthController"
    " - 登录尝试 user=admin"
)
SUCCESS18 = (
    "2026-11-15 14:30:18.000 [http-nio-8080-exec-18] INFO  com.example.AuthController"
    " - 健康检查通过"
)
ERROR_REVISION = (
    "2026-11-15 14:30:17.000 [http-nio-8080-exec-17] ERROR com.example.AuthController"
    " - 登录失败 user=admin"
)
LOGIN_REVISION = (
    "2026-11-15 14:30:17.000 [http-nio-8080-exec-17] INFO  com.example.AuthController"
    " - LOGIN user=admin"
)
SOURCE = "LOG_-0004"
FILE = "/var/log/app/legacy-batch.log"


def _cell(value: str) -> str:
    if any(char in value for char in ',"\n'):
        return '"' + value.replace('"', '""') + '"'
    return value


def csv_text(*rows: list[str]) -> str:
    header = "单号,来源,文件路径,行号,原行内容,修订内容,版本,批准人"
    return "\n".join([header, *(",".join(_cell(cell) for cell in row) for row in rows)]) + "\n"


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class Client:
    def __init__(self, base: str):
        self.base = base.rstrip("/")

    def _call(self, method: str, path: str, *, data: bytes | None = None,
              content_type: str = "application/json") -> tuple[int, dict]:
        request = urllib.request.Request(
            self.base + path, data=data, method=method,
            headers={"Content-Type": content_type} if data is not None else {})
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                return response.status, json.loads(response.read())
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read() or b"{}")

    def get(self, path: str) -> tuple[int, dict]:
        return self._call("GET", path)

    def post(self, path: str, payload: dict | None = None) -> tuple[int, dict]:
        return self._call("POST", path, data=json.dumps(payload or {}).encode())

    def post_csv(self, path: str, text: str) -> tuple[int, dict]:
        return self._call("POST", path, data=text.encode("utf-8"), content_type="text/csv")


def start_server() -> tuple[subprocess.Popen, Client]:
    port = free_port()
    process = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app",
         "--host", "127.0.0.1", "--port", str(port), "--log-level", "error"],
        cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )
    client = Client(f"http://127.0.0.1:{port}")
    for _ in range(100):
        try:
            status, _ = client.get("/api/health")
            if status == 200:
                return process, client
        except (urllib.error.URLError, ConnectionError):
            pass
        time.sleep(0.1)
    process.terminate()
    raise RuntimeError("后端服务未能启动")


class TestCorrectionApi:
    def __init__(self, client: Client):
        self.client = client

    def ingest(self) -> dict:
        status, payload = self.client.post("/api/correction/sources/ingest", {
            "source_id": SOURCE,
            "file_path": FILE,
            "start_line": 17,
            "raw_lines": [FAIL17, SUCCESS18],
        })
        assert status == 200, payload
        return payload

    def test_01_ingest_registers_failure_and_success(self):
        payload = self.ingest()
        assert payload["count"] == 2
        assert payload["failed"] == 1
        lines = {line["line_number"]: line for line in payload["lines"]}
        assert lines[17]["parse_status"] == "field_missing"
        assert lines[17]["failure_reason"] == "字段缺失：level"
        assert lines[18]["parse_status"] == "ok"
        assert lines[17]["checksum"] != lines[18]["checksum"]

    def test_02_template_download(self):
        request = urllib.request.Request(self.client.base + "/api/correction/template.csv")
        with urllib.request.urlopen(request, timeout=10) as response:
            body = response.read().decode("utf-8")
        assert response.status == 200
        assert body.startswith("单号,来源,文件路径,行号,原行内容")

    def test_03_import_text_csv(self):
        text = csv_text(
            ["CRT-BAD-SOURCE", "LOG_-9999", FILE, "17", FAIL17, ERROR_REVISION, "1", "值班员"],
            ["CRT-BAD-EVIDENCE", SOURCE, FILE, "17", "别次故障的原文", ERROR_REVISION, "1", "值班员"],
            ["CRT-BAD-SUCCESS", SOURCE, FILE, "18", SUCCESS18, LOGIN_REVISION, "1", "值班员"],
            ["CRT-ERR", SOURCE, FILE, "17", FAIL17, ERROR_REVISION, "1", "值班员"],
        )
        status, payload = self.client.post_csv("/api/correction/import", text)
        assert status == 200
        assert payload["written"] == 1
        assert payload["rejected"] == 3
        reasons = {row["ticket_id"]: row["reason_code"] for row in payload["rows"]}
        assert reasons["CRT-BAD-SOURCE"] == "source_not_found"
        assert reasons["CRT-BAD-EVIDENCE"] == "original_line_mismatch"
        assert reasons["CRT-BAD-SUCCESS"] == "line_already_successful"

    def test_04_import_json_and_missing_field(self):
        missing_field = (
            "单号,来源,文件路径,行号,原行内容,修订内容,版本,批准人\n"
            f"CRT-NO-ORIGIN,{SOURCE},{FILE},17,,{ERROR_REVISION},1,值班员\n"
        )
        status, payload = self.client.post(
            "/api/correction/import", {"csv": missing_field, "imported_by": "值班员"})
        assert status == 200
        assert payload["rejected"] == 1
        assert payload["rows"][0]["reason_code"] == "missing_required_field"
        assert "原行内容" in payload["rows"][0]["reason"]

    def test_05_import_second_version(self):
        status, payload = self.client.post_csv(
            "/api/correction/import",
            csv_text(["CRT-AUTH", SOURCE, FILE, "17", FAIL17, LOGIN_REVISION, "2", "值班员"]))
        assert status == 200
        assert payload["written"] == 1

    def test_06_lock_at_claim_keeps_first(self):
        status, first = self.client.post(
            "/api/correction/tickets/CRT-ERR/claim", {"version": "1"})
        assert status == 200 and first["ok"] is True
        assert first["ticket"]["status"] == "已领取"

        status, second = self.client.post(
            "/api/correction/tickets/CRT-AUTH/claim", {"version": "2"})
        assert status == 200
        assert second["ok"] is False
        assert second["ticket"]["status"] == "冲突"

    def test_07_classify_writes_error_record(self):
        status, payload = self.client.post("/api/correction/classify", {})
        assert status == 200
        assert payload["classified"] == 1
        record = payload["records"][0]
        assert record["record_id"] == "EVEN-CRT-ERR-1"
        assert record["event_type"]["category_tag"] == "error"
        assert record["parsed_fields"]["level"] == "ERROR"
        correction = record["correction"]
        assert correction["ticket_key"] == "CRT-ERR@1"
        assert correction["original_line"] == FAIL17
        assert correction["failure_reason"] == "字段缺失：level"
        assert correction["chosen_rule"] == "PARS-0004"

    def test_08_ack_requires_record_and_is_idempotent(self):
        status, payload = self.client.post("/api/correction/tickets/CRT-ERR/ack", {"version": "1"})
        assert status == 200 and payload["ok"] is True
        assert payload["event_category"] == "error"
        assert payload["record"]["correction"]["original_line"] == FAIL17

        status, again = self.client.post(
            "/api/correction/tickets/CRT-ERR/ack", {"version": "1"})
        assert again["ok"] is True and again["already_settled"] is True
        assert again["record_id"] == payload["record_id"]

        status, conflicted = self.client.post(
            "/api/correction/tickets/CRT-AUTH/ack", {"version": "2"})
        assert status == 200
        assert conflicted["ok"] is False
        assert conflicted["reason_code"] == "ticket_state_invalid"

    def test_09_retransmit_does_not_grow_records(self):
        text = csv_text(
            ["CRT-ERR", SOURCE, FILE, "17", FAIL17, ERROR_REVISION, "1", "值班员"],
            ["CRT-AUTH", SOURCE, FILE, "17", FAIL17, LOGIN_REVISION, "2", "值班员"],
        )
        _, before = self.client.get("/api/correction/records")
        status, payload = self.client.post_csv("/api/correction/import", text)
        assert status == 200
        assert payload["written"] == 0
        assert payload["duplicates"] == 2
        _, after = self.client.get("/api/correction/records")
        assert after["total"] == before["total"] == 1
        _, tickets = self.client.get("/api/correction/tickets")
        assert tickets["total"] == 2

    def test_10_recover_after_restart_is_clean(self):
        status, payload = self.client.post("/api/correction/recover", {})
        assert status == 200
        assert payload["inconsistent"] == []
        assert payload["pending_rework"] == []
        assert payload["failed_lines_remaining"] == []


def main() -> int:
    process, client = start_server()
    try:
        suite = TestCorrectionApi(client)
        passed = 0
        failed = 0
        for name in sorted(dir(suite)):
            if not name.startswith("test_"):
                continue
            try:
                getattr(suite, name)()
                passed += 1
                print(f"  PASS  test_parse_correction_api: {name}")
            except Exception:
                failed += 1
                print(f"  FAIL  test_parse_correction_api: {name}")
                import traceback
                traceback.print_exc()
        print(f"\ntest_parse_correction_api: {passed} passed, {failed} failed, {passed + failed} total")
        return 1 if failed else 0
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()


if __name__ == "__main__":
    sys.exit(main())
