# 拾音工坊 · 日志采集与事件流处理平台

面向多租户应用日志的结构化采集、事件流解析、格式标准化、敏感脱敏、采样归并、异常告警与归档回溯的日志采集与事件流处理平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/routers/correction.py  解析纠错单导入与失败行回修接口
│   ├── app/core/parse_correction.py  CSV 纠错单导入、领取、分类、结清与恢复
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 失败日志逐行回修

采集侧解析失败的行（格式不匹配、字段缺失）不会直接丢弃，而是进入失败行台账，
由 CSV 解析纠错单逐行回修：

```bash
# 1. 登记来源文件（第 17 行缺 level 失败，第 18 行原本成功）
curl -X POST http://127.0.0.1:8000/api/correction/sources/ingest \
  -H 'Content-Type: application/json' \
  -d '{"source_id":"LOG_-0004","file_path":"/var/log/app/legacy-batch.log","start_line":17,"raw_lines":["...第17行...","...第18行..."]}'

# 2. 导入 CSV 纠错单（text/csv、JSON 或文件上传均可）
curl -X POST http://127.0.0.1:8000/api/correction/import \
  -H 'Content-Type: text/csv' --data-binary @corrections.csv

# 3. 领取（锁定原行）→ 分类 → 结清
curl -X POST http://127.0.0.1:8000/api/correction/tickets/CRT-0001/claim \
  -H 'Content-Type: application/json' -d '{"version":"1","strategy":"lock_at_claim"}'
curl -X POST http://127.0.0.1:8000/api/correction/classify \
  -H 'Content-Type: application/json' -d '{"strategy":"lock_at_claim"}'
curl -X POST http://127.0.0.1:8000/api/correction/tickets/CRT-0001/ack \
  -H 'Content-Type: application/json' -d '{"version":"1"}'
```

CSV 列：`单号,来源,文件路径,行号,原行内容,修订内容,修订字段,版本,批准人`
（`修订内容` 和 `修订字段` 二选一；`修订字段` 支持 `k=v;k2=v2` 或 JSON）。
模板可从 `GET /api/correction/template.csv` 下载。

关键约束：

- 纠错单按 `来源 + 文件路径 + 行号` 定位，必须携带与台账一致的原行内容；
  物理位置相同但原文已更换时单独拒绝（`original_line_mismatch`），整批其他有效单照常写入。
- 缺必填字段、来源对不上、行号不存在、目标行原本已成功都会逐行给出拒绝原因。
- 事件分类只读取有效回修事件批次：修订内容经 `apply_parse_rules` 得到有效字段后
  才交给 `classify_events`，已成功的行不会再次发出。
- 领取时锁定同一原行；两版有效纠错冲突时 `lock_at_claim` 保首次、`reclaim_before_classify`
  以最新版为准，分类记录只会来自其中一份单，不夹用两份修订字段。
- 同一版本并发/重传导入只写入一次；结清必须以分类日志为准，不能只改单状态。
- 重启恢复从分类日志重建结清与记录，已完成的回修不会重新作为失败行送出，
  后续修订使用新版本号，与首次正式结果分开核销。

前端入口：`http://127.0.0.1:5173/correction`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 日志源 | `log_source` | 日志来源 | 源编号、源名称、应用标识 |
| 解析规则 | `parser_rule` | 解析方案 | 方案编号、日志来源、正则表达式 |
| 脱敏规则 | `mask_rule` | 脱敏配置 | 规则编号、目标字段、脱敏方式 |
| 事件分类 | `event_type` | 事件类型 | 类型编号、类型名称、分类标签 |
| 采集作业 | `ingest_job` | 采集任务 | 作业编号、日志来源、采集模式 |
| 批次检查点 | `batch_checkpoint` | 检查点记录 | 记录编号、采集作业、文件路径 |
| 事件存储 | `event_store` | 事件记录 | 记录编号、事件类型、原始内容 |
| 异常告警 | `anomaly_rule` | 告警规则 | 规则编号、事件类型、检测条件 |
| 采样归并 | `sample_job` | 采样计划 | 计划编号、事件类型、采样比例 |
| 字段增强 | `field_enrich` | 增强规则 | 规则编号、目标字段、增强来源 |
| 归档策略 | `archive_policy` | 归档方案 | 方案编号、事件类型、保留天数 |
| 转发路由 | `forward_rule` | 转发规则 | 规则编号、事件类型、目标地址 |
| Schema版本 | `schema_version` | 版本记录 | 版本编号、日志来源、字段变更 |
| 去重过滤 | `dedup_filter` | 去重规则 | 规则编号、事件类型、去重字段 |
| 采集指标 | `ingest_metric` | 指标记录 | 记录编号、日志来源、采集速率 |
| 事件重放 | `replay_record` | 重放任务 | 任务编号、来源事件、重放范围 |
| 质量报告 | `quality_report` | 质量评估 | 报告编号、日志来源、完整率 |
| 标签分类 | `tag_taxonomy` | 标签定义 | 标签编号、标签名称、父级标签 |
| 打包导出 | `export_bundle` | 导出包 | 包编号、事件类型、时间范围 |
| 访问审计 | `audit_event` | 审计记录 | 记录编号、操作模块、操作人 |
| 租户隔离 | `tenant_isolation` | 租户配置 | 租户编号、租户名称、日志来源 |
| 格式适配 | `format_adapt` | 适配器配置 | 适配器编号、输入格式、输出格式 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

领域：数据工程
子方向：数据采集、爬虫与RPA
