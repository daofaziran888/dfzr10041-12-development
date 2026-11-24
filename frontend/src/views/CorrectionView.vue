<template>
  <section class="page" data-testid="correction-page">
    <header class="page-head">
      <div>
        <h2>失败行回修</h2>
        <p class="page-desc">
          导入 CSV 解析纠错单，按来源和源文件行号定位失败行；事件分类只读取有效回修事件批次，
          已成功的行不再发出，原行内容对不上的纠错单会被单独拒绝。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" data-testid="correction-recover" type="button" @click="recover">
          重启恢复
        </button>
        <button class="btn" data-testid="correction-refresh" type="button" @click="reload">刷新</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="card in cards" :key="card.label" class="stat-card">
        <span class="stat-label">{{ card.label }}</span>
        <strong class="stat-value">{{ card.value }}</strong>
      </article>
    </div>

    <h3 class="section-title">1. 登记来源文件</h3>
    <form class="filter-bar" @submit.prevent="registerSource">
      <label class="filter-item">
        <span>来源编号</span>
        <input v-model="sourceForm.sourceId" data-testid="correction-source-id" placeholder="LOG_-0004" />
      </label>
      <label class="filter-item">
        <span>文件路径</span>
        <input v-model="sourceForm.filePath" data-testid="correction-file-path" placeholder="/var/log/app/legacy-batch.log" />
      </label>
      <label class="filter-item">
        <span>起始行号</span>
        <input v-model.number="sourceForm.startLine" data-testid="correction-start-line" type="number" min="1" />
      </label>
      <button class="btn primary" data-testid="correction-register" type="submit">登记行</button>
    </form>
    <textarea
      v-model="sourceForm.rawLines"
      class="correction-textarea"
      data-testid="correction-raw-lines"
      placeholder="每行一条原始日志；第 17 行失败、第 18 行成功可以这样填：&#10;2026-11-15 14:30:17.000 [http-nio-8080-exec-17] com.example.AuthController - 登录尝试 user=admin&#10;2026-11-15 14:30:18.000 [http-nio-8080-exec-18] INFO  com.example.AuthController - 健康检查通过"
    ></textarea>

    <h3 class="section-title">2. 失败行台账（第 17 行等待回修，已成功的行不再发出）</h3>
    <table class="data-table">
      <thead>
        <tr>
          <th>来源</th><th>文件路径</th><th>行号</th><th>原行内容</th>
          <th>解析状态</th><th>失败原因</th><th>证据指纹</th><th>已回修</th>
        </tr>
      </thead>
      <tbody>
        <tr
          v-for="line in lines"
          :key="`${line.source_id}-${line.file_path}-${line.line_number}`"
          :data-testid="`line-${line.line_number}`"
        >
          <td>{{ line.source_id }}</td>
          <td>{{ line.file_path }}</td>
          <td>{{ line.line_number }}</td>
          <td class="mono-cell">{{ line.raw_line }}</td>
          <td>{{ line.parse_status }}</td>
          <td>{{ line.failure_reason || '—' }}</td>
          <td class="mono-cell">{{ line.checksum.slice(0, 12) }}</td>
          <td>{{ line.resolved ? line.resolved_by_ticket : '否' }}</td>
        </tr>
        <tr v-if="!lines.length">
          <td colspan="8" class="empty-state">还没有登记来源文件</td>
        </tr>
      </tbody>
    </table>

    <h3 class="section-title">3. 导入 CSV 纠错单</h3>
    <textarea
      v-model="csvText"
      class="correction-textarea mono-cell"
      data-testid="correction-csv"
      placeholder="单号,来源,文件路径,行号,原行内容,修订内容,版本,批准人"
    ></textarea>
    <div class="filter-bar">
      <label class="filter-item">
        <span>导入人</span>
        <input v-model="importedBy" data-testid="correction-imported-by" placeholder="值班员" />
      </label>
      <button class="btn primary" data-testid="correction-import" type="button" @click="runImport">
        导入纠错单
      </button>
      <a class="btn" href="/api/correction/template.csv">下载模板</a>
    </div>
    <table v-if="importResult" class="data-table" data-testid="correction-import-result">
      <thead>
        <tr><th>CSV 行</th><th>单号</th><th>结果</th><th>原因</th></tr>
      </thead>
      <tbody>
        <tr v-for="row in importResult.rows" :key="`${row.row_number}-${row.ticket_id}`">
          <td>{{ row.row_number }}</td>
          <td>{{ row.ticket_id }}</td>
          <td>
            {{ row.written ? '已写入' : row.duplicate ? '重复（未再写）' : '已拒绝' }}
          </td>
          <td :class="{ 'error-text': !row.ok }">{{ row.reason }}</td>
        </tr>
      </tbody>
    </table>

    <h3 class="section-title">4. 纠错单与领取</h3>
    <div class="filter-bar">
      <label class="filter-item">
        <span>领取策略</span>
        <select v-model="claimStrategy" data-testid="correction-claim-strategy">
          <option value="lock_at_claim">领取时锁定（首次正式回修）</option>
          <option value="reclaim_before_classify">分类前重新领取（以最新修订为准）</option>
        </select>
      </label>
      <button class="btn primary" data-testid="correction-classify" type="button" @click="runClassify">
        重做失败行分类
      </button>
    </div>
    <table class="data-table">
      <thead>
        <tr>
          <th>单号</th><th>版本</th><th>行号</th><th>原行内容</th><th>修订内容</th>
          <th>状态</th><th>事件类型</th><th>记录</th><th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="ticket in tickets" :key="ticket.ticket_key" :data-testid="`ticket-${ticket.ticket_key}`">
          <td>{{ ticket.ticket_id }}</td>
          <td>{{ ticket.version }}</td>
          <td>{{ ticket.line_number }}</td>
          <td class="mono-cell">{{ ticket.original_line }}</td>
          <td class="mono-cell">{{ ticket.corrected_line || JSON.stringify(ticket.corrected_fields) }}</td>
          <td>{{ ticket.status }}</td>
          <td>{{ ticket.event_category || '—' }}</td>
          <td class="mono-cell">{{ ticket.record_id || '—' }}</td>
          <td class="row-actions">
            <button
              class="link"
              :data-testid="`claim-${ticket.ticket_key}`"
              type="button"
              @click="runClaim(ticket)"
            >
              领取
            </button>
            <button
              class="link"
              :data-testid="`ack-${ticket.ticket_key}`"
              type="button"
              @click="runAck(ticket)"
            >
              结清
            </button>
          </td>
        </tr>
        <tr v-if="!tickets.length">
          <td colspan="9" class="empty-state">还没有导入纠错单</td>
        </tr>
      </tbody>
    </table>

    <h3 class="section-title">5. 分类写出的事件记录（可追溯选用的纠错单与原有失败）</h3>
    <table class="data-table" data-testid="correction-records">
      <thead>
        <tr>
          <th>记录编号</th><th>事件类型</th><th>原行证据</th><th>旧失败原因</th>
          <th>选用的单</th><th>版本</th><th>解析规则</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="record in records" :key="record.record_id">
          <td>{{ record.record_id }}</td>
          <td>{{ record.event_type.type_name }}（{{ record.event_type.category_tag }}）</td>
          <td class="mono-cell">{{ record.correction.original_line }}</td>
          <td>{{ record.correction.failure_reason }}</td>
          <td>{{ record.correction.ticket_id }}</td>
          <td>{{ record.correction.version }}</td>
          <td>{{ record.correction.chosen_rule }}</td>
        </tr>
        <tr v-if="!records.length">
          <td colspan="7" class="empty-state">还没有回修写出的事件记录</td>
        </tr>
      </tbody>
    </table>

    <footer class="page-foot">
      <span>{{ statusLine }}</span>
      <span v-if="errorMessage" class="error-text" data-testid="correction-error">{{ errorMessage }}</span>
      <span v-else-if="infoMessage" data-testid="correction-info">{{ infoMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  acknowledgeTicket,
  claimTicket,
  classifyCorrections,
  importCorrectionCsv,
  ingestSourceLines,
  listCorrectionLines,
  listCorrectionRecords,
  listCorrectionTickets,
  recoverCorrections,
  type ClassifyRecord,
  type CorrectionLine,
  type CorrectionTicket,
  type ImportResult,
} from '@/api/correction-service'

const sourceForm = ref({
  sourceId: 'LOG_-0004',
  filePath: '/var/log/app/legacy-batch.log',
  startLine: 17,
  rawLines:
    '2026-11-15 14:30:17.000 [http-nio-8080-exec-17] com.example.AuthController - 登录尝试 user=admin\n' +
    '2026-11-15 14:30:18.000 [http-nio-8080-exec-18] INFO  com.example.AuthController - 健康检查通过',
})
const csvText = ref(
  '单号,来源,文件路径,行号,原行内容,修订内容,版本,批准人\n' +
    'CRT-0001,LOG_-0004,/var/log/app/legacy-batch.log,17,' +
    '2026-11-15 14:30:17.000 [http-nio-8080-exec-17] com.example.AuthController - 登录尝试 user=admin,' +
    '2026-11-15 14:30:17.000 [http-nio-8080-exec-17] ERROR com.example.AuthController - 登录失败 user=admin,' +
    '1,值班员',
)
const importedBy = ref('值班员')
const claimStrategy = ref('lock_at_claim')
const lines = ref<CorrectionLine[]>([])
const pendingLines = ref<CorrectionLine[]>([])
const tickets = ref<CorrectionTicket[]>([])
const records = ref<ClassifyRecord[]>([])
const importResult = ref<ImportResult | null>(null)
const errorMessage = ref('')
const infoMessage = ref('')

const cards = computed(() => [
  { label: '失败行', value: pendingLines.value.length },
  { label: '纠错单', value: tickets.value.length },
  { label: '已回修记录', value: records.value.length },
  {
    label: '待领取/冲突',
    value: tickets.value.filter((ticket) => ['已批准', '冲突'].includes(ticket.status)).length,
  },
])
const statusLine = computed(
  () => `已登记 ${lines.value.length} 行，待回修失败行 ${pendingLines.value.length} 行`,
)

function setMessage(message: string) {
  infoMessage.value = message
  errorMessage.value = ''
}

async function reload() {
  errorMessage.value = ''
  try {
    const [linePayload, ticketPayload, recordPayload] = await Promise.all([
      listCorrectionLines(),
      listCorrectionTickets(),
      listCorrectionRecords(),
    ])
    lines.value = linePayload.items
    pendingLines.value = linePayload.pending
    tickets.value = ticketPayload.items
    records.value = recordPayload.items
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '读取失败'
  }
}

async function registerSource() {
  errorMessage.value = ''
  try {
    const payload = await ingestSourceLines(
      sourceForm.value.sourceId,
      sourceForm.value.filePath,
      sourceForm.value.startLine,
      sourceForm.value.rawLines.split('\n').filter((line) => line.trim() !== ''),
    )
    setMessage(payload.message)
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  }
}

async function runImport() {
  errorMessage.value = ''
  try {
    importResult.value = await importCorrectionCsv(csvText.value, importedBy.value)
    setMessage(importResult.value.message)
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导入失败'
  }
}

async function runClaim(ticket: CorrectionTicket) {
  errorMessage.value = ''
  try {
    const result = await claimTicket(ticket.ticket_id, ticket.version, claimStrategy.value)
    if (!result.ok) throw new Error(result.message)
    setMessage(result.message)
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '领取失败'
    await reload()
  }
}

async function runClassify() {
  errorMessage.value = ''
  try {
    const result = await classifyCorrections(claimStrategy.value)
    setMessage(
      `${result.message}；` +
        result.records
          .map((record) => `${record.record_id} → ${record.event_type.category_tag}`)
          .join('，'),
    )
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '分类失败'
  }
}

async function runAck(ticket: CorrectionTicket) {
  errorMessage.value = ''
  try {
    const result = await acknowledgeTicket(ticket.ticket_id, ticket.version)
    if (!result.ok) throw new Error(result.message)
    setMessage(result.message)
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '结清失败'
  }
}

async function recover() {
  errorMessage.value = ''
  try {
    const result = await recoverCorrections()
    setMessage(result.message)
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '恢复失败'
  }
}

onMounted(reload)
</script>
