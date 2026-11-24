/** 解析纠错与失败行回修接口封装。 */
import { request } from './client'

export type CorrectionLine = {
  source_id: string
  file_path: string
  line_number: number
  raw_line: string
  parse_status: string
  failure_reason: string
  checksum: string
  resolved: boolean
  resolved_by_ticket: string
  resolved_record_id: string
}

export type CorrectionTicket = {
  ticket_id: string
  version: string
  ticket_key: string
  source_id: string
  file_path: string
  line_number: number
  original_line: string
  corrected_line: string
  corrected_fields: Record<string, string>
  status: string
  revision: number
  record_id: string
  event_type_id: string
  event_category: string
  note: string
}

export type ImportRow = {
  row_number: number
  ticket_id: string
  ok: boolean
  written: boolean
  duplicate: boolean
  reason_code: string
  reason: string
  ticket: CorrectionTicket | null
}

export type ImportResult = {
  ok: boolean
  message: string
  batch_id: string
  written: number
  rejected: number
  duplicates: number
  total: number
  rows: ImportRow[]
}

export type ClassifyRecord = {
  record_id: string
  event_type: { type_id: string; type_name: string; category_tag: string; severity: string }
  original_content: string
  parsed_fields: Record<string, string>
  event_time: string
  source_host: string
  correction: Record<string, string | number | string[]>
}

export type ClassifyResult = {
  ok: boolean
  message: string
  classified: number
  records: ClassifyRecord[]
  items: { ticket_id: string; version: string; ok: boolean; status: string; reason: string }[]
}

export type ClaimResult = {
  ok: boolean
  message: string
  ticket: CorrectionTicket | null
  superseded: string[]
}

export type RecoveryResult = {
  ok: boolean
  message: string
  settled_from_journal: string[]
  pending_rework: string[]
  inconsistent: { ticket_key: string; reason: string }[]
  records_restored: number
  failed_lines_remaining: CorrectionLine[]
}

async function readJson<T>(response: Response): Promise<T> {
  const payload = (await response.json()) as T & { message?: string; detail?: string }
  if (!response.ok) {
    throw new Error(payload.detail ?? payload.message ?? `接口返回 ${response.status}`)
  }
  return payload
}

export async function ingestSourceLines(
  sourceId: string,
  filePath: string,
  startLine: number,
  rawLines: string[],
): Promise<{ ok: boolean; message: string; lines: CorrectionLine[] }> {
  const response = await request('/api/correction/sources/ingest', {
    method: 'POST',
    body: JSON.stringify({
      source_id: sourceId,
      file_path: filePath,
      start_line: startLine,
      raw_lines: rawLines,
    }),
  })
  return readJson(response)
}

export async function listCorrectionLines(): Promise<{ items: CorrectionLine[]; pending: CorrectionLine[] }> {
  return readJson(await request('/api/correction/lines'))
}

export async function importCorrectionCsv(csv: string, importedBy = ''): Promise<ImportResult> {
  return readJson(
    await request('/api/correction/import', {
      method: 'POST',
      body: JSON.stringify({ csv, imported_by: importedBy }),
    }),
  )
}

export async function listCorrectionTickets(): Promise<{ items: CorrectionTicket[]; total: number }> {
  return readJson(await request('/api/correction/tickets'))
}

export async function claimTicket(ticketId: string, version: string, strategy: string): Promise<ClaimResult> {
  return readJson(
    await request(`/api/correction/tickets/${encodeURIComponent(ticketId)}/claim`, {
      method: 'POST',
      body: JSON.stringify({ version, strategy }),
    }),
  )
}

export async function classifyCorrections(strategy: string): Promise<ClassifyResult> {
  return readJson(
    await request('/api/correction/classify', {
      method: 'POST',
      body: JSON.stringify({ strategy }),
    }),
  )
}

export async function acknowledgeTicket(
  ticketId: string,
  version: string,
): Promise<{ ok: boolean; message: string; record_id?: string; event_category?: string }> {
  return readJson(
    await request(`/api/correction/tickets/${encodeURIComponent(ticketId)}/ack`, {
      method: 'POST',
      body: JSON.stringify({ version }),
    }),
  )
}

export async function recoverCorrections(): Promise<RecoveryResult> {
  return readJson(
    await request('/api/correction/recover', { method: 'POST', body: JSON.stringify({}) }),
  )
}

export async function listCorrectionRecords(): Promise<{ items: ClassifyRecord[]; total: number }> {
  return readJson(await request('/api/correction/records'))
}
