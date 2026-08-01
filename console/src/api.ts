export type StreamState = 'created' | 'starting' | 'running' | 'stopping' | 'stopped' | 'failed' | 'restarting'
export type StreamMetrics = { input_batches: number; input_messages: number; processing_errors: number; output_batches: number; input_errors: number; input_reconnects: number; output_errors: number; output_messages: number; restarts: number }
export type RuntimeError = { occurred_at_ms: number; stage: string; message: string }
export type StreamStatus = { id: string; state: StreamState; started_at_ms?: number; last_error?: RuntimeError; metrics: StreamMetrics }
export type EngineStatus = { version: string; state: string; uptime_seconds: number; streams_total: number; streams_running: number; streams_failed: number }
export type ApiError = { code: string; message: string; field?: string; stream_id?: string }
export type ControlEvent = { occurred_at_ms: number; event_type: string; stream_id?: string; outcome: string; message?: string }
export type ConfigCandidate = { format: 'yaml' | 'json' | 'toml'; content: string }
export type ConfigIssue = { path: string; message: string }
export type ConfigValidationReport = { valid: boolean; errors: ConfigIssue[] }
export type ConfigVersion = { id: string; created_at_ms: number; format: ConfigCandidate['format']; parent_id?: string }
export type Component = { kind: string; name: string; description?: string; config_schema?: unknown; example?: unknown }

const base = import.meta.env.VITE_API_BASE ?? '/api/v1'
const token = import.meta.env.VITE_API_TOKEN
async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${base}${path}`, { ...init, headers: { 'Content-Type': 'application/json', ...(token ? { Authorization: `Bearer ${token}` } : {}), ...(init?.headers ?? {}) } })
  if (!response.ok) throw await response.json() as ApiError
  return response.json() as Promise<T>
}
export const api = {
  system: () => request<EngineStatus>('/system'),
  streams: () => request<StreamStatus[]>('/streams'),
  events: () => request<ControlEvent[]>('/events'),
  config: () => request<Record<string, unknown>>('/config'),
  validateConfig: (candidate: ConfigCandidate) => request<ConfigValidationReport>('/config/validate', { method: 'POST', body: JSON.stringify(candidate) }),
  applyConfig: (candidate: ConfigCandidate) => request('/config/apply', { method: 'POST', body: JSON.stringify(candidate) }),
  versions: () => request<ConfigVersion[]>('/config/versions'),
  rollback: (id: string) => request(`/config/rollback/${encodeURIComponent(id)}`, { method: 'POST' }),
  components: () => request<Component[]>('/components'),
  schema: () => request<unknown>('/schema'),
  command: (id: string, action: 'start' | 'stop' | 'restart') => request(`/streams/${encodeURIComponent(id)}/${action}`, { method: 'POST' }),
}
