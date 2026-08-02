export type StreamState = 'created' | 'starting' | 'running' | 'stopping' | 'stopped' | 'failed' | 'restarting'
export type DesiredState = 'running' | 'stopped'
export type StreamMetrics = { input_batches: number; input_messages: number; processing_errors: number; output_batches: number; input_errors: number; input_reconnects: number; output_errors: number; output_messages: number; restarts: number }
export type RuntimeError = { occurred_at_ms: number; stage: string; message: string }
export type StreamStatus = { id: string; state: StreamState; desired_state?: DesiredState; transition_started_at_ms?: number; active_operation_id?: string; node_id?: string; started_at_ms?: number; last_error?: RuntimeError; metrics: StreamMetrics }
export type Page<T> = { items: T[]; page: number; page_size: number; total: number }
export type EngineStatus = { version: string; state: string; uptime_seconds: number; streams_total: number; streams_running: number; streams_failed: number }
export type SystemResource = { id: string; version: string; state: string; node_count: number; stream_count: number; active_operations: number; capabilities: string[] }
export type NodeResource = { id: string; role: string; version: string; state: string; uptime_seconds: number; capabilities: string[]; streams_total: number; streams_running: number; streams_failed: number }
export type HubNode = { id: string; version: string; state: 'online'|'stale'|'offline'|'draining'; capabilities: string[]; last_seen_at_ms: number; lease_expires_at_ms: number; streams_total: number; streams_running: number; streams_failed: number }
export type OperationState = 'queued' | 'running' | 'succeeded' | 'failed' | 'cancelled' | 'timed_out'
export type Operation = { id: string; operation: string; resource_type?: string; resource_id: string; node_id?: string; state: OperationState|'dispatched'|'acknowledged'|'node_unavailable'; progress: number; created_at_ms: number; dispatched_at_ms?: number; acknowledged_at_ms?: number; finished_at_ms?: number; correlation_id?: string; error?: string; result?: unknown }
export type ApiError = { code: string; message: string; field?: string; stream_id?: string; correlation_id?: string }
export type ControlEvent = { occurred_at_ms: number; event_type: string; stream_id?: string; outcome: string; message?: string; operation_id?: string; correlation_id?: string }
export type ConfigCandidate = { format: 'yaml' | 'json' | 'toml'; content: string }
export type ConfigIssue = { path: string; message: string }
export type ConfigValidationReport = { valid: boolean; errors: ConfigIssue[] }
export type ConfigVersion = { id: string; created_at_ms: number; format: ConfigCandidate['format']; parent_id?: string }
export type ConfigDiff = { from: string; to: string; changed: boolean; from_format?: string; to_format?: string }
export type Component = { kind: string; name: string; description?: string; schema?: unknown; example?: unknown }
export type MetricsResponse = { items: { node_id: string; metrics: Record<string, number> }[]; aggregate: Record<string, number> }

const base = import.meta.env.VITE_API_BASE ?? '/api/v1'
const token = import.meta.env.VITE_API_TOKEN
export async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${base}${path}`, { ...init, headers: { 'Content-Type': 'application/json', ...(token ? { Authorization: `Bearer ${token}` } : {}), ...(init?.headers ?? {}) } })
  if (!response.ok) throw await response.json() as ApiError
  return response.status === 204 ? undefined as T : await response.json() as T
}
export const api = {
  system: () => request<SystemResource>('/system'), status: () => request<EngineStatus>('/status'), node: () => request<NodeResource>('/nodes'), metrics: (nodeId?: string) => request<MetricsResponse>(`/metrics${nodeId ? `?node_id=${encodeURIComponent(nodeId)}` : ''}`),
  nodes: () => request<Page<HubNode>>('/nodes'), streams: (nodeId?: string) => request<Page<StreamStatus>>(`/streams${nodeId ? `?node_id=${encodeURIComponent(nodeId)}` : ''}`), events: (nodeId?: string) => request<Page<ControlEvent>>(`/events${nodeId ? `?node_id=${encodeURIComponent(nodeId)}` : ''}`), operations: (nodeId?: string) => request<Page<Operation>>(`/operations${nodeId ? `?node_id=${encodeURIComponent(nodeId)}` : ''}`), operation: (id: string) => request<Operation>(`/operations/${encodeURIComponent(id)}`),
  config: (nodeId?: string) => request<Record<string, unknown>>(nodeId ? `/nodes/${encodeURIComponent(nodeId)}/configuration` : '/configuration'), draft: () => request<ConfigCandidate | undefined>('/configuration/draft'), saveDraft: (candidate: ConfigCandidate) => request<ConfigCandidate>('/configuration/draft', { method: 'PUT', body: JSON.stringify(candidate) }), validateConfig: (candidate: ConfigCandidate) => request<ConfigValidationReport>('/configuration/validate', { method: 'POST', body: JSON.stringify(candidate) }), diff: (from: string, to: string) => request<ConfigDiff>(`/configuration/diff?from=${encodeURIComponent(from)}&to=${encodeURIComponent(to)}`),
  applyConfig: (candidate: ConfigCandidate, nodeId?: string) => request(nodeId ? `/nodes/${encodeURIComponent(nodeId)}/configuration/apply` : '/configuration/apply', { method: 'POST', body: JSON.stringify(candidate) }), versions: (nodeId?: string) => request<ConfigVersion[]>(nodeId ? `/nodes/${encodeURIComponent(nodeId)}/configuration/versions` : '/configuration/versions'), rollback: (id: string, nodeId?: string) => request(nodeId ? `/nodes/${encodeURIComponent(nodeId)}/configuration/rollback/${encodeURIComponent(id)}` : `/configuration/rollback/${encodeURIComponent(id)}`, { method: 'POST' }),
  components: () => request<Component[]>('/components'), schema: () => request<unknown>('/schema'), command: (id: string, action: 'start' | 'stop' | 'restart', nodeId?: string) => request<Operation>(nodeId ? `/nodes/${encodeURIComponent(nodeId)}/streams/${encodeURIComponent(id)}/${action}` : `/streams/${encodeURIComponent(id)}/${action}`, { method: 'POST' }), cancel: (id: string) => request<Operation>(`/operations/${encodeURIComponent(id)}`, { method: 'DELETE' }),
}
