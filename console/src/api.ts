export type StreamState = 'created' | 'starting' | 'running' | 'stopping' | 'stopped' | 'failed' | 'restarting'
export type DesiredState = 'running' | 'stopped'
export type ConvergenceState = 'unknown' | 'pending' | 'applying' | 'in_sync' | 'degraded' | 'blocked'
export type StreamMetrics = { input_batches: number; input_messages: number; processing_errors: number; output_batches: number; input_errors: number; input_reconnects: number; output_errors: number; output_messages: number; restarts: number }
export type RuntimeError = { occurred_at_ms: number; stage: string; message: string }
export type StreamStatus = { id: string; state: StreamState; desired_state?: DesiredState; desired_generation?: number; desired_config_version?: string; observed_generation?: number; observed_config_version?: string; convergence?: ConvergenceState; intent_id?: string; attempt_id?: string; retry_count?: number; next_retry_at_ms?: number; transition_started_at_ms?: number; active_operation_id?: string; node_id?: string; started_at_ms?: number; last_error?: RuntimeError; metrics: StreamMetrics }
export type Page<T> = { items: T[]; page: number; page_size: number; total: number }
export type EngineStatus = { version: string; state: string; uptime_seconds: number; streams_total: number; streams_running: number; streams_failed: number }
export type SystemResource = { id: string; version: string; state: string; node_count: number; stream_count: number; active_operations: number; capabilities: string[] }
export type ControlNode = { id: string; version: string; state: string; capabilities: string[]; streams_total: number; streams_running: number; streams_failed: number; role?: string; uptime_seconds?: number; last_seen_at_ms?: number; lease_expires_at_ms?: number }
export type NodeResource = ControlNode
export type HubNode = ControlNode
export type OperationState = 'queued' | 'running' | 'succeeded' | 'failed' | 'cancelled' | 'timed_out'
export type Operation = { id: string; intent_id?: string; attempt_id?: string; operation: string; resource_type?: string; resource_id: string; node_id?: string; state: OperationState|'dispatched'|'acknowledged'|'node_unavailable'|'superseded'; intent_state?: string; convergence_state?: ConvergenceState; generation?: number; observed_generation?: number; observed_state?: string; retry_count?: number; next_retry_at_ms?: number; failure_class?: string; superseded_generation?: number; config_version_id?: string; progress: number; created_at_ms: number; dispatched_at_ms?: number; acknowledged_at_ms?: number; finished_at_ms?: number; correlation_id?: string; error?: string; result?: unknown }
export type ApiError = { code: string; message: string; field?: string; stream_id?: string; correlation_id?: string; details?: Record<string, unknown>; status?: number }
export type ControlEvent = { occurred_at_ms: number; event_type: string; stream_id?: string; outcome: string; message?: string; operation_id?: string; correlation_id?: string; failure_class?: string; generation?: number }
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
  const correlationId = `console-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
  const response = await fetch(`${base}${path}`, { ...init, headers: { 'Content-Type': 'application/json', 'X-Correlation-ID': correlationId, ...(token ? { Authorization: `Bearer ${token}` } : {}), ...(init?.headers ?? {}) } })
  if (!response.ok) {
    const body = await response.json().catch(() => ({})) as Partial<ApiError>
    throw { code: body.code ?? 'request_failed', message: body.message ?? `Request failed (${response.status})`, field: body.field, stream_id: body.stream_id, correlation_id: body.correlation_id ?? response.headers.get('x-correlation-id') ?? correlationId, status: response.status } satisfies ApiError
  }
  return response.status === 204 ? undefined as T : await response.json() as T
}
export const api = {
  system: () => request<SystemResource>('/system'), status: () => request<EngineStatus>('/status'), node: () => request<NodeResource>('/node'), metrics: (nodeId?: string) => request<MetricsResponse>(`/metrics${nodeId ? `?node_id=${encodeURIComponent(nodeId)}` : ''}`),
  nodes: (page = 1, pageSize = 50) => request<Page<ControlNode>>(`/nodes?page=${page}&page_size=${pageSize}`), streams: (nodeId?: string) => request<Page<StreamStatus>>(`/streams${nodeId ? `?node_id=${encodeURIComponent(nodeId)}` : ''}`), events: (nodeId?: string) => request<Page<ControlEvent>>(`/events${nodeId ? `?node_id=${encodeURIComponent(nodeId)}` : ''}`), operations: (nodeId?: string) => request<Page<Operation>>(`/operations${nodeId ? `?node_id=${encodeURIComponent(nodeId)}` : ''}`), operation: (id: string) => request<Operation>(`/operations/${encodeURIComponent(id)}`),
  config: (nodeId?: string) => request<Record<string, unknown>>(nodeId ? `/nodes/${encodeURIComponent(nodeId)}/configuration` : '/configuration'), draft: () => request<ConfigCandidate | undefined>('/configuration/draft'), saveDraft: (candidate: ConfigCandidate) => request<ConfigCandidate>('/configuration/draft', { method: 'PUT', body: JSON.stringify(candidate) }), validateConfig: (candidate: ConfigCandidate) => request<ConfigValidationReport>('/configuration/validate', { method: 'POST', body: JSON.stringify(candidate) }), diff: (from: string, to: string) => request<ConfigDiff>(`/configuration/diff?from=${encodeURIComponent(from)}&to=${encodeURIComponent(to)}`),
  applyConfig: (candidate: ConfigCandidate, nodeId?: string) => request(nodeId ? `/nodes/${encodeURIComponent(nodeId)}/configuration/apply` : '/configuration/apply', { method: 'POST', body: JSON.stringify(candidate) }), versions: (nodeId?: string) => request<ConfigVersion[]>(nodeId ? `/nodes/${encodeURIComponent(nodeId)}/configuration/versions` : '/configuration/versions'), rollback: (id: string, nodeId?: string) => request(nodeId ? `/nodes/${encodeURIComponent(nodeId)}/configuration/rollback/${encodeURIComponent(id)}` : `/configuration/rollback/${encodeURIComponent(id)}`, { method: 'POST' }),
  components: () => request<Component[]>('/components'), schema: () => request<unknown>('/schema'), command: (id: string, action: 'start' | 'stop' | 'restart', nodeId?: string) => request<Operation>(nodeId ? `/nodes/${encodeURIComponent(nodeId)}/streams/${encodeURIComponent(id)}/${action}` : `/streams/${encodeURIComponent(id)}/${action}`, { method: 'POST' }), cancel: (id: string) => request<Operation>(`/operations/${encodeURIComponent(id)}`, { method: 'DELETE' }),
}
