export type StreamState = 'created' | 'starting' | 'running' | 'stopping' | 'stopped' | 'failed' | 'restarting'
export type DesiredState = 'running' | 'stopped'
export type ConvergenceState = 'unknown' | 'pending' | 'applying' | 'in_sync' | 'degraded' | 'blocked'
export type StreamMetrics = { input_batches: number; input_messages: number; processing_errors: number; output_batches: number; input_errors: number; input_reconnects: number; output_errors: number; output_messages: number; restarts: number }
export type RuntimeError = { occurred_at_ms: number; stage: string; message: string }
export type StreamStatus = { id: string; state: StreamState; desired_state?: DesiredState; desired_generation?: number; desired_config_version?: string; observed_generation?: number; observed_config_version?: string; convergence?: ConvergenceState; intent_id?: string; attempt_id?: string; retry_count?: number; next_retry_at_ms?: number; transition_started_at_ms?: number; active_operation_id?: string; node_id?: string; started_at_ms?: number; last_error?: RuntimeError; metrics: StreamMetrics }
export type Page<T> = { items: T[]; page: number; page_size: number; total: number }
export type EngineStatus = { version: string; state: string; uptime_seconds: number; streams_total: number; streams_running: number; streams_failed: number }
export type SystemResource = { id: string; version: string; state: string; node_count: number; stream_count: number; active_operations: number; capabilities: string[] }
export type ControlNode = { id: string; protocol_version?: string; version: string; state: string; capabilities: string[]; streams_total: number; streams_running: number; streams_failed: number; role?: string; uptime_seconds?: number; last_seen_at_ms?: number; lease_expires_at_ms?: number }
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
export type RolloutState = 'pending' | 'applying' | 'paused' | 'converged' | 'cancelled' | 'rolled_back'
export type RolloutTarget = { rollout_id: string; node_id: string; ordinal: number; state: string; attempt_id?: string; error?: string; observed_config_version?: string; updated_at_ms: number }
export type Rollout = { rollout_id: string; config_version_id: string; state: RolloutState|string; batch_size: number; current_batch: number; total_targets: number; actor?: string; correlation_id?: string; created_at_ms: number; updated_at_ms: number }
export type RolloutDetail = { rollout: Rollout; targets: RolloutTarget[] }
export type AuditRecord = { event_id: number; actor?: string; action: string; resource_type: string; resource_id?: string; node_id?: string; stream_id?: string; correlation_id?: string; outcome: string; failure_code?: string; message?: string; occurred_at_ms: number }
export type Job = { job_id: string; version: number; spec?: unknown; spec_json?: string; desired_state: string; observed_state: string; convergence: string; generation: number; node_ids: string[]; checkpoint_id?: string; last_error?: string; updated_at_ms: number }
export type JobMetrics = { watermark_lag_ms: number; state_bytes: number; checkpoint_duration_ms: number; checkpoint_failures: number; recovery_progress: number; task_pressure: number; partition_health: number }
export type JobCheckpoint = { job_id: string; job_version: number; checkpoint_id: string; kind: 'checkpoint'|'savepoint'|string; status: string; manifest_uri?: string; format_version: number; created_at_ms: number; updated_at_ms: number }
export type JobVersion = { job_id: string; version: number; spec_json: string; plan_json: string; created_at_ms: number }
export type JobDetail = { job: Job; plan: unknown; tasks: Array<Record<string, unknown>>; nodes: ControlNode[]; operations: Operation[]; checkpoints: JobCheckpoint[]; metrics: JobMetrics; active_upgrade?: Record<string, unknown> }
export type JobValidation = { valid: boolean; plan?: unknown; required_capabilities: string[]; nodes: Array<{ node_id: string; state: string; capabilities: string[]; compatible: boolean; missing_capabilities: string[] }>; warnings: string[] }
export type JobUpgrade = { upgrade_id: string; state: string; savepoint_id: string; job: Job }

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
  applyConfig: (candidate: ConfigCandidate, nodeId?: string) => request<Operation>(nodeId ? `/nodes/${encodeURIComponent(nodeId)}/configuration/apply` : '/configuration/apply', { method: 'POST', body: JSON.stringify(candidate) }), versions: (nodeId?: string) => request<ConfigVersion[]>(nodeId ? `/nodes/${encodeURIComponent(nodeId)}/configuration/versions` : '/configuration/versions'), rollback: (id: string, nodeId?: string) => request<Operation>(nodeId ? `/nodes/${encodeURIComponent(nodeId)}/configuration/rollback/${encodeURIComponent(id)}` : `/configuration/rollback/${encodeURIComponent(id)}`, { method: 'POST' }),
  components: () => request<Component[]>('/components'), schema: () => request<unknown>('/schema'), command: (id: string, action: 'start' | 'stop' | 'restart', nodeId?: string) => request<Operation>(nodeId ? `/nodes/${encodeURIComponent(nodeId)}/streams/${encodeURIComponent(id)}/${action}` : `/streams/${encodeURIComponent(id)}/${action}`, { method: 'POST' }), cancel: (id: string) => request<Operation>(`/operations/${encodeURIComponent(id)}`, { method: 'DELETE' }),
  jobs: () => request<Job[]>('/jobs'), job: (id: string) => request<Job>(`/jobs/${encodeURIComponent(id)}`), jobDetail: (id: string) => request<JobDetail>(`/jobs/${encodeURIComponent(id)}/detail`), jobVersions: (id: string) => request<JobVersion[]>(`/jobs/${encodeURIComponent(id)}/versions`), validateJob: (spec: unknown, nodeIds: string[] = []) => request<JobValidation>('/jobs/validate', { method: 'POST', body: JSON.stringify({ spec, node_ids: nodeIds }) }), createJob: (spec: unknown, nodeIds: string[] = []) => request<Job>('/jobs', { method: 'POST', body: JSON.stringify({ spec, node_ids: nodeIds, desired_state: 'stopped' }) }), setJobState: (id: string, state: 'running'|'stopped') => request<Job>(`/jobs/${encodeURIComponent(id)}/desired-state`, { method: 'PUT', body: JSON.stringify({ state }) }),
  jobPlan: (id: string) => request<unknown>(`/jobs/${encodeURIComponent(id)}/plan`), checkpoint: (id: string) => request<Job>(`/jobs/${encodeURIComponent(id)}/checkpoints`, { method: 'POST' }), savepoint: (id: string) => request<Job>(`/jobs/${encodeURIComponent(id)}/savepoints`, { method: 'POST' }), upgradeJob: (id: string, spec: unknown, savepointId: string, expectedGeneration: number, nodeIds: string[] = []) => request<JobUpgrade>(`/jobs/${encodeURIComponent(id)}/upgrades`, { method: 'POST', body: JSON.stringify({ spec, node_ids: nodeIds, expected_generation: expectedGeneration, savepoint_id: savepointId }) }), rollbackJobUpgrade: (id: string, upgradeId = 'manual') => request<Job>(`/jobs/${encodeURIComponent(id)}/upgrades/${encodeURIComponent(upgradeId)}/rollback`, { method: 'POST' }),
  rollouts: () => request<Rollout[]>('/rollouts'), rollout: (id: string) => request<RolloutDetail>(`/rollouts/${encodeURIComponent(id)}`), createRollout: (configVersion: string, nodeIds: string[], batchSize: number) => request<Rollout>('/rollouts', { method: 'POST', body: JSON.stringify({ config_version: configVersion, node_ids: nodeIds, batch_size: batchSize }) }), rolloutAction: (id: string, action: 'pause'|'resume'|'cancel'|'rollback', configVersion?: string) => request<Rollout>(`/rollouts/${encodeURIComponent(id)}/actions`, { method: 'POST', body: JSON.stringify({ action, ...(configVersion ? { config_version: configVersion } : {}) }) }), audit: (resourceId?: string) => request<Page<AuditRecord>>(`/audit${resourceId ? `?resource_id=${encodeURIComponent(resourceId)}` : ''}`),
}

export function streamEvents(onEvent: (event: ControlEvent) => void, onState?: (state: 'connected'|'disconnected') => void, nodeId?: string): AbortController {
  const controller = new AbortController()
  const path = `/events/stream${nodeId ? `?node_id=${encodeURIComponent(nodeId)}` : ''}`
  void (async () => {
    let lastEventId: string | undefined
    while (!controller.signal.aborted) {
      try {
        const response = await fetch(`${base}${path}`, { headers: { Accept: 'text/event-stream', ...(token ? { Authorization: `Bearer ${token}` } : {}), ...(lastEventId ? { 'Last-Event-ID': lastEventId } : {}) }, signal: controller.signal })
        if (!response.ok || !response.body) throw new Error(`SSE connection failed (${response.status})`)
        onState?.('connected')
        const reader = response.body.getReader(); const decoder = new TextDecoder(); let buffer = ''; let eventType = 'message'; let data = ''
        const emit = () => { if (!data) return; if (eventType !== 'resync') { try { onEvent(JSON.parse(data) as ControlEvent) } catch { /* bounded server payload; ignore malformed frames */ } } data = ''; eventType = 'message' }
        while (!controller.signal.aborted) {
          const next = await reader.read(); if (next.done) break; buffer += decoder.decode(next.value, { stream: true })
          const frames = buffer.split(/\r?\n\r?\n/); buffer = frames.pop() ?? ''
          for (const frame of frames) { for (const line of frame.split(/\r?\n/)) { if (line.startsWith('id:')) lastEventId = line.slice(3).trim(); if (line.startsWith('event:')) eventType = line.slice(6).trim(); if (line.startsWith('data:')) data += line.slice(5).trim() } emit() }
        }
      } catch { if (!controller.signal.aborted) onState?.('disconnected') }
      if (!controller.signal.aborted) await new Promise(resolve => window.setTimeout(resolve, 1000))
    }
  })()
  return controller
}

export async function waitForOperation(id: string): Promise<Operation> {
  for (let attempt = 0; attempt < 30; attempt += 1) {
    const operation = await api.operation(id)
    const terminalIntent = ['converged', 'blocked', 'cancelled', 'superseded'].includes(operation.intent_state ?? '')
    const terminalState = ['succeeded', 'failed', 'cancelled', 'timed_out', 'node_unavailable'].includes(operation.state)
    if (terminalIntent || terminalState) {
      if ((operation.intent_state && operation.intent_state !== 'converged') || (!operation.intent_state && operation.state !== 'succeeded')) {
        throw new Error(operation.error ?? `Operation ${operation.intent_state ?? operation.state}`)
      }
      return operation
    }
    await new Promise(resolve => window.setTimeout(resolve, 250))
  }
  throw new Error('Operation timed out')
}
