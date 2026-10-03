import { currentLocale, intlLocale, translate } from './i18n'

export type StreamState =
  'created' | 'starting' | 'running' | 'stopping' | 'stopped' | 'failed' | 'restarting'
export type DesiredState = 'running' | 'stopped'
export type ConvergenceState = 'unknown' | 'pending' | 'applying' | 'in_sync' | 'degraded' | 'blocked'
export type StreamMetrics = {
  input_batches: number
  input_messages: number
  processing_errors: number
  output_batches: number
  input_errors: number
  input_reconnects: number
  output_errors: number
  output_messages: number
  restarts: number
}
export type RuntimeError = { occurred_at_ms: number; stage: string; message: string }
export type StreamStatus = {
  id: string
  state: StreamState
  desired_state?: DesiredState
  desired_generation?: number
  desired_config_version?: string
  observed_generation?: number
  observed_config_version?: string
  convergence?: ConvergenceState
  intent_id?: string
  attempt_id?: string
  retry_count?: number
  next_retry_at_ms?: number
  transition_started_at_ms?: number
  active_operation_id?: string
  node_id?: string
  started_at_ms?: number
  last_error?: RuntimeError
  metrics: StreamMetrics
}
export type Page<T> = { items: T[]; page: number; page_size: number; total: number }
export type SystemHaStatus = {
  enabled: boolean
  role: string
  epoch: number
  transitions: number
}
export type EngineStatus = {
  version: string
  state: string
  uptime_seconds: number
  streams_total: number
  streams_running: number
  streams_failed: number
}
export type SystemResource = {
  id: string
  version: string
  state: string
  node_count: number
  online_nodes?: number
  stream_count: number
  active_operations: number
  capabilities: string[]
  ha?: SystemHaStatus
}
export type NodeMaintenanceState = 'active' | 'draining' | 'maintenance'
export type ControlNode = {
  id: string
  protocol_version?: string
  version: string
  state: string
  capabilities: string[]
  streams_total: number
  streams_running: number
  streams_failed: number
  maintenance_state?: NodeMaintenanceState
  last_seen_at_ms?: number
  lease_expires_at_ms?: number
  data_address?: string
}
export type OperationState = 'queued' | 'running' | 'succeeded' | 'failed' | 'cancelled' | 'timed_out'
export type Operation = {
  id: string
  intent_id?: string
  attempt_id?: string
  operation: string
  resource_id: string
  node_id?: string
  state: OperationState | 'dispatched' | 'acknowledged' | 'node_unavailable' | 'superseded'
  intent_state?: string
  convergence_state?: ConvergenceState
  generation?: number
  observed_generation?: number
  observed_state?: string
  retry_count?: number
  next_retry_at_ms?: number
  failure_class?: string
  superseded_generation?: number
  config_version_id?: string
  /** Report payload of a read-only command (configuration validation/diff),
   * present once the command reaches a terminal state. */
  result?: unknown
  progress: number
  created_at_ms: number
  dispatched_at_ms?: number
  acknowledged_at_ms?: number
  finished_at_ms?: number
  correlation_id?: string
  error?: string
}
export type ApiError = {
  code: string
  message: string
  field?: string
  stream_id?: string
  correlation_id?: string
  details?: Record<string, unknown>
  status?: number
}
export type ControlEvent = {
  occurred_at_ms: number
  event_type: string
  stream_id?: string
  node_id?: string
  outcome: string
  message?: string
  operation_id?: string
  correlation_id?: string
}
export type ConfigCandidate = { format: 'yaml' | 'json' | 'toml'; content: string }
export type ConfigIssue = { path: string; message: string }
export type ConfigValidationReport = { valid: boolean; errors: ConfigIssue[] }
export type ConfigVersion = {
  id: string
  created_at_ms: number
  format: ConfigCandidate['format']
  parent_id?: string
}
export type ConfigDiff = {
  from: string
  to: string
  changed: boolean
  from_format?: string
  to_format?: string
}
export type Component = {
  kind: string
  name: string
  description?: string
  schema?: unknown
  example?: unknown
}
export type MetricsResponse = {
  items: { node_id: string; metrics: Record<string, number> }[]
  aggregate: Record<string, number>
}
export type RolloutState = 'pending' | 'applying' | 'paused' | 'converged' | 'cancelled' | 'rolled_back'
export type RolloutTarget = {
  rollout_id: string
  node_id: string
  ordinal: number
  state: string
  attempt_id?: string
  error?: string
  observed_config_version?: string
  updated_at_ms: number
}
export type Rollout = {
  rollout_id: string
  config_version_id: string
  state: RolloutState | string
  batch_size: number
  current_batch: number
  total_targets: number
  actor?: string
  correlation_id?: string
  created_at_ms: number
  updated_at_ms: number
}
export type RolloutDetail = { rollout: Rollout; targets: RolloutTarget[] }
export type AuditRecord = {
  event_id: number
  actor?: string
  action: string
  resource_type: string
  resource_id?: string
  node_id?: string
  stream_id?: string
  correlation_id?: string
  outcome: string
  failure_code?: string
  message?: string
  occurred_at_ms: number
}
export type Job = {
  job_id: string
  version: number
  spec_json?: string
  desired_state: string
  observed_state: string
  convergence: string
  generation: number
  node_ids: string[]
  checkpoint_id?: string
  last_error?: string
  updated_at_ms: number
}
export type JobMetrics = {
  watermark_lag_ms?: number
  checkpoint_duration_ms?: number
  checkpoint_failures?: number
}
export type JobCheckpoint = {
  job_id: string
  job_version: number
  checkpoint_id: string
  kind: 'checkpoint' | 'savepoint' | string
  status: string
  manifest_uri?: string
  format_version: number
  created_at_ms: number
  updated_at_ms: number
}
export type JobVersion = {
  job_id: string
  version: number
  spec_json: string
  plan_json: string
  created_at_ms: number
}
export type JobTask = {
  id: string
  job_id: string
  job_version: number
  task_id: string
  generation: number
  node_id: string
  state: string
  /** True when an executing node reports this task as running; false marks
   * the desired-placement fallback (not yet observed). */
  observed?: boolean
  observed_node_id?: string
}
export type JobDetail = {
  job: Job
  plan: unknown
  tasks: JobTask[]
  nodes: ControlNode[]
  operations: Operation[]
  checkpoints: JobCheckpoint[]
  metrics: JobMetrics
  active_upgrade?: JobUpgradeOrchestration
}
export type JobValidation = {
  valid: boolean
  plan?: unknown
  required_capabilities: string[]
  nodes: Array<{
    node_id: string
    state: string
    capabilities: string[]
    compatible: boolean
    missing_capabilities: string[]
  }>
  warnings: string[]
}
export type JobUpgrade = { upgrade_id: string; state: string; savepoint_id: string | null; job: Job }
export type JobUpgradeOrchestration = {
  upgrade_id: string
  job_id: string
  from_version: number
  to_version: number
  phase: string
  savepoint_id: string | null
  phase_deadline_at_ms: number
  savepoint_retries: number
  verify_timeout_ms: number
  actor: string | null
  correlation_id: string | null
  last_error: string | null
  paused_from: string | null
  created_at_ms: number
  updated_at_ms: number
}

// Polling cadences, centralized so the UI text and the timers cannot drift.
export const SNAPSHOT_INTERVAL_MS = 30_000
export const REFRESH_DEBOUNCE_MS = 1_500
export const JOB_DETAIL_INTERVAL_MS = 5_000
export const ROLLOUT_DETAIL_INTERVAL_MS = 5_000
export const DEFAULT_PAGE_SIZE = 50

export function errorMessage(cause: unknown): string {
  if (typeof cause === 'object' && cause && 'message' in cause) {
    const error = cause as ApiError
    return `${error.message}${error.correlation_id ? ` (ref ${error.correlation_id})` : ''}`
  }
  return cause instanceof Error ? cause.message : translate(currentLocale(), 'api.controlApiUnavailable')
}

export function formatTime(value?: number): string {
  return value ? new Date(value).toLocaleString(intlLocale(currentLocale())) : '—'
}

const base = import.meta.env.VITE_API_BASE ?? '/api/v1'
const token = import.meta.env.VITE_API_TOKEN

/** Default per-request budget: a connection black hole must fail the call
 * (with a readable message) instead of freezing the UI forever. */
const REQUEST_TIMEOUT_MS = 30_000

/** Timeout signal merged with any caller-provided signal. Feature-detected:
 * older runtimes (and jsdom) may lack `AbortSignal.any`/`timeout`, where the
 * caller's signal (if any) applies and the budget is simply absent. */
function requestSignal(init?: RequestInit): AbortSignal | undefined {
  if (typeof AbortSignal.timeout !== 'function') return init?.signal ?? undefined
  const timeout = AbortSignal.timeout(REQUEST_TIMEOUT_MS)
  const caller = init?.signal ?? undefined
  if (!caller) return timeout
  if (typeof AbortSignal.any === 'function') return AbortSignal.any([caller, timeout])
  return caller
}

export async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const correlationId = `console-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
  const signal = requestSignal(init)
  let response: Response
  try {
    response = await fetch(`${base}${path}`, {
      ...init,
      signal,
      headers: {
        'Content-Type': 'application/json',
        'X-Correlation-ID': correlationId,
        ...(token ? { Authorization: `Bearer ${token}` } : {}),
        ...(init?.headers ?? {}),
      },
    })
  } catch (error) {
    if (error instanceof DOMException && error.name === 'TimeoutError') {
      throw Object.assign(new Error(translate(currentLocale(), 'api.requestTimeout')), {
        code: 'request_timeout',
        correlation_id: correlationId,
        status: 0,
      })
    }
    throw error
  }
  if (!response.ok) {
    if (response.status === 401 && !token) {
      void redirectToOidcLogin()
    }
    const body = (await response.json().catch(() => ({}))) as Partial<ApiError>
    throw Object.assign(
      new Error(body.message ?? translate(currentLocale(), 'api.requestFailed', { status: response.status })),
      {
        code: body.code ?? 'request_failed',
        field: body.field,
        stream_id: body.stream_id,
        correlation_id: body.correlation_id ?? response.headers.get('x-correlation-id') ?? correlationId,
        status: response.status,
      },
    )
  }
  return response.status === 204 ? (undefined as T) : ((await response.json()) as T)
}
export const api = {
  system: () => request<SystemResource>('/system'),
  status: () => request<EngineStatus>('/status'),
  metrics: (nodeId?: string) =>
    request<MetricsResponse>(
      `/metrics${nodeId ? `?node_id=${encodeURIComponent(nodeId)}` : ''}`,
      // The Hub negotiates: Prometheus text by default, JSON for consoles.
      { headers: { Accept: 'application/json' } },
    ),
  nodes: (page = 1, pageSize = DEFAULT_PAGE_SIZE) =>
    request<Page<ControlNode>>(`/nodes?page=${page}&page_size=${pageSize}`),
  drainNode: (id: string) =>
    request<ControlNode>(`/nodes/${encodeURIComponent(id)}/drain`, { method: 'POST' }),
  maintainNode: (id: string) =>
    request<ControlNode>(`/nodes/${encodeURIComponent(id)}/maintenance`, { method: 'POST' }),
  resumeNode: (id: string) =>
    request<ControlNode>(`/nodes/${encodeURIComponent(id)}/maintenance`, { method: 'DELETE' }),
  streams: (nodeId?: string) =>
    request<Page<StreamStatus>>(`/streams${nodeId ? `?node_id=${encodeURIComponent(nodeId)}` : ''}`),
  events: (nodeId?: string) =>
    request<Page<ControlEvent>>(`/events${nodeId ? `?node_id=${encodeURIComponent(nodeId)}` : ''}`),
  operations: (nodeId?: string) =>
    request<Page<Operation>>(`/operations${nodeId ? `?node_id=${encodeURIComponent(nodeId)}` : ''}`),
  operation: (id: string) => request<Operation>(`/operations/${encodeURIComponent(id)}`),
  config: (nodeId?: string) =>
    request<Record<string, unknown>>(
      nodeId ? `/nodes/${encodeURIComponent(nodeId)}/configuration` : '/configuration',
    ),
  draft: () => request<ConfigCandidate | undefined>('/configuration/draft'),
  saveDraft: (candidate: ConfigCandidate) =>
    request<ConfigCandidate>('/configuration/draft', { method: 'PUT', body: JSON.stringify(candidate) }),
  validateConfig: (candidate: ConfigCandidate, nodeId?: string) =>
    request<ConfigValidationReport | Operation>(
      nodeId ? `/nodes/${encodeURIComponent(nodeId)}/configuration/validate` : '/configuration/validate',
      { method: 'POST', body: JSON.stringify(candidate) },
    ),
  diff: (from: string, to: string, nodeId?: string) => {
    const query = `from=${encodeURIComponent(from)}&to=${encodeURIComponent(to)}`
    return request<ConfigDiff | Operation>(
      nodeId
        ? `/nodes/${encodeURIComponent(nodeId)}/configuration/diff?${query}`
        : `/configuration/diff?${query}`,
    )
  },
  applyConfig: (candidate: ConfigCandidate, nodeId?: string) =>
    request<Operation>(
      nodeId ? `/nodes/${encodeURIComponent(nodeId)}/configuration/apply` : '/configuration/apply',
      { method: 'POST', body: JSON.stringify(candidate) },
    ),
  versions: (nodeId?: string) =>
    request<ConfigVersion[]>(
      nodeId ? `/nodes/${encodeURIComponent(nodeId)}/configuration/versions` : '/configuration/versions',
    ),
  rollback: (id: string, nodeId?: string) =>
    request<Operation>(
      nodeId
        ? `/nodes/${encodeURIComponent(nodeId)}/configuration/rollback/${encodeURIComponent(id)}`
        : `/configuration/rollback/${encodeURIComponent(id)}`,
      { method: 'POST' },
    ),
  components: () => request<Component[]>('/components'),
  schema: () => request<unknown>('/schema'),
  command: (id: string, action: 'start' | 'stop' | 'restart', nodeId?: string) =>
    request<Operation>(
      nodeId
        ? `/nodes/${encodeURIComponent(nodeId)}/streams/${encodeURIComponent(id)}/${action}`
        : `/streams/${encodeURIComponent(id)}/${action}`,
      { method: 'POST' },
    ),
  cancel: (id: string) => request<Operation>(`/operations/${encodeURIComponent(id)}`, { method: 'DELETE' }),
  jobs: () => request<Job[]>('/jobs'),
  jobDetail: (id: string) => request<JobDetail>(`/jobs/${encodeURIComponent(id)}/detail`),
  jobVersions: (id: string) => request<JobVersion[]>(`/jobs/${encodeURIComponent(id)}/versions`),
  validateJob: (spec: unknown, nodeIds: string[] = []) =>
    request<JobValidation>('/jobs/validate', {
      method: 'POST',
      body: JSON.stringify({ spec, node_ids: nodeIds }),
    }),
  createJob: (spec: unknown, nodeIds: string[] = []) =>
    request<Job>('/jobs', {
      method: 'POST',
      body: JSON.stringify({ spec, node_ids: nodeIds, desired_state: 'stopped' }),
    }),
  setJobState: (id: string, state: 'running' | 'stopped') =>
    request<Job>(`/jobs/${encodeURIComponent(id)}/desired-state`, {
      method: 'PUT',
      body: JSON.stringify({ state }),
    }),
  checkpoint: (id: string) => request<Job>(`/jobs/${encodeURIComponent(id)}/checkpoints`, { method: 'POST' }),
  savepoint: (id: string) => request<Job>(`/jobs/${encodeURIComponent(id)}/savepoints`, { method: 'POST' }),
  upgradeJob: (
    id: string,
    spec: unknown,
    savepointId: string,
    expectedGeneration: number,
    nodeIds: string[] = [],
  ) =>
    request<JobUpgrade>(`/jobs/${encodeURIComponent(id)}/upgrades`, {
      method: 'POST',
      body: JSON.stringify({
        spec,
        node_ids: nodeIds,
        expected_generation: expectedGeneration,
        savepoint_id: savepointId,
      }),
    }),
  upgradeJobAtomic: (id: string, spec: unknown, expectedGeneration: number, nodeIds: string[] = []) =>
    request<JobUpgrade>(`/jobs/${encodeURIComponent(id)}/upgrades`, {
      method: 'POST',
      body: JSON.stringify({
        spec,
        node_ids: nodeIds,
        expected_generation: expectedGeneration,
        mode: 'atomic',
      }),
    }),
  jobUpgrade: (id: string, upgradeId: string) =>
    request<JobUpgradeOrchestration>(
      `/jobs/${encodeURIComponent(id)}/upgrades/${encodeURIComponent(upgradeId)}`,
    ),
  jobUpgradeAction: (id: string, upgradeId: string, action: 'pause' | 'resume' | 'cancel' | 'rollback') =>
    request<JobUpgradeOrchestration>(
      `/jobs/${encodeURIComponent(id)}/upgrades/${encodeURIComponent(upgradeId)}/actions`,
      { method: 'POST', body: JSON.stringify({ action }) },
    ),
  rollbackJobUpgrade: (id: string, upgradeId = 'manual') =>
    request<Job>(`/jobs/${encodeURIComponent(id)}/upgrades/${encodeURIComponent(upgradeId)}/rollback`, {
      method: 'POST',
    }),
  rollouts: () => request<Rollout[]>('/rollouts'),
  rollout: (id: string) => request<RolloutDetail>(`/rollouts/${encodeURIComponent(id)}`),
  createRollout: (configVersion: string, nodeIds: string[], batchSize: number) =>
    request<Rollout>('/rollouts', {
      method: 'POST',
      body: JSON.stringify({ config_version: configVersion, node_ids: nodeIds, batch_size: batchSize }),
    }),
  rolloutAction: (id: string, action: 'pause' | 'resume' | 'cancel' | 'rollback', configVersion?: string) =>
    request<Rollout>(`/rollouts/${encodeURIComponent(id)}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action, ...(configVersion ? { config_version: configVersion } : {}) }),
    }),
  audit: (resourceId?: string) =>
    request<Page<AuditRecord>>(`/audit${resourceId ? `?resource_id=${encodeURIComponent(resourceId)}` : ''}`),
}

export function streamEvents(
  onEvent: (event: ControlEvent) => void,
  onState?: (state: 'connected' | 'disconnected') => void,
  nodeId?: string,
): AbortController {
  const controller = new AbortController()
  const path = `/events/stream${nodeId ? `?node_id=${encodeURIComponent(nodeId)}` : ''}`
  // Exponential backoff with a cap: a Hub outage must not become a
  // reconnect storm, and one clean reconnect resets the ladder.
  const SSE_BACKOFF_FLOOR_MS = 1_000
  const SSE_BACKOFF_MAX_MS = 30_000
  let backoffMs = SSE_BACKOFF_FLOOR_MS
  void (async () => {
    let lastEventId: string | undefined
    while (!controller.signal.aborted) {
      try {
        const response = await fetch(`${base}${path}`, {
          headers: {
            Accept: 'text/event-stream',
            ...(token ? { Authorization: `Bearer ${token}` } : {}),
            ...(lastEventId ? { 'Last-Event-ID': lastEventId } : {}),
          },
          signal: controller.signal,
        })
        if (!response.ok || !response.body) {
          throw new Error(translate(currentLocale(), 'api.sseConnectionFailed', { status: response.status }))
        }
        onState?.('connected')
        backoffMs = SSE_BACKOFF_FLOOR_MS
        const reader = response.body.getReader()
        const decoder = new TextDecoder()
        let buffer = ''
        let eventType = 'message'
        let data = ''
        const emit = () => {
          if (!data) return
          if (eventType !== 'resync') {
            try {
              onEvent(JSON.parse(data) as ControlEvent)
            } catch {
              /* bounded server payload; ignore malformed frames */
            }
          }
          data = ''
          eventType = 'message'
        }
        while (!controller.signal.aborted) {
          const next = await reader.read()
          if (next.done) break
          buffer += decoder.decode(next.value, { stream: true })
          const frames = buffer.split(/\r?\n\r?\n/)
          buffer = frames.pop() ?? ''
          for (const frame of frames) {
            for (const line of frame.split(/\r?\n/)) {
              if (line.startsWith('id:')) lastEventId = line.slice(3).trim()
              if (line.startsWith('event:')) eventType = line.slice(6).trim()
              if (line.startsWith('data:')) data += line.slice(5).trim()
            }
            emit()
          }
        }
      } catch {
        if (!controller.signal.aborted) onState?.('disconnected')
      }
      if (!controller.signal.aborted) {
        await new Promise((resolve) => window.setTimeout(resolve, backoffMs))
        backoffMs = Math.min(backoffMs * 2, SSE_BACKOFF_MAX_MS)
      }
    }
  })()
  return controller
}

export async function waitForOperation(id: string): Promise<Operation> {
  for (let attempt = 0; attempt < 30; attempt += 1) {
    const operation = await api.operation(id)
    const terminalIntent = ['converged', 'blocked', 'superseded'].includes(operation.intent_state ?? '')
    const terminalState = ['succeeded', 'failed', 'cancelled', 'timed_out', 'node_unavailable'].includes(
      operation.state,
    )
    if (terminalIntent || terminalState) {
      if (
        (operation.intent_state && operation.intent_state !== 'converged') ||
        (!operation.intent_state && operation.state !== 'succeeded')
      ) {
        throw new Error(
          operation.error ??
            translate(currentLocale(), 'api.operationState', {
              state: operation.intent_state ?? operation.state,
            }),
        )
      }
      return operation
    }
    await new Promise((resolve) => window.setTimeout(resolve, 250))
  }
  throw new Error(translate(currentLocale(), 'api.operationTimedOut'))
}

// --- Read-only configuration reports in local and Hub mode ---------------
//
// Local control planes answer validation and diff synchronously with the
// report; the Hub dispatches a read-only node command and the report rides
// the terminal operation's `result`. Both helpers accept either shape.

function isOperation(value: unknown): value is Operation {
  return (
    typeof value === 'object' &&
    value !== null &&
    'operation' in value &&
    'progress' in value &&
    !('errors' in value) &&
    !('changed' in value)
  )
}

export async function resolveValidation(
  candidate: ConfigCandidate,
  nodeId?: string,
): Promise<ConfigValidationReport> {
  const response = await api.validateConfig(candidate, nodeId)
  if (!isOperation(response)) return response
  const operation = await waitForOperation(operationId(response))
  return unwrapReport<ConfigValidationReport>(operation, 'validation report')
}

export async function resolveDiff(from: string, to: string, nodeId?: string): Promise<ConfigDiff> {
  const response = await api.diff(from, to, nodeId)
  if (!isOperation(response)) return response
  const operation = await waitForOperation(operationId(response))
  return unwrapReport<ConfigDiff>(operation, 'version diff')
}

function operationId(operation: Operation): string {
  return operation.id
}

/** A tracked read-only command must deliver its report on `result`; an
 * operation that settles without one (e.g. an Agent too old to know the
 * command) gets a named error instead of a downstream TypeError. */
function unwrapReport<T>(operation: Operation, what: string): T {
  if (operation.result === undefined || operation.result === null) {
    throw new Error(translate(currentLocale(), 'api.reportMissing', { what }))
  }
  return operation.result as T
}

// --- OIDC browser login integration -------------------------------------

export interface OidcStatus {
  login_enabled: boolean
  authenticated: boolean
  principal: { id: string; roles: string[] } | null
}

let oidcProbe: Promise<OidcStatus> | null = null

/** Test hook: clears the cached OIDC status probe. */
export function resetOidcStatusCacheForTests(): void {
  oidcProbe = null
}

/** Probes (once) whether the Hub offers the OIDC browser login flow and
 * whether the current session cookie is still valid. */
export function oidcStatus(): Promise<OidcStatus> {
  oidcProbe ??= fetch(`${base}/auth/oidc/status`)
    .then((response) =>
      response.ok ? response.json() : { login_enabled: false, authenticated: false, principal: null },
    )
    .catch(() => ({ login_enabled: false, authenticated: false, principal: null })) as Promise<OidcStatus>
  return oidcProbe
}

const REDIRECT_GUARD_MS = 10_000

/** Redirects the browser to the Hub OIDC login endpoint, at most once per
 * guard window so a misconfigured deployment cannot loop. Never redirects
 * when the Hub has no OIDC login flow configured. */
export async function redirectToOidcLogin(): Promise<void> {
  const status = await oidcStatus()
  if (!status.login_enabled) return
  const guard = 'arkflow_oidc_redirected_at'
  const last = Number(sessionStorage.getItem(guard) ?? 0)
  if (Date.now() - last < REDIRECT_GUARD_MS) return
  sessionStorage.setItem(guard, String(Date.now()))
  window.location.assign(`${base}/auth/oidc/login`)
}

/** Ends the browser session server-side and reloads the console. */
export async function oidcLogout(): Promise<void> {
  await fetch(`${base}/auth/oidc/logout`)
  sessionStorage.removeItem('arkflow_oidc_redirected_at')
  window.location.reload()
}
