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

// List queries pass the backend's own parameter names straight through, so
// the URL state, the fetch, and the API contract stay one vocabulary.
export type EventListQuery = {
  node_id?: string
  event_type?: string
  stream_id?: string
  correlation_id?: string
  page?: number
  page_size?: number
}
export type OperationListQuery = {
  node_id?: string
  resource_id?: string
  operation?: string
  state?: string
  correlation_id?: string
  page?: number
  page_size?: number
}
export type AuditListQuery = { resource_id?: string; page?: number; page_size?: number }

function withQuery(path: string, query: Record<string, string | number | undefined>): string {
  const params = new URLSearchParams()
  for (const [key, value] of Object.entries(query)) {
    if (value !== undefined && value !== '') params.set(key, String(value))
  }
  const search = params.toString()
  return search ? `${path}?${search}` : path
}
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
  approval_required?: boolean
}
/** Operator actions the control plane can authorize, mirroring the v1 wire
 * contract (serde snake_case). The echo lists what the caller may do. */
export type OperatorAction =
  | 'read'
  | 'operate'
  | 'configure'
  | 'manage_nodes'
  | 'manage_rollouts'
  | 'manage_webhooks'
  | 'approve_rollouts'
  | 'read_audit'
  | 'manage_users'

/** The `/system` identity echo: who is calling, their roles, and the actions
 * the same enforcement predicate grants them. Absent on older control
 * planes — the console then stays ungated rather than locking everything. */
export type PrincipalEcho = { id: string; roles: string[]; actions: OperatorAction[] }

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
  principal?: PrincipalEcho | null
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
  labels?: Record<string, string>
}
// Webhook alerting subscription (see the `webhook-alerting` capability).
// The signing secret is write-only: responses only report `has_secret`.
export type WebhookSubscription = {
  id: string
  url: string
  event_types: string[]
  enabled: boolean
  created_by: string | null
  created_at_ms: number
  has_secret: boolean
}
// Client-side form state: the API takes the same shape without
// `has_secret`/`created_*` (full-replace semantics).
export type WebhookSubscriptionInput = {
  id: string
  url: string
  event_types: string[]
  enabled: boolean
  secret: string | null
}
export type WebhookTestOutcome = {
  delivered: boolean
  status?: number
  error?: string
}
/** Roles in the Hub user directory; `admin` is the only role that holds
 * `manage_users`. */
export type UserRole = 'admin' | 'operator' | 'viewer'
/** A resource-type scope, optionally narrowed to one resource id. */
export type ResourceScope = { resource_type: string; resource_id?: string }
/** A managed directory user. Responses carry metadata only: the credential
 * secrets and their hashes are never part of the wire contract. */
export type User = {
  user_id: string
  display_name: string
  role: UserRole | string
  scopes: ResourceScope[]
  disabled: boolean
  oidc_subject: string | null
  tokens_valid_after_ms: number
  created_at_ms: number
  updated_at_ms: number
}
/** Credential metadata; plaintext tokens exist only in the issuance/rotation
 * response (`IssuedCredential.token`). */
export type CredentialView = {
  credential_id: string
  label: string
  created_at_ms: number
  expires_at_ms: number | null
  last_used_at_ms: number | null
  revoked_at_ms: number | null
}
/** The one and only response that carries a plaintext credential token. */
export type IssuedCredential = { credential: CredentialView; token: string }
export type RevokedTokens = { revoked_sessions: number }
export type CreateUserInput = {
  user_id: string
  display_name?: string
  role: UserRole
  scopes?: ResourceScope[]
  oidc_subject?: string
}
/** Partial update: omitted fields are left unchanged; an empty `oidc_subject`
 * clears the directory binding. */
export type UpdateUserInput = {
  display_name?: string
  role?: UserRole
  scopes?: ResourceScope[]
  disabled?: boolean
  oidc_subject?: string
}
/** Optional credential issuance parameters: `expires_at_ms` is absolute. */
export type IssueCredentialInput = { label?: string; expires_at_ms?: number }
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
export type ConfigApproval = {
  request_id: string
  config_version_id: string
  targets: string[]
  batch_size: number
  requester_id: string
  status: 'pending' | 'approving' | 'approved' | 'rejected' | 'expired'
  approver_id?: string
  decided_at_ms?: number
  rollout_id?: string
  correlation_id: string
  expires_at_ms: number
  created_at_ms: number
  updated_at_ms: number
  failure_note?: string
}
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
/** Durable Job pause state machine phase. `paused` is the settled state;
 * `capturing`/`stopping` mark a pause in flight and `resuming` means the Job
 * is being brought back. Absent on Jobs that were never paused. */
export type JobPausePhase = 'capturing' | 'stopping' | 'paused' | 'resuming'
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
  /** Present only while the Job carries durable pause bookkeeping. */
  pause_phase?: JobPausePhase
  /** Recovery artifact (checkpoint/savepoint) captured at the pause generation. */
  pause_artifact_id?: string
  /** Recovery pointer before the pause captured its artifact. */
  pause_prev_checkpoint_id?: string
  /** Bounded capture re-dispatch counter (additive on the wire). */
  pause_retry_count?: number
  paused_at_ms?: number
  paused_by?: string
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

/** True when a mutation was rejected for authorization (403): show the
 * permission degradation instead of a raw server error, and never retry. */
export function isPermissionError(cause: unknown): boolean {
  return (cause as ApiError | null)?.status === 403
}

export function errorMessage(cause: unknown): string {
  if (typeof cause === 'object' && cause && 'message' in cause) {
    const error = cause as ApiError
    if (isPermissionError(error)) {
      return translate(currentLocale(), 'perm.denied', { message: error.message })
    }
    return `${error.message}${error.correlation_id ? ` (ref ${error.correlation_id})` : ''}`
  }
  return cause instanceof Error ? cause.message : translate(currentLocale(), 'api.controlApiUnavailable')
}

export function formatTime(value?: number): string {
  return value ? new Date(value).toLocaleString(intlLocale(currentLocale())) : '—'
}

const base = import.meta.env.VITE_API_BASE ?? '/api/v1'

// --- Runtime bearer credential -----------------------------------------
//
// `VITE_API_TOKEN` is only the optional build-time default (it is inlined in
// the public bundle, so it is not a secret store). Operators type a bearer
// token at runtime instead; it is kept in sessionStorage so it never outlives
// the browser tab and is never read by a later session (unlike localStorage).
export const RUNTIME_TOKEN_STORAGE_KEY = 'arkflow.console.api_token'

function storedRuntimeToken(): string | null {
  try {
    return sessionStorage.getItem(RUNTIME_TOKEN_STORAGE_KEY)
  } catch {
    return null
  }
}

let runtimeToken: string | null = storedRuntimeToken()

/** The bearer token sent with every request: a runtime-entered token wins
 * over the optional build-time default. `null` means unauthenticated. */
export function currentBearerToken(): string | null {
  const envToken = import.meta.env.VITE_API_TOKEN
  return runtimeToken ?? (envToken ? envToken : null)
}

/** Where the active bearer credential comes from, for the Settings panel. */
export function bearerTokenSource(): 'runtime' | 'environment' | 'none' {
  if (runtimeToken) return 'runtime'
  return import.meta.env.VITE_API_TOKEN ? 'environment' : 'none'
}

/** Replaces (or, with null/empty, clears) the runtime bearer token. The next
 * request uses it immediately; callers refetch to retry the failed call. */
export function setRuntimeBearerToken(value: string | null): void {
  runtimeToken = value && value.trim() ? value.trim() : null
  try {
    if (runtimeToken) sessionStorage.setItem(RUNTIME_TOKEN_STORAGE_KEY, runtimeToken)
    else sessionStorage.removeItem(RUNTIME_TOKEN_STORAGE_KEY)
  } catch {
    // Storage unavailable: the token still works for this page lifetime.
  }
}

export type AuthChallenge = {
  /** The console must ask for a credential before it can continue. */
  required: boolean
  /** A credential was sent and rejected, as opposed to none being sent. */
  tokenRejected: boolean
}

const NO_AUTH_CHALLENGE: AuthChallenge = { required: false, tokenRejected: false }

let authChallenge: AuthChallenge = NO_AUTH_CHALLENGE
const authChallengeListeners = new Set<(challenge: AuthChallenge) => void>()

function publishAuthChallenge(next: AuthChallenge): void {
  if (authChallenge.required === next.required && authChallenge.tokenRejected === next.tokenRejected) return
  authChallenge = next
  for (const listener of authChallengeListeners) listener(next)
}

export function currentAuthChallenge(): AuthChallenge {
  return authChallenge
}

export function subscribeAuthChallenge(listener: (challenge: AuthChallenge) => void): () => void {
  authChallengeListeners.add(listener)
  listener(authChallenge)
  return () => {
    authChallengeListeners.delete(listener)
  }
}

/** Dismisses the token prompt once a credential has been accepted. */
export function clearAuthChallenge(): void {
  publishAuthChallenge(NO_AUTH_CHALLENGE)
}

/** Test hook: forgets the cached challenge and its listeners. */
export function resetAuthChallengeForTests(): void {
  authChallengeListeners.clear()
  authChallenge = NO_AUTH_CHALLENGE
}

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
  const token = currentBearerToken()
  // The timeout signal covers the FULL exchange: fetch resolves on headers,
  // and a stalled body rejects `response.json()` with the same
  // TimeoutError, so every read stays inside the translated window.
  const timeoutError = () =>
    Object.assign(new Error(translate(currentLocale(), 'api.requestTimeout')), {
      code: 'request_timeout',
      correlation_id: correlationId,
      status: 0,
    })
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
      throw timeoutError()
    }
    throw error
  }
  let body: unknown
  try {
    if (!response.ok) {
      // A 401 has two recoveries: the Hub's OIDC browser login when it is
      // enabled (unchanged), otherwise the console asks for a runtime bearer
      // token. A sent-but-rejected token never redirects away from the prompt.
      if (response.status === 401) void handleUnauthorized(Boolean(token))
      // A stalled error-body read rejects with the shared TimeoutError —
      // it must surface as request_timeout, not be flattened into an empty
      // body that reports a generic request_failed.
      const errorBody = (await response.json().catch((error: unknown) => {
        if (error instanceof DOMException && error.name === 'TimeoutError') throw error
        return {}
      })) as Partial<ApiError>
      throw Object.assign(
        new Error(
          errorBody.message ?? translate(currentLocale(), 'api.requestFailed', { status: response.status }),
        ),
        {
          code: errorBody.code ?? 'request_failed',
          field: errorBody.field,
          stream_id: errorBody.stream_id,
          correlation_id:
            errorBody.correlation_id ?? response.headers.get('x-correlation-id') ?? correlationId,
          status: response.status,
        },
      )
    }
    // A successful call proves the active credential works; drop the prompt.
    if (authChallenge.required) clearAuthChallenge()
    body =
      response.status === 204
        ? undefined
        : await response.json().catch((error: unknown) => {
            // Idempotent mutations (DELETE) may answer 200 with no body; that
            // is a success, not a parse failure.
            if (error instanceof SyntaxError) return undefined
            throw error
          })
  } catch (error) {
    if (error instanceof DOMException && error.name === 'TimeoutError') {
      throw timeoutError()
    }
    throw error
  }
  return body as T
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
  nodes: (page = 1, pageSize = DEFAULT_PAGE_SIZE, label?: string) =>
    request<Page<ControlNode>>(
      `/nodes?page=${page}&page_size=${pageSize}${label ? `&label=${encodeURIComponent(label)}` : ''}`,
    ),
  replaceNodeLabels: (id: string, labels: Record<string, string>) =>
    request<{ node_id: string; labels: Record<string, string> }>(`/nodes/${encodeURIComponent(id)}/labels`, {
      method: 'PUT',
      body: JSON.stringify({ labels }),
    }),
  webhooks: () => request<{ subscriptions: WebhookSubscription[] }>('/webhooks'),
  replaceWebhooks: (subscriptions: WebhookSubscriptionInput[]) =>
    request<{ subscriptions: WebhookSubscription[] }>('/webhooks', {
      method: 'PUT',
      body: JSON.stringify({ subscriptions }),
    }),
  testWebhook: (id: string) =>
    request<WebhookTestOutcome>(`/webhooks/${encodeURIComponent(id)}/test`, {
      method: 'POST',
    }),
  drainNode: (id: string) =>
    request<ControlNode>(`/nodes/${encodeURIComponent(id)}/drain`, { method: 'POST' }),
  maintainNode: (id: string) =>
    request<ControlNode>(`/nodes/${encodeURIComponent(id)}/maintenance`, { method: 'POST' }),
  resumeNode: (id: string) =>
    request<ControlNode>(`/nodes/${encodeURIComponent(id)}/maintenance`, { method: 'DELETE' }),
  streams: (nodeId?: string) =>
    request<Page<StreamStatus>>(`/streams${nodeId ? `?node_id=${encodeURIComponent(nodeId)}` : ''}`),
  events: (query: EventListQuery = {}) => request<Page<ControlEvent>>(withQuery('/events', query)),
  operations: (query: OperationListQuery = {}) => request<Page<Operation>>(withQuery('/operations', query)),
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
  /** Durable pause: captures the recovery artifact, then stops the Job (202).
   * The Hub returns the updated Job as soon as the pause intent is recorded. */
  pauseJob: (id: string) => request<Job>(`/jobs/${encodeURIComponent(id)}/actions/pause`, { method: 'POST' }),
  /** Resume a paused Job from its pause artifact (202). The generation is
   * bumped by the Hub; events after the artifact barrier are replayed. */
  resumeJob: (id: string) =>
    request<Job>(`/jobs/${encodeURIComponent(id)}/actions/resume`, { method: 'POST' }),
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
  createRollout: (
    configVersion: string,
    nodeIds: string[],
    batchSize: number,
    targetSelector?: Record<string, string>,
  ) =>
    request<Rollout>('/rollouts', {
      method: 'POST',
      body: JSON.stringify(
        targetSelector
          ? { config_version: configVersion, target_selector: targetSelector, batch_size: batchSize }
          : { config_version: configVersion, node_ids: nodeIds, batch_size: batchSize },
      ),
    }),
  configApprovals: (status?: string) =>
    request<ConfigApproval[]>(`/config-approvals${status ? `?status=${encodeURIComponent(status)}` : ''}`),
  configApproval: (id: string) => request<ConfigApproval>(`/config-approvals/${encodeURIComponent(id)}`),
  submitConfigApproval: (
    configVersion: string,
    nodeIds: string[],
    batchSize: number,
    targetSelector?: Record<string, string>,
  ) =>
    request<ConfigApproval>('/config-approvals', {
      method: 'POST',
      body: JSON.stringify(
        targetSelector
          ? { config_version: configVersion, target_selector: targetSelector, batch_size: batchSize }
          : { config_version: configVersion, node_ids: nodeIds, batch_size: batchSize },
      ),
    }),
  decideConfigApproval: (id: string, decision: 'approve' | 'reject', note?: string) =>
    request<ConfigApproval>(`/config-approvals/${encodeURIComponent(id)}/decide`, {
      method: 'POST',
      body: JSON.stringify({ decision, note }),
    }),
  rolloutAction: (id: string, action: 'pause' | 'resume' | 'cancel' | 'rollback', configVersion?: string) =>
    request<Rollout>(`/rollouts/${encodeURIComponent(id)}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action, ...(configVersion ? { config_version: configVersion } : {}) }),
    }),
  audit: (query: AuditListQuery = {}) => request<Page<AuditRecord>>(withQuery('/audit', query)),
  // --- Managed users & credentials (Hub only, `manage_users`) -------------
  users: () => request<User[]>('/users'),
  createUser: (input: CreateUserInput) =>
    request<User>('/users', { method: 'POST', body: JSON.stringify(input) }),
  updateUser: (id: string, input: UpdateUserInput) =>
    request<User>(`/users/${encodeURIComponent(id)}`, { method: 'PATCH', body: JSON.stringify(input) }),
  deleteUser: (id: string) => request<void>(`/users/${encodeURIComponent(id)}`, { method: 'DELETE' }),
  userCredentials: (id: string) => request<CredentialView[]>(`/users/${encodeURIComponent(id)}/credentials`),
  /** Issues a credential; the plaintext `token` in the response is the only
   * time the secret leaves the Hub. */
  issueCredential: (userId: string, input: IssueCredentialInput = {}) =>
    request<IssuedCredential>(`/users/${encodeURIComponent(userId)}/credentials`, {
      method: 'POST',
      body: JSON.stringify(input),
    }),
  /** Rotates a credential: the old token stops working, the response carries
   * the replacement plaintext token once. */
  rotateCredential: (credentialId: string) =>
    request<IssuedCredential>(`/credentials/${encodeURIComponent(credentialId)}/rotate`, {
      method: 'POST',
    }),
  /** Idempotent revocation; succeeds even when the credential is already gone. */
  revokeCredential: (credentialId: string) =>
    request<unknown>(`/credentials/${encodeURIComponent(credentialId)}`, { method: 'DELETE' }),
  /** Invalidates every browser session for a user and bumps the validity
   * boundary, so already-issued tokens stop authenticating too. */
  revokeUserTokens: (userId: string) =>
    request<RevokedTokens>(`/users/${encodeURIComponent(userId)}/revoke-tokens`, { method: 'POST' }),
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
      let deliveredAnyEvent = false
      // Per-attempt abort: the connect deadline cancels a black-holed fetch
      // (a bare Promise.race would let it dangle until the caller's
      // cleanup), and the outer controller forwards cancellation through
      // the WHOLE attempt including body reads.
      const attempt = new AbortController()
      const forwardAbort = () => attempt.abort()
      controller.signal.addEventListener('abort', forwardAbort)
      try {
        const connectTimer = window.setTimeout(() => {
          attempt.abort()
        }, REQUEST_TIMEOUT_MS)
        let response: Response
        try {
          // Read the credential per attempt: a token switched at runtime
          // applies on the next reconnect without recreating the stream.
          const token = currentBearerToken()
          response = await fetch(`${base}${path}`, {
            headers: {
              Accept: 'text/event-stream',
              ...(token ? { Authorization: `Bearer ${token}` } : {}),
              ...(lastEventId ? { 'Last-Event-ID': lastEventId } : {}),
            },
            signal: attempt.signal,
          })
        } finally {
          window.clearTimeout(connectTimer)
        }
        if (!response.ok || !response.body) {
          throw new Error(translate(currentLocale(), 'api.sseConnectionFailed', { status: response.status }))
        }
        onState?.('connected')
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
              deliveredAnyEvent = true
            } catch {
              /* bounded server payload; ignore malformed frames */
            }
          }
          data = ''
          eventType = 'message'
        }
        while (!attempt.signal.aborted) {
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
        // Covers both real failures and the connect-deadline abort; outer
        // cleanup (controller aborted) reports nothing and exits the loop.
        if (!controller.signal.aborted) onState?.('disconnected')
      } finally {
        controller.signal.removeEventListener('abort', forwardAbort)
      }
      if (!controller.signal.aborted) {
        // Reset the ladder BEFORE the sleep when this connection actually
        // delivered an event (a healthy stream that drops reconnects after
        // the floor delay, not a stale multiplier); a connect-then-close
        // Hub with no frames keeps backing off instead of retrying every
        // second.
        if (deliveredAnyEvent) backoffMs = SSE_BACKOFF_FLOOR_MS
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

/** 401 recovery: the Hub's OIDC login flow keeps its redirect behaviour, but
 * when no browser login is configured (or the Hub rejected the token the
 * caller sent) the console asks for a runtime bearer token instead. */
async function handleUnauthorized(tokenWasSent: boolean): Promise<void> {
  if (!tokenWasSent) {
    const status = await oidcStatus()
    if (status.login_enabled) {
      await redirectToOidcLogin()
      return
    }
  }
  publishAuthChallenge({ required: true, tokenRejected: tokenWasSent })
}
