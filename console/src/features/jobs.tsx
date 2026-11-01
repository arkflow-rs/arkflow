import { useEffect, useMemo, useState } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'
import {
  api,
  ControlNode,
  errorMessage,
  formatTime,
  isPermissionError,
  Job,
  JobCheckpoint,
  JobDetail,
} from '../api'
import { currentLocale, intlLocale, useT } from '../i18n'
import { useConfirm } from './confirm'
import { SkeletonRows } from './shared'
import {
  useJobDetail,
  useJobVersions,
  useJobs,
  useMutationFailure,
  useNodes,
  useRollbackJobUpgrade,
} from '../queries'
import { JobEditor as VisualJobEditor } from './job-editor'

type JobsProps = {
  onError: (message: string) => void
  canMutate?: boolean
}

const pretty = (value: unknown) => JSON.stringify(value, null, 2)

/** Pause phases, mapped to their operator-facing dictionary keys so the en/zh
 * wording stays in the i18n tables rather than in this component. */
const PAUSE_PHASE_KEYS = {
  capturing: 'jobs.pausePhase.capturing',
  stopping: 'jobs.pausePhase.stopping',
  paused: 'jobs.pausePhase.paused',
  resuming: 'jobs.pausePhase.resuming',
} as const

/** A Job may only be paused once it is fully running and converged; pausing a
 * stopped/transitional Job is a 409 `job_not_running`. */
const canPauseJob = (job: Job) =>
  !job.pause_phase &&
  job.desired_state === 'running' &&
  job.observed_state === 'running' &&
  job.convergence === 'converged'

/** Surface the server problem code verbatim (a 409 `pause_conflict`, for
 * example) so operators can act on the exact contract code. */
function jobMutationError(cause: unknown): string {
  const code = (cause as { code?: unknown } | null)?.code
  const message = errorMessage(cause)
  return typeof code === 'string' && code !== '' && code !== 'request_failed'
    ? `${code}: ${message}`
    : message
}

/** Badge for the durable pause state machine; nothing renders for a Job that
 * was never paused (`pause_phase` is additive/optional on the wire). */
function PausePhaseBadge({ job }: { job: Job }) {
  const t = useT()
  const phase = job.pause_phase
  if (!phase) return null
  return (
    <span className={`state ${phase === 'paused' ? 'paused' : 'applying'}`}>
      {t(PAUSE_PHASE_KEYS[phase])}
    </span>
  )
}

/** The single pause/resume control: a settled pause offers Resume; an
 * in-flight pause or resume stays disabled until the state machine settles; a
 * plain Job offers Pause only when it is running and converged. */
function PauseResumeAction({
  job,
  canMutate,
  busy,
  onPause,
  onResume,
}: {
  job: Job
  canMutate: boolean
  busy: boolean
  onPause: () => void
  onResume: () => void
}) {
  const t = useT()
  const denied = !canMutate ? t('perm.requires', { action: 'operate' }) : undefined
  if (job.pause_phase === 'paused') {
    return (
      <button disabled={!canMutate || busy} title={denied} onClick={onResume}>
        {t('jobs.resume')}
      </button>
    )
  }
  if (job.pause_phase === 'resuming') {
    return (
      <button disabled title={t('jobs.resumeInProgress')}>
        {t('jobs.resume')}
      </button>
    )
  }
  if (job.pause_phase) {
    return (
      <button disabled title={t('jobs.pauseInProgress')}>
        {t('jobs.pause')}
      </button>
    )
  }
  const pausable = canPauseJob(job)
  return (
    <button
      disabled={!canMutate || busy || !pausable}
      title={denied ?? (pausable ? undefined : t('jobs.pauseNotRunning'))}
      onClick={onPause}
    >
      {t('jobs.pause')}
    </button>
  )
}

export function Jobs({ onError, canMutate = true }: JobsProps) {
  const t = useT()
  const queryClient = useQueryClient()
  const reportMutationFailure = useMutationFailure()
  const confirm = useConfirm()
  const jobsQuery = useJobs()
  const jobs = jobsQuery.data ?? []
  const nodes = useNodes().data?.items ?? []
  const [selectedJobId, setSelectedJobId] = useState<string>()
  const detailQuery = useJobDetail(selectedJobId)
  const selected = detailQuery.data
  const pollError = detailQuery.isError ? errorMessage(detailQuery.error) : ''
  const [filter, setFilter] = useState('')
  const [state, setState] = useState('all')
  const [plan, setPlan] = useState<{ jobId: string; version: number; planJson: string }>()
  const [editor, setEditor] = useState<{
    mode: 'create' | 'upgrade' | 'atomic'
    job?: Job
    savepoint?: JobCheckpoint
  }>()
  const [busy, setBusy] = useState('')
  const invalidateJobs = () => {
    void queryClient.invalidateQueries({ queryKey: ['live', 'jobs'] })
    void queryClient.invalidateQueries({ queryKey: ['live', 'job-detail'] })
  }
  const visible = useMemo(
    () =>
      jobs.filter(
        (job) =>
          (!filter ||
            `${job.job_id} ${job.node_ids.join(' ')}`.toLowerCase().includes(filter.toLowerCase())) &&
          (state === 'all' ||
            job.observed_state === state ||
            job.convergence === state ||
            (state === 'paused' && job.pause_phase === 'paused')),
      ),
    [jobs, filter, state],
  )
  const reportJobFailure = (cause: unknown) =>
    isPermissionError(cause) ? reportMutationFailure(onError, cause) : onError(jobMutationError(cause))
  const action = async (label: string, fn: () => Promise<unknown>) => {
    try {
      setBusy(label)
      await fn()
      setBusy('')
      invalidateJobs()
    } catch (cause) {
      setBusy('')
      reportJobFailure(cause)
    }
  }
  const setStateFor = async (job: Job) => {
    const stopping = job.desired_state === 'running'
    if (
      !(await confirm({
        title: t(stopping ? 'jobs.confirmStop' : 'jobs.confirmStart', { jobId: job.job_id }),
        confirmLabel: stopping ? t('jobs.stop') : t('jobs.start'),
      }))
    )
      return
    void action(t(stopping ? 'jobs.stopping' : 'jobs.starting'), async () => {
      await api.setJobState(job.job_id, stopping ? 'stopped' : 'running')
      toast.success(t('toast.accepted'))
    })
  }
  const pauseFor = async (job: Job) => {
    if (
      !(await confirm({
        title: t('jobs.confirmPause', { jobId: job.job_id }),
        body: t('jobs.confirmPauseBody'),
        confirmLabel: t('jobs.pause'),
      }))
    )
      return
    void action(t('jobs.pausing'), async () => {
      await api.pauseJob(job.job_id)
      toast.success(t('toast.accepted'))
    })
  }
  const resumeFor = async (job: Job) => {
    if (
      !(await confirm({
        title: t('jobs.confirmResume', { jobId: job.job_id }),
        body: t('jobs.confirmResumeBody'),
        confirmLabel: t('jobs.resume'),
      }))
    )
      return
    void action(t('jobs.resuming'), async () => {
      await api.resumeJob(job.job_id)
      toast.success(t('toast.accepted'))
    })
  }
  const stopPausedFor = async (job: Job) => {
    if (
      !(await confirm({
        title: t('jobs.confirmStop', { jobId: job.job_id }),
        body: t('jobs.confirmStopPausedBody'),
        confirmLabel: t('jobs.stop'),
      }))
    )
      return
    void action(t('jobs.stopping'), async () => {
      await api.setJobState(job.job_id, 'stopped')
      toast.success(t('toast.accepted'))
    })
  }

  return (
    <>
      <section className="panel">
        <div className="panel-title">
          <div>
            <h3>{t('jobs.title')}</h3>
            <span>{t('jobs.countSummary', { visible: visible.length, total: jobs.length })}</span>
          </div>
          <button disabled={!canMutate} onClick={() => setEditor({ mode: 'create' })}>
            {t('jobs.createJob')}
          </button>
        </div>
        <div className="toolbar">
          <input
            aria-label={t('jobs.filterLabel')}
            placeholder={t('jobs.filterPlaceholder')}
            value={filter}
            onChange={(event) => setFilter(event.target.value)}
          />
          <select
            aria-label={t('jobs.stateFilterLabel')}
            value={state}
            onChange={(event) => setState(event.target.value)}
          >
            <option value="all">{t('jobs.allStates')}</option>
            <option value="running">{t('jobs.state.running')}</option>
            <option value="stopped">{t('jobs.state.stopped')}</option>
            <option value="paused">{t('jobs.state.paused')}</option>
            <option value="failed">{t('jobs.state.failed')}</option>
            <option value="degraded">{t('jobs.state.degraded')}</option>
          </select>
        </div>
        {jobsQuery.isPending ? (
          <SkeletonRows rows={4} />
        ) : visible.length === 0 ? (
          <p className="empty">
            {t('jobs.empty')}
            {(filter || state !== 'all') && (
              <button
                onClick={() => {
                  setFilter('')
                  setState('all')
                }}
              >
                {t('common.clearFilters')}
              </button>
            )}
          </p>
        ) : (
          <div className="table">
            {visible.map((job) => (
              <div
                className={`row ${selected?.job.job_id === job.job_id ? 'selected' : ''}`}
                key={job.job_id}
              >
                <button className="link-button" onClick={() => setSelectedJobId(job.job_id)}>
                  <strong>{job.job_id}</strong>
                  <small>
                    {t('jobs.rowSummary', {
                      version: job.version,
                      generation: job.generation,
                      nodes: job.node_ids.join(', ') || t('jobs.automaticPlacement'),
                    })}
                  </small>
                </button>
                <div>
                  <span className={`state ${job.observed_state}`}>{job.observed_state}</span>
                  <PausePhaseBadge job={job} />
                  <small>
                    {t('jobs.desiredSummary', { desired: job.desired_state, convergence: job.convergence })}
                  </small>
                  {job.last_error && <small className="error-text">{job.last_error}</small>}
                </div>
                <div className="actions">
                  <PauseResumeAction
                    job={job}
                    canMutate={canMutate}
                    busy={!!busy}
                    onPause={() => void pauseFor(job)}
                    onResume={() => void resumeFor(job)}
                  />
                  <button
                    disabled={!canMutate || !!busy}
                    onClick={() => (job.pause_phase ? void stopPausedFor(job) : void setStateFor(job))}
                  >
                    {job.pause_phase || job.desired_state === 'running' ? t('jobs.stop') : t('jobs.start')}
                  </button>
                </div>
              </div>
            ))}
          </div>
        )}
      </section>
      {selected && (
        <JobDetailPanel
          detail={selected}
          pollError={pollError}
          nodes={nodes}
          canMutate={canMutate}
          busy={!!busy}
          onClose={() => setSelectedJobId(undefined)}
          onError={onError}
          onRefresh={invalidateJobs}
          onAction={action}
          onUpgrade={(savepoint) => setEditor({ mode: 'upgrade', job: selected.job, savepoint })}
          onAtomicUpgrade={() => setEditor({ mode: 'atomic', job: selected.job })}
          onViewPlan={(planJson) =>
            setPlan({ jobId: selected.job.job_id, version: selected.job.version, planJson })
          }
        />
      )}
      {editor && (
        <VisualJobEditor
          mode={editor.mode}
          job={editor.job}
          savepoint={editor.savepoint}
          nodes={nodes}
          busy={!!busy}
          canMutate={canMutate}
          onClose={() => setEditor(undefined)}
          onError={onError}
          onSaved={() => {
            setEditor(undefined)
            invalidateJobs()
          }}
          onRefresh={invalidateJobs}
          onAction={action}
        />
      )}
      {plan && (
        <PlanModal
          title={t('jobs.planTitle', { jobId: plan.jobId, version: plan.version })}
          content={plan.planJson}
          onClose={() => setPlan(undefined)}
        />
      )}
    </>
  )
}

export function ActiveUpgradeCard({
  job,
  upgrade,
  canMutate,
  busy,
  onAction,
  onDone,
}: {
  job: Job
  upgrade: NonNullable<JobDetail['active_upgrade']>
  canMutate: boolean
  busy: boolean
  onAction: (label: string, fn: () => Promise<unknown>) => Promise<void>
  onDone: () => void
}) {
  const t = useT()
  // `upgrade` is captured at render time: the parent polls `detail` every few
  // seconds and the orchestration may reach a terminal state between render
  // and click, so the handlers re-check instead of trusting it.
  const run = (label: string, action: 'pause' | 'resume' | 'cancel' | 'rollback') =>
    upgrade &&
    onAction(label, async () => {
      await api.jobUpgradeAction(job.job_id, upgrade.upgrade_id, action)
      onDone()
    })
  return (
    <div className="upgrade-progress">
      <h4>{t('jobs.upgradeProgressTitle')}</h4>
      <div className="version">
        <span>
          <strong>{upgrade.phase}</strong> ·{' '}
          {t('jobs.upgradeProgressVersions', { from: upgrade.from_version, to: upgrade.to_version })}
        </span>
        <span>
          {upgrade.savepoint_id
            ? t('jobs.upgradeSavepoint', { id: upgrade.savepoint_id })
            : t('jobs.upgradeSavepointPending')}
        </span>
        <span className="actions">
          {upgrade.phase === 'paused' ? (
            <button
              disabled={!canMutate || busy}
              title={!canMutate ? t('perm.requires', { action: 'operate' }) : undefined}
              onClick={() => void run(t('jobs.resumingUpgrade'), 'resume')}
            >
              {t('jobs.resumeUpgrade')}
            </button>
          ) : (
            <button
              disabled={!canMutate || busy}
              title={!canMutate ? t('perm.requires', { action: 'operate' }) : undefined}
              onClick={() => void run(t('jobs.pausingUpgrade'), 'pause')}
            >
              {t('jobs.pauseUpgrade')}
            </button>
          )}
          {upgrade.phase === 'verifying' && (
            <button
              disabled={!canMutate || busy}
              title={!canMutate ? t('perm.requires', { action: 'operate' }) : undefined}
              onClick={() => void run(t('jobs.rollingBackUpgrade'), 'rollback')}
            >
              {t('jobs.rollbackUpgrade')}
            </button>
          )}
          <button
            disabled={!canMutate || busy}
            title={!canMutate ? t('perm.requires', { action: 'operate' }) : undefined}
            onClick={() => void run(t('jobs.cancellingUpgrade'), 'cancel')}
          >
            {t('jobs.cancelUpgrade')}
          </button>
        </span>
      </div>
      {upgrade.last_error && <div className="error-row">{upgrade.last_error}</div>}
      <p>
        <small>{t('jobs.upgradeReplayNote')}</small>
      </p>
    </div>
  )
}

function JobDetailPanel({
  detail,
  pollError,
  canMutate,
  busy,
  onClose,
  onError,
  onRefresh,
  onAction,
  onUpgrade,
  onAtomicUpgrade,
  onViewPlan,
}: {
  detail: JobDetail
  pollError: string
  nodes: ControlNode[]
  canMutate: boolean
  busy: boolean
  onClose: () => void
  onError: (message: string) => void
  onRefresh: () => void
  onAction: (label: string, fn: () => Promise<unknown>) => Promise<void>
  onUpgrade: (savepoint: JobCheckpoint) => void
  onAtomicUpgrade: () => void
  onViewPlan: (planJson: string) => void
}) {
  const t = useT()
  const confirm = useConfirm()
  const [tab, setTab] = useState<'overview' | 'plan' | 'tasks' | 'recovery' | 'versions'>('overview')
  const checkpoints = detail.checkpoints ?? []
  const pausing = Boolean(detail.job.pause_phase)
  const runArtifact = (kind: 'checkpoint' | 'savepoint') =>
    void onAction(kind === 'checkpoint' ? t('jobs.checkpointing') : t('jobs.creatingSavepoint'), async () => {
      if (kind === 'checkpoint') await api.checkpoint(detail.job.job_id)
      else await api.savepoint(detail.job.job_id)
      onRefresh()
    })
  const pauseJob = async () => {
    if (
      !(await confirm({
        title: t('jobs.confirmPause', { jobId: detail.job.job_id }),
        body: t('jobs.confirmPauseBody'),
        confirmLabel: t('jobs.pause'),
      }))
    )
      return
    void onAction(t('jobs.pausing'), async () => {
      await api.pauseJob(detail.job.job_id)
      onRefresh()
    })
  }
  const resumeJob = async () => {
    if (
      !(await confirm({
        title: t('jobs.confirmResume', { jobId: detail.job.job_id }),
        body: t('jobs.confirmResumeBody'),
        confirmLabel: t('jobs.resume'),
      }))
    )
      return
    void onAction(t('jobs.resuming'), async () => {
      await api.resumeJob(detail.job.job_id)
      onRefresh()
    })
  }
  const stopJob = async () => {
    if (
      !(await confirm({
        title: t('jobs.confirmStop', { jobId: detail.job.job_id }),
        body: pausing ? t('jobs.confirmStopPausedBody') : undefined,
        confirmLabel: t('jobs.stop'),
      }))
    )
      return
    void onAction(t('jobs.stopping'), async () => {
      await api.setJobState(detail.job.job_id, 'stopped')
      onRefresh()
    })
  }
  const tabLabels = {
    overview: t('jobs.tabOverview'),
    plan: t('jobs.tabPlan'),
    tasks: t('jobs.tabTasks'),
    recovery: t('jobs.tabRecovery'),
    versions: t('jobs.tabVersions'),
  }
  return (
    <section className="panel detail">
      <div className="panel-title">
        <div>
          <span className="eyebrow">{t('jobs.detailEyebrow')}</span>
          <h3>{detail.job.job_id}</h3>
          <small>
            {t('jobs.versionGeneration', { version: detail.job.version, generation: detail.job.generation })}
          </small>
        </div>
        <div className="actions">
          <button onClick={onClose}>{t('jobs.close')}</button>
          <button disabled={!canMutate || busy} onClick={() => runArtifact('checkpoint')}>
            {t('jobs.checkpoint')}
          </button>
          <button disabled={!canMutate || busy} onClick={() => runArtifact('savepoint')}>
            {t('jobs.savepoint')}
          </button>
          <PauseResumeAction
            job={detail.job}
            canMutate={canMutate}
            busy={busy}
            onPause={() => void pauseJob()}
            onResume={() => void resumeJob()}
          />
          {detail.job.desired_state === 'running' && !pausing && (
            <button disabled={!canMutate || busy} onClick={onAtomicUpgrade}>
              {t('jobs.atomicUpgrade')}
            </button>
          )}
          {(detail.job.desired_state === 'running' || pausing) && (
            <button disabled={!canMutate || busy} onClick={() => void stopJob()}>
              {t('jobs.stop')}
            </button>
          )}
        </div>
      </div>
      {pollError && <div className="warning">{t('jobs.livePaused', { error: pollError })}</div>}
      <div className="job-tabs">
        {(['overview', 'plan', 'tasks', 'recovery', 'versions'] as const).map((value) => (
          <button className={tab === value ? 'active' : ''} key={value} onClick={() => setTab(value)}>
            {tabLabels[value]}
          </button>
        ))}
      </div>
      {tab === 'overview' && (
        <>
          <div className="detail-grid">
            <div>
              <span className={`state ${detail.job.observed_state}`}>{detail.job.observed_state}</span>
              <PausePhaseBadge job={detail.job} />
              <p>
                {t('jobs.desiredLabel')} <strong>{detail.job.desired_state}</strong> ·{' '}
                {t('jobs.convergenceLabel')} <strong>{detail.job.convergence}</strong>
              </p>
              {detail.job.pause_phase && (
                <>
                  <p>
                    {t('jobs.pauseArtifactLabel')}{' '}
                    <strong>{detail.job.pause_artifact_id ?? t('jobs.noneValue')}</strong>
                  </p>
                  <p>
                    {t('jobs.pausedAtLabel')} <strong>{formatTime(detail.job.paused_at_ms)}</strong>
                    {detail.job.paused_by ? ` · ${t('jobs.pausedByLabel')} ${detail.job.paused_by}` : ''}
                  </p>
                </>
              )}
              <p>
                {t('jobs.nodesLabel')}{' '}
                <strong>
                  {detail.nodes.map((node) => node.id).join(', ') || t('jobs.automaticPlacement')}
                </strong>
              </p>
              <p>
                {t('jobs.latestRecoveryLabel')}{' '}
                <strong>{detail.job.checkpoint_id ?? t('jobs.noneValue')}</strong>
              </p>
              {detail.job.last_error && <div className="error-row">{detail.job.last_error}</div>}
            </div>
            <div className="metric-list">
              {Object.entries(detail.metrics ?? {}).length ? (
                Object.entries(detail.metrics ?? {}).map(([key, value]) => (
                  <div className="metric" key={key}>
                    <span>{key.replaceAll('_', ' ')}</span>
                    <strong>
                      {typeof value === 'number'
                        ? value.toLocaleString(intlLocale(currentLocale()))
                        : String(value)}
                    </strong>
                  </div>
                ))
              ) : (
                <p className="empty">{t('jobs.noMeasuredMetrics')}</p>
              )}
            </div>
          </div>
          {detail.active_upgrade && (
            <ActiveUpgradeCard
              job={detail.job}
              upgrade={detail.active_upgrade}
              canMutate={canMutate}
              busy={busy}
              onAction={onAction}
              onDone={onRefresh}
            />
          )}
          <h4>{t('jobs.nodeCompatibility')}</h4>
          {detail.nodes.length ? (
            detail.nodes.map((node) => (
              <div className="version" key={node.id}>
                <span>
                  <strong>{node.id}</strong> · {node.state}
                </span>
                <small>{node.capabilities.join(' · ')}</small>
              </div>
            ))
          ) : (
            <p className="empty">{t('jobs.noAssignedNodes')}</p>
          )}
        </>
      )}
      {tab === 'plan' && <pre className="schema job-plan">{pretty(detail.plan)}</pre>}
      {tab === 'tasks' && (
        <div className="table">
          {detail.tasks.length ? (
            detail.tasks.map((task, index) => (
              <div className="row" key={task.id || task.task_id || index}>
                <div>
                  <strong>{task.task_id || `task-${index}`}</strong>
                  <small>
                    {t('jobs.taskPlacement', {
                      node: task.observed_node_id ?? task.node_id ?? t('jobs.unassignedValue'),
                    })}
                  </small>
                </div>
                <span className={`state ${pausing ? 'paused' : (task.state ?? 'queued')}`}>
                  {pausing ? t('jobs.taskNotObservedDuringPause') : (task.state ?? 'queued')}
                  {!pausing && task.observed === false && ` · ${t('jobs.taskNotObserved')}`}
                </span>
                <small>
                  {t('jobs.taskGeneration', { generation: String(task.generation ?? detail.job.generation) })}
                </small>
              </div>
            ))
          ) : (
            <p className="empty">{t('jobs.noTasks')}</p>
          )}
        </div>
      )}
      {tab === 'recovery' && (
        <>
          <div className="actions recovery-actions">
            <button disabled={!canMutate || busy} onClick={() => runArtifact('checkpoint')}>
              {t('jobs.createCheckpoint')}
            </button>
            <button disabled={!canMutate || busy} onClick={() => runArtifact('savepoint')}>
              {t('jobs.createSavepoint')}
            </button>
          </div>
          {checkpoints.length ? (
            checkpoints.map((checkpoint) => (
              <div className="version" key={checkpoint.checkpoint_id}>
                <span>
                  <strong>{checkpoint.checkpoint_id}</strong> · {checkpoint.kind} · v{checkpoint.job_version}
                  <small>{formatTime(checkpoint.updated_at_ms)}</small>
                </span>
                <div className="actions">
                  <span className={`state ${checkpoint.status}`}>{checkpoint.status}</span>
                  {checkpoint.kind === 'savepoint' && checkpoint.status === 'completed' && (
                    <button disabled={!canMutate || busy} onClick={() => onUpgrade(checkpoint)}>
                      {t('jobs.upgradeFromSavepoint')}
                    </button>
                  )}
                </div>
              </div>
            ))
          ) : (
            <p className="empty">{t('jobs.noCheckpoints')}</p>
          )}
        </>
      )}
      {tab === 'versions' && (
        <JobVersions
          jobId={detail.job.job_id}
          currentVersion={detail.job.version}
          canMutate={canMutate}
          busy={busy}
          onError={onError}
          onRefresh={onRefresh}
          onAction={onAction}
          onViewPlan={onViewPlan}
        />
      )}
    </section>
  )
}

function JobVersions({
  jobId,
  currentVersion,
  canMutate,
  busy,
  onError,
  onRefresh,
  onAction,
  onViewPlan,
}: {
  jobId: string
  currentVersion: number
  canMutate: boolean
  busy: boolean
  onError: (message: string) => void
  onRefresh: () => void
  onAction: (label: string, fn: () => Promise<unknown>) => Promise<void>
  onViewPlan: (planJson: string) => void
}) {
  const t = useT()
  const versionsQuery = useJobVersions(jobId)
  const rollback = useRollbackJobUpgrade(jobId)
  const versions = versionsQuery.data ?? []
  useEffect(() => {
    if (versionsQuery.isError) onError(errorMessage(versionsQuery.error))
  }, [versionsQuery.isError, versionsQuery.error, onError])
  return (
    <div>
      {versions.length ? (
        versions.map((version) => (
          <div className="version" key={version.version}>
            <span>
              <strong>v{version.version}</strong> · {formatTime(version.created_at_ms)}
              <small>
                {version.version === currentVersion ? t('jobs.versionCurrent') : t('jobs.versionAvailable')}
              </small>
            </span>
            <div className="actions">
              <button onClick={() => onViewPlan(version.plan_json)}>{t('jobs.viewPlan')}</button>
              {version.version < currentVersion && (
                <button
                  disabled={!canMutate || busy}
                  onClick={() =>
                    void onAction(t('jobs.restoring'), async () => {
                      await rollback.mutateAsync(`restore-v${version.version}`)
                      onRefresh()
                    })
                  }
                >
                  {t('jobs.restore')}
                </button>
              )}
            </div>
          </div>
        ))
      ) : (
        <p className="empty">{t('jobs.noVersions')}</p>
      )}
    </div>
  )
}

function PlanModal({ title, content, onClose }: { title: string; content: string; onClose: () => void }) {
  const t = useT()
  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      if (event.key === 'Escape') onClose()
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [onClose])
  return (
    <div className="modal-overlay" role="presentation" onClick={onClose}>
      <div
        className="modal"
        role="dialog"
        aria-modal="true"
        aria-label={title}
        onClick={(event) => event.stopPropagation()}
      >
        <div className="panel-title">
          <h3>{title}</h3>
          <button onClick={onClose}>{t('jobs.close')}</button>
        </div>
        <pre className="schema job-plan">{content}</pre>
      </div>
    </div>
  )
}
