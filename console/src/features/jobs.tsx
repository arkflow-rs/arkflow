import { useEffect, useMemo, useState } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'
import { api, ControlNode, errorMessage, formatTime, Job, JobCheckpoint, JobDetail } from '../api'
import { currentLocale, intlLocale, useT } from '../i18n'
import { useConfirm } from './confirm'
import { SkeletonRows } from './shared'
import { useJobDetail, useJobs, useNodes } from '../queries'
import { JobEditor as VisualJobEditor } from './job-editor'

type JobsProps = {
  onError: (message: string) => void
  canMutate?: boolean
}

const pretty = (value: unknown) => JSON.stringify(value, null, 2)

export function Jobs({ onError, canMutate = true }: JobsProps) {
  const t = useT()
  const queryClient = useQueryClient()
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
  const [editor, setEditor] = useState<{ mode: 'create' | 'upgrade'; job?: Job; savepoint?: JobCheckpoint }>()
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
          (state === 'all' || job.observed_state === state || job.convergence === state),
      ),
    [jobs, filter, state],
  )
  const action = async (label: string, fn: () => Promise<unknown>) => {
    try {
      setBusy(label)
      await fn()
      setBusy('')
      invalidateJobs()
    } catch (cause) {
      setBusy('')
      onError(errorMessage(cause))
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
            <option value="running">Running</option>
            <option value="stopped">Stopped</option>
            <option value="failed">Failed</option>
            <option value="degraded">Degraded</option>
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
                  <small>
                    {t('jobs.desiredSummary', { desired: job.desired_state, convergence: job.convergence })}
                  </small>
                  {job.last_error && <small className="error-text">{job.last_error}</small>}
                </div>
                <div className="actions">
                  <button disabled={!canMutate || !!busy} onClick={() => setStateFor(job)}>
                    {job.desired_state === 'running' ? t('jobs.stop') : t('jobs.start')}
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

function JobDetailPanel({
  detail,
  pollError,
  nodes,
  canMutate,
  busy,
  onClose,
  onError,
  onRefresh,
  onAction,
  onUpgrade,
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
  onViewPlan: (planJson: string) => void
}) {
  const t = useT()
  const [tab, setTab] = useState<'overview' | 'plan' | 'tasks' | 'recovery' | 'versions'>('overview')
  const checkpoints = detail.checkpoints ?? []
  const runArtifact = (kind: 'checkpoint' | 'savepoint') =>
    void onAction(kind === 'checkpoint' ? t('jobs.checkpointing') : t('jobs.creatingSavepoint'), async () => {
      if (kind === 'checkpoint') await api.checkpoint(detail.job.job_id)
      else await api.savepoint(detail.job.job_id)
      onRefresh()
    })
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
          {detail.job.desired_state === 'running' && (
            <button
              disabled={!canMutate || busy}
              onClick={() =>
                void onAction(t('jobs.stopping'), () => api.setJobState(detail.job.job_id, 'stopped'))
              }
            >
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
              <p>
                {t('jobs.desiredLabel')} <strong>{detail.job.desired_state}</strong> ·{' '}
                {t('jobs.convergenceLabel')} <strong>{detail.job.convergence}</strong>
              </p>
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
              {Object.entries(detail.metrics ?? {}).map(([key, value]) => (
                <div className="metric" key={key}>
                  <span>{key.replaceAll('_', ' ')}</span>
                  <strong>
                    {typeof value === 'number'
                      ? value.toLocaleString(intlLocale(currentLocale()))
                      : String(value)}
                  </strong>
                </div>
              ))}
            </div>
          </div>
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
              <div className="row" key={`${String(task.task_id ?? task.id ?? index)}`}>
                <div>
                  <strong>{String(task.task_id ?? task.id ?? `task-${index}`)}</strong>
                  <small>
                    {t('jobs.taskPlacement', {
                      node: String(task.node_id ?? t('jobs.unassignedValue')),
                      attempt: String(task.attempt_id ?? '—'),
                    })}
                  </small>
                </div>
                <span className={`state ${String(task.state ?? 'assigned')}`}>
                  {String(task.state ?? 'assigned')}
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
  const [versions, setVersions] = useState<
    Array<{ version: number; spec_json: string; plan_json: string; created_at_ms: number }>
  >([])
  useEffect(() => {
    void api
      .jobVersions(jobId)
      .then(setVersions)
      .catch((cause) => onError(errorMessage(cause)))
  }, [jobId, onError])
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
                      await api.rollbackJobUpgrade(jobId, `restore-v${version.version}`)
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
