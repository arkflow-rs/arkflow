import { useEffect, useMemo, useState } from 'react'
import { api, ControlNode, Job, JobCheckpoint, JobDetail } from '../api'
import { JobEditor as VisualJobEditor } from './job-editor'

type JobsProps = { jobs: Job[]; nodes: ControlNode[]; onRefresh: () => void; onError: (message: string) => void; canMutate?: boolean }

const pretty = (value: unknown) => JSON.stringify(value, null, 2)
const stamp = (value?: number) => value ? new Date(value).toLocaleString() : '—'
const message = (cause: unknown) => typeof cause === 'object' && cause && 'message' in cause ? String(cause.message) : cause instanceof Error ? cause.message : 'Control API unavailable'

export function Jobs({ jobs, nodes, onRefresh, onError, canMutate = true }: JobsProps) {
  const [filter, setFilter] = useState('')
  const [state, setState] = useState('all')
  const [selected, setSelected] = useState<JobDetail>()
  const [editor, setEditor] = useState<{ mode: 'create'|'upgrade'; job?: Job; savepoint?: JobCheckpoint }>()
  const [busy, setBusy] = useState('')
  const visible = useMemo(() => jobs.filter(job => (!filter || `${job.job_id} ${job.node_ids.join(' ')}`.toLowerCase().includes(filter.toLowerCase())) && (state === 'all' || job.observed_state === state || job.convergence === state)), [jobs, filter, state])

  const load = async (jobId: string) => {
    try { setSelected(await api.jobDetail(jobId)) } catch (cause) { onError(message(cause)) }
  }
  useEffect(() => {
    if (!selected) return
    const timer = window.setInterval(() => { void api.jobDetail(selected.job.job_id).then(setSelected).catch(() => undefined) }, 5000)
    return () => window.clearInterval(timer)
  }, [selected?.job.job_id])
  const action = async (label: string, fn: () => Promise<unknown>) => {
    try { setBusy(label); await fn(); setBusy(''); onRefresh(); if (selected) await load(selected.job.job_id) } catch (cause) { setBusy(''); onError(message(cause)) }
  }
  const setStateFor = (job: Job) => {
    if (!window.confirm(`${job.desired_state === 'running' ? 'Stop' : 'Start'} ${job.job_id}?`)) return
    void action(job.desired_state === 'running' ? 'Stopping…' : 'Starting…', () => api.setJobState(job.job_id, job.desired_state === 'running' ? 'stopped' : 'running'))
  }

  return <>
    <section className="panel">
      <div className="panel-title"><div><h3>Stateful Jobs</h3><span>{visible.length} of {jobs.length} registered</span></div><button disabled={!canMutate} onClick={() => setEditor({ mode: 'create' })}>Create Job</button></div>
      <div className="toolbar"><input aria-label="Job filter" placeholder="Filter by Job or node" value={filter} onChange={event => setFilter(event.target.value)} /><select aria-label="Job state" value={state} onChange={event => setState(event.target.value)}><option value="all">All states</option><option value="running">Running</option><option value="stopped">Stopped</option><option value="failed">Failed</option><option value="degraded">Degraded</option></select></div>
      {visible.length === 0 ? <p className="empty">No distributed Jobs match the current filters.</p> : <div className="table">{visible.map(job => <div className={`row ${selected?.job.job_id === job.job_id ? 'selected' : ''}`} key={job.job_id}>
        <button className="link-button" onClick={() => void load(job.job_id)}><strong>{job.job_id}</strong><small>version {job.version} · generation {job.generation} · {job.node_ids.join(', ') || 'automatic placement'}</small></button>
        <div><span className={`state ${job.observed_state}`}>{job.observed_state}</span><small>desired: {job.desired_state} · {job.convergence}</small>{job.last_error && <small className="error-text">{job.last_error}</small>}</div>
        <div className="actions"><button disabled={!canMutate || !!busy} onClick={() => setStateFor(job)}>{job.desired_state === 'running' ? 'Stop' : 'Start'}</button></div>
      </div>)}</div>}
    </section>
    {selected && <JobDetailPanel detail={selected} nodes={nodes} canMutate={canMutate} busy={!!busy} onClose={() => setSelected(undefined)} onError={onError} onRefresh={() => void load(selected.job.job_id)} onAction={action} onUpgrade={(savepoint) => setEditor({ mode: 'upgrade', job: selected.job, savepoint })} />}
    {editor && <VisualJobEditor mode={editor.mode} job={editor.job} savepoint={editor.savepoint} nodes={nodes} busy={!!busy} onClose={() => setEditor(undefined)} onError={onError} onSaved={() => { setEditor(undefined); onRefresh(); if (editor.job) void load(editor.job.job_id) }} onAction={action} />}
  </>
}

function JobDetailPanel({ detail, nodes, canMutate, busy, onClose, onError, onRefresh, onAction, onUpgrade }: { detail: JobDetail; nodes: ControlNode[]; canMutate: boolean; busy: boolean; onClose: () => void; onError: (message: string) => void; onRefresh: () => void; onAction: (label: string, fn: () => Promise<unknown>) => Promise<void>; onUpgrade: (savepoint: JobCheckpoint) => void }) {
  const [tab, setTab] = useState<'overview'|'plan'|'tasks'|'recovery'|'versions'>('overview')
  const checkpoints = detail.checkpoints ?? []
  const completedSavepoints = checkpoints.filter(checkpoint => checkpoint.kind === 'savepoint' && checkpoint.status === 'completed')
  const runArtifact = (kind: 'checkpoint'|'savepoint') => void onAction(kind === 'checkpoint' ? 'Checkpointing…' : 'Creating savepoint…', async () => { if (kind === 'checkpoint') await api.checkpoint(detail.job.job_id); else await api.savepoint(detail.job.job_id); onRefresh() })
  return <section className="panel detail">
    <div className="panel-title"><div><span className="eyebrow">JOB DETAIL</span><h3>{detail.job.job_id}</h3><small>version {detail.job.version} · generation {detail.job.generation}</small></div><div className="actions"><button onClick={onClose}>Close</button><button disabled={!canMutate || busy} onClick={() => runArtifact('checkpoint')}>Checkpoint</button><button disabled={!canMutate || busy} onClick={() => runArtifact('savepoint')}>Savepoint</button>{detail.job.desired_state === 'running' && <button disabled={!canMutate || busy} onClick={() => void onAction('Stopping…', () => api.setJobState(detail.job.job_id, 'stopped'))}>Stop</button>}</div></div>
    <div className="job-tabs">{(['overview','plan','tasks','recovery','versions'] as const).map(value => <button className={tab === value ? 'active' : ''} key={value} onClick={() => setTab(value)}>{value[0].toUpperCase() + value.slice(1)}</button>)}</div>
    {tab === 'overview' && <><div className="detail-grid"><div><span className={`state ${detail.job.observed_state}`}>{detail.job.observed_state}</span><p>Desired: <strong>{detail.job.desired_state}</strong> · convergence <strong>{detail.job.convergence}</strong></p><p>Nodes: <strong>{detail.nodes.map(node => node.id).join(', ') || 'automatic placement'}</strong></p><p>Latest recovery: <strong>{detail.job.checkpoint_id ?? 'none'}</strong></p>{detail.job.last_error && <div className="error-row">{detail.job.last_error}</div>}</div><div className="metric-list">{Object.entries(detail.metrics ?? {}).map(([key, value]) => <div className="metric" key={key}><span>{key.replaceAll('_', ' ')}</span><strong>{typeof value === 'number' ? value.toLocaleString() : String(value)}</strong></div>)}</div></div><h4>Node compatibility</h4>{detail.nodes.length ? detail.nodes.map(node => <div className="version" key={node.id}><span><strong>{node.id}</strong> · {node.state}</span><small>{node.capabilities.join(' · ')}</small></div>) : <p className="empty">No assigned nodes.</p>}</>}
    {tab === 'plan' && <pre className="schema job-plan">{pretty(detail.plan)}</pre>}
    {tab === 'tasks' && <div className="table">{detail.tasks.length ? detail.tasks.map((task, index) => <div className="row" key={`${String(task.task_id ?? task.id ?? index)}`}><div><strong>{String(task.task_id ?? task.id ?? `task-${index}`)}</strong><small>node {String(task.node_id ?? 'unassigned')} · attempt {String(task.attempt_id ?? '—')}</small></div><span className={`state ${String(task.state ?? 'assigned')}`}>{String(task.state ?? 'assigned')}</span><small>generation {String(task.generation ?? detail.job.generation)}</small></div>) : <p className="empty">No task assignments reported.</p>}</div>}
    {tab === 'recovery' && <><div className="actions recovery-actions"><button disabled={!canMutate || busy} onClick={() => runArtifact('checkpoint')}>Create checkpoint</button><button disabled={!canMutate || busy} onClick={() => runArtifact('savepoint')}>Create savepoint</button></div>{checkpoints.length ? checkpoints.map(checkpoint => <div className="version" key={checkpoint.checkpoint_id}><span><strong>{checkpoint.checkpoint_id}</strong> · {checkpoint.kind} · v{checkpoint.job_version}<small>{stamp(checkpoint.updated_at_ms)}</small></span><div className="actions"><span className={`state ${checkpoint.status}`}>{checkpoint.status}</span>{checkpoint.kind === 'savepoint' && checkpoint.status === 'completed' && <button disabled={!canMutate || busy} onClick={() => onUpgrade(checkpoint)}>Upgrade from this savepoint</button>}</div></div>) : <p className="empty">No checkpoint or savepoint history.</p>}</>}
    {tab === 'versions' && <JobVersions jobId={detail.job.job_id} currentVersion={detail.job.version} canMutate={canMutate} busy={busy} onError={onError} onRefresh={onRefresh} onAction={onAction} />}
  </section>
}

function JobVersions({ jobId, currentVersion, canMutate, busy, onError, onRefresh, onAction }: { jobId: string; currentVersion: number; canMutate: boolean; busy: boolean; onError: (message: string) => void; onRefresh: () => void; onAction: (label: string, fn: () => Promise<unknown>) => Promise<void> }) {
  const [versions, setVersions] = useState<Array<{ version: number; spec_json: string; plan_json: string; created_at_ms: number }>>([])
  useEffect(() => { void api.jobVersions(jobId).then(setVersions).catch(cause => onError(message(cause))) }, [jobId, onError])
  return <div>{versions.length ? versions.map(version => <div className="version" key={version.version}><span><strong>v{version.version}</strong> · {stamp(version.created_at_ms)}<small>{version.version === currentVersion ? 'current' : 'available for recovery'}</small></span><div className="actions"><button onClick={() => window.alert(version.plan_json)}>View plan</button>{version.version < currentVersion && <button disabled={!canMutate || busy} onClick={() => void onAction('Restoring…', async () => { await api.rollbackJobUpgrade(jobId, `restore-v${version.version}`); onRefresh() })}>Restore</button>}</div></div>) : <p className="empty">No version history recorded.</p>}</div>
}
