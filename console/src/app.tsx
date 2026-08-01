import { useCallback, useEffect, useState } from 'react'
import { api, Component, ConfigIssue, ConfigVersion, EngineStatus, StreamStatus } from './api'

type Page = 'dashboard' | 'streams' | 'configuration' | 'components'

export function App() {
  const [page, setPage] = useState<Page>('dashboard')
  const [system, setSystem] = useState<EngineStatus | null>(null)
  const [streams, setStreams] = useState<StreamStatus[]>([])
  const [error, setError] = useState('')
  const refresh = useCallback(async () => {
    try { setError(''); const [nextSystem, nextStreams] = await Promise.all([api.system(), api.streams()]); setSystem(nextSystem); setStreams(nextStreams) }
    catch (cause) { setError(errorMessage(cause)) }
  }, [])
  useEffect(() => { void refresh(); const timer = window.setInterval(() => void refresh(), 5000); return () => window.clearInterval(timer) }, [refresh])
  const command = async (id: string, action: 'start' | 'stop' | 'restart') => {
    if (!window.confirm(`${action} ${id}?`)) return
    try { await api.command(id, action); await refresh() } catch (cause) { setError(errorMessage(cause)) }
  }
  return <div className="shell"><aside><h1>arkflow</h1><p>Control plane</p><nav>{([['dashboard', 'Dashboard'], ['streams', 'Streams'], ['configuration', 'Configuration'], ['components', 'Components']] as const).map(([key, label]) => <a key={key} className={page === key ? 'active' : ''} onClick={() => setPage(key)}>{label}</a>)}</nav></aside><main><header><div><span className="eyebrow">CONTROL PLANE</span><h2>{page[0].toUpperCase() + page.slice(1)}</h2></div><button onClick={() => void refresh()}>Refresh</button></header>{error && <div className="error">{error}</div>}{page === 'dashboard' && <Dashboard system={system} streams={streams} />}{page === 'streams' && <StreamPage streams={streams} command={command} />}{page === 'configuration' && <Configuration onError={setError} />}{page === 'components' && <Components onError={setError} />}</main></div>
}

function Dashboard({ system, streams }: { system: EngineStatus | null; streams: StreamStatus[] }) {
  const recentErrors = streams.filter(stream => stream.last_error).slice(0, 5)
  return <><section className="cards"><Card label="Engine" value={system?.state ?? 'Loading…'} /><Card label="Streams" value={system?.streams_total ?? '—'} /><Card label="Running" value={system?.streams_running ?? '—'} /><Card label="Failed" value={system?.streams_failed ?? '—'} /></section><section className="panel"><div className="panel-title"><h3>Recent errors</h3><span>Bounded runtime history</span></div>{recentErrors.length === 0 ? <p className="empty">No recent errors.</p> : recentErrors.map(stream => <p key={stream.id} className="error-row"><strong>{stream.id}</strong> {stream.last_error?.message}</p>)}</section></>
}

function StreamPage({ streams, command }: { streams: StreamStatus[]; command: (id: string, action: 'start' | 'stop' | 'restart') => Promise<void> }) {
  return <section className="panel"><div className="panel-title"><h3>Streams</h3><span>Updates every 5 seconds</span></div>{streams.length === 0 ? <p className="empty">No streams registered.</p> : <div className="table">{streams.map(stream => <div className="row" key={stream.id}><div><strong>{stream.id}</strong><small>{stream.last_error?.message ?? `${stream.metrics.input_messages} input messages · ${stream.metrics.output_messages} output messages`}</small></div><span className={`state ${stream.state}`}>{stream.state}</span><div className="actions">{stream.state !== 'running' && <button onClick={() => void command(stream.id, 'start')}>Start</button>}{stream.state === 'running' && <button onClick={() => void command(stream.id, 'stop')}>Stop</button>}<button onClick={() => void command(stream.id, 'restart')}>Restart</button></div></div>)}</div>}</section>
}

function Configuration({ onError }: { onError: (message: string) => void }) {
  const [content, setContent] = useState('streams: []\n')
  const [format, setFormat] = useState<'yaml' | 'json'>('yaml')
  const [issues, setIssues] = useState<ConfigIssue[]>([])
  const [versions, setVersions] = useState<ConfigVersion[]>([])
  const candidate = { format, content }
  const validate = async () => { try { const result = await api.validateConfig(candidate); setIssues(result.errors) } catch (cause) { onError(errorMessage(cause)) } }
  const publish = async () => { try { await api.applyConfig(candidate); await loadVersions() } catch (cause) { onError(errorMessage(cause)) } }
  const loadVersions = async () => { try { setVersions(await api.versions()) } catch (cause) { onError(errorMessage(cause)) } }
  useEffect(() => { void (async () => { try { const current = await api.config(); setContent(JSON.stringify(current, null, 2)); setFormat('json'); await loadVersions() } catch (cause) { onError(errorMessage(cause)) } })() }, [])
  return <section className="panel config"><div className="panel-title"><h3>Configuration</h3><div className="actions"><select value={format} onChange={event => setFormat(event.target.value as 'yaml' | 'json')}><option value="yaml">YAML</option><option value="json">JSON</option></select><button onClick={() => void validate()}>Validate</button><button onClick={() => void publish()}>Publish</button></div></div><textarea value={content} onChange={event => setContent(event.target.value)} spellCheck={false} aria-label="Configuration editor" />{issues.length > 0 && <div className="validation">{issues.map((issue, index) => <p key={index}><strong>{issue.path || 'document'}</strong>: {issue.message}</p>)}</div>}<h3 className="subheading">Versions</h3>{versions.length === 0 ? <p className="empty">No saved versions.</p> : versions.map(version => <div className="version" key={version.id}><span>{version.id} · {version.format}</span><button onClick={() => { if (window.confirm(`Rollback ${version.id}?`)) void api.rollback(version.id).catch(cause => onError(errorMessage(cause))) }}>Rollback</button></div>)}</section>
}

function Components({ onError }: { onError: (message: string) => void }) {
  const [components, setComponents] = useState<Component[]>([])
  const [schema, setSchema] = useState<unknown>(null)
  useEffect(() => { Promise.all([api.components(), api.schema()]).then(([items, nextSchema]) => { setComponents(items); setSchema(nextSchema) }).catch(cause => onError(errorMessage(cause))) }, [onError])
  return <section className="panel"><div className="panel-title"><h3>Component catalogue</h3><span>{components.length} registered</span></div>{components.length === 0 ? <p className="empty">No component metadata available.</p> : <div className="component-grid">{components.map(component => <article key={`${component.kind}-${component.name}`}><strong>{component.name}</strong><span>{component.kind}</span><p>{component.description ?? 'No description'}</p></article>)}</div>}<h3 className="subheading">Configuration schema</h3><pre className="schema">{schema ? JSON.stringify(schema, null, 2) : 'Loading…'}</pre></section>
}

function Card({ label, value }: { label: string; value: string | number }) { return <div className="card"><span>{label}</span><strong>{value}</strong></div> }
function errorMessage(cause: unknown) { return typeof cause === 'object' && cause && 'message' in cause ? String(cause.message) : 'Control API unavailable' }
