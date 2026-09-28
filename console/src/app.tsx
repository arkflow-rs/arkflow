import { useCallback, useEffect, useState } from 'react'
import {
  api,
  ControlNode,
  errorMessage,
  oidcLogout,
  oidcStatus,
  REFRESH_DEBOUNCE_MS,
  SNAPSHOT_INTERVAL_MS,
  streamEvents,
  waitForOperation,
} from './api'
import { Configuration, Components, Events, Jobs, Overview, Runtime, Settings, Snapshot } from './features'
import { Audit } from './features/audit'
import { Rollouts } from './features/rollouts'
import { useSetLocale, useLocale, useT, type Locale } from './i18n'

const PAGES = [
  'overview',
  'runtime',
  'jobs',
  'configuration',
  'rollouts',
  'components',
  'events',
  'audit',
  'settings',
] as const
type Page = (typeof PAGES)[number]

function pageFromLocation(): Page {
  const value = new URLSearchParams(window.location.search).get('page')
  return PAGES.some(([key]) => key === value) ? (value as Page) : 'overview'
}

function nodeFromLocation(): string {
  return new URLSearchParams(window.location.search).get('node_id') ?? ''
}

function syncLocation(page: Page, nodeId: string) {
  const params = new URLSearchParams()
  if (page !== 'overview') params.set('page', page)
  if (nodeId) params.set('node_id', nodeId)
  window.history.replaceState(null, '', `${window.location.pathname}${params.toString() ? `?${params}` : ''}`)
}

export function App() {
  const t = useT()
  const locale = useLocale()
  const setLocale = useSetLocale()
  const [page, setPage] = useState<Page>(pageFromLocation)
  const [selectedNode, setSelectedNode] = useState(nodeFromLocation)
  const [snapshot, setSnapshot] = useState<Snapshot>({
    system: null,
    status: null,
    nodes: [],
    streams: [],
    jobs: [],
    operations: [],
    events: [],
  })
  const [error, setError] = useState('')
  const [stale, setStale] = useState(false)
  const [live, setLive] = useState(false)
  const [refreshing, setRefreshing] = useState(false)
  const [oidcAuthenticated, setOidcAuthenticated] = useState(false)

  const goTo = (next: Page) => {
    setPage(next)
    syncLocation(next, selectedNode)
  }

  const refresh = useCallback(async () => {
    setRefreshing(true)
    try {
      const [system, status, nodes, streams, jobs, operations, events, metrics] = await Promise.all([
        api.system(),
        api.status().catch(() => null),
        api.nodes(),
        api.streams(selectedNode || undefined),
        api.jobs(),
        api.operations(selectedNode || undefined),
        api.events(selectedNode || undefined),
        api.metrics(selectedNode || undefined).catch(() => undefined),
      ])
      setSnapshot({
        system,
        status: status ?? null,
        nodes: nodes.items as ControlNode[],
        streams: streams.items,
        jobs,
        operations: operations.items,
        events: events.items,
        metrics,
        totals: {
          nodes: nodes.total,
          streams: streams.total,
          operations: operations.total,
          events: events.total,
        },
      })
      setStale(false)
      setError('')
    } catch (cause) {
      setStale(true)
      setError(errorMessage(cause))
    } finally {
      setRefreshing(false)
    }
  }, [selectedNode])

  useEffect(() => {
    void oidcStatus()
      .then((status) => setOidcAuthenticated(status.authenticated))
      .catch(() => setOidcAuthenticated(false))
    void refresh()
    const timer = window.setInterval(() => void refresh(), SNAPSHOT_INTERVAL_MS)
    // Live events arrive in bursts; coalesce them into one debounced snapshot
    // refresh instead of issuing a full fan-out per event.
    let pending: number | undefined
    const controller = streamEvents(
      () => {
        window.clearTimeout(pending)
        pending = window.setTimeout(() => void refresh(), REFRESH_DEBOUNCE_MS)
      },
      (state) => setLive(state === 'connected'),
      selectedNode || undefined,
    )
    return () => {
      window.clearInterval(timer)
      window.clearTimeout(pending)
      controller.abort()
    }
  }, [refresh, selectedNode])

  const selectedNodeState = snapshot.nodes.find((node) => node.id === selectedNode)
  const canMutate =
    !selectedNodeState || selectedNodeState.state === 'online' || selectedNodeState.state === 'running'

  const command = async (id: string, action: 'start' | 'stop' | 'restart') => {
    try {
      const operation = await api.command(id, action, selectedNode || undefined)
      await waitForOperation(operation.id)
      await refresh()
    } catch (cause) {
      setError(errorMessage(cause))
    }
  }

  const pageTitle = t(`nav.${page}`)
  return (
    <div className="shell">
      <aside>
        <h1>arkflow</h1>
        <p>{t('brand.tagline')}</p>
        <nav>
          {PAGES.map((key) => (
            <a
              key={key}
              href={`?page=${key}`}
              className={page === key ? 'active' : ''}
              aria-current={page === key ? 'page' : undefined}
              onClick={(event) => {
                event.preventDefault()
                goTo(key)
              }}
            >
              {t(`nav.${key}`)}
            </a>
          ))}
        </nav>
      </aside>
      <main>
        <header>
          <div>
            <span className="eyebrow">{t('header.eyebrow')}</span>
            <h2>{pageTitle}</h2>
          </div>
          <div className="actions">
            {oidcAuthenticated && (
              <button
                onClick={() => {
                  setOidcAuthenticated(false)
                  void oidcLogout()
                }}
              >
                {t('header.signOut')}
              </button>
            )}
            <span className={`connection ${live ? 'connected' : 'disconnected'}`}>
              {live ? t('header.liveEvents') : t('header.snapshotMode')}
            </span>
            <select
              aria-label={t('header.computeNode')}
              value={selectedNode}
              onChange={(event) => {
                const value = event.target.value
                setSelectedNode(value)
                syncLocation(page, value)
              }}
            >
              <option value="">{t('header.allNodes')}</option>
              {snapshot.nodes.map((node) => (
                <option key={node.id} value={node.id}>
                  {node.id} · {node.state}
                </option>
              ))}
            </select>
            <select
              aria-label={t('header.language')}
              value={locale}
              onChange={(event) => setLocale(event.target.value as Locale)}
            >
              <option value="zh">中文</option>
              <option value="en">English</option>
            </select>
            <button disabled={refreshing} onClick={() => void refresh()}>
              {refreshing ? t('header.refreshing') : t('header.refresh')}
            </button>
          </div>
        </header>
        {selectedNodeState &&
          selectedNodeState.state !== 'online' &&
          selectedNodeState.state !== 'running' && (
            <div className="warning">
              {t('warning.nodeUnavailable', { node: selectedNode, state: selectedNodeState.state })}
            </div>
          )}
        {stale && <div className="warning">{t('warning.staleState')}</div>}
        {error && <div className="error">{error}</div>}
        {page === 'overview' && (
          <Overview snapshot={snapshot} onError={setError} onNodesChanged={() => void refresh()} />
        )}
        {page === 'runtime' && (
          <Runtime
            streams={snapshot.streams}
            operations={snapshot.operations}
            events={snapshot.events}
            command={command}
            canMutate={canMutate}
            onOperationChanged={() => void refresh()}
            streamTotal={snapshot.totals?.streams}
            operationTotal={snapshot.totals?.operations}
          />
        )}
        {page === 'jobs' && (
          <Jobs
            jobs={snapshot.jobs}
            nodes={snapshot.nodes}
            canMutate={canMutate}
            onError={setError}
            onRefresh={() => void refresh()}
          />
        )}
        {page === 'configuration' && <Configuration onError={setError} nodeId={selectedNode || undefined} />}
        {page === 'rollouts' && <Rollouts nodes={snapshot.nodes} onError={setError} />}
        {page === 'components' && <Components onError={setError} />}
        {page === 'events' && <Events events={snapshot.events} total={snapshot.totals?.events} />}
        {page === 'audit' && <Audit onError={setError} />}
        {page === 'settings' && <Settings status={snapshot.status} />}
      </main>
    </div>
  )
}
