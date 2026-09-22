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
import { Rollouts } from './features/rollouts'

const PAGES = [
  ['overview', 'Overview'],
  ['runtime', 'Streams'],
  ['jobs', 'Jobs'],
  ['configuration', 'Configuration'],
  ['rollouts', 'Rollouts'],
  ['components', 'Components'],
  ['events', 'Events'],
  ['settings', 'Settings'],
] as const
type Page = (typeof PAGES)[number][0]

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

  const pageTitle = PAGES.find(([key]) => key === page)?.[1] ?? page
  return (
    <div className="shell">
      <aside>
        <h1>arkflow</h1>
        <p>Control plane</p>
        <nav>
          {PAGES.map(([key, label]) => (
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
              {label}
            </a>
          ))}
        </nav>
      </aside>
      <main>
        <header>
          <div>
            <span className="eyebrow">CONTROL PLANE</span>
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
                Sign out
              </button>
            )}
            <span className={`connection ${live ? 'connected' : 'disconnected'}`}>
              {live ? 'Live events' : 'Snapshot mode'}
            </span>
            <select
              aria-label="Compute node"
              value={selectedNode}
              onChange={(event) => {
                const value = event.target.value
                setSelectedNode(value)
                syncLocation(page, value)
              }}
            >
              <option value="">All nodes</option>
              {snapshot.nodes.map((node) => (
                <option key={node.id} value={node.id}>
                  {node.id} · {node.state}
                </option>
              ))}
            </select>
            <button disabled={refreshing} onClick={() => void refresh()}>
              {refreshing ? 'Refreshing…' : 'Refresh'}
            </button>
          </div>
        </header>
        {selectedNodeState &&
          selectedNodeState.state !== 'online' &&
          selectedNodeState.state !== 'running' && (
            <div className="warning">
              Node {selectedNode} is {selectedNodeState.state}; mutating actions are disabled.
            </div>
          )}
        {stale && (
          <div className="warning">
            Showing the last known state. Retry when the control API is available.
          </div>
        )}
        {error && <div className="error">{error}</div>}
        {page === 'overview' && <Overview snapshot={snapshot} />}
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
        {page === 'settings' && <Settings status={snapshot.status} />}
      </main>
    </div>
  )
}
