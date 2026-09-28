import { useCallback, useEffect, useState, type ReactNode } from 'react'
import {
  BrowserRouter,
  Navigate,
  NavLink,
  Route,
  Routes,
  useLocation,
  useNavigate,
  useSearchParams,
} from 'react-router'
import { QueryClient, QueryClientProvider, useQueryClient } from '@tanstack/react-query'
import { Toaster } from 'sonner'
import {
  api,
  errorMessage,
  oidcLogout,
  oidcStatus,
  REFRESH_DEBOUNCE_MS,
  streamEvents,
  waitForOperation,
} from './api'
import { useNodes, useLiveError } from './queries'
import { Overview } from './features/overview'
import { Runtime } from './features/runtime'
import { Jobs } from './features/jobs'
import { Configuration } from './features/configuration'
import { Components } from './features/components'
import { Events } from './features/events'
import { Settings } from './features/settings'
import { Audit } from './features/audit'
import { ConfirmProvider } from './features/confirm'
import { resolvedTheme } from './theme'
import { Rollouts } from './features/rollouts'
import { useSetLocale, useLocale, useT, type Locale } from './i18n'
import { useTheme, type ThemeSetting } from './theme'

const NAV_ITEMS = [
  { path: '/', key: 'overview' },
  { path: '/runtime', key: 'runtime' },
  { path: '/events', key: 'events' },
  { path: '/jobs', key: 'jobs' },
  { path: '/rollouts', key: 'rollouts' },
  { path: '/configuration', key: 'configuration' },
  { path: '/components', key: 'components' },
  { path: '/audit', key: 'audit' },
  { path: '/settings', key: 'settings' },
] as const

const NAV_GROUPS = [
  { key: 'observe', items: ['overview', 'runtime', 'events'] },
  { key: 'deliver', items: ['jobs', 'rollouts'] },
  { key: 'admin', items: ['configuration', 'components', 'audit', 'settings'] },
] as const

const ITEM_BY_KEY = new Map(NAV_ITEMS.map((item) => [item.key, item]))

function createQueryClient() {
  return new QueryClient({
    defaultOptions: { queries: { retry: 1, retryDelay: 200, refetchOnWindowFocus: false } },
  })
}

function AppProviders({ children }: { children: ReactNode }) {
  const [queryClient] = useState(createQueryClient)
  return (
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>{children}</BrowserRouter>
    </QueryClientProvider>
  )
}

export function App() {
  return (
    <AppProviders>
      <ConfirmProvider>
        <ConsoleShell />
      </ConfirmProvider>
    </AppProviders>
  )
}

function ConsoleShell() {
  const t = useT()
  const locale = useLocale()
  const setLocale = useSetLocale()
  const { setting: themeSetting, setSetting: setThemeSetting } = useTheme()
  const location = useLocation()
  const [searchParams, setSearchParams] = useSearchParams()
  const queryClient = useQueryClient()
  const selectedNode = searchParams.get('node_id') ?? ''
  const nodesQuery = useNodes()
  const liveError = useLiveError()
  const nodes = nodesQuery.data?.items ?? []
  const [error, setError] = useState('')
  const [live, setLive] = useState(false)
  const [oidcAuthenticated, setOidcAuthenticated] = useState(false)

  const refresh = useCallback(() => {
    void queryClient.invalidateQueries({ queryKey: ['live'] })
  }, [queryClient])

  useEffect(() => {
    void oidcStatus()
      .then((status) => setOidcAuthenticated(status.authenticated))
      .catch(() => setOidcAuthenticated(false))
    // Live events arrive in bursts; coalesce them into one debounced
    // invalidation of the live queries instead of a fan-out per event.
    let pending: number | undefined
    const controller = streamEvents(
      () => {
        window.clearTimeout(pending)
        pending = window.setTimeout(() => refresh(), REFRESH_DEBOUNCE_MS)
      },
      (state) => setLive(state === 'connected'),
      selectedNode || undefined,
    )
    return () => {
      window.clearTimeout(pending)
      controller.abort()
    }
  }, [refresh, selectedNode])

  // Query failures render in their own banner so they never clobber error
  // state from mutations and page callbacks; keepPreviousData placeholders on
  // the live queries preserve the last snapshot while the banner is up.
  const stale = nodesQuery.isError || liveError !== null
  const queryError = nodesQuery.isError ? nodesQuery.error : liveError

  const selectedNodeState = nodes.find((node) => node.id === selectedNode)
  const canMutate =
    !selectedNodeState || selectedNodeState.state === 'online' || selectedNodeState.state === 'running'

  const command = async (id: string, action: 'start' | 'stop' | 'restart') => {
    try {
      const operation = await api.command(id, action, selectedNode || undefined)
      await waitForOperation(operation.id)
      await queryClient.invalidateQueries({ queryKey: ['live'] })
    } catch (cause) {
      setError(errorMessage(cause))
    }
  }

  const setNodeParam = (value: string) => {
    const next = new URLSearchParams(searchParams)
    if (value) next.set('node_id', value)
    else next.delete('node_id')
    setSearchParams(next)
  }

  // Legacy `?page=` links never worked as deep links until recently and carry
  // no path semantics; redirect them to their route, preserving other params.
  const navigate = useNavigate()
  const legacyPage = searchParams.get('page')
  useEffect(() => {
    if (!legacyPage) return
    const known = NAV_ITEMS.find((item) => item.key === legacyPage)
    if (!known) return
    const next = new URLSearchParams(searchParams)
    next.delete('page')
    const search = next.toString()
    navigate(`${known.path}${search ? `?${search}` : ''}`, { replace: true })
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [legacyPage, navigate])

  const current = NAV_ITEMS.find((item) => item.path === location.pathname) ?? NAV_ITEMS[0]
  const pageTitle = t(`nav.${current.key}`)
  const navSearch = selectedNode ? `?node_id=${encodeURIComponent(selectedNode)}` : ''
  return (
    <div className="shell">
      <Toaster theme={resolvedTheme(themeSetting)} position="bottom-right" />
      <aside>
        <h1>arkflow</h1>
        <p>{t('brand.tagline')}</p>
        <nav>
          {NAV_GROUPS.map((group) => (
            <div className="nav-group" key={group.key}>
              <p className="nav-group-label">{t(`nav.group.${group.key}`)}</p>
              {group.items.map((key) => {
                const item = ITEM_BY_KEY.get(key)
                if (!item) return null
                return (
                  <NavLink
                    key={item.key}
                    to={navSearch ? `${item.path}${navSearch}` : item.path}
                    end={item.path === '/'}
                    className={({ isActive }) => (isActive ? 'active' : '')}
                  >
                    {t(`nav.${item.key}`)}
                  </NavLink>
                )
              })}
            </div>
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
              onChange={(event) => setNodeParam(event.target.value)}
            >
              <option value="">{t('header.allNodes')}</option>
              {nodes.map((node) => (
                <option key={node.id} value={node.id}>
                  {node.id} · {node.state}
                </option>
              ))}
            </select>
            <select
              aria-label={t('header.theme')}
              value={themeSetting}
              onChange={(event) => setThemeSetting(event.target.value as ThemeSetting)}
            >
              <option value="dark">{t('theme.dark')}</option>
              <option value="light">{t('theme.light')}</option>
              <option value="system">{t('theme.system')}</option>
            </select>
            <select
              aria-label={t('header.language')}
              value={locale}
              onChange={(event) => setLocale(event.target.value as Locale)}
            >
              <option value="zh">中文</option>
              <option value="en">English</option>
            </select>
            <button disabled={nodesQuery.isFetching} onClick={refresh}>
              {nodesQuery.isFetching ? t('header.refreshing') : t('header.refresh')}
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
        {queryError && <div className="error">{errorMessage(queryError)}</div>}
        <Routes>
          <Route path="/" element={<Overview onError={setError} />} />
          <Route
            path="/runtime"
            element={<Runtime command={command} canMutate={canMutate} onError={setError} />}
          />
          <Route path="/jobs" element={<Jobs onError={setError} canMutate={canMutate} />} />
          <Route
            path="/configuration"
            element={<Configuration onError={setError} nodeId={selectedNode || undefined} />}
          />
          <Route path="/rollouts" element={<Rollouts onError={setError} />} />
          <Route path="/components" element={<Components onError={setError} />} />
          <Route path="/events" element={<Events />} />
          <Route path="/audit" element={<Audit onError={setError} />} />
          <Route path="/settings" element={<Settings />} />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </main>
    </div>
  )
}
