import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { BrowserRouter } from 'react-router'
import {
  bearerTokenSource,
  currentBearerToken,
  RUNTIME_TOKEN_STORAGE_KEY,
  resetAuthChallengeForTests,
  resetOidcStatusCacheForTests,
  setRuntimeBearerToken,
} from './api'
import { App } from './app'
import { ConfirmProvider } from './features/confirm'
import { Settings } from './features/settings'

const LOCALE_STORAGE_KEY = 'arkflow.console.locale'

const json = (body: unknown, status = 200) =>
  Promise.resolve({
    ok: status < 400,
    status,
    headers: new Headers(),
    json: async () => body,
  })

const page = (items: unknown[]) => ({ items, page: 1, page_size: items.length || 50, total: items.length })

const hubSystem = {
  id: 'arkflow-control-hub',
  version: 'hub',
  state: 'running',
  node_count: 0,
  stream_count: 0,
  active_operations: 0,
  capabilities: ['fleet_aggregation'],
  principal: { id: 'root', roles: ['admin'], actions: ['read', 'manage_users'] },
}

beforeEach(() => {
  window.localStorage.removeItem(LOCALE_STORAGE_KEY)
  sessionStorage.clear()
  setRuntimeBearerToken(null)
  resetAuthChallengeForTests()
  resetOidcStatusCacheForTests()
})

afterEach(() => {
  cleanup()
  vi.unstubAllEnvs()
  vi.unstubAllGlobals()
  sessionStorage.clear()
  setRuntimeBearerToken(null)
  resetAuthChallengeForTests()
  resetOidcStatusCacheForTests()
  window.history.replaceState({}, '', '/')
})

const authorizationOf = (init?: RequestInit) => new Headers(init?.headers).get('Authorization')

describe('runtime bearer credential', () => {
  it('prefers the runtime token over the build-time default and keeps it in sessionStorage', async () => {
    vi.stubEnv('VITE_API_TOKEN', 'env-token')
    expect(currentBearerToken()).toBe('env-token')
    expect(bearerTokenSource()).toBe('environment')

    setRuntimeBearerToken('runtime-token')
    expect(currentBearerToken()).toBe('runtime-token')
    expect(bearerTokenSource()).toBe('runtime')
    expect(sessionStorage.getItem(RUNTIME_TOKEN_STORAGE_KEY)).toBe('runtime-token')
    // Deliberately not persisted to localStorage.
    expect(window.localStorage.getItem(RUNTIME_TOKEN_STORAGE_KEY)).toBeNull()

    const fetchMock = vi.fn((url: string, _init?: RequestInit) => {
      expect(String(url)).toContain('/users')
      return json([])
    })
    vi.stubGlobal('fetch', fetchMock)
    const { api } = await import('./api')
    await api.users()
    expect(authorizationOf(fetchMock.mock.calls[0][1])).toBe('Bearer runtime-token')

    setRuntimeBearerToken(null)
    expect(currentBearerToken()).toBe('env-token')
    expect(sessionStorage.getItem(RUNTIME_TOKEN_STORAGE_KEY)).toBeNull()
  })

  it('prompts for a token after a 401 without an OIDC flow and retries with it', async () => {
    vi.stubEnv('VITE_API_TOKEN', '')
    const fetchMock = vi.fn((url: string, init?: RequestInit) => {
      if (url.endsWith('/auth/oidc/status'))
        return json({ login_enabled: false, authenticated: false, principal: null })
      if (url.endsWith('/system'))
        return authorizationOf(init) === 'Bearer operator-secret'
          ? json(hubSystem)
          : json({ code: 'unauthorized', message: 'authentication required' }, 401)
      if (url.includes('/nodes?')) return json(page([]))
      return json(page([]))
    })
    vi.stubGlobal('fetch', fetchMock)
    render(<App />)

    expect(await screen.findByTestId('runtime-token-prompt')).toBeInTheDocument()
    expect(screen.getByText(/does not offer an OIDC browser login/i)).toBeInTheDocument()

    fireEvent.change(screen.getByLabelText('Bearer token'), { target: { value: 'operator-secret' } })
    fireEvent.click(screen.getByRole('button', { name: 'Use token' }))

    await waitFor(() =>
      expect(fetchMock).toHaveBeenCalledWith(
        expect.stringContaining('/system'),
        expect.objectContaining({
          headers: expect.objectContaining({ Authorization: 'Bearer operator-secret' }),
        }),
      ),
    )
    expect(await screen.findByText('root (admin)')).toBeInTheDocument()
    expect(sessionStorage.getItem(RUNTIME_TOKEN_STORAGE_KEY)).toBe('operator-secret')
    await waitFor(() => expect(screen.queryByTestId('runtime-token-prompt')).not.toBeInTheDocument())
  })

  it('asks for a replacement when the sent token is rejected, without redirecting to OIDC', async () => {
    vi.stubEnv('VITE_API_TOKEN', 'stale-token')
    const redirects: string[] = []
    const realLocation = window.location
    Object.defineProperty(window, 'location', {
      writable: true,
      value: {
        assign: (value: string) => redirects.push(value),
        origin: realLocation.origin,
        href: realLocation.href,
        pathname: realLocation.pathname,
        search: realLocation.search,
        hash: realLocation.hash,
      },
    })
    const fetchMock = vi.fn((url: string) => {
      if (url.endsWith('/auth/oidc/status'))
        return json({ login_enabled: true, authenticated: false, principal: null })
      if (url.endsWith('/system')) return json({ code: 'unauthorized', message: 'token rejected' }, 401)
      return json(page([]))
    })
    vi.stubGlobal('fetch', fetchMock)
    try {
      render(<App />)

      expect(await screen.findByTestId('runtime-token-prompt')).toBeInTheDocument()
      expect(screen.getByText(/bearer token sent with the last request was rejected/i)).toBeInTheDocument()
      // A sent-and-rejected token does not redirect to the OIDC login: the
      // prompt is the recovery path even when the Hub offers that flow.
      expect(redirects).toEqual([])
    } finally {
      Object.defineProperty(window, 'location', { writable: true, value: realLocation })
    }
  })

  it('saves and clears the runtime token from the Settings panel', async () => {
    vi.stubEnv('VITE_API_TOKEN', '')
    const fetchMock = vi.fn((url: string) => {
      if (url.endsWith('/status')) return json({ version: 'v', state: 'running', uptime_seconds: 1 })
      if (url.endsWith('/webhooks')) return json({ subscriptions: [] })
      return json(page([]))
    })
    vi.stubGlobal('fetch', fetchMock)
    render(
      <QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}>
        <ConfirmProvider>
          <BrowserRouter>
            <Settings hubMode={true} />
          </BrowserRouter>
        </ConfirmProvider>
      </QueryClientProvider>,
    )
    expect(await screen.findByText('API credential')).toBeInTheDocument()
    expect(screen.getByText('None — protected control planes answer 401')).toBeInTheDocument()

    fireEvent.change(screen.getByLabelText('Operator bearer token'), {
      target: { value: 'panel-token' },
    })
    fireEvent.click(screen.getByRole('button', { name: 'Use token' }))
    await waitFor(() => expect(sessionStorage.getItem(RUNTIME_TOKEN_STORAGE_KEY)).toBe('panel-token'))
    expect(await screen.findByText('Entered at runtime (this tab only)')).toBeInTheDocument()

    fireEvent.click(await screen.findByRole('button', { name: 'Clear token' }))
    await waitFor(() => expect(sessionStorage.getItem(RUNTIME_TOKEN_STORAGE_KEY)).toBeNull())
    expect(await screen.findByText('None — protected control planes answer 401')).toBeInTheDocument()
  })
})
