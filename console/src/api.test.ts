import { afterEach, describe, expect, it, vi } from 'vitest'
import { oidcLogout, oidcStatus, redirectToOidcLogin, request, streamEvents } from './api'

describe('control-plane event stream', () => {
  afterEach(() => {
    vi.useRealTimers()
    vi.restoreAllMocks()
  })

  it('reconnects with the last durable event id after a dropped stream', async () => {
    vi.useFakeTimers()
    const events: unknown[] = []
    let calls = 0
    const fetchMock = vi.fn((_url: string, init?: RequestInit) => {
      calls += 1
      if (calls === 2) expect(new Headers(init?.headers).get('Last-Event-ID')).toBe('42')
      const payload =
        calls === 1
          ? 'id: 42\nevent: stream_changed\ndata: {"event_type":"stream_changed","outcome":"accepted"}\n\n'
          : ''
      const stream = new ReadableStream<Uint8Array>({
        start(controller) {
          if (payload) controller.enqueue(new TextEncoder().encode(payload))
          controller.close()
        },
      })
      return Promise.resolve({ ok: true, status: 200, body: stream })
    })
    globalThis.fetch = fetchMock as unknown as typeof fetch
    const controller = streamEvents((event) => events.push(event))
    await vi.waitFor(() => expect(events).toHaveLength(1))
    await vi.advanceTimersByTimeAsync(1000)
    await vi.waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2))
    controller.abort()
    expect(events[0]).toMatchObject({ event_type: 'stream_changed' })
  })
})

describe('OIDC console integration', () => {
  afterEach(() => {
    vi.restoreAllMocks()
    sessionStorage.clear()
    delete (globalThis as Record<string, unknown>).fetch
  })

  it('redirects a 401 to the OIDC login when the flow is enabled', async () => {
    sessionStorage.clear()
    const locations: string[] = []
    Object.defineProperty(window, 'location', {
      writable: true,
      value: { assign: (value: string) => locations.push(value) },
    })
    vi.stubEnv('VITE_API_TOKEN', '')
    const fetchMock = vi.fn((url: string | URL | Request) => {
      const path = String(url)
      if (path.endsWith('/auth/oidc/status')) {
        return Promise.resolve({
          ok: true,
          status: 200,
          json: () => Promise.resolve({ login_enabled: true, authenticated: false, principal: null }),
        })
      }
      return Promise.resolve({
        ok: false,
        status: 401,
        headers: new Headers(),
        json: () => Promise.resolve({}),
      })
    })
    globalThis.fetch = fetchMock as unknown as typeof fetch

    await expect(request('/system')).rejects.toMatchObject({ status: 401 })
    await new Promise((resolve) => setTimeout(resolve, 0))
    expect(locations).toEqual(['/api/v1/auth/oidc/login'])

    // The redirect guard must prevent an immediate loop.
    await expect(request('/system')).rejects.toMatchObject({ status: 401 })
    await new Promise((resolve) => setTimeout(resolve, 0))
    expect(locations).toEqual(['/api/v1/auth/oidc/login'])
    vi.unstubAllEnvs()
  })

  it('does not redirect when a static token is configured', async () => {
    sessionStorage.clear()
    const locations: string[] = []
    vi.stubEnv('VITE_API_TOKEN', 'static-token')
    const fetchMock = vi.fn((url: string | URL | Request) => {
      const path = String(url)
      if (path.endsWith('/auth/oidc/status')) {
        return Promise.resolve({
          ok: true,
          status: 200,
          json: () => Promise.resolve({ login_enabled: true, authenticated: false, principal: null }),
        })
      }
      return Promise.resolve({
        ok: false,
        status: 401,
        headers: new Headers(),
        json: () => Promise.resolve({}),
      })
    })
    globalThis.fetch = fetchMock as unknown as typeof fetch

    await expect(request('/system')).rejects.toMatchObject({ status: 401 })
    await new Promise((resolve) => setTimeout(resolve, 0))
    expect(locations).toEqual([])
    vi.unstubAllEnvs()
  })

  it('probes the status endpoint once and caches the result', async () => {
    sessionStorage.clear()
    const fetchMock = vi.fn((url: string | URL | Request) => {
      expect(String(url)).toContain('/auth/oidc/status')
      return Promise.resolve({
        ok: true,
        status: 200,
        json: () =>
          Promise.resolve({
            login_enabled: true,
            authenticated: true,
            principal: { id: 'u1', roles: ['viewer'] },
          }),
      })
    })
    globalThis.fetch = fetchMock as unknown as typeof fetch
    const first = await oidcStatus()
    const second = await oidcStatus()
    expect(first).toEqual({
      login_enabled: true,
      authenticated: true,
      principal: { id: 'u1', roles: ['viewer'] },
    })
    expect(second).toBe(first)
    expect(fetchMock).toHaveBeenCalledTimes(1)
  })
})

function locations_missing(): boolean {
  return true
}
