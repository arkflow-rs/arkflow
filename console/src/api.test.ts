import { afterEach, describe, expect, it, vi } from 'vitest'
import {
  oidcStatus,
  request,
  resetOidcStatusCacheForTests,
  resolveDiff,
  resolveValidation,
  streamEvents,
} from './api'

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
    resetOidcStatusCacheForTests()
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
    resetOidcStatusCacheForTests()
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
    resetOidcStatusCacheForTests()
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

describe('read-only configuration reports', () => {
  afterEach(() => {
    vi.restoreAllMocks()
    delete (globalThis as Record<string, unknown>).fetch
  })

  it('unwraps a tracked Hub validation operation into the report', async () => {
    let postedUrl = ''
    const fetchMock = vi.fn((url: string) => {
      if (url.includes('/configuration/validate')) {
        postedUrl = url
        return Promise.resolve({
          ok: true,
          status: 202,
          json: async () => ({
            id: 'op-1',
            operation: 'validate_configuration',
            progress: 0,
            state: 'queued',
            created_at_ms: 1,
          }),
        })
      }
      if (url.includes('/operations/op-1'))
        return Promise.resolve({
          ok: true,
          json: async () => ({
            id: 'op-1',
            operation: 'validate_configuration',
            progress: 100,
            state: 'succeeded',
            created_at_ms: 1,
            result: { valid: false, errors: [{ path: 'streams', message: 'unknown input' }] },
          }),
        })
      return Promise.reject(new Error(`unexpected ${url}`))
    })
    globalThis.fetch = fetchMock as unknown as typeof fetch
    const report = await resolveValidation({ format: 'yaml', content: 'streams: []\n' }, 'node-a')
    expect(postedUrl).toContain('/nodes/node-a/configuration/validate')
    expect(report.valid).toBe(false)
    expect(report.errors[0]?.path).toBe('streams')
  })

  it('passes the synchronous local-mode report through unchanged', async () => {
    const fetchMock = vi.fn(() =>
      Promise.resolve({ ok: true, json: async () => ({ valid: true, errors: [] }) }),
    )
    globalThis.fetch = fetchMock as unknown as typeof fetch
    const report = await resolveValidation({ format: 'yaml', content: 'streams: []\n' })
    expect(report.valid).toBe(true)
  })

  it('unwraps a tracked Hub diff operation into the diff metadata', async () => {
    const fetchMock = vi.fn((url: string) => {
      if (url.includes('/configuration/diff'))
        return Promise.resolve({
          ok: true,
          status: 202,
          json: async () => ({
            id: 'op-2',
            operation: 'diff_configuration',
            progress: 0,
            state: 'queued',
            created_at_ms: 1,
          }),
        })
      if (url.includes('/operations/op-2'))
        return Promise.resolve({
          ok: true,
          json: async () => ({
            id: 'op-2',
            operation: 'diff_configuration',
            progress: 100,
            state: 'succeeded',
            created_at_ms: 1,
            result: { from: 'v1', to: 'v2', changed: true, from_format: 'yaml', to_format: 'json' },
          }),
        })
      return Promise.reject(new Error(`unexpected ${url}`))
    })
    globalThis.fetch = fetchMock as unknown as typeof fetch
    const diff = await resolveDiff('v1', 'v2', 'node-a')
    expect(diff.changed).toBe(true)
    expect(diff.from_format).toBe('yaml')
  })
})
