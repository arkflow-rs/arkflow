import { cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { App } from './app'

const fetchMock = vi.fn()
const page = (items: unknown[]) => ({ items, page: 1, page_size: items.length || 50, total: items.length })
beforeEach(() => {
  fetchMock.mockReset()
  globalThis.fetch = fetchMock
  window.history.replaceState(null, '', '/')
  fetchMock.mockImplementation((url: string) =>
    Promise.resolve({
      ok: true,
      json: async () => {
        if (url.endsWith('/system'))
          return {
            version: 'test',
            state: 'running',
            uptime_seconds: 4,
            streams_total: 1,
            streams_running: 1,
            streams_failed: 0,
            capabilities: [],
          }
        if (url.includes('/nodes?'))
          return page([
            {
              id: 'local-node',
              role: 'standalone',
              version: 'test',
              state: 'running',
              capabilities: [],
              streams_total: 1,
              streams_running: 1,
              streams_failed: 0,
            },
          ])
        if (url.includes('/streams'))
          return page([
            {
              id: 'orders',
              state: 'running',
              metrics: { input_messages: 3, output_messages: 2 },
              last_error: undefined,
            },
          ])
        return page([])
      },
    }),
  )
})
afterEach(() => {
  cleanup()
  vi.restoreAllMocks()
})

describe('console application', () => {
  it('renders dashboard state and stream metrics', async () => {
    render(<App />)
    expect(await screen.findByText('Fleet health')).toBeInTheDocument()
    expect(fetchMock).toHaveBeenCalledWith(
      expect.stringContaining('/nodes?page=1&page_size=50'),
      expect.objectContaining({
        headers: expect.objectContaining({ 'X-Correlation-ID': expect.any(String) }),
      }),
    )
    fireEvent.click(screen.getByText('Streams', { selector: 'a' }))
    expect(screen.getByText(/3 input messages/)).toBeInTheDocument()
  })

  it('requires confirmation before lifecycle commands', async () => {
    render(<App />)
    fireEvent.click(screen.getByText('Streams', { selector: 'a' }))
    await screen.findByText('orders')
    fireEvent.click(screen.getByRole('button', { name: 'Stop' }))
    const dialog = await screen.findByRole('alertdialog')
    fireEvent.click(within(dialog).getByRole('button', { name: 'Cancel' }))
    await waitFor(() =>
      expect(fetchMock).not.toHaveBeenCalledWith(expect.stringContaining('/stop'), expect.anything()),
    )
  })

  it('keeps redacted configuration values as display-only content', async () => {
    fetchMock.mockImplementation((url: string) =>
      Promise.resolve({
        ok: true,
        json: async () => {
          if (url.endsWith('/system'))
            return {
              version: 'test',
              state: 'running',
              uptime_seconds: 4,
              streams_total: 0,
              streams_running: 0,
              streams_failed: 0,
              capabilities: [],
            }
          if (url.includes('/nodes?')) return page([])
          if (url.includes('/configuration')) return {}
          return page([])
        },
      }),
    )
    render(<App />)
    fireEvent.click(screen.getByText('Configuration', { selector: 'a' }))
    // The page waits for /system to decide local vs Hub mode before loading.
    expect(await screen.findByLabelText('Configuration editor')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Publish' })).toBeDisabled()
    expect(screen.queryByText('api_token')).not.toBeInTheDocument()
  })

  it('shows stale state when the control plane becomes unavailable', async () => {
    fetchMock.mockRejectedValue(new Error('connection refused'))
    render(<App />)
    expect(await screen.findByText(/last known state/i)).toBeInTheDocument()
    expect(screen.getByText(/connection refused/i)).toBeInTheDocument()
  })

  it('explains a standby Hub instead of the generic stale banner', async () => {
    fetchMock.mockImplementation(() =>
      Promise.resolve({
        ok: false,
        status: 503,
        json: async () => ({
          code: 'hub_standby',
          message: 'This Hub instance is a standby',
          correlation_id: 'c-1',
        }),
      }),
    )
    render(<App />)
    expect(await screen.findByText(/standby and does not hold the control-plane lease/i)).toBeInTheDocument()
    expect(screen.queryByText(/last known state/i)).toBeNull()
  })

  it('renders HA leadership and JSON metrics in Hub mode', async () => {
    fetchMock.mockImplementation((url: string) =>
      Promise.resolve({
        ok: true,
        json: async () => {
          if (url.endsWith('/system'))
            return {
              id: 'arkflow-control-hub',
              version: 'hub',
              state: 'running',
              node_count: 1,
              stream_count: 0,
              active_operations: 0,
              capabilities: [],
              ha: { enabled: true, role: 'leader', epoch: 3, transitions: 1 },
            }
          if (url.endsWith('/status'))
            return {
              version: 'hub',
              state: 'running',
              uptime_seconds: 9,
              streams_total: 2,
              streams_running: 1,
              streams_failed: 1,
            }
          if (url.includes('/metrics'))
            return {
              items: [{ node_id: 'n1', metrics: { input_batches: 5 } }],
              aggregate: { input_batches: 5 },
            }
          if (url.includes('/nodes?'))
            return page([
              {
                id: 'n1',
                version: 'agent',
                state: 'online',
                capabilities: [],
                streams_total: 2,
                streams_running: 1,
                streams_failed: 1,
              },
            ])
          return page([])
        },
      }),
    )
    render(<App />)
    expect(await screen.findByText(/HA: leader · epoch 3/)).toBeInTheDocument()
    expect(await screen.findByText('input batches')).toBeInTheDocument()
    await waitFor(() =>
      expect(fetchMock).toHaveBeenCalledWith(
        expect.stringMatching(/\/metrics/),
        expect.objectContaining({
          headers: expect.objectContaining({ Accept: 'application/json' }),
        }),
      ),
    )
  })

  it('selects a node and disables mutations when its lease is stale', async () => {
    fetchMock.mockImplementation((url: string) =>
      Promise.resolve({
        ok: true,
        json: async () => {
          if (url.endsWith('/system'))
            return { version: 'hub', state: 'running', node_count: 1, capabilities: [] }
          if (url.includes('/nodes?'))
            return page([
              {
                id: 'node-a',
                state: 'stale',
                capabilities: [],
                last_seen_at_ms: Date.now() - 5000,
                lease_expires_at_ms: Date.now() - 1000,
                streams_total: 1,
                streams_running: 1,
                streams_failed: 0,
              },
            ])
          if (url.includes('/streams'))
            return {
              items: [
                {
                  id: 'orders',
                  node_id: 'node-a',
                  state: 'running',
                  metrics: { input_messages: 0, output_messages: 0 },
                },
              ],
              page: 1,
              page_size: 1,
              total: 1,
            }
          return { items: [], page: 1, page_size: 0, total: 0 }
        },
      }),
    )
    render(<App />)
    const selector = await screen.findByLabelText('Compute node')
    await waitFor(() => {
      const select = screen.getByLabelText('Compute node') as HTMLSelectElement
      expect([...select.options].some((option) => option.value === 'node-a')).toBe(true)
    })
    fireEvent.change(selector, { target: { value: 'node-a' } })
    await waitFor(() => expect(window.location.search).toContain('node_id=node-a'))
    expect(await screen.findByText(/mutating actions are disabled/i)).toBeInTheDocument()
    fireEvent.click(screen.getByText('Streams', { selector: 'a' }))
    expect((await screen.findByRole('button', { name: 'Start' })).hasAttribute('disabled')).toBe(true)
  })

  it('shows fleet-wide audit records on the audit page', async () => {
    fetchMock.mockImplementation((url: string) =>
      Promise.resolve({
        ok: true,
        json: async () => {
          if (url.endsWith('/system'))
            return { version: 'hub', state: 'running', node_count: 1, capabilities: [] }
          if (url.includes('/nodes?')) return page([])
          if (url.includes('/audit'))
            return {
              items: [
                {
                  event_id: 1,
                  action: 'node.drain',
                  actor: 'operator',
                  resource_type: 'node',
                  resource_id: 'node-a',
                  outcome: 'accepted',
                  occurred_at_ms: 1,
                },
              ],
              page: 1,
              page_size: 50,
              total: 1,
            }
          return page([])
        },
      }),
    )
    render(<App />)
    fireEvent.click(screen.getByText('Audit', { selector: 'a' }))
    expect(await screen.findByText('node.drain')).toBeInTheDocument()
    expect(fetchMock).toHaveBeenCalledWith(
      expect.stringContaining('/audit'),
      expect.objectContaining({
        headers: expect.objectContaining({ 'X-Correlation-ID': expect.any(String) }),
      }),
    )
  })

  it('tracks a Hub lifecycle operation to a terminal state', async () => {
    let operationReads = 0
    fetchMock.mockImplementation((url: string, init?: RequestInit) =>
      Promise.resolve({
        ok: true,
        json: async () => {
          if (url.endsWith('/system'))
            return { version: 'hub', state: 'running', node_count: 1, capabilities: [] }
          if (url.includes('/nodes?'))
            return page([
              {
                id: 'node-a',
                state: 'online',
                capabilities: ['stream_lifecycle'],
                streams_total: 1,
                streams_running: 1,
                streams_failed: 0,
              },
            ])
          if (url.includes('/operations/hop-1')) {
            operationReads += 1
            return {
              id: 'hop-1',
              operation: 'start',
              resource_type: 'stream',
              resource_id: 'orders',
              node_id: 'node-a',
              state: 'succeeded',
              progress: 100,
              created_at_ms: 1,
              correlation_id: 'console-test',
            }
          }
          if (url.includes('/operations?'))
            return page([
              {
                id: 'hop-1',
                operation: 'start',
                resource_type: 'stream',
                resource_id: 'orders',
                node_id: 'node-a',
                state: operationReads ? 'succeeded' : 'queued',
                progress: operationReads ? 100 : 0,
                created_at_ms: 1,
                correlation_id: 'console-test',
              },
            ])
          if (init?.method === 'POST' && url.includes('/nodes/node-a/streams/orders/start'))
            return {
              id: 'hop-1',
              operation: 'start',
              resource_type: 'stream',
              resource_id: 'orders',
              node_id: 'node-a',
              state: 'queued',
              progress: 0,
              created_at_ms: 1,
              correlation_id: 'console-test',
            }
          if (url.includes('/streams'))
            return page([
              {
                id: 'orders',
                node_id: 'node-a',
                state: 'running',
                metrics: { input_messages: 0, output_messages: 0 },
              },
            ])
          return page([])
        },
      }),
    )
    render(<App />)
    const selector = await screen.findByLabelText('Compute node')
    await waitFor(() => {
      const select = screen.getByLabelText('Compute node') as HTMLSelectElement
      expect([...select.options].some((option) => option.value === 'node-a')).toBe(true)
    })
    fireEvent.change(selector, { target: { value: 'node-a' } })
    await waitFor(() => expect(window.location.search).toContain('node_id=node-a'))
    fireEvent.click(screen.getByText('Streams', { selector: 'a' }))
    fireEvent.click(await screen.findByRole('button', { name: 'Start' }))
    fireEvent.click(within(await screen.findByRole('alertdialog')).getByRole('button', { name: 'Start' }))
    expect(await screen.findByText('succeeded')).toBeInTheDocument()
    expect(operationReads).toBeGreaterThan(0)
    expect(fetchMock).toHaveBeenCalledWith(
      expect.stringContaining('/nodes/node-a/streams/orders/start'),
      expect.objectContaining({
        headers: expect.objectContaining({ 'X-Correlation-ID': expect.any(String) }),
      }),
    )
  })

  it('shows a permission failure without retrying the mutation', async () => {
    fetchMock.mockImplementation((url: string, init?: RequestInit) => {
      if (init?.method === 'POST' && url.includes('/nodes/node-a/streams/orders/start'))
        return Promise.resolve({
          ok: false,
          status: 403,
          headers: new Headers(),
          json: async () => ({ code: 'forbidden', message: 'Operator is not authorized' }),
        })
      if (url.endsWith('/system'))
        return Promise.resolve({
          ok: true,
          json: async () => ({ version: 'hub', state: 'running', node_count: 1, capabilities: [] }),
        })
      if (url.includes('/nodes?'))
        return Promise.resolve({
          ok: true,
          json: async () =>
            page([
              {
                id: 'node-a',
                state: 'online',
                capabilities: ['stream_lifecycle'],
                streams_total: 1,
                streams_running: 1,
                streams_failed: 0,
              },
            ]),
        })
      if (url.includes('/streams'))
        return Promise.resolve({
          ok: true,
          json: async () =>
            page([
              {
                id: 'orders',
                node_id: 'node-a',
                state: 'running',
                metrics: { input_messages: 0, output_messages: 0 },
              },
            ]),
        })
      return Promise.resolve({ ok: true, json: async () => page([]) })
    })
    render(<App />)
    const selector = await screen.findByLabelText('Compute node')
    await waitFor(() => {
      const select = screen.getByLabelText('Compute node') as HTMLSelectElement
      expect([...select.options].some((option) => option.value === 'node-a')).toBe(true)
    })
    fireEvent.change(selector, { target: { value: 'node-a' } })
    await waitFor(() => expect(window.location.search).toContain('node_id=node-a'))
    fireEvent.click(screen.getByText('Streams', { selector: 'a' }))
    fireEvent.click(await screen.findByRole('button', { name: 'Start' }))
    fireEvent.click(within(await screen.findByRole('alertdialog')).getByRole('button', { name: 'Start' }))
    expect(await screen.findByText(/not authorized/i)).toBeInTheDocument()
    expect(
      fetchMock.mock.calls.filter(([url, init]) => String(url).includes('/start') && init?.method === 'POST'),
    ).toHaveLength(1)
  })

  it('switches the interface language and persists the choice', async () => {
    const { LOCALE_STORAGE_KEY, LocaleProvider } = await import('./i18n')
    render(
      <LocaleProvider>
        <App />
      </LocaleProvider>,
    )
    expect(await screen.findByText('Fleet health')).toBeInTheDocument()
    fireEvent.change(screen.getByRole('combobox', { name: 'Language' }), { target: { value: 'zh' } })
    expect(await screen.findByText('集群健康')).toBeInTheDocument()
    expect(window.localStorage.getItem(LOCALE_STORAGE_KEY)).toBe('zh')
    cleanup()
    render(
      <LocaleProvider>
        <App />
      </LocaleProvider>,
    )
    expect(await screen.findByText('集群健康')).toBeInTheDocument()
    window.localStorage.removeItem(LOCALE_STORAGE_KEY)
  })

  it('opens the page addressed by the URL path', async () => {
    fetchMock.mockImplementation((url: string) =>
      Promise.resolve({
        ok: true,
        json: async () => {
          if (url.endsWith('/system'))
            return {
              version: 'test',
              state: 'running',
              uptime_seconds: 4,
              streams_total: 0,
              streams_running: 0,
              streams_failed: 0,
              capabilities: [],
            }
          if (url.endsWith('/jobs')) return []
          return page([])
        },
      }),
    )
    window.history.replaceState(null, '', '/jobs')
    render(<App />)
    expect(await screen.findByText('No distributed Jobs match the current filters.')).toBeInTheDocument()
    expect(screen.getByLabelText('Job filter')).toBeInTheDocument()
    expect(window.location.pathname).toBe('/jobs')
  })

  it('redirects legacy ?page= links to their path, preserving other params', async () => {
    fetchMock.mockImplementation((url: string) =>
      Promise.resolve({
        ok: true,
        json: async () => {
          if (url.endsWith('/system'))
            return {
              version: 'test',
              state: 'running',
              uptime_seconds: 4,
              streams_total: 0,
              streams_running: 0,
              streams_failed: 0,
              capabilities: [],
            }
          if (url.endsWith('/jobs')) return []
          return page([])
        },
      }),
    )
    window.history.replaceState(null, '', '/?page=jobs&node_id=node-a')
    render(<App />)
    expect(await screen.findByText('No distributed Jobs match the current filters.')).toBeInTheDocument()
    expect(window.location.pathname).toBe('/jobs')
    expect(window.location.search).toBe('?node_id=node-a')
  })

  it('navigates with browser back', async () => {
    render(<App />)
    fireEvent.click(screen.getByText('Streams', { selector: 'a' }))
    expect(window.location.pathname).toBe('/runtime')
    window.history.back()
    expect(await screen.findByText('Fleet health')).toBeInTheDocument()
    expect(window.location.pathname).toBe('/')
  })

  it('refetches node-scoped resources when the node filter changes', async () => {
    render(<App />)
    await screen.findByText('Fleet health')
    fireEvent.click(screen.getByText('Streams', { selector: 'a' }))
    await screen.findByText('orders')
    const scopedCalls = () =>
      fetchMock.mock.calls.filter(([url]) => String(url).includes('/streams?node_id=local-node')).length
    await waitFor(() => {
      const select = screen.getByLabelText('Compute node') as HTMLSelectElement
      expect([...select.options].some((option) => option.value === 'local-node')).toBe(true)
    })
    const selector = screen.getByLabelText('Compute node') as HTMLSelectElement
    fireEvent.change(selector, { target: { value: 'local-node' } })
    await waitFor(() => expect(window.location.search).toContain('node_id=local-node'))
    await waitFor(() => expect(scopedCalls()).toBeGreaterThan(0))
  })

  it('keeps the last snapshot visible behind the stale banner when the API fails', async () => {
    render(<App />)
    await screen.findByText('Fleet health')
    const refreshButton = await screen.findByRole('button', { name: /refresh/i })
    await waitFor(() => expect(refreshButton).toBeEnabled())
    fetchMock.mockRejectedValue(new Error('connection refused'))
    fireEvent.click(refreshButton)
    expect(await screen.findByText(/last known state/i)).toBeInTheDocument()
    expect(screen.getByText('Fleet health')).toBeInTheDocument()
  })

  it('marks the view stale when a page-scoped live query fails while nodes stay healthy', async () => {
    render(<App />)
    fireEvent.click(screen.getByText('Streams', { selector: 'a' }))
    await screen.findByText('orders')
    fetchMock.mockImplementation((url: string) =>
      Promise.resolve({
        ok: url.includes('/nodes?'),
        status: url.includes('/nodes?') ? 200 : 500,
        headers: new Headers(),
        json: async () => {
          if (url.includes('/nodes?'))
            return page([
              {
                id: 'local-node',
                state: 'online',
                version: 'test',
                capabilities: [],
                streams_total: 0,
                streams_running: 0,
                streams_failed: 0,
              },
            ])
          return { code: 'internal', message: 'storage unavailable' }
        },
      }),
    )
    const refreshButton = await screen.findByRole('button', { name: /refresh/i })
    await waitFor(() => expect(refreshButton).toBeEnabled())
    fireEvent.click(refreshButton)
    expect(await screen.findByText(/last known state/i)).toBeInTheDocument()
    expect(screen.getByText('orders')).toBeInTheDocument()
  })

  it('switches the theme, persists it, and updates the document', async () => {
    const { THEME_STORAGE_KEY } = await import('./theme')
    render(<App />)
    await screen.findByText('Fleet health')
    fireEvent.change(screen.getByRole('combobox', { name: 'Theme' }), { target: { value: 'light' } })
    await waitFor(() => expect(document.documentElement.dataset.theme).toBe('light'))
    expect(window.localStorage.getItem(THEME_STORAGE_KEY)).toBe('light')
    window.localStorage.removeItem(THEME_STORAGE_KEY)
  })

  it('renders skeleton rows while data is loading', async () => {
    fetchMock.mockImplementation(() => new Promise(() => undefined))
    render(<App />)
    await waitFor(() => expect(document.querySelector('.skeleton')).toBeInTheDocument())
  })

  it('keeps the configuration draft while live resources refresh', async () => {
    fetchMock.mockImplementation((url: string) =>
      Promise.resolve({
        ok: true,
        json: async () => {
          if (url.endsWith('/configuration/draft')) return null
          if (url.endsWith('/configuration')) return { streams: [] }
          return page([])
        },
      }),
    )
    window.history.replaceState(null, '', '/configuration')
    render(<App />)
    const editor = await screen.findByLabelText('Configuration editor')
    await waitFor(() => expect(editor).toHaveValue('{\n  "streams": []\n}'))
    fireEvent.change(editor, { target: { value: 'streams: [] # my draft' } })
    const configCalls = () =>
      fetchMock.mock.calls.filter(([url]) => String(url).endsWith('/configuration')).length
    fireEvent.click(screen.getByRole('button', { name: /refresh/i }))
    await new Promise((resolve) => setTimeout(resolve, 50))
    expect(configCalls()).toBe(1)
    expect(editor).toHaveValue('streams: [] # my draft')
  })
})
