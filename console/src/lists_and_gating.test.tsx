import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { BrowserRouter } from 'react-router'
import type { ReactElement } from 'react'
import { Events } from './features/events'
import { Audit } from './features/audit'
import { Approvals } from './features/approvals'
import { ConfirmProvider } from './features/confirm'
import { Runtime } from './features/runtime'
import { OperationRow } from './features/shared'

afterEach(() => {
  cleanup()
  vi.unstubAllGlobals()
  window.history.replaceState({}, '', '/')
})

const renderWithQueries = (ui: ReactElement) =>
  render(
    <QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}>
      <ConfirmProvider>
        <BrowserRouter>{ui}</BrowserRouter>
      </ConfirmProvider>
    </QueryClientProvider>,
  )

describe('server-side lists and role gating', () => {
  it('paginates events through the server and reflects filters in the URL', async () => {
    const fetchMock = vi.fn((url: string) => {
      if (url.includes('/events?')) {
        const params = new URLSearchParams(url.slice(url.indexOf('?') + 1))
        expect(params.get('page')).toBe('2')
        expect(params.get('event_type')).toBe('stream_changed')
        return Promise.resolve({
          ok: true,
          json: async () => ({
            items: [
              {
                event_id: 2,
                event_type: 'stream_changed',
                stream_id: 's-2',
                outcome: 'success',
                occurred_at_ms: 2,
              },
            ],
            page: 2,
            page_size: 50,
            total: 60,
          }),
        })
      }
      throw new Error(`unexpected fetch: ${url}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    window.history.replaceState({}, '', '/events?page=2&event_type=stream_changed')
    renderWithQueries(<Events />)
    expect(await screen.findByText('stream_changed')).toBeTruthy()
    expect(screen.getByText('Page 2 of 2')).toBeTruthy()
    expect(window.location.search).toContain('event_type=stream_changed')
  })

  it('normalizes an out-of-range page size onto a supported size', async () => {
    const fetchMock = vi.fn((url: string) => {
      if (url.includes('/events?')) {
        const params = new URLSearchParams(url.slice(url.indexOf('?') + 1))
        // The server clamps page_size to 1..=100: asking for 200 would return
        // 100 rows while the page count below assumed 200.
        expect(params.get('page_size')).toBe('100')
        return Promise.resolve({
          ok: true,
          json: async () => ({ items: [], page: 2, page_size: 100, total: 150 }),
        })
      }
      throw new Error(`unexpected fetch: ${url}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    window.history.replaceState({}, '', '/events?page=2&page_size=200')
    renderWithQueries(<Events />)
    expect(await screen.findByText('Page 2 of 2')).toBeTruthy()
  })

  it('clears event filters without dropping the selected node', async () => {
    const fetchMock = vi.fn((url: string) => {
      if (url.includes('/events?'))
        return Promise.resolve({
          ok: true,
          json: async () => ({ items: [], page: 1, page_size: 50, total: 0 }),
        })
      throw new Error(`unexpected fetch: ${url}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    window.history.replaceState(
      {},
      '',
      '/events?node_id=node-a&event_type=stream_changed&page=2&page_size=50',
    )
    renderWithQueries(<Events />)
    fireEvent.click(await screen.findByText('Clear filters'))
    await waitFor(() => expect(window.location.search).not.toContain('event_type'))
    expect(window.location.search).toContain('node_id=node-a')
    expect(window.location.search).not.toContain('page=')
    expect(window.location.search).toContain('page_size=50')
  })

  it('audits with server-side resource filtering instead of a client text filter', async () => {
    const fetchMock = vi.fn((url: string) => {
      if (url.includes('/audit?')) {
        expect(url).toContain('resource_id=job-7')
        return Promise.resolve({
          ok: true,
          json: async () => ({
            items: [
              {
                event_id: 9,
                actor: 'ops',
                action: 'job.start',
                resource_type: 'job',
                resource_id: 'job-7',
                outcome: 'success',
                occurred_at_ms: 5,
              },
            ],
            page: 1,
            page_size: 50,
            total: 1,
          }),
        })
      }
      throw new Error(`unexpected fetch: ${url}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    window.history.replaceState({}, '', '/audit?resource_id=job-7')
    renderWithQueries(<Audit onError={() => {}} />)
    expect(await screen.findByText('job.start')).toBeTruthy()
    expect(window.location.search).toContain('resource_id=job-7')
  })

  it('gates approvals decide buttons when the principal lacks approve_rollouts', async () => {
    const fetchMock = vi.fn((url: string) => {
      if (url.includes('/config-approvals'))
        return Promise.resolve({
          ok: true,
          json: async () => [
            {
              request_id: 'approval-x',
              requester_id: 'ops',
              config_version_id: 'v1',
              targets: ['node-a'],
              batch_size: 1,
              status: 'pending',
              created_at_ms: 1,
            },
          ],
        })
      throw new Error(`unexpected fetch: ${url}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    renderWithQueries(<Approvals onError={() => {}} canMutate={false} />)
    const approve = await screen.findByText('Approve')
    expect((approve as HTMLButtonElement).disabled).toBe(true)
    const reject = screen.getByText('Reject')
    expect((reject as HTMLButtonElement).disabled).toBe(true)
  })

  it('keeps the webhook add button disabled without manage_webhooks', async () => {
    const fetchMock = vi.fn((url: string) => {
      if (url.endsWith('/webhooks'))
        return Promise.resolve({ ok: true, json: async () => ({ subscriptions: [] }) })
      if (url.endsWith('/status'))
        return Promise.resolve({
          ok: true,
          json: async () => ({ version: 'v', state: 'running', uptime_seconds: 1 }),
        })
      throw new Error(`unexpected fetch: ${url}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    const { Settings } = await import('./features/settings')
    renderWithQueries(<Settings hubMode={true} canManageWebhooks={false} />)
    const add = await screen.findByText('Add subscription')
    expect((add as HTMLButtonElement).disabled).toBe(true)
  })

  it('disables webhook row actions without manage_webhooks', async () => {
    const fetchMock = vi.fn((url: string) => {
      if (url.endsWith('/webhooks'))
        return Promise.resolve({
          ok: true,
          json: async () => ({
            subscriptions: [
              {
                id: 'hook-1',
                url: 'https://example.com/hook',
                event_types: ['rollout'],
                enabled: true,
                created_by: 'ops',
                created_at_ms: 1,
                has_secret: false,
              },
            ],
          }),
        })
      if (url.endsWith('/status'))
        return Promise.resolve({
          ok: true,
          json: async () => ({ version: 'v', state: 'running', uptime_seconds: 1 }),
        })
      throw new Error(`unexpected fetch: ${url}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    const { Settings } = await import('./features/settings')
    renderWithQueries(<Settings hubMode={true} canManageWebhooks={false} />)
    for (const label of ['Test', 'Pause', 'Edit']) {
      const button = await screen.findByText(label)
      expect((button as HTMLButtonElement).disabled).toBe(true)
      expect(button.getAttribute('title')).toContain('manage_webhooks')
    }
  })

  it('keeps rollout actions visible but disabled without manage_rollouts', async () => {
    const rollout = {
      rollout_id: 'r-1',
      config_version_id: 'v1',
      state: 'running',
      batch_size: 1,
      current_batch: 0,
      total_targets: 1,
      created_at_ms: 1,
      updated_at_ms: 1,
    }
    const fetchMock = vi.fn((url: string) => {
      if (url.endsWith('/rollouts')) return Promise.resolve({ ok: true, json: async () => [rollout] })
      if (url.includes('/rollouts/r-1'))
        return Promise.resolve({
          ok: true,
          json: async () => ({ rollout, targets: [{ node_id: 'node-a', state: 'applying' }] }),
        })
      if (url.includes('/audit?'))
        return Promise.resolve({
          ok: true,
          json: async () => ({ items: [], page: 1, page_size: 50, total: 0 }),
        })
      if (url.includes('/nodes?'))
        return Promise.resolve({
          ok: true,
          json: async () => ({ items: [], page: 1, page_size: 50, total: 0 }),
        })
      if (url.endsWith('/status'))
        return Promise.resolve({
          ok: true,
          json: async () => ({ version: 'v', state: 'running', uptime_seconds: 1 }),
        })
      throw new Error(`unexpected fetch: ${url}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    const { Rollouts } = await import('./features/rollouts')
    renderWithQueries(<Rollouts onError={() => {}} canMutate={false} />)
    fireEvent.click(await screen.findByText('r-1'))
    for (const label of ['Pause', 'Cancel', 'Rollback']) {
      const button = await screen.findByText(label)
      expect((button as HTMLButtonElement).disabled).toBe(true)
      expect(button.getAttribute('title')).toContain('manage_rollouts')
    }
  })

  it('carries operation filters and pagination in the URL', async () => {
    const calls: string[] = []
    const fetchMock = vi.fn((url: string) => {
      calls.push(String(url))
      if (url.includes('/operations?'))
        return Promise.resolve({
          ok: true,
          json: async () => ({
            items: [
              {
                id: 'op-1',
                operation: 'stream.start',
                resource_id: 'orders',
                state: 'running',
                progress: 50,
                created_at_ms: 1,
              },
            ],
            page: 2,
            page_size: 25,
            total: 60,
          }),
        })
      if (url.includes('/streams'))
        return Promise.resolve({
          ok: true,
          json: async () => ({ items: [], page: 1, page_size: 50, total: 0 }),
        })
      if (url.includes('/events'))
        return Promise.resolve({
          ok: true,
          json: async () => ({ items: [], page: 1, page_size: 50, total: 0 }),
        })
      throw new Error(`unexpected fetch: ${url}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    window.history.replaceState(
      {},
      '',
      '/runtime?operation=stream.start&state=running&correlation_id=c-1&page=2&page_size=25',
    )
    renderWithQueries(<Runtime command={async () => {}} />)
    expect(await screen.findByText('Page 2 of 3')).toBeTruthy()
    const operationsCall = calls.find((url) => url.includes('/operations?'))
    expect(operationsCall).toContain('operation=stream.start')
    expect(operationsCall).toContain('state=running')
    expect(operationsCall).toContain('correlation_id=c-1')
    expect(operationsCall).toContain('page=2')
    expect(operationsCall).toContain('page_size=25')
  })

  it('fetches operation history for the selected stream independently', async () => {
    const calls: string[] = []
    const fetchMock = vi.fn((url: string) => {
      calls.push(String(url))
      if (url.includes('/operations?')) {
        const params = new URLSearchParams(url.slice(url.indexOf('?') + 1))
        if (params.get('resource_id') === 'orders')
          return Promise.resolve({
            ok: true,
            json: async () => ({
              items: [
                {
                  id: 'op-detail',
                  operation: 'stream.restart',
                  resource_id: 'orders',
                  state: 'succeeded',
                  progress: 100,
                  created_at_ms: 1,
                },
              ],
              page: 1,
              page_size: 10,
              total: 1,
            }),
          })
        return Promise.resolve({
          ok: true,
          json: async () => ({ items: [], page: 1, page_size: 25, total: 0 }),
        })
      }
      if (url.includes('/streams'))
        return Promise.resolve({
          ok: true,
          json: async () => ({
            items: [
              {
                id: 'orders',
                state: 'running',
                metrics: { input_messages: 0, output_messages: 0 },
              },
            ],
            page: 1,
            page_size: 50,
            total: 1,
          }),
        })
      if (url.includes('/events'))
        return Promise.resolve({
          ok: true,
          json: async () => ({ items: [], page: 1, page_size: 50, total: 0 }),
        })
      throw new Error(`unexpected fetch: ${url}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    window.history.replaceState({}, '', '/runtime')
    renderWithQueries(<Runtime command={async () => {}} />)
    fireEvent.click(await screen.findByText('orders'))
    expect(await screen.findByText('stream.restart')).toBeTruthy()
    expect(calls.some((url) => url.includes('/operations?') && url.includes('resource_id=orders'))).toBe(true)
  })

  it('disables operation cancellation without operate', () => {
    renderWithQueries(
      <OperationRow
        operation={{
          id: 'op-1',
          operation: 'stream.start',
          resource_id: 'orders',
          state: 'running',
          progress: 10,
          created_at_ms: 1,
        }}
        canMutate={false}
      />,
    )
    const cancel = screen.getByText('Cancel')
    expect((cancel as HTMLButtonElement).disabled).toBe(true)
    expect(cancel.getAttribute('title')).toContain('operate')
  })

  it('pushes a history entry when paging events', async () => {
    const fetchMock = vi.fn((url: string) => {
      if (url.includes('/events?'))
        return Promise.resolve({
          ok: true,
          json: async () => ({ items: [], page: 1, page_size: 50, total: 150 }),
        })
      throw new Error(`unexpected fetch: ${url}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    window.history.replaceState({}, '', '/events')
    const push = vi.spyOn(window.history, 'pushState')
    renderWithQueries(<Events />)
    fireEvent.click(await screen.findByText('Next'))
    await waitFor(() => expect(push).toHaveBeenCalled())
    push.mockRestore()
  })

  it('disables the open webhook form when manage_webhooks is revoked', async () => {
    const fetchMock = vi.fn((url: string) => {
      if (url.endsWith('/webhooks'))
        return Promise.resolve({ ok: true, json: async () => ({ subscriptions: [] }) })
      if (url.endsWith('/status'))
        return Promise.resolve({
          ok: true,
          json: async () => ({ version: 'v', state: 'running', uptime_seconds: 1 }),
        })
      throw new Error(`unexpected fetch: ${url}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    const { Settings } = await import('./features/settings')
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    const tree = (canManageWebhooks: boolean) => (
      <QueryClientProvider client={client}>
        <ConfirmProvider>
          <BrowserRouter>
            <Settings hubMode={true} canManageWebhooks={canManageWebhooks} />
          </BrowserRouter>
        </ConfirmProvider>
      </QueryClientProvider>
    )
    const view = render(tree(true))
    fireEvent.click(await screen.findByText('Add subscription'))
    expect((screen.getByText('Save set') as HTMLButtonElement).disabled).toBe(false)
    // Revoked mid-edit: the open form must not keep a mutation enabled.
    view.rerender(tree(false))
    const save = screen.getByText('Save set')
    expect((save as HTMLButtonElement).disabled).toBe(true)
    expect(save.getAttribute('title')).toContain('manage_webhooks')
  })

  it('gates approval resubmission on manage_rollouts instead of approve_rollouts', async () => {
    const fetchMock = vi.fn((url: string) => {
      if (url.includes('/config-approvals'))
        return Promise.resolve({
          ok: true,
          json: async () => [
            {
              request_id: 'approval-r',
              requester_id: 'ops',
              config_version_id: 'v1',
              targets: ['node-a'],
              batch_size: 1,
              status: 'rejected',
              created_at_ms: 1,
            },
          ],
        })
      throw new Error(`unexpected fetch: ${url}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    // canMutate (approve_rollouts) is held, but resubmission is a submission
    // the Hub enforces as manage_rollouts: it must gate on that alone.
    renderWithQueries(<Approvals onError={() => {}} canMutate={true} canSubmit={false} />)
    const resubmit = await screen.findByText('Resubmit')
    expect((resubmit as HTMLButtonElement).disabled).toBe(true)
  })
})
