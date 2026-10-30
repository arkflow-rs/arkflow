import { cleanup, render, screen } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { BrowserRouter } from 'react-router'
import type { ReactElement } from 'react'
import { Events } from './features/events'
import { Audit } from './features/audit'
import { Approvals } from './features/approvals'
import { ConfirmProvider } from './features/confirm'

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
})
