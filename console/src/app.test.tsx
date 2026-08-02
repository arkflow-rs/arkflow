import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { App } from './app'

const fetchMock = vi.fn()
const page = (items: unknown[]) => ({ items, page: 1, page_size: items.length || 50, total: items.length })
beforeEach(() => {
  fetchMock.mockReset()
  globalThis.fetch = fetchMock
  window.history.replaceState(null, '', '/')
  fetchMock.mockImplementation((url: string) => Promise.resolve({ ok: true, json: async () => {
    if (url.endsWith('/system')) return { version: 'test', state: 'running', uptime_seconds: 4, streams_total: 1, streams_running: 1, streams_failed: 0, capabilities: [] }
    if (url.includes('/nodes?')) return page([{ id: 'local-node', role: 'standalone', version: 'test', state: 'running', capabilities: [], streams_total: 1, streams_running: 1, streams_failed: 0 }])
    if (url.includes('/streams')) return page([{ id: 'orders', state: 'running', metrics: { input_messages: 3, output_messages: 2 }, last_error: undefined }])
    return page([])
  } }))
})
afterEach(() => { cleanup(); vi.restoreAllMocks() })

describe('console application', () => {
  it('renders dashboard state and stream metrics', async () => {
    render(<App />)
    expect(await screen.findByText('Fleet health')).toBeInTheDocument()
    expect(fetchMock).toHaveBeenCalledWith(expect.stringContaining('/nodes?page=1&page_size=50'), expect.objectContaining({ headers: expect.objectContaining({ 'X-Correlation-ID': expect.any(String) }) }))
    fireEvent.click(screen.getByText('Streams', { selector: 'a' }))
    expect(screen.getByText(/3 input messages/)).toBeInTheDocument()
  })

  it('requires confirmation before lifecycle commands', async () => {
    vi.spyOn(window, 'confirm').mockReturnValue(false)
    render(<App />)
    fireEvent.click(screen.getByText('Streams', { selector: 'a' }))
    await screen.findByText('orders')
    fireEvent.click(screen.getByRole('button', { name: 'Stop' }))
    await waitFor(() => expect(fetchMock).not.toHaveBeenCalledWith(expect.stringContaining('/stop'), expect.anything()))
  })

  it('keeps redacted configuration values as display-only content', async () => {
    fetchMock.mockImplementation((url: string) => Promise.resolve({ ok: true, json: async () => {
      if (url.endsWith('/system')) return { version: 'test', state: 'running', uptime_seconds: 4, streams_total: 0, streams_running: 0, streams_failed: 0, capabilities: [] }
      if (url.includes('/nodes?')) return page([])
      if (url.includes('/configuration')) return {}
      return page([])
    } }))
    render(<App />)
    fireEvent.click(screen.getByText('Configuration', { selector: 'a' }))
    expect(screen.getByLabelText('Configuration editor')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Publish' })).toBeDisabled()
    expect(screen.queryByText('api_token')).not.toBeInTheDocument()
  })

  it('shows stale state when the control plane becomes unavailable', async () => {
    fetchMock.mockRejectedValue(new Error('connection refused'))
    render(<App />)
    expect(await screen.findByText(/last known state/i)).toBeInTheDocument()
    expect(screen.getByText(/connection refused/i)).toBeInTheDocument()
  })

  it('selects a node and disables mutations when its lease is stale', async () => {
    fetchMock.mockImplementation((url: string) => Promise.resolve({ ok: true, json: async () => {
      if (url.endsWith('/system')) return { version: 'hub', state: 'running', node_count: 1, capabilities: [] }
      if (url.includes('/nodes?')) return page([{ id: 'node-a', state: 'stale', capabilities: [], last_seen_at_ms: Date.now() - 5000, lease_expires_at_ms: Date.now() - 1000, streams_total: 1, streams_running: 1, streams_failed: 0 }])
      if (url.includes('/streams')) return { items: [{ id: 'orders', node_id: 'node-a', state: 'running', metrics: { input_messages: 0, output_messages: 0 } }], page: 1, page_size: 1, total: 1 }
      return { items: [], page: 1, page_size: 0, total: 0 }
    } }))
    render(<App />)
    const selector = await screen.findByLabelText('Compute node')
    fireEvent.change(selector, { target: { value: 'node-a' } })
    expect(window.location.search).toContain('node_id=node-a')
    expect(await screen.findByText(/mutating actions are disabled/i)).toBeInTheDocument()
    fireEvent.click(screen.getByText('Streams', { selector: 'a' }))
    expect((await screen.findByRole('button', { name: 'Start' })).hasAttribute('disabled')).toBe(true)
  })

  it('tracks a Hub lifecycle operation to a terminal state', async () => {
    vi.spyOn(window, 'confirm').mockReturnValue(true)
    let operationReads = 0
    fetchMock.mockImplementation((url: string, init?: RequestInit) => Promise.resolve({ ok: true, json: async () => {
      if (url.endsWith('/system')) return { version: 'hub', state: 'running', node_count: 1, capabilities: [] }
      if (url.includes('/nodes?')) return page([{ id: 'node-a', state: 'online', capabilities: ['stream_lifecycle'], streams_total: 1, streams_running: 1, streams_failed: 0 }])
      if (url.includes('/operations/hop-1')) { operationReads += 1; return { id: 'hop-1', operation: 'start', resource_type: 'stream', resource_id: 'orders', node_id: 'node-a', state: 'succeeded', progress: 100, created_at_ms: 1, correlation_id: 'console-test' } }
      if (url.includes('/operations?')) return page([{ id: 'hop-1', operation: 'start', resource_type: 'stream', resource_id: 'orders', node_id: 'node-a', state: operationReads ? 'succeeded' : 'queued', progress: operationReads ? 100 : 0, created_at_ms: 1, correlation_id: 'console-test' }])
      if (init?.method === 'POST' && url.includes('/nodes/node-a/streams/orders/start')) return { id: 'hop-1', operation: 'start', resource_type: 'stream', resource_id: 'orders', node_id: 'node-a', state: 'queued', progress: 0, created_at_ms: 1, correlation_id: 'console-test' }
      if (url.includes('/streams')) return page([{ id: 'orders', node_id: 'node-a', state: 'running', metrics: { input_messages: 0, output_messages: 0 } }])
      return page([])
    } }))
    render(<App />)
    fireEvent.change(await screen.findByLabelText('Compute node'), { target: { value: 'node-a' } })
    fireEvent.click(screen.getByText('Streams', { selector: 'a' }))
    fireEvent.click(await screen.findByRole('button', { name: 'Start' }))
    expect(await screen.findByText('succeeded')).toBeInTheDocument()
    expect(operationReads).toBeGreaterThan(0)
    expect(fetchMock).toHaveBeenCalledWith(expect.stringContaining('/nodes/node-a/streams/orders/start'), expect.objectContaining({ headers: expect.objectContaining({ 'X-Correlation-ID': expect.any(String) }) }))
  })
})
