import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { App } from './app'

const fetchMock = vi.fn()
beforeEach(() => {
  fetchMock.mockReset()
  globalThis.fetch = fetchMock
  fetchMock.mockImplementation((url: string) => Promise.resolve({ ok: true, json: async () => url.endsWith('/system') ? { version: 'test', state: 'running', uptime_seconds: 4, streams_total: 1, streams_running: 1, streams_failed: 0 } : [{ id: 'orders', state: 'running', metrics: { input_messages: 3, output_messages: 2 }, last_error: undefined }] }))
})
afterEach(() => { cleanup(); vi.restoreAllMocks() })

describe('console application', () => {
  it('renders dashboard state and stream metrics', async () => {
    render(<App />)
    expect(await screen.findByText('running')).toBeInTheDocument()
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
    fetchMock.mockImplementation((url: string) => Promise.resolve({ ok: true, json: async () => url.endsWith('/system') ? { version: 'test', state: 'running', uptime_seconds: 4, streams_total: 0, streams_running: 0, streams_failed: 0 } : [] }))
    render(<App />)
    fireEvent.click(screen.getByText('Configuration', { selector: 'a' }))
    expect(screen.getByLabelText('Configuration editor')).toBeInTheDocument()
    expect(screen.queryByText('api_token')).not.toBeInTheDocument()
  })

  it('shows stale state when the control plane becomes unavailable', async () => {
    fetchMock.mockRejectedValue(new Error('connection refused'))
    render(<App />)
    expect(await screen.findByText(/last known state/i)).toBeInTheDocument()
    expect(screen.getByText(/connection refused/i)).toBeInTheDocument()
  })
})
