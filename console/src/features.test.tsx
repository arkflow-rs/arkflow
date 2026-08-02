import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'
import { Configuration, convertConfiguration } from './features'

describe('configuration workflow', () => {
  it('converts YAML to JSON and preserves equivalent values', () => {
    expect(JSON.parse(convertConfiguration('streams: []\n', 'yaml', 'json'))).toEqual({ streams: [] })
    expect(convertConfiguration('{"streams":[]}', 'json', 'yaml')).toContain('streams: []')
  })

  it('waits for a successful publish operation before reloading', async () => {
    const fetchMock = vi.fn((url: string, init?: RequestInit) => {
      if (url.endsWith('/configuration/draft')) return Promise.resolve({ ok: true, json: async () => ({ format: 'json', content: '{"streams":[]}' }) })
      if (url.endsWith('/configuration')) return Promise.resolve({ ok: true, json: async () => ({ streams: [] }) })
      if (url.endsWith('/configuration/versions')) return Promise.resolve({ ok: true, json: async () => [] })
      if (url.endsWith('/configuration/validate')) return Promise.resolve({ ok: true, json: async () => ({ valid: true, errors: [] }) })
      if (init?.method === 'POST' && url.endsWith('/configuration/apply')) return Promise.resolve({ ok: true, json: async () => ({ id: 'op-1', operation: 'apply_configuration', state: 'queued', progress: 0, created_at_ms: 1 }) })
      if (url.endsWith('/operations/op-1')) return Promise.resolve({ ok: true, json: async () => ({ id: 'op-1', operation: 'apply_configuration', state: 'succeeded', progress: 100, created_at_ms: 1 }) })
      return Promise.resolve({ ok: true, json: async () => [] })
    })
    globalThis.fetch = fetchMock as unknown as typeof fetch
    render(<Configuration onError={vi.fn()} />)
    await screen.findByDisplayValue('{"streams":[]}')
    fireEvent.click(screen.getByRole('button', { name: 'Validate' }))
    await screen.findByText(/Draft is saved/)
    await waitFor(() => expect(screen.getByRole('button', { name: 'Publish' })).not.toBeDisabled())
    fireEvent.click(screen.getByRole('button', { name: 'Publish' }))
    await waitFor(() => expect(fetchMock).toHaveBeenCalledWith(expect.stringContaining('/operations/op-1'), expect.anything()))
    expect(fetchMock.mock.calls.filter(([url]) => url.endsWith('/configuration')).length).toBeGreaterThan(1)
  })
})
