import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'
import { Configuration, convertConfiguration } from './features'
import { Jobs } from './features/jobs'
import { Rollouts } from './features/rollouts'

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

describe('rollout workflow', () => {
  it('creates a bounded rollout from selected nodes', async () => {
    const fetchMock = vi.fn((url: string, init?: RequestInit) => {
      if (url.endsWith('/rollouts') && init?.method === 'POST') return Promise.resolve({ ok: true, json: async () => ({ rollout_id: 'r-1', config_version_id: 'cfg-1', state: 'pending', batch_size: 1, current_batch: 0, total_targets: 1, created_at_ms: 1, updated_at_ms: 1 }) })
      if (url.endsWith('/rollouts')) return Promise.resolve({ ok: true, json: async () => [] })
      if (url.endsWith('/rollouts/r-1')) return Promise.resolve({ ok: true, json: async () => ({ rollout: { rollout_id: 'r-1', config_version_id: 'cfg-1', state: 'pending', batch_size: 1, current_batch: 0, total_targets: 1, created_at_ms: 1, updated_at_ms: 1 }, targets: [{ rollout_id: 'r-1', node_id: 'node-a', ordinal: 0, state: 'pending', updated_at_ms: 1 }] }) })
      if (url.includes('/audit')) return Promise.resolve({ ok: true, json: async () => ({ items: [], page: 1, page_size: 50, total: 0 }) })
      return Promise.resolve({ ok: true, json: async () => ({}) })
    })
    globalThis.fetch = fetchMock as unknown as typeof fetch
    render(<Rollouts nodes={[{ id: 'node-a', state: 'online', version: 'test', capabilities: [], streams_total: 0, streams_running: 0, streams_failed: 0 }]} onError={vi.fn()} />)
    fireEvent.change(await screen.findByLabelText('Configuration version'), { target: { value: 'cfg-1' } })
    fireEvent.click(screen.getByRole('checkbox', { name: /node-a/ }))
    fireEvent.click(screen.getByRole('button', { name: 'Create rollout' }))
    expect(await screen.findByText('r-1')).toBeInTheDocument()
    expect(fetchMock).toHaveBeenCalledWith(expect.stringContaining('/rollouts'), expect.objectContaining({ method: 'POST' }))
  })

  it('renders rollout state transitions and exposes the next allowed action', async () => {
    let state = 'applying'
    const rollout = () => ({ rollout_id: 'r-1', config_version_id: 'cfg-1', state, batch_size: 1, current_batch: 0, total_targets: 1, created_at_ms: 1, updated_at_ms: 1 })
    const fetchMock = vi.fn((url: string, init?: RequestInit) => {
      if (url.endsWith('/rollouts') && init?.method === 'POST') return Promise.resolve({ ok: true, json: async () => rollout() })
      if (url.endsWith('/rollouts')) return Promise.resolve({ ok: true, json: async () => [rollout()] })
      if (url.endsWith('/rollouts/r-1/actions')) {
        state = 'paused'
        return Promise.resolve({ ok: true, json: async () => rollout() })
      }
      if (url.endsWith('/rollouts/r-1')) return Promise.resolve({ ok: true, json: async () => ({ rollout: rollout(), targets: [{ rollout_id: 'r-1', node_id: 'node-a', ordinal: 0, state, updated_at_ms: 1 }] }) })
      if (url.includes('/audit')) return Promise.resolve({ ok: true, json: async () => ({ items: [{ event_id: 1, action: 'rollout.pause', outcome: 'accepted', occurred_at_ms: 1 }], page: 1, page_size: 50, total: 1 }) })
      return Promise.resolve({ ok: true, json: async () => ({}) })
    })
    globalThis.fetch = fetchMock as unknown as typeof fetch
    render(<Rollouts nodes={[{ id: 'node-a', state: 'online', version: 'test', capabilities: [], streams_total: 0, streams_running: 0, streams_failed: 0 }]} onError={vi.fn()} />)
    fireEvent.click(await screen.findByRole('button', { name: /r-1/ }))
    expect(await screen.findByText('applying')).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'Pause' }))
    expect(await screen.findByText('paused')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Resume' })).toBeInTheDocument()
    expect(fetchMock).toHaveBeenCalledWith(expect.stringContaining('/rollouts/r-1/actions'), expect.objectContaining({ method: 'POST' }))
  })
})

describe('distributed Job workbench', () => {
  it('validates a Job plan before creating it in stopped state', async () => {
    vi.spyOn(window, 'confirm').mockReturnValue(true)
    const fetchMock = vi.fn((url: string, init?: RequestInit) => {
      if (url.endsWith('/jobs/validate')) return Promise.resolve({ ok: true, json: async () => ({ valid: true, plan: { tasks: [] }, required_capabilities: [], nodes: [], warnings: [] }) })
      if (url.endsWith('/jobs') && init?.method === 'POST') return Promise.resolve({ ok: true, json: async () => ({ job_id: 'new-job', version: 1, desired_state: 'stopped', observed_state: 'validated', convergence: 'pending', generation: 1, node_ids: [], updated_at_ms: 1 }) })
      return Promise.resolve({ ok: true, json: async () => [] })
    })
    globalThis.fetch = fetchMock as unknown as typeof fetch
    const refresh = vi.fn()
    render(<Jobs jobs={[]} nodes={[]} onRefresh={refresh} onError={vi.fn()} />)
    fireEvent.click(screen.getByRole('button', { name: 'Create Job' }))
    fireEvent.click(screen.getByRole('button', { name: 'Validate Plan' }))
    expect(await screen.findByText('Plan is valid')).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'Create stopped' }))
    await waitFor(() => expect(fetchMock).toHaveBeenCalledWith(expect.stringContaining('/jobs'), expect.objectContaining({ method: 'POST' })))
    expect(refresh).toHaveBeenCalled()
  })
})
