import { cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { BrowserRouter } from 'react-router'
import type { ReactElement } from 'react'
import { Components } from './features/components'
import { Configuration, convertConfiguration } from './features/configuration'
import { Overview } from './features/overview'
import { ConfirmProvider } from './features/confirm'
import { Audit } from './features/audit'
import { Jobs } from './features/jobs'
import { JobEditor } from './features/job-editor'
import type { Job, JobCheckpoint } from './api'
import { Rollouts } from './features/rollouts'

afterEach(() => cleanup())

const page = (items: unknown[]) => ({ items, page: 1, page_size: items.length || 50, total: items.length })
const renderWithQueries = (ui: ReactElement) =>
  render(
    <QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}>
      <ConfirmProvider>
        <BrowserRouter>{ui}</BrowserRouter>
      </ConfirmProvider>
    </QueryClientProvider>,
  )

describe('configuration workflow', () => {
  it('compares node versions through the Hub dispatch and renders the tracked diff', async () => {
    const fetchMock = vi.fn((url: string, init?: RequestInit) => {
      if (url.endsWith('/system'))
        return Promise.resolve({
          ok: true,
          json: async () => ({
            id: 'arkflow-control-hub',
            version: 'hub',
            state: 'running',
            node_count: 1,
            stream_count: 0,
            active_operations: 0,
            capabilities: [],
          }),
        })
      if (url.includes('/nodes?'))
        return Promise.resolve({
          ok: true,
          json: async () =>
            page([
              {
                id: 'node-a',
                version: 'agent',
                state: 'online',
                capabilities: [],
                streams_total: 0,
                streams_running: 0,
                streams_failed: 0,
              },
            ]),
        })
      if (url.includes('/nodes/node-a/configuration/diff')) {
        // The Hub proxies the comparison as a read-only node command.
        expect(url).toContain('from=v2')
        expect(url).toContain('to=v1')
        expect(init?.method).toBeUndefined()
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
      }
      if (url.includes('/operations/op-2'))
        return Promise.resolve({
          ok: true,
          json: async () => ({
            id: 'op-2',
            operation: 'diff_configuration',
            progress: 100,
            state: 'succeeded',
            created_at_ms: 1,
            result: { from: 'v2', to: 'v1', changed: true, from_format: 'json', to_format: 'yaml' },
          }),
        })
      if (url.includes('/nodes/node-a/configuration/versions'))
        return Promise.resolve({
          ok: true,
          json: async () => [
            { id: 'v2', created_at_ms: 2, format: 'json' },
            { id: 'v1', created_at_ms: 1, format: 'yaml' },
          ],
        })
      if (url.includes('/nodes/node-a/configuration'))
        return Promise.resolve({ ok: true, json: async () => ({ streams: [] }) })
      return Promise.resolve({ ok: true, json: async () => page([]) })
    })
    globalThis.fetch = fetchMock as unknown as typeof fetch
    renderWithQueries(<Configuration onError={vi.fn()} nodeId="node-a" />)
    // The active snapshot and its version list render in node-scoped mode.
    await screen.findByText('v2', { selector: 'strong' })
    fireEvent.click(screen.getAllByRole('button', { name: 'Compare' })[0])
    // The Hub-delivered diff renders exactly like the local one.
    expect(await screen.findByText(/Comparing v2 → v1/)).toBeInTheDocument()
    expect(screen.getByText(/Content differs\./)).toBeInTheDocument()
    expect(screen.getByText(/Formats: json → yaml/)).toBeInTheDocument()
  })

  it('converts YAML to JSON and preserves equivalent values', () => {
    expect(JSON.parse(convertConfiguration('streams: []\n', 'yaml', 'json'))).toEqual({ streams: [] })
    expect(convertConfiguration('{"streams":[]}', 'json', 'yaml')).toContain('streams: []')
  })

  it('waits for a successful publish operation before reloading', async () => {
    const fetchMock = vi.fn((url: string, init?: RequestInit) => {
      if (url.endsWith('/configuration/draft'))
        return Promise.resolve({
          ok: true,
          json: async () => ({ format: 'json', content: '{"streams":[]}' }),
        })
      if (url.endsWith('/configuration'))
        return Promise.resolve({ ok: true, json: async () => ({ streams: [] }) })
      if (url.endsWith('/configuration/versions')) return Promise.resolve({ ok: true, json: async () => [] })
      if (url.endsWith('/configuration/validate'))
        return Promise.resolve({ ok: true, json: async () => ({ valid: true, errors: [] }) })
      if (init?.method === 'POST' && url.endsWith('/configuration/apply'))
        return Promise.resolve({
          ok: true,
          json: async () => ({
            id: 'op-1',
            operation: 'apply_configuration',
            state: 'queued',
            progress: 0,
            created_at_ms: 1,
          }),
        })
      if (url.endsWith('/operations/op-1'))
        return Promise.resolve({
          ok: true,
          json: async () => ({
            id: 'op-1',
            operation: 'apply_configuration',
            state: 'succeeded',
            progress: 100,
            created_at_ms: 1,
          }),
        })
      return Promise.resolve({ ok: true, json: async () => [] })
    })
    globalThis.fetch = fetchMock as unknown as typeof fetch
    renderWithQueries(<Configuration onError={vi.fn()} />)
    await screen.findByDisplayValue('{"streams":[]}')
    fireEvent.click(screen.getByRole('button', { name: 'Validate' }))
    await screen.findByText(/Draft is saved/)
    await waitFor(() => expect(screen.getByRole('button', { name: 'Publish' })).not.toBeDisabled())
    fireEvent.click(screen.getByRole('button', { name: 'Publish' }))
    await waitFor(() =>
      expect(fetchMock).toHaveBeenCalledWith(expect.stringContaining('/operations/op-1'), expect.anything()),
    )
    expect(fetchMock.mock.calls.filter(([url]) => url.endsWith('/configuration')).length).toBeGreaterThan(1)
  })
})

describe('rollout workflow', () => {
  it('creates a bounded rollout from selected nodes', async () => {
    const fetchMock = vi.fn((url: string, init?: RequestInit) => {
      if (url.endsWith('/rollouts') && init?.method === 'POST')
        return Promise.resolve({
          ok: true,
          json: async () => ({
            rollout_id: 'r-1',
            config_version_id: 'cfg-1',
            state: 'pending',
            batch_size: 1,
            current_batch: 0,
            total_targets: 1,
            created_at_ms: 1,
            updated_at_ms: 1,
          }),
        })
      if (url.endsWith('/rollouts')) return Promise.resolve({ ok: true, json: async () => [] })
      if (url.endsWith('/rollouts/r-1'))
        return Promise.resolve({
          ok: true,
          json: async () => ({
            rollout: {
              rollout_id: 'r-1',
              config_version_id: 'cfg-1',
              state: 'pending',
              batch_size: 1,
              current_batch: 0,
              total_targets: 1,
              created_at_ms: 1,
              updated_at_ms: 1,
            },
            targets: [
              { rollout_id: 'r-1', node_id: 'node-a', ordinal: 0, state: 'pending', updated_at_ms: 1 },
            ],
          }),
        })
      if (url.includes('/nodes?'))
        return Promise.resolve({
          ok: true,
          json: async () =>
            page([
              {
                id: 'node-a',
                state: 'online',
                version: 'test',
                capabilities: [],
                streams_total: 0,
                streams_running: 0,
                streams_failed: 0,
              },
            ]),
        })
      if (url.includes('/audit'))
        return Promise.resolve({
          ok: true,
          json: async () => ({ items: [], page: 1, page_size: 50, total: 0 }),
        })
      return Promise.resolve({ ok: true, json: async () => ({}) })
    })
    globalThis.fetch = fetchMock as unknown as typeof fetch
    renderWithQueries(<Rollouts onError={vi.fn()} />)
    fireEvent.change(await screen.findByLabelText('Configuration version'), { target: { value: 'cfg-1' } })
    fireEvent.click(screen.getByRole('checkbox', { name: /node-a/ }))
    fireEvent.click(screen.getByRole('button', { name: 'Create rollout' }))
    expect(await screen.findByText('r-1')).toBeInTheDocument()
    expect(fetchMock).toHaveBeenCalledWith(
      expect.stringContaining('/rollouts'),
      expect.objectContaining({ method: 'POST' }),
    )
  })

  it('renders rollout state transitions and exposes the next allowed action', async () => {
    let state = 'applying'
    const rollout = () => ({
      rollout_id: 'r-1',
      config_version_id: 'cfg-1',
      state,
      batch_size: 1,
      current_batch: 0,
      total_targets: 1,
      created_at_ms: 1,
      updated_at_ms: 1,
    })
    const fetchMock = vi.fn((url: string, init?: RequestInit) => {
      if (url.endsWith('/rollouts') && init?.method === 'POST')
        return Promise.resolve({ ok: true, json: async () => rollout() })
      if (url.endsWith('/rollouts')) return Promise.resolve({ ok: true, json: async () => [rollout()] })
      if (url.endsWith('/rollouts/r-1/actions')) {
        state = 'paused'
        return Promise.resolve({ ok: true, json: async () => rollout() })
      }
      if (url.endsWith('/rollouts/r-1'))
        return Promise.resolve({
          ok: true,
          json: async () => ({
            rollout: rollout(),
            targets: [{ rollout_id: 'r-1', node_id: 'node-a', ordinal: 0, state, updated_at_ms: 1 }],
          }),
        })
      if (url.includes('/nodes?'))
        return Promise.resolve({
          ok: true,
          json: async () =>
            page([
              {
                id: 'node-a',
                state: 'online',
                version: 'test',
                capabilities: [],
                streams_total: 0,
                streams_running: 0,
                streams_failed: 0,
              },
            ]),
        })
      if (url.includes('/audit'))
        return Promise.resolve({
          ok: true,
          json: async () => ({
            items: [{ event_id: 1, action: 'rollout.pause', outcome: 'accepted', occurred_at_ms: 1 }],
            page: 1,
            page_size: 50,
            total: 1,
          }),
        })
      return Promise.resolve({ ok: true, json: async () => ({}) })
    })
    globalThis.fetch = fetchMock as unknown as typeof fetch
    renderWithQueries(<Rollouts onError={vi.fn()} />)
    fireEvent.click(await screen.findByRole('button', { name: /r-1/ }))
    expect(await screen.findByText('applying')).toBeInTheDocument()
    fireEvent.click(await screen.findByRole('button', { name: 'Pause' }))
    expect((await screen.findAllByText('paused')).length).toBeGreaterThan(0)
    expect(screen.getByRole('button', { name: 'Resume' })).toBeInTheDocument()
    expect(fetchMock).toHaveBeenCalledWith(
      expect.stringContaining('/rollouts/r-1/actions'),
      expect.objectContaining({ method: 'POST' }),
    )
  })
})

describe('distributed Job workbench', () => {
  it('validates a Job plan before creating it in stopped state', async () => {
    const fetchMock = vi.fn((url: string, init?: RequestInit) => {
      if (url.endsWith('/jobs/validate'))
        return Promise.resolve({
          ok: true,
          json: async () => ({
            valid: true,
            plan: { tasks: [] },
            required_capabilities: [],
            nodes: [],
            warnings: [],
          }),
        })
      if (url.endsWith('/jobs') && init?.method === 'POST')
        return Promise.resolve({
          ok: true,
          json: async () => ({
            job_id: 'new-job',
            version: 1,
            desired_state: 'stopped',
            observed_state: 'validated',
            convergence: 'pending',
            generation: 1,
            node_ids: [],
            updated_at_ms: 1,
          }),
        })
      return Promise.resolve({ ok: true, json: async () => [] })
    })
    globalThis.fetch = fetchMock as unknown as typeof fetch
    renderWithQueries(<Jobs onError={vi.fn()} />)
    fireEvent.click(screen.getByRole('button', { name: 'Create Job' }))
    fireEvent.click(screen.getByRole('button', { name: 'Validate Plan' }))
    expect(await screen.findByText('Plan is valid')).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'Create stopped' }))
    fireEvent.click(
      within(await screen.findByRole('alertdialog')).getByRole('button', { name: 'Create Job' }),
    )
    await waitFor(() =>
      expect(fetchMock).toHaveBeenCalledWith(
        expect.stringContaining('/jobs'),
        expect.objectContaining({
          method: 'POST',
          headers: expect.objectContaining({ 'X-Correlation-ID': expect.any(String) }),
        }),
      ),
    )
    await waitFor(() =>
      expect(
        fetchMock.mock.calls.filter(
          ([url, init]) =>
            String(url).endsWith('/jobs') && (init as RequestInit | undefined)?.method !== 'POST',
        ).length,
      ).toBeGreaterThanOrEqual(2),
    )
  })

  it('requires a fresh validation after Job settings change', async () => {
    const fetchMock = vi.fn((url: string) => {
      if (url.endsWith('/jobs/validate'))
        return Promise.resolve({
          ok: true,
          json: async () => ({ valid: true, plan: {}, required_capabilities: [], nodes: [], warnings: [] }),
        })
      return Promise.resolve({ ok: true, json: async () => [] })
    })
    globalThis.fetch = fetchMock as unknown as typeof fetch
    const view = renderWithQueries(<Jobs onError={vi.fn()} />)
    const local = within(view.container)
    fireEvent.click(local.getByRole('button', { name: 'Create Job' }))
    fireEvent.click(local.getByRole('button', { name: 'Validate Plan' }))
    await waitFor(() => expect(local.getByRole('button', { name: 'Create stopped' })).not.toBeDisabled())
    fireEvent.change(local.getByLabelText('Job ID'), { target: { value: 'changed-job' } })
    expect(local.getByRole('button', { name: 'Create stopped' })).toBeDisabled()
  })

  it('renders only measured Job metrics and distinguishes observed tasks', async () => {
    const fetchMock = vi.fn((url: string) => {
      if (url.endsWith('/jobs'))
        return Promise.resolve({
          ok: true,
          json: async () => [
            {
              job_id: 'scoped-job',
              version: 1,
              desired_state: 'running',
              observed_state: 'running',
              convergence: 'in_sync',
              generation: 1,
              node_ids: ['n1'],
              updated_at_ms: 1,
            },
          ],
        })
      if (url.endsWith('/jobs/scoped-job/detail'))
        return Promise.resolve({
          ok: true,
          json: async () => ({
            job: {
              job_id: 'scoped-job',
              version: 1,
              desired_state: 'running',
              observed_state: 'running',
              convergence: 'in_sync',
              generation: 1,
              node_ids: ['n1'],
              updated_at_ms: 1,
            },
            plan: {},
            nodes: [],
            operations: [],
            checkpoints: [],
            metrics: { watermark_lag_ms: 5, checkpoint_duration_ms: 7, checkpoint_failures: 2 },
            tasks: [
              {
                id: 't1:n1:1',
                job_id: 'scoped-job',
                job_version: 1,
                task_id: 't1',
                generation: 1,
                node_id: 'n1',
                state: 'running',
                observed: true,
                observed_node_id: 'n1',
              },
              {
                id: 't2:n1:1',
                job_id: 'scoped-job',
                job_version: 1,
                task_id: 't2',
                generation: 1,
                node_id: 'n1',
                state: 'queued',
                observed: false,
              },
            ],
          }),
        })
      return Promise.resolve({ ok: true, json: async () => [] })
    })
    globalThis.fetch = fetchMock as unknown as typeof fetch
    renderWithQueries(<Jobs onError={vi.fn()} />)
    fireEvent.click(await screen.findByRole('button', { name: /scoped-job/ }))
    // Only the measured gauges render; the removed always-zero keys never appear.
    await waitFor(() => expect(screen.getByText('watermark lag ms')).toBeInTheDocument())
    expect(screen.getByText('checkpoint duration ms')).toBeInTheDocument()
    expect(screen.getByText('checkpoint failures')).toBeInTheDocument()
    expect(screen.queryByText('state bytes')).toBeNull()
    expect(screen.queryByText('recovery progress')).toBeNull()
    expect(screen.queryByText('task pressure')).toBeNull()
    expect(screen.queryByText('partition health')).toBeNull()
    fireEvent.click(screen.getByRole('button', { name: 'Tasks' }))
    const observedRow = screen.getByText('t1').closest('div.row')
    expect(within(observedRow as HTMLElement).getByText('running')).toBeInTheDocument()
    expect(within(observedRow as HTMLElement).queryByText(/not observed yet/)).toBeNull()
    const fallbackRow = screen.getByText('t2').closest('div.row')
    expect(within(fallbackRow as HTMLElement).getByText(/queued · not observed yet/)).toBeInTheDocument()
  })

  it('shows a retryable component-catalogue failure in the Job palette', async () => {
    const fetchMock = vi.fn((url: string) => {
      if (url.endsWith('/components'))
        return Promise.resolve({
          ok: false,
          status: 503,
          json: async () => ({ message: 'catalogue unavailable' }),
          headers: new Headers(),
        })
      return Promise.resolve({ ok: true, json: async () => [] })
    })
    globalThis.fetch = fetchMock as unknown as typeof fetch
    const view = renderWithQueries(<Jobs onError={vi.fn()} />)
    const local = within(view.container)
    fireEvent.click(local.getByRole('button', { name: 'Create Job' }))
    expect(await local.findByText('Component catalogue could not be loaded.')).toBeInTheDocument()
    expect(local.getByRole('button', { name: 'Retry' })).toBeInTheDocument()
  })

  it('filters the Job palette by component kind and search term', async () => {
    const components = [
      { kind: 'input', name: 'generate', description: 'Generate records' },
      { kind: 'processor', name: 'json_to_arrow', description: 'Decode JSON' },
      { kind: 'output', name: 'stdout', description: 'Write output' },
    ]
    globalThis.fetch = vi.fn((url: string) =>
      Promise.resolve({ ok: true, json: async () => (url.endsWith('/components') ? components : []) }),
    ) as unknown as typeof fetch
    const view = renderWithQueries(<Jobs onError={vi.fn()} />)
    const local = within(view.container)
    fireEvent.click(local.getByRole('button', { name: 'Create Job' }))
    expect(await local.findByText('generate')).toBeInTheDocument()
    expect(local.queryByText('stdout')).not.toBeInTheDocument()
    fireEvent.click(local.getByRole('tab', { name: 'processor' }))
    expect(await local.findByText('json_to_arrow')).toBeInTheDocument()
    fireEvent.change(local.getByLabelText('Component search'), { target: { value: 'missing' } })
    expect(local.getByText('No matching components.')).toBeInTheDocument()
  })
})

describe('component catalogue', () => {
  it('filters entries and shows details only for the selected component', async () => {
    const components = [
      {
        kind: 'input',
        name: 'generate',
        description: 'Generate records',
        schema: { type: 'object' },
        example: { batch_size: 1 },
      },
      { kind: 'processor', name: 'json_to_arrow', description: 'Decode JSON', schema: { type: 'object' } },
    ]
    globalThis.fetch = vi.fn((url: string) =>
      Promise.resolve({ ok: true, json: async () => (url.endsWith('/components') ? components : {}) }),
    ) as unknown as typeof fetch
    const view = renderWithQueries(<Components onError={vi.fn()} />)
    const local = within(view.container)
    expect(await local.findByText('generate')).toBeInTheDocument()
    expect(local.getAllByText('Generate records')).toHaveLength(2)
    fireEvent.click(local.getByRole('tab', { name: 'processor' }))
    expect(local.getAllByText('json_to_arrow')).toHaveLength(2)
    expect(local.queryByText('Generate records')).not.toBeInTheDocument()
  })
})

describe('fleet maintenance actions', () => {
  afterEach(() => {
    cleanup()
    vi.restoreAllMocks()
  })

  const fleetNodes = (maintenanceState?: string) =>
    page([
      {
        id: 'node-a',
        state: 'online',
        version: 'test',
        capabilities: [],
        streams_total: 1,
        streams_running: 1,
        streams_failed: 0,
        ...(maintenanceState ? { maintenance_state: maintenanceState } : {}),
      },
    ])

  it('drains a node after confirmation and refreshes the fleet', async () => {
    const fetchMock = vi.fn((url: string, _init?: RequestInit) => {
      if (url.includes('/nodes?')) return Promise.resolve({ ok: true, json: async () => fleetNodes() })
      if (url.includes('/nodes/node-a/drain'))
        return Promise.resolve({
          ok: true,
          json: async () => ({
            id: 'node-a',
            state: 'online',
            maintenance_state: 'draining',
            version: 'test',
            capabilities: [],
            streams_total: 1,
            streams_running: 1,
            streams_failed: 0,
          }),
        })
      return Promise.resolve({ ok: true, json: async () => ({}) })
    })
    globalThis.fetch = fetchMock as unknown as typeof fetch
    renderWithQueries(<Overview onError={vi.fn()} />)
    fireEvent.click(await screen.findByRole('button', { name: 'Drain' }))
    fireEvent.click(within(await screen.findByRole('alertdialog')).getByRole('button', { name: 'Confirm' }))
    await waitFor(() =>
      expect(fetchMock).toHaveBeenCalledWith(
        expect.stringContaining('/nodes/node-a/drain'),
        expect.objectContaining({ method: 'POST' }),
      ),
    )
    await waitFor(() =>
      expect(fetchMock).toHaveBeenCalledWith(expect.stringContaining('/nodes?'), expect.anything()),
    )
  })

  it('does not drain without confirmation', async () => {
    const fetchMock = vi.fn((url: string) =>
      Promise.resolve({
        ok: true,
        json: async () => (url.includes('/nodes?') ? fleetNodes() : {}),
      }),
    )
    globalThis.fetch = fetchMock as unknown as typeof fetch
    renderWithQueries(<Overview onError={vi.fn()} />)
    fireEvent.click(await screen.findByRole('button', { name: 'Drain' }))
    const dialog = await screen.findByRole('alertdialog')
    fireEvent.click(within(dialog).getByRole('button', { name: 'Cancel' }))
    expect(fetchMock).not.toHaveBeenCalledWith(
      expect.stringContaining('/drain'),
      expect.objectContaining({ method: 'POST' }),
    )
  })

  it('offers Resume instead of Drain while a node is draining', async () => {
    const fetchMock = vi.fn((url: string) =>
      Promise.resolve({
        ok: true,
        json: async () => (url.includes('/nodes?') ? fleetNodes('draining') : {}),
      }),
    )
    globalThis.fetch = fetchMock as unknown as typeof fetch
    renderWithQueries(<Overview onError={vi.fn()} />)
    expect(await screen.findByText('draining')).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Drain' })).not.toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Resume' })).toBeInTheDocument()
  })

  it('resumes a node out of maintenance with the delete verb', async () => {
    const fetchMock = vi.fn((url: string, _init?: RequestInit) => {
      if (url.includes('/nodes?'))
        return Promise.resolve({ ok: true, json: async () => fleetNodes('maintenance') })
      if (url.includes('/nodes/node-a/maintenance'))
        return Promise.resolve({
          ok: true,
          json: async () => ({
            id: 'node-a',
            state: 'online',
            maintenance_state: 'active',
            version: 'test',
            capabilities: [],
            streams_total: 1,
            streams_running: 1,
            streams_failed: 0,
          }),
        })
      return Promise.resolve({ ok: true, json: async () => ({}) })
    })
    globalThis.fetch = fetchMock as unknown as typeof fetch
    renderWithQueries(<Overview onError={vi.fn()} />)
    fireEvent.click(await screen.findByRole('button', { name: 'Resume' }))
    fireEvent.click(within(await screen.findByRole('alertdialog')).getByRole('button', { name: 'Confirm' }))
    await waitFor(() =>
      expect(fetchMock).toHaveBeenCalledWith(
        expect.stringContaining('/nodes/node-a/maintenance'),
        expect.objectContaining({ method: 'DELETE' }),
      ),
    )
    await waitFor(() =>
      expect(fetchMock).toHaveBeenCalledWith(expect.stringContaining('/nodes?'), expect.anything()),
    )
  })

  it('surfaces a permission failure from the maintenance endpoint', async () => {
    const fetchMock = vi.fn((url: string) => {
      if (url.includes('/nodes?')) return Promise.resolve({ ok: true, json: async () => fleetNodes() })
      return Promise.resolve({
        ok: false,
        status: 403,
        headers: new Headers(),
        json: async () => ({ code: 'forbidden', message: 'Operator is not authorized' }),
      })
    })
    globalThis.fetch = fetchMock as unknown as typeof fetch
    const onError = vi.fn()
    renderWithQueries(<Overview onError={onError} />)
    fireEvent.click(await screen.findByRole('button', { name: 'Maintain' }))
    fireEvent.click(within(await screen.findByRole('alertdialog')).getByRole('button', { name: 'Confirm' }))
    await waitFor(() => expect(onError).toHaveBeenCalledWith(expect.stringContaining('not authorized')))
  })
})

describe('audit history', () => {
  afterEach(() => cleanup())

  it('lists fleet-wide audit records and filters them locally', async () => {
    const fetchMock = vi.fn((url: string) => {
      if (url.includes('/nodes?'))
        return Promise.resolve({
          ok: true,
          json: async () =>
            page([
              {
                id: 'node-a',
                state: 'online',
                version: 'test',
                capabilities: [],
                streams_total: 0,
                streams_running: 0,
                streams_failed: 0,
              },
            ]),
        })
      if (url.includes('/audit'))
        return Promise.resolve({
          ok: true,
          json: async () => ({
            items: [
              {
                event_id: 2,
                action: 'node.drain',
                actor: 'operator',
                resource_type: 'node',
                resource_id: 'node-a',
                outcome: 'accepted',
                occurred_at_ms: 2,
              },
              {
                event_id: 1,
                action: 'job.start',
                actor: 'operator',
                resource_type: 'job',
                resource_id: 'orders',
                node_id: 'node-a',
                correlation_id: 'c-1',
                outcome: 'succeeded',
                occurred_at_ms: 1,
              },
            ],
            page: 1,
            page_size: 50,
            total: 2,
          }),
        })
      return Promise.resolve({ ok: true, json: async () => ({}) })
    })
    globalThis.fetch = fetchMock as unknown as typeof fetch
    renderWithQueries(<Audit onError={vi.fn()} />)
    expect(await screen.findByText('node.drain')).toBeInTheDocument()
    expect(screen.getByText('job.start')).toBeInTheDocument()
    fireEvent.change(screen.getByLabelText('Audit filter'), { target: { value: 'node.drain' } })
    expect(screen.queryByText('job.start')).not.toBeInTheDocument()
    expect(screen.getByText('node.drain')).toBeInTheDocument()
  })

  it('reports load failures through the error channel', async () => {
    const fetchMock = vi.fn((url: string) => {
      if (url.includes('/nodes?'))
        return Promise.resolve({
          ok: true,
          json: async () =>
            page([
              {
                id: 'node-a',
                state: 'online',
                version: 'test',
                capabilities: [],
                streams_total: 0,
                streams_running: 0,
                streams_failed: 0,
              },
            ]),
        })
      if (url.includes('/audit'))
        return Promise.resolve({
          ok: false,
          status: 401,
          headers: new Headers(),
          json: async () => ({ message: 'A valid operator token is required' }),
        })
      return Promise.resolve({ ok: true, json: async () => ({}) })
    })
    globalThis.fetch = fetchMock as unknown as typeof fetch
    const onError = vi.fn()
    renderWithQueries(<Audit onError={onError} />)
    await waitFor(() => expect(onError).toHaveBeenCalledWith(expect.stringContaining('operator token')))
  })
})

describe('job editor determinism', () => {
  const componentCatalogue = [
    { kind: 'input', name: 'generate', description: 'Generate', schema: null, example: {} },
    { kind: 'output', name: 'drop', description: 'Drop', schema: null, example: {} },
  ]
  const editorFetchMock = () =>
    vi.fn((url: string) => {
      if (url.endsWith('/components'))
        return Promise.resolve({ ok: true, json: async () => componentCatalogue })
      if (url.endsWith('/jobs/validate'))
        return Promise.resolve({
          ok: true,
          json: async () => ({
            valid: true,
            warnings: [],
            plan: undefined,
            required_capabilities: [],
            nodes: [],
          }),
        })
      return Promise.resolve({ ok: true, json: async () => [] })
    })

  const upgradeJob = {
    job_id: 'orders',
    version: 3,
    generation: 7,
    desired_state: 'stopped',
    state: 'stopped',
    node_ids: [],
    // The Hub serializes job specs as spec_json only.
    spec_json: JSON.stringify({
      id: 'orders',
      version: 3,
      operators: [{ id: 'kept-source', kind: 'source', component: 'generate', config: {} }],
      sources: [{ operator_id: 'kept-source', input_type: 'generate' }],
      sinks: [],
      edges: [],
    }),
  }

  it('resets the draft when the editor target switches from create to upgrade', async () => {
    globalThis.fetch = editorFetchMock() as unknown as typeof fetch
    const view = render(
      <JobEditor
        mode="create"
        nodes={[]}
        busy={false}
        onClose={vi.fn()}
        onError={vi.fn()}
        onSaved={vi.fn()}
        onRefresh={vi.fn()}
        onAction={async (_label, fn) => {
          await fn()
        }}
      />,
    )
    await screen.findByText('generate')
    fireEvent.click(view.getByRole('button', { name: /generate/ }))
    await view.findAllByText('generate-1')
    // Switch the same mounted editor to an upgrade target.
    view.rerender(
      <JobEditor
        mode="upgrade"
        job={upgradeJob as unknown as Job}
        savepoint={
          {
            checkpoint_id: 'sp-1',
            kind: 'savepoint',
            status: 'completed',
            job_version: 3,
            format_version: 1,
            created_at_ms: 1,
          } as unknown as JobCheckpoint
        }
        nodes={[]}
        busy={false}
        onClose={vi.fn()}
        onError={vi.fn()}
        onSaved={vi.fn()}
        onRefresh={vi.fn()}
        onAction={async (_label, fn) => {
          await fn()
        }}
      />,
    )
    // The create draft is gone: the editor shows the target Job's id.
    await view.findByDisplayValue('orders')
    expect(view.queryByText('generate-1')).toBeNull()
    expect(view.getByText(/Recovery: sp-1/)).toBeTruthy()
  })

  it('serializes rescale and per-task resources, and clears them back to absent', async () => {
    const bodies: string[] = []
    const fetchMock = vi.fn((url: string, init?: RequestInit) => {
      if (url.endsWith('/components'))
        return Promise.resolve({ ok: true, json: async () => componentCatalogue })
      if (url.endsWith('/jobs/validate')) {
        bodies.push(String(init?.body))
        return Promise.resolve({
          ok: true,
          json: async () => ({
            valid: true,
            warnings: [],
            plan: undefined,
            required_capabilities: [],
            nodes: [],
          }),
        })
      }
      return Promise.resolve({ ok: true, json: async () => ({}) })
    })
    globalThis.fetch = fetchMock as unknown as typeof fetch
    const view = render(
      <JobEditor
        mode="create"
        nodes={[]}
        busy={false}
        onClose={vi.fn()}
        onError={vi.fn()}
        onSaved={vi.fn()}
        onRefresh={vi.fn()}
        onAction={async (_label, fn) => {
          await fn()
        }}
      />,
    )
    await screen.findByText('Rescale recovery (redistribute keyed state on parallelism changes)')
    fireEvent.click(view.getByLabelText(/Rescale recovery/))
    fireEvent.change(view.getByLabelText('CPU per task (millicores)'), { target: { value: '500' } })
    fireEvent.change(view.getByLabelText('Memory per task (bytes)'), { target: { value: '1024' } })
    fireEvent.click(screen.getByRole('button', { name: 'Validate Plan' }))
    await waitFor(() => expect(bodies).toHaveLength(1))
    expect(JSON.parse(bodies[0]).spec).toMatchObject({
      rescale: true,
      resources: { cpu_millicores: 500, memory_bytes: 1024 },
    })
    // Clearing the inputs serializes both back to their absent form.
    fireEvent.change(view.getByLabelText('CPU per task (millicores)'), { target: { value: '' } })
    fireEvent.change(view.getByLabelText('Memory per task (bytes)'), { target: { value: '' } })
    fireEvent.click(screen.getByRole('button', { name: 'Validate Plan' }))
    await waitFor(() => expect(bodies).toHaveLength(2))
    const cleared = JSON.parse(bodies[1]).spec
    expect(cleared.rescale).toBe(true)
    expect(cleared.resources).toBeUndefined()
  })

  it('generates collision-free node ids across add/delete/add', async () => {
    const fetchMock = editorFetchMock()
    globalThis.fetch = fetchMock as unknown as typeof fetch
    const view = render(
      <JobEditor
        mode="create"
        nodes={[]}
        busy={false}
        onClose={vi.fn()}
        onError={vi.fn()}
        onSaved={vi.fn()}
        onRefresh={vi.fn()}
        onAction={async (_label, fn) => {
          await fn()
        }}
      />,
    )
    await screen.findAllByRole('button', { name: /generate/ })
    fireEvent.click(view.getAllByRole('button', { name: /generate/ })[0])
    await view.findByRole('button', { name: 'Delete node' })
    // Delete the only node, then add another: with `length + 1` ids the new
    // node would collide with the deleted `generate-1`.
    fireEvent.click(view.getByRole('button', { name: 'Delete node' }))
    fireEvent.click(view.getAllByRole('button', { name: /generate/ })[0])
    await view.findByRole('button', { name: 'Delete node' })
    fireEvent.click(view.getByRole('button', { name: 'Validate Plan' }))
    await waitFor(() =>
      expect(fetchMock.mock.calls.some(([url]) => url.endsWith('/jobs/validate'))).toBe(true),
    )
    const validateCall = fetchMock.mock.calls.find((call) => call[0].endsWith('/jobs/validate'))
    const body = JSON.parse((validateCall?.at(1) as RequestInit | undefined)?.body as string)
    const operatorIds = body.spec.operators.map((operator: { id: string }) => operator.id)
    expect(new Set(operatorIds).size).toBe(operatorIds.length)
    expect(operatorIds).toContain('generate-2')
    expect(operatorIds).not.toContain('generate-1')
    view.unmount()
  })

  it('does not unlock submission for a graph edited while validating', async () => {
    let releaseValidation: (() => void) | undefined
    const fetchMock = vi.fn((url: string) => {
      if (url.endsWith('/components'))
        return Promise.resolve({ ok: true, json: async () => componentCatalogue })
      if (url.endsWith('/jobs/validate'))
        return new Promise((resolve) => {
          releaseValidation = () =>
            resolve({
              ok: true,
              json: async () => ({ valid: true, warnings: [], required_capabilities: [], nodes: [] }),
            })
        })
      return Promise.resolve({ ok: true, json: async () => [] })
    })
    globalThis.fetch = fetchMock as unknown as typeof fetch
    const view = render(
      <JobEditor
        mode="create"
        nodes={[]}
        busy={false}
        onClose={vi.fn()}
        onError={vi.fn()}
        onSaved={vi.fn()}
        onRefresh={vi.fn()}
        onAction={async (_label, fn) => {
          await fn()
        }}
      />,
    )
    await screen.findAllByRole('button', { name: /generate/ })
    fireEvent.click(view.getAllByRole('button', { name: /generate/ })[0])
    await view.findByRole('button', { name: 'Delete node' })
    fireEvent.click(view.getByRole('button', { name: 'Validate Plan' }))
    // Edit the graph while the validation request is in flight (a second
    // generate node changes the derived spec)...
    fireEvent.click(view.getAllByRole('button', { name: /generate/ })[0])
    await view.findByRole('button', { name: 'Delete node' })
    // ...then let the stale response arrive.
    releaseValidation?.()
    await waitFor(() =>
      expect(fetchMock.mock.calls.filter(([url]) => url.endsWith('/jobs/validate')).length).toBe(1),
    )
    expect(view.getByRole('button', { name: 'Create stopped' })).toBeDisabled()
  })
})
