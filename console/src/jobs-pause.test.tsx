import { cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { BrowserRouter } from 'react-router'
import type { ReactElement } from 'react'
import type { Job } from './api'
import { ConfirmProvider } from './features/confirm'
import { Jobs } from './features/jobs'

afterEach(() => {
  cleanup()
  vi.unstubAllGlobals()
  window.history.replaceState({}, '', '/')
})

const renderJobs = (props: { canMutate?: boolean; onError?: (message: string) => void } = {}) => {
  const ui: ReactElement = (
    <QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}>
      <ConfirmProvider>
        <BrowserRouter>
          <Jobs onError={props.onError ?? vi.fn()} canMutate={props.canMutate} />
        </BrowserRouter>
      </ConfirmProvider>
    </QueryClientProvider>
  )
  return render(ui)
}

const page = (items: unknown[]) => ({ items, page: 1, page_size: 50, total: items.length })
const ok = (payload: unknown, status = 200) =>
  Promise.resolve({ ok: true, status, json: async () => payload, headers: new Headers() })
const fail = (status: number, payload: unknown) =>
  Promise.resolve({ ok: false, status, json: async () => payload, headers: new Headers() })

const runningJob: Job = {
  job_id: 'orders',
  version: 2,
  desired_state: 'running',
  observed_state: 'running',
  convergence: 'converged',
  generation: 3,
  node_ids: ['n1'],
  updated_at_ms: 1,
}

const pausedJob: Job = {
  ...runningJob,
  desired_state: 'paused',
  observed_state: 'stopped',
  pause_phase: 'paused',
  pause_artifact_id: 'savepoint-pause-g3-1',
  paused_at_ms: 10,
  paused_by: 'ops',
}

const rowFor = async (jobId: string): Promise<HTMLElement> => {
  const button = await screen.findByRole('button', { name: new RegExp(jobId) })
  return button.closest('div.row') as HTMLElement
}

const jobDetail = (job: Job) => ({
  job,
  plan: {},
  tasks: [
    {
      id: 't1:n1:3',
      job_id: job.job_id,
      job_version: job.version,
      task_id: 't1',
      generation: job.generation,
      node_id: 'n1',
      state: 'running',
      observed: true,
      observed_node_id: 'n1',
    },
  ],
  nodes: [],
  operations: [],
  checkpoints: [],
  metrics: {},
})

describe('Job pause/resume controls', () => {
  it('confirms a pause that names the artifact-then-stop sequence and dispatches it', async () => {
    const fetchMock = vi.fn((url: string, init?: RequestInit) => {
      if (url.endsWith('/jobs/orders/actions/pause') && init?.method === 'POST')
        return ok({ ...runningJob, desired_state: 'paused', pause_phase: 'capturing' }, 202)
      if (url.endsWith('/jobs') && init?.method !== 'POST') return ok([runningJob])
      if (url.includes('/nodes?')) return ok(page([]))
      return ok([])
    })
    vi.stubGlobal('fetch', fetchMock)

    renderJobs()
    const row = await rowFor('orders')
    fireEvent.click(within(row).getByRole('button', { name: 'Pause' }))

    const dialog = await screen.findByRole('alertdialog')
    expect(within(dialog).getByText(/captures a recovery artifact/)).toBeInTheDocument()
    expect(within(dialog).getByText(/cold start/)).toBeInTheDocument()
    fireEvent.click(within(dialog).getByRole('button', { name: 'Pause' }))

    await waitFor(() =>
      expect(fetchMock).toHaveBeenCalledWith(
        expect.stringContaining('/jobs/orders/actions/pause'),
        expect.objectContaining({ method: 'POST' }),
      ),
    )
  })

  it('renders the paused phase badge and resumes through the actions endpoint', async () => {
    const fetchMock = vi.fn((url: string, init?: RequestInit) => {
      if (url.endsWith('/jobs/orders/actions/resume') && init?.method === 'POST')
        return ok({ ...pausedJob, desired_state: 'running', pause_phase: 'resuming' }, 202)
      if (url.endsWith('/jobs') && init?.method !== 'POST') return ok([pausedJob])
      if (url.includes('/nodes?')) return ok(page([]))
      return ok([])
    })
    vi.stubGlobal('fetch', fetchMock)

    renderJobs()
    const row = await rowFor('orders')
    expect(within(row).getByText('Paused')).toBeInTheDocument()
    expect(within(row).getByText(/desired: paused/)).toBeInTheDocument()
    // A paused Job has no ambiguous Start: only Resume and Stop.
    expect(within(row).queryByRole('button', { name: 'Start' })).toBeNull()

    fireEvent.click(within(row).getByRole('button', { name: 'Resume' }))
    const dialog = await screen.findByRole('alertdialog')
    expect(within(dialog).getByText(/replays the data processed after its barrier/)).toBeInTheDocument()
    fireEvent.click(within(dialog).getByRole('button', { name: 'Resume' }))

    await waitFor(() =>
      expect(fetchMock).toHaveBeenCalledWith(
        expect.stringContaining('/jobs/orders/actions/resume'),
        expect.objectContaining({ method: 'POST' }),
      ),
    )
  })

  it('disables pause/resume while a pause transition is in flight', async () => {
    const capturing: Job = {
      ...runningJob,
      desired_state: 'paused',
      observed_state: 'running',
      pause_phase: 'capturing',
      paused_at_ms: 10,
      paused_by: 'ops',
    }
    vi.stubGlobal(
      'fetch',
      vi.fn((url: string) => {
        if (url.endsWith('/jobs')) return ok([capturing])
        return ok(page([]))
      }),
    )

    renderJobs()
    const row = await rowFor('orders')
    expect(within(row).getByText('Capturing artifact')).toBeInTheDocument()
    const pause = within(row).getByRole('button', { name: 'Pause' })
    expect(pause).toBeDisabled()
    expect(pause).toHaveAttribute('title', 'Pause in progress')
    // The settle action stays available so an operator can abandon the pause.
    expect(within(row).getByRole('button', { name: 'Stop' })).not.toBeDisabled()
  })

  it('gates pause and resume behind the operate permission without hiding them', async () => {
    const fetchMock = vi.fn((url: string) => {
      if (url.endsWith('/jobs')) return ok([runningJob, { ...pausedJob, job_id: 'billing' }])
      return ok(page([]))
    })
    vi.stubGlobal('fetch', fetchMock)

    renderJobs({ canMutate: false })
    const running = await rowFor('orders')
    const pause = within(running).getByRole('button', { name: 'Pause' })
    expect(pause).toBeDisabled()
    expect(pause).toHaveAttribute('title', 'Requires the operate permission')

    const paused = await rowFor('billing')
    const resume = within(paused).getByRole('button', { name: 'Resume' })
    expect(resume).toBeDisabled()
    expect(resume).toHaveAttribute('title', 'Requires the operate permission')
  })

  it('filters the Jobs list down to paused Jobs', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn((url: string) => {
        if (url.endsWith('/jobs')) return ok([runningJob, { ...pausedJob, job_id: 'billing' }])
        return ok(page([]))
      }),
    )

    renderJobs()
    expect(await screen.findByRole('button', { name: /orders/ })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /billing/ })).toBeInTheDocument()
    expect(screen.getByRole('option', { name: 'Paused' })).toBeInTheDocument()

    fireEvent.change(screen.getByLabelText('Job state'), { target: { value: 'paused' } })
    expect(screen.queryByRole('button', { name: /orders/ })).toBeNull()
    expect(screen.getByRole('button', { name: /billing/ })).toBeInTheDocument()
  })

  it('shows paused tasks as not observed instead of stale running and resumes from the detail panel', async () => {
    const fetchMock = vi.fn((url: string, init?: RequestInit) => {
      if (url.endsWith('/jobs/billing/actions/resume') && init?.method === 'POST')
        return ok({ ...pausedJob, desired_state: 'running', pause_phase: 'resuming' }, 202)
      if (url.endsWith('/jobs/billing/detail')) return ok(jobDetail({ ...pausedJob, job_id: 'billing' }))
      if (url.endsWith('/jobs') && init?.method !== 'POST') return ok([{ ...pausedJob, job_id: 'billing' }])
      if (url.includes('/nodes?')) return ok(page([]))
      return ok([])
    })
    vi.stubGlobal('fetch', fetchMock)

    renderJobs()
    fireEvent.click(await screen.findByRole('button', { name: /billing/ }))
    const panel = (await screen.findByText('JOB DETAIL')).closest('section') as HTMLElement
    expect(within(panel).getByText('Paused')).toBeInTheDocument()
    expect(within(panel).getByText(/Pause artifact:/)).toBeInTheDocument()
    expect(within(panel).getByText('savepoint-pause-g3-1')).toBeInTheDocument()

    fireEvent.click(within(panel).getByRole('button', { name: 'Tasks' }))
    const taskRow = screen.getByText('t1').closest('div.row') as HTMLElement
    expect(within(taskRow).getByText('not observed')).toBeInTheDocument()
    expect(within(taskRow).queryByText('running')).toBeNull()

    fireEvent.click(within(panel).getByRole('button', { name: 'Resume' }))
    const dialog = await screen.findByRole('alertdialog')
    fireEvent.click(within(dialog).getByRole('button', { name: 'Resume' }))
    await waitFor(() =>
      expect(fetchMock).toHaveBeenCalledWith(
        expect.stringContaining('/jobs/billing/actions/resume'),
        expect.objectContaining({ method: 'POST' }),
      ),
    )
  })

  it('settles a paused Job back to stopped through desired-state', async () => {
    const fetchMock = vi.fn((url: string, init?: RequestInit) => {
      if (url.endsWith('/jobs/orders/desired-state') && init?.method === 'PUT')
        return ok({ ...pausedJob, desired_state: 'stopped', pause_phase: undefined })
      if (url.endsWith('/jobs') && init?.method !== 'POST') return ok([pausedJob])
      if (url.includes('/nodes?')) return ok(page([]))
      return ok([])
    })
    vi.stubGlobal('fetch', fetchMock)

    renderJobs()
    const row = await rowFor('orders')
    fireEvent.click(within(row).getByRole('button', { name: 'Stop' }))
    const dialog = await screen.findByRole('alertdialog')
    expect(within(dialog).getByText(/clears the pause state/)).toBeInTheDocument()
    fireEvent.click(within(dialog).getByRole('button', { name: 'Stop' }))

    await waitFor(() =>
      expect(fetchMock).toHaveBeenCalledWith(
        expect.stringContaining('/jobs/orders/desired-state'),
        expect.objectContaining({ method: 'PUT', body: JSON.stringify({ state: 'stopped' }) }),
      ),
    )
  })

  it('surfaces the server problem code verbatim on a rejected pause', async () => {
    const onError = vi.fn()
    const fetchMock = vi.fn((url: string, init?: RequestInit) => {
      if (url.endsWith('/jobs/orders/actions/pause') && init?.method === 'POST')
        return fail(409, { code: 'pause_requires_recovery_policy', message: 'Recovery policy is fail' })
      if (url.endsWith('/jobs') && init?.method !== 'POST') return ok([runningJob])
      if (url.includes('/nodes?')) return ok(page([]))
      return ok([])
    })
    vi.stubGlobal('fetch', fetchMock)

    renderJobs({ onError })
    const row = await rowFor('orders')
    fireEvent.click(within(row).getByRole('button', { name: 'Pause' }))
    fireEvent.click(within(await screen.findByRole('alertdialog')).getByRole('button', { name: 'Pause' }))

    await waitFor(() => expect(onError).toHaveBeenCalled())
    expect(onError).toHaveBeenCalledWith(expect.stringContaining('pause_requires_recovery_policy'))
  })
})
