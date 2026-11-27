import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { dispatchAiSseFrame, streamAiCopilotChat } from './api'
import { App } from './app'

const fetchMock = vi.fn()
const page = (items: unknown[]) => ({ items, page: 1, page_size: items.length || 50, total: items.length })

function sseResponse(frames: Array<{ event: string; data: unknown }>): Response {
  const payload = frames.map((f) => `event: ${f.event}\ndata: ${JSON.stringify(f.data)}\n\n`).join('')
  const encoder = new TextEncoder()
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(encoder.encode(payload))
      controller.close()
    },
  })
  return new Response(stream, {
    status: 200,
    headers: { 'Content-Type': 'text/event-stream' },
  })
}

function setupHubFetch(options: {
  id?: string
  capabilities?: string[]
  role?: 'admin' | 'operator' | 'viewer'
  actions?: string[]
  onAiChat?: (body: unknown) => Response
}) {
  const {
    id = 'arkflow-control-hub',
    capabilities = ['node_registry', 'command_dispatch', 'fleet_aggregation', 'ai_copilot'],
    role = 'admin',
    actions = ['read', 'operate', 'configure', 'manage_nodes', 'manage_rollouts', 'read_audit'],
    onAiChat,
  } = options

  fetchMock.mockImplementation((url: string, init?: RequestInit) => {
    if (url.endsWith('/ai/chat')) {
      const body = init?.body ? JSON.parse(String(init.body)) : {}
      return Promise.resolve(
        onAiChat
          ? onAiChat(body)
          : sseResponse([
              {
                event: 'delta',
                data: { text: 'OK' },
              },
              {
                event: 'done',
                data: { usage: { prompt_tokens: 10, completion_tokens: 5 }, tool_calls: [] },
              },
            ]),
      )
    }
    if (url.endsWith('/auth/oidc/status')) {
      return Promise.resolve({
        ok: true,
        json: async () => ({ login_enabled: false, authenticated: false, principal: null }),
      })
    }
    if (url.endsWith('/system')) {
      return Promise.resolve({
        ok: true,
        json: async () => ({
          id,
          version: 'test',
          state: 'running',
          capabilities,
          principal: { id: 'alice', roles: [role], scopes: [], actions },
        }),
      })
    }
    if (url.endsWith('/status')) {
      return Promise.resolve({
        ok: true,
        json: async () => ({
          version: 'test',
          state: 'running',
          uptime_seconds: 10,
          streams_total: 0,
          streams_running: 0,
          streams_failed: 0,
        }),
      })
    }
    if (url.includes('/nodes/node-1/configuration/versions')) {
      return Promise.resolve({ ok: true, json: async () => [] })
    }
    if (url.includes('/nodes/node-1/configuration')) {
      return Promise.resolve({ ok: true, json: async () => ({ streams: [] }) })
    }
    if (url.includes('/nodes?')) {
      return Promise.resolve({
        ok: true,
        json: async () =>
          page([
            {
              id: 'node-1',
              version: '0.6.0',
              state: 'online',
              capabilities: [],
              streams_total: 0,
              streams_running: 0,
              streams_failed: 0,
            },
          ]),
      })
    }
    if (url.endsWith('/components')) {
      return Promise.resolve({
        ok: true,
        json: async () => [
          { kind: 'input', name: 'generate', description: 'Generate' },
          { kind: 'output', name: 'stdout', description: 'Stdout' },
        ],
      })
    }
    return Promise.resolve({ ok: true, json: async () => page([]) })
  })
}

beforeEach(() => {
  fetchMock.mockReset()
  sessionStorage.clear()
  globalThis.fetch = fetchMock as unknown as typeof fetch
  window.history.replaceState(null, '', '/')
})

afterEach(() => {
  cleanup()
  vi.restoreAllMocks()
})

describe('AI Copilot SSE and Console workflows', () => {
  it('dispatches parsed SSE frames to callbacks', () => {
    const onToolStart = vi.fn()
    const onToolResult = vi.fn()
    const onDelta = vi.fn()
    const onProposal = vi.fn()
    const onDone = vi.fn()
    const onError = vi.fn()
    const cbs = { onToolStart, onToolResult, onDelta, onProposal, onDone, onError }

    dispatchAiSseFrame('tool_start', '{"call_id":"c1","tool":"list_components","summary":"Listing"}', cbs)
    dispatchAiSseFrame(
      'tool_result',
      '{"call_id":"c1","tool":"list_components","ok":true,"summary":"Done"}',
      cbs,
    )
    dispatchAiSseFrame('delta', '{"text":"Hello "}', cbs)
    dispatchAiSseFrame(
      'proposal',
      '{"kind":"diagnostic_action","severity":"warn","title":"Inspect events","summary":"Check errors","route":"/events","action_label":"Open Events"}',
      cbs,
    )
    dispatchAiSseFrame(
      'done',
      '{"usage":{"prompt_tokens":5,"completion_tokens":3},"tool_calls":["list_components"]}',
      cbs,
    )

    expect(onToolStart).toHaveBeenCalledWith({ call_id: 'c1', tool: 'list_components', summary: 'Listing' })
    expect(onToolResult).toHaveBeenCalledWith({
      call_id: 'c1',
      tool: 'list_components',
      ok: true,
      summary: 'Done',
    })
    expect(onDelta).toHaveBeenCalledWith('Hello ')
    expect(onProposal).toHaveBeenCalledWith(expect.objectContaining({ kind: 'diagnostic_action' }))
    expect(onDone).toHaveBeenCalledTimes(1)
    expect(onError).not.toHaveBeenCalled()
  })

  it('surfaces HTTP 429 problem envelopes from streamAiCopilotChat', async () => {
    fetchMock.mockResolvedValueOnce(
      new Response(JSON.stringify({ code: 'ai_rate_limit_exceeded', message: 'Slow down' }), {
        status: 429,
        headers: { 'Content-Type': 'application/json' },
      }),
    )
    const onError = vi.fn()
    streamAiCopilotChat([{ role: 'user', content: 'hi' }], undefined, { onError })
    await waitFor(() => expect(onError).toHaveBeenCalledWith('ai_rate_limit_exceeded', 'Slow down'))
  })

  it('hides Copilot trigger in local standalone mode and when Hub lacks ai_copilot capability', async () => {
    setupHubFetch({ id: 'arkflow-control-plane', capabilities: [] })
    render(<App />)
    await screen.findByText('Fleet health')
    expect(screen.queryByTestId('copilot-header-button')).not.toBeInTheDocument()

    cleanup()
    setupHubFetch({ id: 'arkflow-control-hub', capabilities: ['node_registry'] })
    render(<App />)
    await screen.findByText('Fleet health')
    expect(screen.queryByTestId('copilot-header-button')).not.toBeInTheDocument()
  })

  it('streams Stream proposal and loads it into Configuration candidate editor without calling /configuration/draft', async () => {
    setupHubFetch({
      onAiChat: () =>
        sseResponse([
          {
            event: 'tool_start',
            data: { call_id: 'c1', tool: 'emit_stream_config_proposal', summary: 'Validating' },
          },
          {
            event: 'tool_result',
            data: { call_id: 'c1', tool: 'emit_stream_config_proposal', ok: true, summary: 'Validated' },
          },
          { event: 'delta', data: { text: 'Generated a verified Stream config.' } },
          {
            event: 'proposal',
            data: {
              kind: 'stream_config',
              title: 'Generate to Stdout',
              summary: 'Verified pipeline',
              format: 'yaml',
              content: 'streams:\n  - id: demo\n',
              node_id: 'node-1',
              validated: true,
            },
          },
          {
            event: 'done',
            data: {
              usage: { prompt_tokens: 20, completion_tokens: 15 },
              tool_calls: ['emit_stream_config_proposal'],
            },
          },
        ]),
    })

    render(<App />)
    const trigger = await screen.findByTestId('copilot-header-button')
    fireEvent.click(trigger)
    const drawer = await screen.findByTestId('copilot-drawer')
    expect(drawer).toBeInTheDocument()

    const input = screen.getByPlaceholderText(/Describe a pipeline/i)
    fireEvent.change(input, { target: { value: 'Create a generate stream' } })
    fireEvent.click(screen.getByRole('button', { name: 'Send' }))

    const card = await screen.findByTestId('copilot-proposal-stream')
    expect(card).toHaveTextContent('Generate to Stdout')
    expect(screen.getByText('Generated a verified Stream config.')).toBeInTheDocument()

    fireEvent.click(screen.getByRole('button', { name: 'Apply to Configuration' }))
    await waitFor(() => expect(window.location.pathname).toBe('/configuration'))
    await waitFor(() => {
      const editor = screen.getByLabelText('Configuration editor') as HTMLTextAreaElement
      expect(editor.value).toBe('streams:\n  - id: demo\n')
      expect(editor.readOnly).toBe(false)
    })
    expect(screen.getByTestId('copilot-config-diff')).toBeInTheDocument()
    expect(fetchMock.mock.calls.some(([u]) => String(u).endsWith('/configuration/draft'))).toBe(false)
  })

  it('imports a validated JobSpec proposal directly into the Job DAG editor', async () => {
    setupHubFetch({
      onAiChat: () =>
        sseResponse([
          {
            event: 'proposal',
            data: {
              kind: 'job_spec',
              title: 'Demo DAG',
              summary: 'Source to sink',
              spec: {
                id: 'demo-job',
                version: 1,
                parallelism: 1,
                max_parallelism: 128,
                sources: [{ operator_id: 'src', component: 'generate', config: {} }],
                operators: [],
                sinks: [{ operator_id: 'out', component: 'stdout', config: {} }],
                edges: [{ from: 'src', to: 'out', partitioning: 'partitioned' }],
              },
              validated: true,
            },
          },
          {
            event: 'done',
            data: {
              usage: { prompt_tokens: 10, completion_tokens: 10 },
              tool_calls: ['emit_job_spec_proposal'],
            },
          },
        ]),
    })

    render(<App />)
    fireEvent.click(await screen.findByTestId('copilot-header-button'))
    fireEvent.change(screen.getByPlaceholderText(/Describe a pipeline/i), {
      target: { value: 'Build a job DAG' },
    })
    fireEvent.click(screen.getByRole('button', { name: 'Send' }))

    await screen.findByTestId('copilot-proposal-job')
    fireEvent.click(screen.getByRole('button', { name: 'Import to Job DAG' }))

    await waitFor(() => expect(window.location.pathname).toBe('/jobs'))
    expect(await screen.findByDisplayValue('demo-job')).toBeInTheDocument()
  })

  it('disables configuration proposal application for viewer role', async () => {
    setupHubFetch({
      role: 'viewer',
      actions: ['read', 'read_audit'],
      onAiChat: () =>
        sseResponse([
          {
            event: 'proposal',
            data: {
              kind: 'diagnostic_action',
              severity: 'critical',
              title: 'Check Events',
              summary: 'Investigate failed stream',
              route: '/events',
              action_label: 'Open Events',
            },
          },
          { event: 'done', data: { usage: { prompt_tokens: 5, completion_tokens: 5 }, tool_calls: [] } },
        ]),
    })

    render(<App />)
    fireEvent.click(await screen.findByTestId('copilot-header-button'))
    expect(screen.queryByRole('button', { name: /Generate a Kafka/i })).not.toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: /Diagnose recent fleet errors/i }))

    const diagCard = await screen.findByTestId('copilot-proposal-diagnostic')
    expect(diagCard).toHaveTextContent('Check Events')
    fireEvent.click(screen.getByRole('button', { name: 'Open Events' }))
    await waitFor(() => expect(window.location.pathname).toBe('/events'))
  })
})
