import { afterEach, describe, expect, it, vi } from 'vitest'
import { streamEvents } from './api'

describe('control-plane event stream', () => {
  afterEach(() => {
    vi.useRealTimers()
    vi.restoreAllMocks()
  })

  it('reconnects with the last durable event id after a dropped stream', async () => {
    vi.useFakeTimers()
    const events: unknown[] = []
    let calls = 0
    const fetchMock = vi.fn((_url: string, init?: RequestInit) => {
      calls += 1
      if (calls === 2) expect(new Headers(init?.headers).get('Last-Event-ID')).toBe('42')
      const payload = calls === 1
        ? 'id: 42\nevent: stream_changed\ndata: {"event_type":"stream_changed","outcome":"accepted"}\n\n'
        : ''
      const stream = new ReadableStream<Uint8Array>({
        start(controller) {
          if (payload) controller.enqueue(new TextEncoder().encode(payload))
          controller.close()
        },
      })
      return Promise.resolve({ ok: true, status: 200, body: stream })
    })
    globalThis.fetch = fetchMock as unknown as typeof fetch
    const controller = streamEvents(event => events.push(event))
    await vi.waitFor(() => expect(events).toHaveLength(1))
    await vi.advanceTimersByTimeAsync(1000)
    await vi.waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2))
    controller.abort()
    expect(events[0]).toMatchObject({ event_type: 'stream_changed' })
  })
})
