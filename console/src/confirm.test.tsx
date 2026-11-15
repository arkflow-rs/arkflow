import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { afterEach, describe, expect, it } from 'vitest'
import { useState } from 'react'
import { ConfirmProvider, useConfirm, usePrompt } from './features/confirm'

afterEach(() => cleanup())

function Harness() {
  const confirm = useConfirm()
  const prompt = usePrompt()
  const [log, setLog] = useState<string[]>([])
  const record = (entry: string) => setLog((current) => [...current, entry])
  return (
    <>
      <button
        onClick={() => {
          void confirm({ title: 'First?', confirmLabel: 'Yes' }).then((ok) => record(`confirm:${ok}`))
        }}
      >
        ask-confirm
      </button>
      <button
        onClick={() => {
          void prompt({
            title: 'Second',
            label: 'Name',
            confirmLabel: 'OK',
            initialValue: 'draft',
          }).then((value) => record(`prompt:${String(value)}`))
        }}
      >
        ask-prompt
      </button>
      <output data-testid="log">{log.join(',')}</output>
    </>
  )
}

function renderHarness() {
  render(
    <ConfirmProvider>
      <Harness />
    </ConfirmProvider>,
  )
}

describe('confirm provider queue', () => {
  it('queues a second request and initializes its prompt from its own options', async () => {
    renderHarness()
    fireEvent.click(screen.getByText('ask-confirm'))
    fireEvent.click(screen.getByText('ask-prompt'))

    // Only the first dialog is visible; the prompt waits in the queue.
    expect(screen.getByText('First?')).toBeTruthy()
    expect(screen.queryByText('Second')).toBeNull()

    fireEvent.click(screen.getByRole('button', { name: 'Yes' }))
    await waitFor(() => expect(screen.getByText('Second')).toBeTruthy())
    const input = screen.getByLabelText('Name') as HTMLInputElement
    expect(input.value).toBe('draft')

    fireEvent.change(input, { target: { value: 'renamed' } })
    fireEvent.click(screen.getByRole('button', { name: 'OK' }))
    await waitFor(() => expect(screen.getByTestId('log').textContent).toBe('confirm:true,prompt:renamed'))
  })

  it('resolves a cancelled prompt as null and keeps later callers queued', async () => {
    renderHarness()
    fireEvent.click(screen.getByText('ask-prompt'))
    fireEvent.click(screen.getByText('ask-confirm'))

    expect(screen.getByLabelText('Name')).toBeTruthy()
    fireEvent.click(screen.getByRole('button', { name: 'Cancel' }))
    await waitFor(() => expect(screen.getByText('First?')).toBeTruthy())

    // The prompt resolved null; the queued confirm is still pending and
    // settles with its own answer.
    fireEvent.click(screen.getByRole('button', { name: 'Yes' }))
    await waitFor(() => expect(screen.getByTestId('log').textContent).toBe('prompt:null,confirm:true'))
  })
})
