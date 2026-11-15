import * as AlertDialog from '@radix-ui/react-alert-dialog'
import { createContext, useContext, useEffect, useRef, useState, type ReactNode } from 'react'
import { useT } from '../i18n'

type ConfirmOptions = {
  title: string
  body?: string
  confirmLabel: string
  cancelLabel?: string
}

type PromptOptions = {
  title: string
  body?: string
  label: string
  confirmLabel: string
  cancelLabel?: string
  initialValue?: string
}

type PendingDialog =
  | {
      id: number
      kind: 'confirm'
      options: ConfirmOptions
      resolve: (value: boolean) => void
    }
  | {
      id: number
      kind: 'prompt'
      options: PromptOptions
      resolve: (value: string | null) => void
    }

type ConfirmApi = {
  confirm: (options: ConfirmOptions) => Promise<boolean>
  prompt: (options: PromptOptions) => Promise<string | null>
}

const ConfirmContext = createContext<ConfirmApi>({
  confirm: () => Promise.resolve(false),
  prompt: () => Promise.resolve(null),
})

export function ConfirmProvider({ children }: { children: ReactNode }) {
  const t = useT()
  // Queue instead of a single slot: two confirmations issued in one render
  // commit (or a second one arriving while the first is open) used to
  // overwrite the earlier resolver, leaving its caller pending forever.
  const [queue, setQueue] = useState<PendingDialog[]>([])
  const pending = queue[0]
  const queueRef = useRef(queue)
  const inputRef = useRef<HTMLInputElement>(null)
  const nextDialogId = useRef(0)

  // The unmount cleanup below must settle the latest committed queue.
  // Syncing the ref in an effect keeps render pure (writing a ref during
  // render trips react-hooks/refs).
  useEffect(() => {
    queueRef.current = queue
  }, [queue])

  // Settle every outstanding caller when the provider unmounts.
  useEffect(
    () => () => {
      queueRef.current.forEach((dialog) => {
        if (dialog.kind === 'confirm') dialog.resolve(false)
        else dialog.resolve(null)
      })
    },
    [],
  )

  const api: ConfirmApi = {
    confirm: (options) =>
      new Promise((resolve) => {
        const id = nextDialogId.current++
        setQueue((current) => [...current, { id, kind: 'confirm', options, resolve }])
      }),
    prompt: (options) =>
      new Promise((resolve) => {
        const id = nextDialogId.current++
        setQueue((current) => [...current, { id, kind: 'prompt', options, resolve }])
      }),
  }

  // Settle the dialog by id, not by queue position: Radix's close primitive
  // also reports onOpenChange(false) for the same click, so the action and
  // the close callback both run settle. Resolving by the dialog's own id
  // makes the second call a no-op instead of resolving the NEXT queued
  // caller with the wrong answer.
  const close = (id: number, value: boolean | string | null) => {
    setQueue((current) => {
      const index = current.findIndex((dialog) => dialog.id === id)
      if (index === -1) return current
      current[index].resolve(value as never)
      return [...current.slice(0, index), ...current.slice(index + 1)]
    })
  }

  const settle = (accepted: boolean) => {
    if (!pending) return
    if (!accepted) {
      close(pending.id, pending.kind === 'prompt' ? null : false)
      return
    }
    close(pending.id, pending.kind === 'prompt' ? (inputRef.current?.value ?? '') : true)
  }

  return (
    <ConfirmContext.Provider value={api}>
      {children}
      {pending && (
        <AlertDialog.Root open onOpenChange={(open) => !open && settle(false)}>
          <AlertDialog.Portal>
            <AlertDialog.Overlay className="dialog-overlay" />
            <AlertDialog.Content className="dialog">
              <AlertDialog.Title asChild>
                <h3>{pending.options.title}</h3>
              </AlertDialog.Title>
              {pending.options.body && (
                <AlertDialog.Description asChild>
                  <p>{pending.options.body}</p>
                </AlertDialog.Description>
              )}
              {pending.kind === 'prompt' && (
                <label className="dialog-input">
                  <span>{pending.options.label}</span>
                  {/* Uncontrolled and keyed by the dialog: a queued prompt
                      starts from its own initial value without a state-sync
                      effect, and the settle handler reads the live value. */}
                  <input
                    key={pending.id}
                    ref={inputRef}
                    className="dialog-input"
                    defaultValue={pending.options.initialValue ?? ''}
                    autoFocus
                  />
                </label>
              )}
              <div className="dialog-actions">
                <AlertDialog.Cancel asChild>
                  <button type="button" onClick={() => settle(false)}>
                    {pending.options.cancelLabel ?? t('common.cancel')}
                  </button>
                </AlertDialog.Cancel>
                <AlertDialog.Action asChild>
                  <button type="button" className="danger" onClick={() => settle(true)}>
                    {pending.options.confirmLabel}
                  </button>
                </AlertDialog.Action>
              </div>
            </AlertDialog.Content>
          </AlertDialog.Portal>
        </AlertDialog.Root>
      )}
    </ConfirmContext.Provider>
  )
}

export function useConfirm() {
  return useContext(ConfirmContext).confirm
}

export function usePrompt() {
  return useContext(ConfirmContext).prompt
}
