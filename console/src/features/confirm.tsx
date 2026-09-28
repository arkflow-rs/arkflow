import * as AlertDialog from '@radix-ui/react-alert-dialog'
import { createContext, useContext, useState, type ReactNode } from 'react'
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
  | { kind: 'confirm'; options: ConfirmOptions; resolve: (value: boolean) => void }
  | { kind: 'prompt'; options: PromptOptions; resolve: (value: string | null) => void }

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
  const [pending, setPending] = useState<PendingDialog>()
  const [inputValue, setInputValue] = useState('')

  const api: ConfirmApi = {
    confirm: (options) =>
      new Promise((resolve) => {
        setPending({ kind: 'confirm', options, resolve })
      }),
    prompt: (options) =>
      new Promise((resolve) => {
        setInputValue(options.initialValue ?? '')
        setPending({ kind: 'prompt', options, resolve })
      }),
  }

  const close = (value: boolean | string | null) => {
    pending?.resolve(value as never)
    setPending(undefined)
  }

  const settle = (accepted: boolean) => {
    if (!pending) return
    if (!accepted) {
      close(pending.kind === 'prompt' ? null : false)
      return
    }
    close(pending.kind === 'prompt' ? inputValue : true)
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
                  <input
                    className="dialog-input"
                    value={inputValue}
                    onChange={(event) => setInputValue(event.target.value)}
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
