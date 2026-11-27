import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from 'react'
import { useLocation, useNavigate, useSearchParams } from 'react-router'
import { toast } from 'sonner'
import {
  streamAiCopilotChat,
  type AiChatMessagePayload,
  type AiJobSpecProposal,
  type AiProposal,
  type AiStreamConfigProposal,
  type AiUiContext,
} from '../api'
import { useT } from '../i18n'

const STORAGE_KEY = 'arkflow_copilot_session_v1'

export type CopilotToolStep = {
  call_id: string
  tool: string
  status: 'running' | 'done' | 'error'
  summary: string
}

export type CopilotMessage = {
  id: string
  role: 'user' | 'assistant'
  content: string
  tools?: CopilotToolStep[]
  proposals?: AiProposal[]
  error?: string
}

export type PendingConfigProposal = {
  id: number
  proposal: AiStreamConfigProposal
}

export type PendingJobProposal = {
  id: number
  proposal: AiJobSpecProposal
}

type CopilotContextValue = {
  enabled: boolean
  canConfigure: boolean
  open: boolean
  setOpen: (open: boolean) => void
  streaming: boolean
  messages: CopilotMessage[]
  context: AiUiContext | undefined
  setContext: (ctx: AiUiContext | undefined) => void
  openWithContext: (ctx: AiUiContext, autoPrompt?: string) => void
  sendMessage: (text: string, overrideContext?: AiUiContext) => void
  stop: () => void
  clear: () => void
  applyProposal: (proposal: AiProposal) => void
  pendingConfigProposal: PendingConfigProposal | null
  consumeConfigProposal: (id: number) => void
  pendingJobProposal: PendingJobProposal | null
  consumeJobProposal: (id: number) => void
}

const NOOP_CONTEXT: CopilotContextValue = {
  enabled: false,
  canConfigure: false,
  open: false,
  setOpen: () => {},
  streaming: false,
  messages: [],
  context: undefined,
  setContext: () => {},
  openWithContext: () => {},
  sendMessage: () => {},
  stop: () => {},
  clear: () => {},
  applyProposal: () => {},
  pendingConfigProposal: null,
  consumeConfigProposal: () => {},
  pendingJobProposal: null,
  consumeJobProposal: () => {},
}

const CopilotContext = createContext<CopilotContextValue>(NOOP_CONTEXT)

export function useCopilot(): CopilotContextValue {
  return useContext(CopilotContext)
}

function loadStoredMessages(): CopilotMessage[] {
  try {
    const raw = sessionStorage.getItem(STORAGE_KEY)
    if (!raw) return []
    const parsed = JSON.parse(raw)
    return Array.isArray(parsed) ? (parsed as CopilotMessage[]) : []
  } catch {
    return []
  }
}

function saveStoredMessages(messages: CopilotMessage[]): void {
  try {
    sessionStorage.setItem(STORAGE_KEY, JSON.stringify(messages.slice(-40)))
  } catch {
    /* ignore quota/storage errors */
  }
}

let nextIdSeq = 1
const makeId = (prefix: string) => `${prefix}-${Date.now()}-${nextIdSeq++}`

export function CopilotProvider({
  enabled,
  canConfigure,
  children,
}: {
  enabled: boolean
  canConfigure: boolean
  children: ReactNode
}) {
  const navigate = useNavigate()
  const location = useLocation()
  const [searchParams] = useSearchParams()
  const selectedNode = searchParams.get('node_id') ?? ''

  const [open, setOpen] = useState(false)
  const [streaming, setStreaming] = useState(false)
  const [messages, setMessages] = useState<CopilotMessage[]>(() => (enabled ? loadStoredMessages() : []))
  const [context, setContext] = useState<AiUiContext | undefined>(undefined)
  const [pendingConfigProposal, setPendingConfigProposal] = useState<PendingConfigProposal | null>(null)
  const [pendingJobProposal, setPendingJobProposal] = useState<PendingJobProposal | null>(null)

  const abortRef = useRef<AbortController | null>(null)
  const messagesRef = useRef(messages)
  useEffect(() => {
    messagesRef.current = messages
    if (enabled) saveStoredMessages(messages)
  }, [messages, enabled])

  useEffect(() => {
    if (!enabled) {
      abortRef.current?.abort()
      abortRef.current = null
    }
  }, [enabled])

  useEffect(() => {
    return () => {
      abortRef.current?.abort()
    }
  }, [])

  const stop = useCallback(() => {
    abortRef.current?.abort()
    abortRef.current = null
    setStreaming(false)
  }, [])

  const clear = useCallback(() => {
    stop()
    setMessages([])
    try {
      sessionStorage.removeItem(STORAGE_KEY)
    } catch {
      /* ignore */
    }
  }, [stop])

  const sendMessage = useCallback(
    (rawText: string, overrideContext?: AiUiContext) => {
      const text = rawText.trim()
      if (!enabled || !text || streaming) return

      const effectiveContext: AiUiContext = {
        page: location.pathname,
        ...(selectedNode ? { node_id: selectedNode } : {}),
        ...(overrideContext ?? context ?? {}),
      }

      const historyPayload: AiChatMessagePayload[] = [
        ...messagesRef.current
          .filter((m) => m.content.trim().length > 0 && !m.error)
          .map((m) => ({ role: m.role, content: m.content })),
        { role: 'user', content: text },
      ]

      const userMsg: CopilotMessage = { id: makeId('user'), role: 'user', content: text }
      const assistantId = makeId('assistant')
      const assistantMsg: CopilotMessage = {
        id: assistantId,
        role: 'assistant',
        content: '',
        tools: [],
        proposals: [],
      }

      setMessages((prev) => [...prev, userMsg, assistantMsg])
      setStreaming(true)

      const updateAssistant = (updater: (msg: CopilotMessage) => CopilotMessage) => {
        setMessages((prev) => prev.map((m) => (m.id === assistantId ? updater(m) : m)))
      }

      abortRef.current?.abort()
      abortRef.current = streamAiCopilotChat(historyPayload, effectiveContext, {
        onToolStart: (ev) => {
          updateAssistant((msg) => ({
            ...msg,
            tools: [
              ...(msg.tools ?? []),
              {
                call_id: ev.call_id,
                tool: ev.tool,
                status: 'running',
                summary: ev.summary,
              },
            ],
          }))
        },
        onToolResult: (ev) => {
          updateAssistant((msg) => ({
            ...msg,
            tools: (msg.tools ?? []).map((t) =>
              t.call_id === ev.call_id
                ? {
                    ...t,
                    status: ev.ok ? 'done' : 'error',
                    summary: ev.summary,
                  }
                : t,
            ),
          }))
        },
        onDelta: (delta) => {
          updateAssistant((msg) => ({
            ...msg,
            content: msg.content + delta,
          }))
        },
        onProposal: (proposal) => {
          updateAssistant((msg) => ({
            ...msg,
            proposals: [...(msg.proposals ?? []), proposal],
          }))
        },
        onDone: () => {
          abortRef.current = null
          setStreaming(false)
        },
        onError: (code, message) => {
          abortRef.current = null
          setStreaming(false)
          updateAssistant((msg) => ({
            ...msg,
            error: `${code}: ${message}`,
          }))
        },
      })
    },
    [enabled, streaming, location.pathname, selectedNode, context],
  )

  const openWithContext = useCallback(
    (nextContext: AiUiContext, autoPrompt?: string) => {
      if (!enabled) return
      setContext(nextContext)
      setOpen(true)
      if (autoPrompt && autoPrompt.trim()) {
        sendMessage(autoPrompt, nextContext)
      }
    },
    [enabled, sendMessage],
  )

  const applyProposal = useCallback(
    (proposal: AiProposal) => {
      if (proposal.kind === 'stream_config') {
        const targetNode = proposal.node_id ?? (selectedNode || undefined)
        setPendingConfigProposal({ id: nextIdSeq++, proposal })
        navigate(targetNode ? `/configuration?node_id=${encodeURIComponent(targetNode)}` : '/configuration')
        return
      }
      if (proposal.kind === 'job_spec') {
        setPendingJobProposal({ id: nextIdSeq++, proposal })
        navigate(selectedNode ? `/jobs?node_id=${encodeURIComponent(selectedNode)}` : '/jobs')
        return
      }
      if (proposal.kind === 'diagnostic_action') {
        const params = new URLSearchParams(proposal.params ?? {})
        if (selectedNode && !params.has('node_id')) {
          params.set('node_id', selectedNode)
        }
        const query = params.toString()
        navigate(`${proposal.route}${query ? `?${query}` : ''}`)
      }
    },
    [navigate, selectedNode],
  )

  const consumeConfigProposal = useCallback((id: number) => {
    setPendingConfigProposal((current) => (current?.id === id ? null : current))
  }, [])

  const consumeJobProposal = useCallback((id: number) => {
    setPendingJobProposal((current) => (current?.id === id ? null : current))
  }, [])

  const value = useMemo<CopilotContextValue>(
    () => ({
      enabled,
      canConfigure,
      open: enabled && open,
      setOpen,
      streaming: enabled && streaming,
      messages,
      context,
      setContext,
      openWithContext,
      sendMessage,
      stop,
      clear,
      applyProposal,
      pendingConfigProposal,
      consumeConfigProposal,
      pendingJobProposal,
      consumeJobProposal,
    }),
    [
      enabled,
      canConfigure,
      open,
      streaming,
      messages,
      context,
      openWithContext,
      sendMessage,
      stop,
      clear,
      applyProposal,
      pendingConfigProposal,
      consumeConfigProposal,
      pendingJobProposal,
      consumeJobProposal,
    ],
  )

  return <CopilotContext.Provider value={value}>{children}</CopilotContext.Provider>
}

export function CopilotDrawer() {
  const t = useT()
  const {
    enabled,
    canConfigure,
    open,
    setOpen,
    streaming,
    messages,
    context,
    setContext,
    sendMessage,
    stop,
    clear,
    applyProposal,
  } = useCopilot()
  const [input, setInput] = useState('')
  const bottomRef = useRef<HTMLDivElement | null>(null)

  useEffect(() => {
    if (!open) return
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') setOpen(false)
    }
    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  }, [open, setOpen])

  useEffect(() => {
    bottomRef.current?.scrollIntoView?.({ behavior: 'smooth' })
  }, [messages.length, streaming])

  if (!enabled || !open) return null

  const contextSummary = context
    ? [context.page, context.node_id, context.job_id, context.stream_id].filter(Boolean).join(' · ')
    : ''

  const submit = () => {
    if (!input.trim() || streaming) return
    sendMessage(input)
    setInput('')
  }

  return (
    <aside
      className="copilot-drawer"
      role="dialog"
      aria-label={t('copilot.title')}
      data-testid="copilot-drawer"
    >
      <div className="copilot-header">
        <div>
          <h3>{t('copilot.title')}</h3>
          <small>{t('copilot.subtitle')}</small>
        </div>
        <div className="actions">
          <button type="button" onClick={clear} disabled={messages.length === 0 && !streaming}>
            {t('copilot.clear')}
          </button>
          <button type="button" onClick={() => setOpen(false)}>
            {t('copilot.close')}
          </button>
        </div>
      </div>

      {contextSummary && (
        <div className="copilot-context">
          <span>{t('copilot.contextAttached', { summary: contextSummary })}</span>
          <button type="button" onClick={() => setContext(undefined)}>
            {t('copilot.clearContext')}
          </button>
        </div>
      )}

      <div className="copilot-messages">
        {messages.length === 0 ? (
          <div className="copilot-empty">
            <p>{t('copilot.emptyHint')}</p>
            <div className="copilot-starters">
              {canConfigure && (
                <>
                  <button type="button" onClick={() => sendMessage(t('copilot.starterStream'))}>
                    {t('copilot.starterStream')}
                  </button>
                  <button type="button" onClick={() => sendMessage(t('copilot.starterJob'))}>
                    {t('copilot.starterJob')}
                  </button>
                </>
              )}
              <button type="button" onClick={() => sendMessage(t('copilot.starterDiagnose'))}>
                {t('copilot.starterDiagnose')}
              </button>
            </div>
          </div>
        ) : (
          messages.map((msg) => (
            <div key={msg.id} className={`copilot-msg copilot-msg-${msg.role}`}>
              <div className="copilot-msg-role">{msg.role === 'user' ? 'You' : 'Copilot'}</div>
              {msg.tools && msg.tools.length > 0 && (
                <div className="copilot-tools">
                  {msg.tools.map((step) => (
                    <span key={step.call_id} className={`copilot-tool-chip ${step.status}`}>
                      {step.status === 'running' ? '… ' : step.status === 'done' ? '✓ ' : '✗ '}
                      {step.summary}
                    </span>
                  ))}
                </div>
              )}
              {msg.content && <pre className="copilot-msg-body">{msg.content}</pre>}
              {msg.proposals &&
                msg.proposals.map((proposal, idx) => (
                  <ProposalCard
                    key={`${msg.id}-p-${idx}`}
                    proposal={proposal}
                    canConfigure={canConfigure}
                    onApply={() => applyProposal(proposal)}
                  />
                ))}
              {msg.error && <p className="error-text">{msg.error}</p>}
            </div>
          ))
        )}
        <div ref={bottomRef} />
      </div>

      <div className="copilot-composer">
        <textarea
          rows={3}
          aria-label={t('copilot.placeholder')}
          placeholder={t('copilot.placeholder')}
          value={input}
          onChange={(event) => setInput(event.target.value)}
          onKeyDown={(event) => {
            if (event.key === 'Enter' && !event.shiftKey) {
              event.preventDefault()
              submit()
            }
          }}
        />
        <div className="actions">
          {streaming ? (
            <button type="button" onClick={stop}>
              {t('copilot.stop')}
            </button>
          ) : (
            <button type="button" disabled={!input.trim()} onClick={submit}>
              {t('copilot.send')}
            </button>
          )}
        </div>
      </div>
    </aside>
  )
}

function ProposalCard({
  proposal,
  canConfigure,
  onApply,
}: {
  proposal: AiProposal
  canConfigure: boolean
  onApply: () => void
}) {
  const t = useT()

  const copyText = (value: string) => {
    void navigator.clipboard?.writeText(value).then(() => {
      toast.success(t('copilot.copied'))
    })
  }

  if (proposal.kind === 'stream_config') {
    return (
      <div className="copilot-proposal" data-testid="copilot-proposal-stream">
        <div className="copilot-proposal-head">
          <strong>{proposal.title}</strong>
          <span className="state converged">{t('copilot.validatedBadge')}</span>
        </div>
        <p>{proposal.summary}</p>
        <pre className="copilot-proposal-code">{proposal.content}</pre>
        <div className="actions">
          <button
            type="button"
            disabled={!canConfigure}
            title={!canConfigure ? t('copilot.requiresConfigure') : undefined}
            onClick={onApply}
          >
            {t('copilot.applyConfig')}
          </button>
          <button type="button" onClick={() => copyText(proposal.content)}>
            {t('copilot.copy')}
          </button>
        </div>
      </div>
    )
  }

  if (proposal.kind === 'job_spec') {
    const pretty = JSON.stringify(proposal.spec, null, 2)
    return (
      <div className="copilot-proposal" data-testid="copilot-proposal-job">
        <div className="copilot-proposal-head">
          <strong>{proposal.title}</strong>
          <span className="state converged">{t('copilot.validatedBadge')}</span>
        </div>
        <p>{proposal.summary}</p>
        <pre className="copilot-proposal-code">{pretty}</pre>
        <div className="actions">
          <button
            type="button"
            disabled={!canConfigure}
            title={!canConfigure ? t('copilot.requiresConfigure') : undefined}
            onClick={onApply}
          >
            {t('copilot.importDag')}
          </button>
          <button type="button" onClick={() => copyText(pretty)}>
            {t('copilot.copy')}
          </button>
        </div>
      </div>
    )
  }

  return (
    <div
      className={`copilot-proposal copilot-proposal-${proposal.severity}`}
      data-testid="copilot-proposal-diagnostic"
    >
      <div className="copilot-proposal-head">
        <strong>{proposal.title}</strong>
        <span className={`state ${proposal.severity === 'critical' ? 'failed' : 'applying'}`}>
          {proposal.severity}
        </span>
      </div>
      <p>{proposal.summary}</p>
      <div className="actions">
        <button type="button" onClick={onApply}>
          {proposal.action_label}
        </button>
      </div>
    </div>
  )
}
