import { useEffect, useState } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import {
  bearerTokenSource,
  clearAuthChallenge,
  currentAuthChallenge,
  setRuntimeBearerToken,
  subscribeAuthChallenge,
  type AuthChallenge,
} from '../api'
import { useT } from '../i18n'

/**
 * Inline shell prompt shown after a 401 when the Hub offers no OIDC browser
 * login (or when the token that was sent was rejected). Entering a token
 * stores it for this tab only and retries every mounted query.
 */
export function RuntimeTokenPrompt() {
  const t = useT()
  const queryClient = useQueryClient()
  const [challenge, setChallenge] = useState<AuthChallenge>(currentAuthChallenge)
  const [value, setValue] = useState('')
  const [retrying, setRetrying] = useState(false)

  useEffect(() => subscribeAuthChallenge(setChallenge), [])

  if (!challenge.required) return null

  const apply = async (token: string | null) => {
    setRuntimeBearerToken(token)
    setValue('')
    clearAuthChallenge()
    setRetrying(true)
    try {
      await queryClient.invalidateQueries()
    } finally {
      setRetrying(false)
    }
  }

  return (
    <section className="panel token-prompt" data-testid="runtime-token-prompt">
      <div className="panel-title">
        <h3>{t('token.requiredTitle')}</h3>
        <span>{t('token.requiredSubtitle')}</span>
      </div>
      <p>{challenge.tokenRejected ? t('token.rejectedBody') : t('token.requiredBody')}</p>
      <form
        className="toolbar"
        onSubmit={(event) => {
          event.preventDefault()
          if (value.trim()) void apply(value)
        }}
      >
        <input
          aria-label={t('token.label')}
          type="password"
          autoComplete="off"
          placeholder={t('token.placeholder')}
          value={value}
          onChange={(event) => setValue(event.target.value)}
        />
        <button type="submit" disabled={!value.trim() || retrying}>
          {t('token.use')}
        </button>
        <button type="button" disabled={retrying} onClick={() => void apply(null)}>
          {t('token.retry')}
        </button>
      </form>
      <small>{t('token.storedInSession')}</small>
    </section>
  )
}

/**
 * Settings control for the active bearer credential: shows where it comes
 * from, lets the operator switch it at runtime, and clears it again. The
 * value is never rendered back, and it lives in sessionStorage only.
 */
export function RuntimeCredentialPanel({ onError }: { onError?: (message: string) => void }) {
  const t = useT()
  const queryClient = useQueryClient()
  const [source, setSource] = useState(bearerTokenSource)
  const [value, setValue] = useState('')
  const [notice, setNotice] = useState('')
  const [busy, setBusy] = useState(false)

  const sourceLabel =
    source === 'runtime'
      ? t('settings.credentials.sourceRuntime')
      : source === 'environment'
        ? t('settings.credentials.sourceEnvironment')
        : t('settings.credentials.sourceNone')

  const apply = async (token: string | null) => {
    setRuntimeBearerToken(token)
    setSource(bearerTokenSource())
    setValue('')
    setNotice(token ? t('settings.credentials.saved') : t('settings.credentials.cleared'))
    clearAuthChallenge()
    setBusy(true)
    try {
      await queryClient.invalidateQueries()
    } catch (cause) {
      onError?.(t('settings.credentials.refreshFailed', { message: String(cause) }))
    } finally {
      setBusy(false)
    }
  }

  return (
    <section className="panel">
      <div className="panel-title">
        <h3>{t('settings.credentials.title')}</h3>
        <span>{t('settings.credentials.subtitle')}</span>
      </div>
      <p>
        <small>{t('settings.credentials.source')}</small>
        <strong>{sourceLabel}</strong>
      </p>
      <form
        className="toolbar"
        onSubmit={(event) => {
          event.preventDefault()
          if (value.trim()) void apply(value)
        }}
      >
        <input
          aria-label={t('settings.credentials.inputLabel')}
          type="password"
          autoComplete="off"
          placeholder={t('settings.credentials.inputPlaceholder')}
          value={value}
          onChange={(event) => setValue(event.target.value)}
        />
        <button type="submit" disabled={busy || !value.trim()}>
          {t('settings.credentials.use')}
        </button>
        <button type="button" disabled={busy || source !== 'runtime'} onClick={() => void apply(null)}>
          {t('settings.credentials.clear')}
        </button>
      </form>
      <small>{t('settings.credentials.storageNote')}</small>
      {notice ? <p className="muted">{notice}</p> : null}
    </section>
  )
}
