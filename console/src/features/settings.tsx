import { useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { useT } from '../i18n'
import { useMutationFailure, useStatus, useWebhooks } from '../queries'
import { api, SNAPSHOT_INTERVAL_MS, WebhookSubscription } from '../api'

type FormState = {
  id: string
  url: string
  eventTypes: string
  secret: string
  enabled: boolean
}

const EMPTY_FORM: FormState = { id: '', url: '', eventTypes: '', secret: '', enabled: true }

export function Settings({
  hubMode,
  canManageWebhooks = true,
}: {
  hubMode: boolean | undefined
  canManageWebhooks?: boolean
}) {
  const t = useT()
  const status = useStatus().data ?? null
  return (
    <>
      <section className="panel">
        <div className="panel-title">
          <h3>{t('settings.title')}</h3>
          <span>{t('settings.subtitle')}</span>
        </div>
        <div className="settings-grid">
          <p>
            <small>{t('settings.apiVersion')}</small>
            <strong>v1</strong>
          </p>
          <p>
            <small>{t('settings.backend')}</small>
            <strong>{status?.version ?? t('common.loading')}</strong>
          </p>
          <p>
            <small>{t('settings.snapshotPolling')}</small>
            <strong>{t('settings.pollingValue', { seconds: SNAPSHOT_INTERVAL_MS / 1000 })}</strong>
          </p>
        </div>
        <p>{t('settings.credentialsNote')}</p>
      </section>
      {hubMode !== false && <WebhooksPanel canMutate={canManageWebhooks} />}
    </>
  )
}

function WebhooksPanel({ canMutate = true }: { canMutate?: boolean }) {
  const t = useT()
  const webhooks = useWebhooks()
  const queryClient = useQueryClient()
  // Webhook mutations render their own error strings; the shared failure
  // path still applies for the 403 identity refresh.
  const reportMutationFailure = useMutationFailure()
  const [form, setForm] = useState<FormState | null>(null)

  const subscriptions = webhooks.data?.subscriptions ?? []

  const replace = useMutation({
    mutationFn: api.replaceWebhooks,
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['webhooks'] }),
    onError: (cause) => reportMutationFailure(undefined, cause),
  })

  const runTest = useMutation({
    mutationFn: (id: string) => api.testWebhook(id),
    onError: (cause) => reportMutationFailure(undefined, cause),
  })

  if (webhooks.isError) {
    return (
      <section className="panel">
        <div className="panel-title">
          <h3>{t('settings.webhooks.title')}</h3>
          <span>{t('settings.webhooks.subtitle')}</span>
        </div>
        <p className="error-text">
          {t('settings.webhooks.unavailable', { message: String(webhooks.error) })}
        </p>
      </section>
    )
  }

  return (
    <section className="panel">
      <div className="panel-title">
        <h3>{t('settings.webhooks.title')}</h3>
        <span>{t('settings.webhooks.subtitle')}</span>
      </div>
      <p>{t('settings.webhooks.deliveryNote')}</p>
      <table className="table">
        <thead>
          <tr>
            <th>{t('settings.webhooks.id')}</th>
            <th>{t('settings.webhooks.url')}</th>
            <th>{t('settings.webhooks.eventTypes')}</th>
            <th>{t('settings.webhooks.status')}</th>
            <th>{t('settings.webhooks.actions')}</th>
          </tr>
        </thead>
        <tbody>
          {webhooks.isLoading ? (
            <tr>
              <td colSpan={5}>{t('common.loading')}</td>
            </tr>
          ) : subscriptions.length === 0 ? (
            <tr>
              <td colSpan={5}>{t('settings.webhooks.empty')}</td>
            </tr>
          ) : (
            subscriptions.map((subscription) => (
              <WebhookRow
                key={subscription.id}
                subscription={subscription}
                canMutate={canMutate}
                onEdit={() =>
                  setForm({
                    id: subscription.id,
                    url: subscription.url,
                    eventTypes: subscription.event_types.join(', '),
                    secret: '',
                    enabled: subscription.enabled,
                  })
                }
                onToggle={() =>
                  replace.mutate(
                    subscriptions.map((entry) => ({
                      id: entry.id,
                      url: entry.url,
                      event_types: entry.event_types,
                      enabled: entry.id === subscription.id ? !entry.enabled : entry.enabled,
                      // Omitted secret: the Hub retains the stored one.
                      secret: null,
                    })),
                  )
                }
                onTest={() => runTest.mutate(subscription.id)}
              />
            ))
          )}
        </tbody>
      </table>
      {runTest.isPending || runTest.data || runTest.isError ? (
        <p className="muted">
          {runTest.isPending
            ? t('settings.webhooks.testing')
            : runTest.isError
              ? t('settings.webhooks.testError', { message: String(runTest.error) })
              : runTest.data?.delivered
                ? t('settings.webhooks.testDelivered', { status: runTest.data.status ?? 200 })
                : t('settings.webhooks.testFailed', {
                    detail: runTest.data?.status
                      ? `HTTP ${runTest.data.status}`
                      : (runTest.data?.error ?? t('common.unknown')),
                  })}
        </p>
      ) : null}
      <div className="toolbar">
        <button
          type="button"
          disabled={!canMutate}
          title={!canMutate ? t('perm.requires', { action: 'manage_webhooks' }) : undefined}
          onClick={() => setForm({ ...EMPTY_FORM })}
        >
          {t('settings.webhooks.add')}
        </button>
      </div>
      {replace.isError ? (
        <p className="error-text">
          {t('settings.webhooks.replaceError', { message: String(replace.error) })}
        </p>
      ) : null}
      {form ? (
        <WebhookForm
          form={form}
          hasSecret={subscriptions.some((entry) => entry.id === form.id && entry.has_secret)}
          onChange={setForm}
          onCancel={() => setForm(null)}
          onSubmit={() =>
            replace.mutate(
              [
                // Omitted secret (null) on unchanged rows: the Hub
                // retains the stored secret for the same id.
                ...subscriptions
                  .filter((entry) => entry.id !== form.id)
                  .map((entry) => ({
                    id: entry.id,
                    url: entry.url,
                    event_types: entry.event_types,
                    enabled: entry.enabled,
                    secret: null,
                  })),
                {
                  id: form.id.trim(),
                  url: form.url.trim(),
                  event_types: form.eventTypes
                    .split(',')
                    .map((entry) => entry.trim())
                    .filter(Boolean),
                  enabled: form.enabled,
                  secret: form.secret.trim() || null,
                },
              ],
              {
                onSuccess: () => setForm(null),
              },
            )
          }
        />
      ) : null}
    </section>
  )
}

function WebhookRow({
  subscription,
  canMutate,
  onEdit,
  onToggle,
  onTest,
}: {
  subscription: WebhookSubscription
  canMutate: boolean
  onEdit: () => void
  onToggle: () => void
  onTest: () => void
}) {
  const t = useT()
  const gatedTitle = !canMutate ? t('perm.requires', { action: 'manage_webhooks' }) : undefined
  return (
    <tr>
      <td>
        <code>{subscription.id}</code>
        {subscription.has_secret ? <small> · {t('settings.webhooks.signed')}</small> : null}
      </td>
      <td className="wrap">{subscription.url}</td>
      <td>
        <code>{subscription.event_types.join(', ')}</code>
      </td>
      <td>{subscription.enabled ? t('settings.webhooks.enabled') : t('settings.webhooks.paused')}</td>
      <td>
        <div className="toolbar">
          <button type="button" disabled={!canMutate} title={gatedTitle} onClick={onTest}>
            {t('settings.webhooks.test')}
          </button>
          <button type="button" disabled={!canMutate} title={gatedTitle} onClick={onToggle}>
            {subscription.enabled ? t('settings.webhooks.pause') : t('settings.webhooks.enable')}
          </button>
          <button type="button" disabled={!canMutate} title={gatedTitle} onClick={onEdit}>
            {t('settings.webhooks.edit')}
          </button>
        </div>
      </td>
    </tr>
  )
}

function WebhookForm({
  form,
  hasSecret,
  onChange,
  onCancel,
  onSubmit,
}: {
  form: FormState
  hasSecret: boolean
  onChange: (next: FormState) => void
  onCancel: () => void
  onSubmit: () => void
}) {
  const t = useT()
  return (
    <form
      className="stack"
      onSubmit={(event) => {
        event.preventDefault()
        onSubmit()
      }}
    >
      <label>
        <span>{t('settings.webhooks.id')}</span>
        <input value={form.id} onChange={(event) => onChange({ ...form, id: event.target.value })} required />
      </label>
      <label>
        <span>{t('settings.webhooks.url')}</span>
        <input
          value={form.url}
          onChange={(event) => onChange({ ...form, url: event.target.value })}
          placeholder="https://example.com/hook"
          required
        />
      </label>
      <label>
        <span>{t('settings.webhooks.eventTypes')}</span>
        <input
          value={form.eventTypes}
          onChange={(event) => onChange({ ...form, eventTypes: event.target.value })}
          placeholder="rollout., node."
          required
        />
      </label>
      <label>
        <span>{t('settings.webhooks.secret')}</span>
        <input
          type="password"
          value={form.secret}
          onChange={(event) => onChange({ ...form, secret: event.target.value })}
        />
        <small>
          {hasSecret ? t('settings.webhooks.secretReplaceHint') : t('settings.webhooks.secretHint')}
        </small>
      </label>
      <label className="checkbox">
        <input
          type="checkbox"
          checked={form.enabled}
          onChange={(event) => onChange({ ...form, enabled: event.target.checked })}
        />
        <span>{t('settings.webhooks.enabled')}</span>
      </label>
      <div className="toolbar">
        <button type="submit">{t('settings.webhooks.save')}</button>
        <button type="button" onClick={onCancel}>
          {t('common.cancel')}
        </button>
      </div>
    </form>
  )
}
