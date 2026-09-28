import { useEffect, useState } from 'react'
import { parse as parseYaml, stringify as stringifyYaml } from 'yaml'
import { api, errorMessage, formatTime, waitForOperation } from '../api'
import type { ConfigCandidate, ConfigDiff, ConfigIssue, ConfigVersion } from '../api'
import { useT } from '../i18n'

export function convertConfiguration(
  content: string,
  from: ConfigCandidate['format'],
  to: ConfigCandidate['format'],
): string {
  if (from === to) return content
  if (from !== 'yaml' && from !== 'json')
    throw new Error(`${from.toUpperCase()} conversion is not available in the console`)
  const value = parseYaml(content)
  if (to === 'json') return JSON.stringify(value, null, 2)
  return stringifyYaml(value)
}

export function Configuration({ onError, nodeId }: { onError: (message: string) => void; nodeId?: string }) {
  const [content, setContent] = useState('streams: []\n')
  const [format, setFormat] = useState<ConfigCandidate['format']>('yaml')
  const [issues, setIssues] = useState<ConfigIssue[]>([])
  const [validatedCandidate, setValidatedCandidate] = useState<string>()
  const [versions, setVersions] = useState<ConfigVersion[]>([])
  const [saved, setSaved] = useState('')
  const [savedFormat, setSavedFormat] = useState<ConfigCandidate['format']>('yaml')
  const [busy, setBusy] = useState('')
  const [diff, setDiff] = useState<ConfigDiff>()
  const [editable, setEditable] = useState(false)
  const [activeSnapshot, setActiveSnapshot] = useState(false)
  const t = useT()
  const candidate = { format, content }
  const identity = `${format}\u0000${content}`
  const dirty = content !== saved || format !== savedFormat
  const validated = editable && !dirty && validatedCandidate === identity && issues.length === 0
  const load = async () => {
    try {
      const [draft, config, history] = await Promise.all([
        nodeId ? Promise.resolve(undefined) : api.draft(),
        api.config(nodeId),
        api.versions(nodeId),
      ])
      const next = draft ?? { format: 'json' as const, content: JSON.stringify(config, null, 2) }
      setContent(next.content)
      setFormat(next.format)
      setSaved(next.content)
      setSavedFormat(next.format)
      setEditable(Boolean(draft))
      setActiveSnapshot(!draft)
      setVersions(history)
      setIssues([])
      setValidatedCandidate(undefined)
    } catch (cause) {
      onError(errorMessage(cause))
    }
  }
  useEffect(() => {
    void load()
  }, [nodeId])
  const run = async (label: string, action: () => Promise<unknown>) => {
    try {
      setBusy(label)
      await action()
      setBusy('')
      return true
    } catch (cause) {
      setBusy('')
      onError(errorMessage(cause))
      return false
    }
  }
  const validate = () =>
    void run(t('config.busyValidating'), async () => {
      const report = await api.validateConfig(candidate)
      setIssues(report.errors)
      setValidatedCandidate(report.valid ? identity : undefined)
    })
  const saveDraft = () =>
    void run(t('config.busySavingDraft'), async () => {
      await api.saveDraft(candidate)
      setSaved(content)
      setSavedFormat(format)
      setEditable(true)
      setActiveSnapshot(false)
      setValidatedCandidate(undefined)
    })
  const publish = () =>
    void run(t('config.busyPublishing'), async () => {
      const operation = await api.applyConfig(candidate, nodeId)
      await waitForOperation(operation.id)
      await load()
    })
  const rollback = async (id: string) => {
    if (!window.confirm(t('config.confirmRollback', { id }))) return
    await run(t('config.busyRollingBack'), async () => {
      const operation = await api.rollback(id, nodeId)
      await waitForOperation(operation.id)
      await load()
    })
  }
  const compare = async (id: string) => {
    const to = versions.find((version) => version.id !== id)?.id
    if (!to) return
    await run(t('config.busyComparing'), async () => setDiff(await api.diff(id, to)))
  }
  const changeFormat = (next: ConfigCandidate['format']) => {
    if (next === format) return
    try {
      const converted = convertConfiguration(content, format, next)
      setFormat(next)
      setContent(converted)
      setIssues([])
      setValidatedCandidate(undefined)
    } catch (cause) {
      setIssues([
        {
          path: 'document',
          message: t('config.cannotConvert', {
            message: cause instanceof Error ? cause.message : String(cause),
          }),
        },
      ])
    }
  }
  return (
    <section className="panel config">
      <div className="panel-title">
        <div>
          <h3>
            {t('config.title')} {nodeId && `· ${nodeId}`}
          </h3>
          <small>
            {activeSnapshot
              ? t('config.activeRedacted')
              : dirty
                ? t('config.unsavedDraft')
                : t('config.draftSaved')}
            {busy && ` · ${busy}`}
            {!validated && t('config.validateBeforePublish')}
          </small>
        </div>
        <div className="actions">
          <select
            aria-label={t('config.formatLabel')}
            value={format}
            disabled={!editable || !!busy}
            onChange={(event) => changeFormat(event.target.value as ConfigCandidate['format'])}
          >
            <option value="yaml">YAML</option>
            <option value="json">JSON</option>
          </select>
          <button disabled={!editable || !dirty || !!busy} onClick={saveDraft}>
            {t('config.saveDraft')}
          </button>
          <button disabled={!editable || !!busy} onClick={validate}>
            {t('config.validate')}
          </button>
          <button disabled={!validated || !!busy} onClick={publish}>
            {t('config.publish')}
          </button>
        </div>
      </div>
      <textarea
        aria-label={t('config.editorLabel')}
        value={content}
        readOnly={!editable || !!busy}
        onChange={(event) => {
          setContent(event.target.value)
          setIssues([])
          setValidatedCandidate(undefined)
        }}
        spellCheck={false}
      />
      {issues.length > 0 && (
        <div className="validation">
          <strong>{t('config.validationIssues', { count: issues.length })}</strong>
          {issues.map((issue, index) => (
            <p key={index}>
              <strong>{issue.path || t('config.document')}</strong>: {issue.message}
            </p>
          ))}
        </div>
      )}
      {versions.length > 0 && (
        <>
          <h3 className="subheading">{t('config.versionHistory')}</h3>
          {versions.map((version) => (
            <div className="version" key={version.id}>
              <span>
                <strong>{version.id}</strong> · {version.format} · {formatTime(version.created_at_ms)}
              </span>
              <div className="actions">
                <button disabled={versions.length < 2 || !!busy} onClick={() => void compare(version.id)}>
                  {t('config.compare')}
                </button>
                <button disabled={!!busy} onClick={() => void rollback(version.id)}>
                  {t('config.rollback')}
                </button>
              </div>
            </div>
          ))}
        </>
      )}
      {diff && (
        <div className="validation">
          <strong>{t('config.comparing', { from: diff.from, to: diff.to })}</strong>
          <p>
            {diff.changed ? t('config.contentDiffers') : t('config.noContentChanges')}{' '}
            {t('config.formats', {
              from: diff.from_format ?? t('common.unknown'),
              to: diff.to_format ?? t('common.unknown'),
            })}
          </p>
          <button onClick={() => setDiff(undefined)}>{t('common.close')}</button>
        </div>
      )}
    </section>
  )
}
