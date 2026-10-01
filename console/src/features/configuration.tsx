import { useEffect, useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'
import { parse as parseYaml, stringify as stringifyYaml } from 'yaml'
import { api, errorMessage, formatTime, resolveDiff, resolveValidation, waitForOperation } from '../api'
import type { ConfigCandidate, ConfigDiff, ConfigIssue, ConfigVersion } from '../api'
import { useT } from '../i18n'
import { useSystem } from '../queries'
import { useConfirm } from './confirm'

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

export function Configuration({
  onError,
  nodeId,
  canMutate = true,
}: {
  onError: (message: string) => void
  nodeId?: string
  canMutate?: boolean
}) {
  const t = useT()
  const queryClient = useQueryClient()
  const confirm = useConfirm()
  // Hub deployments manage configuration per node; there is no fleet-wide
  // draft or active config to load until a node is selected. The mode stays
  // undetermined until /system settles, so a slow first load cannot briefly
  // fire the local-mode (fleet-global) endpoints against a Hub.
  const systemQuery = useSystem()
  const systemKnown = !systemQuery.isPending
  const isHub = systemQuery.data?.id === 'arkflow-control-hub'
  const fleetMode = systemKnown && Boolean(isHub) && !nodeId
  // Static config resources: never polled and never invalidated by the SSE
  // live-key sweep, so an open draft is only re-synced on nodeId change or
  // after an explicit publish/rollback.
  const draftQuery = useQuery({
    queryKey: ['config-draft', nodeId ?? null],
    queryFn: api.draft,
    staleTime: Infinity,
    enabled: systemKnown && !nodeId && !isHub,
  })
  const configQuery = useQuery({
    queryKey: ['config', nodeId ?? null],
    queryFn: () => api.config(nodeId),
    staleTime: Infinity,
    enabled: systemKnown && !fleetMode,
  })
  const versionsQuery = useQuery({
    queryKey: ['config-versions', nodeId ?? null],
    queryFn: () => api.versions(nodeId),
    staleTime: Infinity,
    enabled: systemKnown && !fleetMode,
  })
  const [content, setContent] = useState('streams: []\n')
  const [format, setFormat] = useState<ConfigCandidate['format']>('yaml')
  const [issues, setIssues] = useState<ConfigIssue[]>([])
  const [validatedCandidate, setValidatedCandidate] = useState<string>()
  const [saved, setSaved] = useState('')
  const [savedFormat, setSavedFormat] = useState<ConfigCandidate['format']>('yaml')
  const [busy, setBusy] = useState('')
  const [diff, setDiff] = useState<ConfigDiff>()
  const [editable, setEditable] = useState(false)
  const [activeSnapshot, setActiveSnapshot] = useState(false)
  const versions = versionsQuery.data ?? []
  const candidate = { format, content }
  const identity = `${format}\u0000${content}`
  const dirty = content !== saved || format !== savedFormat
  const validated = editable && !dirty && validatedCandidate === identity && issues.length === 0
  useEffect(() => {
    if (configQuery.data === undefined) return
    // A global draft loads independently of the config; wait for it to settle
    // so its arrival cannot be transiently treated as "no draft", and report
    // a draft-load failure instead of silently showing the active snapshot.
    const draftRelevant = !nodeId && !isHub
    if (draftRelevant && draftQuery.isPending) return
    if (draftRelevant && draftQuery.isError) {
      onError(errorMessage(draftQuery.error))
      return
    }
    const draft = draftRelevant ? draftQuery.data : undefined
    const next = draft ?? { format: 'json' as const, content: JSON.stringify(configQuery.data, null, 2) }
    setContent(next.content)
    setFormat(next.format)
    setSaved(next.content)
    setSavedFormat(next.format)
    setEditable(Boolean(draft))
    setActiveSnapshot(!draft)
    setIssues([])
    setValidatedCandidate(undefined)
  }, [
    configQuery.data,
    draftQuery.data,
    draftQuery.isPending,
    draftQuery.isError,
    draftQuery.error,
    isHub,
    nodeId,
    onError,
  ])
  const reload = async () => {
    await queryClient.invalidateQueries({ queryKey: ['config'] })
    await queryClient.invalidateQueries({ queryKey: ['config-draft'] })
    await queryClient.invalidateQueries({ queryKey: ['config-versions'] })
  }
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
      const report = await resolveValidation(candidate, nodeId)
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
      await reload()
      toast.success(t('toast.accepted'))
    })
  const rollback = async (id: string) => {
    if (!canMutate) return
    if (
      !(await confirm({
        title: t('config.confirmRollback', { id }),
        confirmLabel: t('config.rollback'),
      }))
    )
      return
    await run(t('config.busyRollingBack'), async () => {
      const operation = await api.rollback(id, nodeId)
      await waitForOperation(operation.id)
      await reload()
      toast.success(t('toast.accepted'))
    })
  }
  const compare = async (id: string) => {
    const to = versions.find((version) => version.id !== id)?.id
    if (!to) return
    await run(t('config.busyComparing'), async () => setDiff(await resolveDiff(id, to, nodeId)))
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
  // Mode undetermined (first /system fetch): hold the neutral state rather
  // than flashing an editable local-mode draft against a Hub.
  if (!systemKnown) {
    return (
      <section className="panel config">
        <div className="panel-title">
          <div>
            <h3>{t('config.title')}</h3>
            <small>{t('common.loading')}</small>
          </div>
        </div>
        <p className="empty">{t('common.loading')}</p>
      </section>
    )
  }
  // Hub mode without a selected node: configuration is node-scoped, so say
  // so instead of surfacing the fleet-wide 404s as load errors.
  if (fleetMode) {
    return (
      <section className="panel config">
        <div className="panel-title">
          <div>
            <h3>{t('config.title')}</h3>
            <small>{t('config.hubSelectNode')}</small>
          </div>
        </div>
        <p className="empty">{t('config.hubFleetNotice')}</p>
      </section>
    )
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
