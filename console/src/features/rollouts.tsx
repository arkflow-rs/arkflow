import { useEffect, useState } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'
import { api, AuditRecord, errorMessage, formatTime, RolloutDetail } from '../api'
import { useT } from '../i18n'
import { usePrompt } from './confirm'
import { useNodes, useRolloutDetail, useRollouts } from '../queries'

export function Rollouts({ onError }: { onError: (message: string) => void }) {
  const t = useT()
  const queryClient = useQueryClient()
  const prompt = usePrompt()
  const nodes = useNodes().data?.items ?? []
  const rollouts = useRollouts().data ?? []
  const [selectedId, setSelectedId] = useState<string>()
  const detailQuery = useRolloutDetail(selectedId)
  const selected = detailQuery.data
  const pollError = detailQuery.isError ? errorMessage(detailQuery.error) : ''
  const [version, setVersion] = useState('')
  const [targets, setTargets] = useState<string[]>([])
  const [batch, setBatch] = useState(1)
  const [busy, setBusy] = useState(false)
  const [audit, setAudit] = useState<AuditRecord[]>([])
  const invalidate = () => {
    void queryClient.invalidateQueries({ queryKey: ['live', 'rollouts'] })
    void queryClient.invalidateQueries({ queryKey: ['live', 'rollout-detail'] })
  }
  const create = async () => {
    if (!version.trim() || targets.length === 0) return
    try {
      setBusy(true)
      const rollout = await api.createRollout(version.trim(), targets, batch)
      setSelectedId(rollout.rollout_id)
      setBusy(false)
      invalidate()
    } catch (cause) {
      setBusy(false)
      onError(errorMessage(cause))
    }
  }
  const action = async (id: string, operation: 'pause' | 'resume' | 'cancel' | 'rollback') => {
    const rollbackVersion =
      operation === 'rollback'
        ? ((await prompt({
            title: t('rollouts.rollbackPrompt'),
            label: t('rollouts.configurationVersion'),
            confirmLabel: t('rollouts.rollback'),
          })) ?? undefined)
        : undefined
    if (operation === 'rollback' && !rollbackVersion) return
    try {
      setBusy(true)
      const rollout = await api.rolloutAction(id, operation, rollbackVersion)
      setSelectedId(rollout.rollout_id)
      setBusy(false)
      invalidate()
      toast.success(t('toast.accepted'))
    } catch (cause) {
      setBusy(false)
      onError(errorMessage(cause))
    }
  }
  return (
    <>
      <section className="panel">
        <div className="panel-title">
          <h3>{t('rollouts.title')}</h3>
          <span>{t('rollouts.recorded', { count: rollouts.length })}</span>
        </div>
        <div className="toolbar">
          <input
            aria-label={t('rollouts.configurationVersion')}
            placeholder={t('rollouts.configurationVersion')}
            value={version}
            onChange={(event) => setVersion(event.target.value)}
          />
          <select
            aria-label={t('rollouts.batchSizeLabel')}
            value={batch}
            onChange={(event) => setBatch(Number(event.target.value))}
          >
            {[1, 2, 5, 10].map((value) => (
              <option key={value} value={value}>
                {t('rollouts.perBatch', { value })}
              </option>
            ))}
          </select>
          <button disabled={busy || !version.trim() || targets.length === 0} onClick={() => void create()}>
            {t('rollouts.create')}
          </button>
        </div>
        <div className="node-picker">
          {nodes.map((node) => (
            <label key={node.id}>
              <input
                type="checkbox"
                checked={targets.includes(node.id)}
                onChange={(event) =>
                  setTargets((current) =>
                    event.target.checked ? [...current, node.id] : current.filter((id) => id !== node.id),
                  )
                }
              />
              {node.id} · {node.state}
            </label>
          ))}
        </div>
        {rollouts.length === 0 ? (
          <p className="empty">{t('rollouts.empty')}</p>
        ) : (
          <div className="table">
            {rollouts.map((item) => (
              <button
                className="rollout-row"
                key={item.rollout_id}
                onClick={() => setSelectedId(item.rollout_id)}
              >
                <strong>{item.rollout_id}</strong>
                <span>
                  {item.config_version_id} ·{' '}
                  {t('rollouts.batches', {
                    current: item.current_batch + 1,
                    total: Math.max(1, Math.ceil(item.total_targets / item.batch_size)),
                  })}
                </span>
                <span className={`state ${item.state}`}>{item.state}</span>
              </button>
            ))}
          </div>
        )}
      </section>
      {selected && (
        <RolloutDetailPanel
          detail={selected}
          pollError={pollError}
          audit={audit}
          setAudit={setAudit}
          busy={busy}
          onAction={action}
          onClose={() => setSelectedId(undefined)}
        />
      )}
    </>
  )
}

function RolloutDetailPanel({
  detail,
  pollError,
  audit,
  setAudit,
  busy,
  onAction,
  onClose,
}: {
  detail: RolloutDetail
  pollError: string
  audit: AuditRecord[]
  setAudit: (records: AuditRecord[]) => void
  busy: boolean
  onAction: (id: string, action: 'pause' | 'resume' | 'cancel' | 'rollback') => void
  onClose: () => void
}) {
  const t = useT()
  useEffect(() => {
    void api
      .audit(detail.rollout.rollout_id)
      .then((page) => setAudit(page.items))
      .catch(() => setAudit([]))
  }, [detail.rollout.rollout_id, setAudit])
  const canAct = !['converged', 'cancelled', 'rolled_back'].includes(detail.rollout.state)
  return (
    <section className="panel detail">
      <div className="panel-title">
        <div>
          <span className="eyebrow">{t('rollouts.detailEyebrow')}</span>
          <h3>{detail.rollout.rollout_id}</h3>
        </div>
        <div className="actions">
          <button onClick={onClose}>{t('rollouts.close')}</button>
          {canAct && (
            <button
              disabled={busy}
              onClick={() =>
                onAction(detail.rollout.rollout_id, detail.rollout.state === 'paused' ? 'resume' : 'pause')
              }
            >
              {detail.rollout.state === 'paused' ? t('rollouts.resume') : t('rollouts.pause')}
            </button>
          )}
          {canAct && (
            <button disabled={busy} onClick={() => onAction(detail.rollout.rollout_id, 'cancel')}>
              {t('rollouts.cancel')}
            </button>
          )}
          {canAct && (
            <button disabled={busy} onClick={() => onAction(detail.rollout.rollout_id, 'rollback')}>
              {t('rollouts.rollback')}
            </button>
          )}
        </div>
      </div>
      {pollError && <div className="warning">{t('rollouts.liveProgressPaused', { error: pollError })}</div>}
      <p>
        <strong>{detail.rollout.config_version_id}</strong> ·{' '}
        {t('rollouts.batchSize', { size: detail.rollout.batch_size })} · {detail.rollout.state}
      </p>
      <div className="table">
        {detail.targets.map((target) => (
          <div className="rollout-target" key={target.node_id}>
            <strong>{target.node_id}</strong>
            <span>{target.state}</span>
            <small>
              {target.observed_config_version ?? t('rollouts.targetNotObserved')}
              {target.error ? ` · ${target.error}` : ''}
            </small>
          </div>
        ))}
      </div>
      <h4>{t('rollouts.auditHistory')}</h4>
      {audit.length ? (
        audit.map((item) => (
          <div className="event-row" key={item.event_id}>
            <strong>{item.action}</strong>
            <span>
              {item.outcome} · {item.actor ?? t('rollouts.unknownActor')}
            </span>
            <small>{formatTime(item.occurred_at_ms)}</small>
          </div>
        ))
      ) : (
        <p className="empty">{t('rollouts.noAuditRecords')}</p>
      )}
    </section>
  )
}
