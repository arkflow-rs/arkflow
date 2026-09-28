import { useEffect, useState } from 'react'
import {
  api,
  AuditRecord,
  ControlNode,
  errorMessage,
  formatTime,
  ROLLOUT_DETAIL_INTERVAL_MS,
  Rollout,
  RolloutDetail,
} from '../api'
import { useT } from '../i18n'

export function Rollouts({ nodes, onError }: { nodes: ControlNode[]; onError: (message: string) => void }) {
  const t = useT()
  const [items, setItems] = useState<Rollout[]>([])
  const [selected, setSelected] = useState<RolloutDetail>()
  const [pollError, setPollError] = useState('')
  const [version, setVersion] = useState('')
  const [targets, setTargets] = useState<string[]>([])
  const [batch, setBatch] = useState(1)
  const [busy, setBusy] = useState(false)
  const [audit, setAudit] = useState<AuditRecord[]>([])
  const refresh = async () => {
    try {
      const next = await api.rollouts()
      setItems(next)
      if (selected) setSelected(await api.rollout(selected.rollout.rollout_id))
    } catch (cause) {
      onError(errorMessage(cause))
    }
  }
  useEffect(() => {
    void refresh()
  }, [])
  // An active rollout advances on the Hub; poll the open detail so per-node
  // progress updates without a manual action.
  useEffect(() => {
    if (!selected) return
    const rolloutId = selected.rollout.rollout_id
    const timer = window.setInterval(() => {
      void api
        .rollout(rolloutId)
        .then((detail) => {
          setSelected(detail)
          setPollError('')
        })
        .catch((cause) => setPollError(errorMessage(cause)))
    }, ROLLOUT_DETAIL_INTERVAL_MS)
    return () => window.clearInterval(timer)
  }, [selected?.rollout.rollout_id])
  const create = async () => {
    if (!version.trim() || targets.length === 0) return
    try {
      setBusy(true)
      const rollout = await api.createRollout(version.trim(), targets, batch)
      setItems((items) => [rollout, ...items])
      setSelected(await api.rollout(rollout.rollout_id))
      setBusy(false)
    } catch (cause) {
      setBusy(false)
      onError(errorMessage(cause))
    }
  }
  const action = async (id: string, operation: 'pause' | 'resume' | 'cancel' | 'rollback') => {
    const rollbackVersion =
      operation === 'rollback' ? (window.prompt(t('rollouts.rollbackPrompt')) ?? undefined) : undefined
    if (operation === 'rollback' && !rollbackVersion) return
    try {
      setBusy(true)
      const rollout = await api.rolloutAction(id, operation, rollbackVersion)
      setSelected(await api.rollout(rollout.rollout_id))
      await refresh()
      setBusy(false)
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
          <span>{t('rollouts.recorded', { count: items.length })}</span>
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
        {items.length === 0 ? (
          <p className="empty">{t('rollouts.empty')}</p>
        ) : (
          <div className="table">
            {items.map((item) => (
              <button
                className="rollout-row"
                key={item.rollout_id}
                onClick={() =>
                  void api
                    .rollout(item.rollout_id)
                    .then(setSelected)
                    .catch((cause) => onError(errorMessage(cause)))
                }
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
          onClose={() => {
            setSelected(undefined)
            setPollError('')
          }}
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
