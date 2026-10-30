import * as AlertDialog from '@radix-ui/react-alert-dialog'
import { useState } from 'react'
import { useSearchParams } from 'react-router'
import { useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'
import { api, errorMessage, formatTime } from '../api'
import type { ControlNode } from '../api'
import { useT } from '../i18n'
import { useEvents, useMetrics, useNodes, useOperations, useStatus, useStreams, useSystem } from '../queries'
import { useConfirm } from './confirm'
import { Card, EventRow, SkeletonRows, active, number } from './shared'

export function Overview({
  onError,
  canManageNodes = true,
}: {
  onError?: (message: string) => void
  canManageNodes?: boolean
}) {
  const t = useT()
  const nodeActionTitle = canManageNodes ? undefined : t('perm.requires', { action: 'manage_nodes' })
  const queryClient = useQueryClient()
  const confirm = useConfirm()
  const [searchParams] = useSearchParams()
  const nodeId = searchParams.get('node_id') ?? undefined
  const systemQuery = useSystem()
  const statusQuery = useStatus()
  const nodesQuery = useNodes()
  const streamsQuery = useStreams(nodeId)
  const operationsQuery = useOperations({ node_id: nodeId })
  const eventsQuery = useEvents({ node_id: nodeId })
  const metricsQuery = useMetrics(nodeId)
  const system = systemQuery.data ?? null
  const status = statusQuery.data ?? null
  const nodes = nodesQuery.data?.items ?? []
  const streams = streamsQuery.data?.items ?? []
  const operations = operationsQuery.data?.items ?? []
  const events = eventsQuery.data?.items ?? []
  const metrics = metricsQuery.data?.aggregate ?? {}
  const setMaintenance = async (node: ControlNode, action: 'drain' | 'maintain' | 'resume') => {
    const confirmKey =
      action === 'drain'
        ? 'overview.confirmDrain'
        : action === 'maintain'
          ? 'overview.confirmMaintain'
          : 'overview.confirmResume'
    if (
      !(await confirm({
        title: t(confirmKey, { id: node.id }),
        confirmLabel: t('common.confirm'),
      }))
    )
      return
    const call =
      action === 'drain' ? api.drainNode : action === 'maintain' ? api.maintainNode : api.resumeNode
    call(node.id)
      .then(() => {
        void queryClient.invalidateQueries({ queryKey: ['live', 'nodes'] })
        toast.success(t('toast.accepted'))
      })
      .catch((cause) => onError?.(errorMessage(cause)))
  }
  const [editingLabels, setEditingLabels] = useState<ControlNode | null>(null)
  const running = streams.filter((stream) => stream.state === 'running').length
  const failed = streams.filter((stream) => stream.state === 'failed').length
  const online = nodes.filter((node) => node.state === 'online').length
  const stale = nodes.filter((node) => node.state !== 'online').length
  const activeOperations = operations.filter((operation) => active(operation.state)).length
  return (
    <>
      <section className="cards">
        <Card
          label={t('overview.cardControlPlane')}
          value={
            system?.ha?.enabled
              ? t('overview.haRole', { role: system.ha.role, epoch: system.ha.epoch })
              : (system?.state ?? t('common.loading'))
          }
          hint={
            system?.ha?.enabled
              ? t('overview.haHint', {
                  transitions: system.ha.transitions,
                  version: system.version,
                })
              : system?.version
          }
        />
        <Card
          label={t('overview.cardNodesOnline')}
          value={`${online}/${nodes.length || system?.node_count || 0}`}
          hint={stale ? t('overview.nodesNeedAttention', { count: stale }) : t('overview.allNodesHealthy')}
        />
        <Card
          label={t('overview.cardStreamsRunning')}
          value={`${running}/${streams.length || status?.streams_total || 0}`}
          hint={failed ? t('overview.streamsFailed', { count: failed }) : t('overview.noFailures')}
        />
        <Card
          label={t('overview.cardActiveOperations')}
          value={activeOperations || system?.active_operations || 0}
          hint={t('overview.queuedAndRunning')}
        />
      </section>
      <section className="overview-grid">
        <section className="panel">
          <div className="panel-title">
            <h3>{t('overview.fleetHealth')}</h3>
            <span>{t('common.registered', { count: nodes.length })}</span>
          </div>
          {nodesQuery.isPending ? (
            <SkeletonRows rows={3} />
          ) : nodes.length === 0 ? (
            <p className="empty">{t('overview.noNodes')}</p>
          ) : (
            <div className="node-grid">
              {nodes.map((node) => {
                const maintenance = node.maintenance_state ?? 'active'
                return (
                  <article className="node-card" key={node.id}>
                    <div className="panel-title">
                      <strong>{node.id}</strong>
                      <span className={`state ${node.state}`}>{node.state}</span>
                      {maintenance !== 'active' && (
                        <span className={`state ${maintenance}`}>{maintenance}</span>
                      )}
                    </div>
                    <small>
                      {t('overview.lastSeen', {
                        time: formatTime(node.last_seen_at_ms),
                        protocol: node.protocol_version ?? t('common.unknown'),
                        version: node.version,
                      })}
                    </small>
                    <div className="node-stats">
                      <span>
                        <strong>{node.streams_running}</strong> {t('overview.running')}
                      </span>
                      <span>
                        <strong>{node.streams_total}</strong> {t('overview.total')}
                      </span>
                      <span>
                        <strong>{node.streams_failed}</strong> {t('overview.failed')}
                      </span>
                    </div>
                    <small>{(node.capabilities ?? []).join(' · ') || t('overview.noCapabilities')}</small>
                    {Object.keys(node.labels ?? {}).length > 0 && (
                      <div className="labels">
                        {Object.entries(node.labels ?? {}).map(([key, value]) => (
                          <span className="label-chip" key={key}>{`${key}=${value}`}</span>
                        ))}
                      </div>
                    )}
                    <div className="actions">
                      <button
                        disabled={!canManageNodes}
                        title={nodeActionTitle}
                        onClick={() => setEditingLabels(node)}
                      >
                        {t('overview.labels')}
                      </button>
                      {maintenance === 'active' ? (
                        <>
                          <button
                            disabled={!canManageNodes}
                            title={nodeActionTitle}
                            onClick={() => void setMaintenance(node, 'drain')}
                          >
                            {t('overview.drain')}
                          </button>
                          <button
                            disabled={!canManageNodes}
                            title={nodeActionTitle}
                            onClick={() => void setMaintenance(node, 'maintain')}
                          >
                            {t('overview.maintain')}
                          </button>
                        </>
                      ) : (
                        <button
                          disabled={!canManageNodes}
                          title={nodeActionTitle}
                          onClick={() => void setMaintenance(node, 'resume')}
                        >
                          {t('overview.resume')}
                        </button>
                      )}
                    </div>
                  </article>
                )
              })}
            </div>
          )}
        </section>
        <section className="panel">
          <div className="panel-title">
            <h3>{t('overview.aggregateMetrics')}</h3>
            <span>{t('overview.latestReport')}</span>
          </div>
          <div className="metric-list">
            {metricsQuery.isPending ? (
              <SkeletonRows rows={3} />
            ) : Object.entries(metrics).length ? (
              Object.entries(metrics).map(([key, value]) => (
                <div className="metric" key={key}>
                  <span>{key.replaceAll('_', ' ')}</span>
                  <strong>{number(value)}</strong>
                </div>
              ))
            ) : (
              <p className="empty">{t('overview.noMetrics')}</p>
            )}
          </div>
        </section>
      </section>
      <section className="panel">
        <div className="panel-title">
          <h3>{t('overview.recentActivity')}</h3>
          <span>{t('overview.eventsCount', { count: events.length })}</span>
        </div>
        {eventsQuery.isPending ? (
          <SkeletonRows rows={3} />
        ) : events.length ? (
          events
            .slice(0, 8)
            .map((event, index) => <EventRow event={event} key={`${event.occurred_at_ms}-${index}`} />)
        ) : (
          <p className="empty">{t('overview.noRecentEvents')}</p>
        )}
      </section>
      {editingLabels && (
        <LabelsEditor
          node={editingLabels}
          onClose={() => setEditingLabels(null)}
          onSaved={() => {
            setEditingLabels(null)
            void queryClient.invalidateQueries({ queryKey: ['live', 'nodes'] })
            toast.success(t('toast.accepted'))
          }}
          onError={(message) => onError?.(message)}
        />
      )}
    </>
  )
}

/// Full-replacement label editor: one `key=value` per line. The submitted
/// map becomes the node's complete label set (see the node-labels capability).
function LabelsEditor({
  node,
  onClose,
  onSaved,
  onError,
}: {
  node: ControlNode
  onClose: () => void
  onSaved: () => void
  onError: (message: string) => void
}) {
  const t = useT()
  const [draft, setDraft] = useState(
    Object.entries(node.labels ?? {})
      .map(([key, value]) => `${key}=${value}`)
      .join('\n'),
  )
  const [busy, setBusy] = useState(false)
  const save = async () => {
    const labels: Record<string, string> = {}
    for (const line of draft.split('\n')) {
      const trimmed = line.trim()
      if (!trimmed) continue
      const separator = trimmed.indexOf('=')
      const key = separator > 0 ? trimmed.slice(0, separator) : ''
      const value = separator > 0 ? trimmed.slice(separator + 1) : ''
      if (!key || !value || trimmed.includes('=', separator + 1)) {
        toast.error(t('overview.labelsBadLine', { line: trimmed }))
        return
      }
      labels[key] = value
    }
    try {
      setBusy(true)
      await api.replaceNodeLabels(node.id, labels)
      setBusy(false)
      onSaved()
    } catch (cause) {
      setBusy(false)
      onError(errorMessage(cause))
    }
  }
  return (
    <AlertDialog.Root open onOpenChange={(open) => !open && onClose()}>
      <AlertDialog.Portal>
        <AlertDialog.Overlay className="dialog-overlay" />
        <AlertDialog.Content className="dialog">
          <AlertDialog.Title asChild>
            <h3>{t('overview.labelsEditorTitle', { id: node.id })}</h3>
          </AlertDialog.Title>
          <AlertDialog.Description asChild>
            <p>{t('overview.labelsEditorHint')}</p>
          </AlertDialog.Description>
          <textarea
            className="dialog-input"
            rows={6}
            value={draft}
            placeholder={'env=prod\ntier=edge'}
            onChange={(event) => setDraft(event.target.value)}
          />
          <div className="dialog-actions">
            <AlertDialog.Cancel asChild>
              <button type="button">{t('common.cancel')}</button>
            </AlertDialog.Cancel>
            <AlertDialog.Action asChild>
              <button type="button" disabled={busy} onClick={() => void save()}>
                {t('common.confirm')}
              </button>
            </AlertDialog.Action>
          </div>
        </AlertDialog.Content>
      </AlertDialog.Portal>
    </AlertDialog.Root>
  )
}
