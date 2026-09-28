import { useSearchParams } from 'react-router'
import { useQueryClient } from '@tanstack/react-query'
import { api, errorMessage, formatTime } from '../api'
import type { ControlNode } from '../api'
import { useT } from '../i18n'
import { useEvents, useMetrics, useNodes, useOperations, useStatus, useStreams, useSystem } from '../queries'
import { Card, EventRow, active, number } from './shared'

export function Overview({ onError }: { onError?: (message: string) => void }) {
  const t = useT()
  const queryClient = useQueryClient()
  const [searchParams] = useSearchParams()
  const nodeId = searchParams.get('node_id') ?? undefined
  const systemQuery = useSystem()
  const statusQuery = useStatus()
  const nodesQuery = useNodes()
  const streamsQuery = useStreams(nodeId)
  const operationsQuery = useOperations(nodeId)
  const eventsQuery = useEvents(nodeId)
  const metricsQuery = useMetrics(nodeId)
  const system = systemQuery.data ?? null
  const status = statusQuery.data ?? null
  const nodes = nodesQuery.data?.items ?? []
  const streams = streamsQuery.data?.items ?? []
  const operations = operationsQuery.data?.items ?? []
  const events = eventsQuery.data?.items ?? []
  const metrics = metricsQuery.data?.aggregate ?? {}
  const setMaintenance = (node: ControlNode, action: 'drain' | 'maintain' | 'resume') => {
    const confirmKey =
      action === 'drain'
        ? 'overview.confirmDrain'
        : action === 'maintain'
          ? 'overview.confirmMaintain'
          : 'overview.confirmResume'
    if (!window.confirm(t(confirmKey, { id: node.id }))) return
    const call =
      action === 'drain' ? api.drainNode : action === 'maintain' ? api.maintainNode : api.resumeNode
    call(node.id)
      .then(() => queryClient.invalidateQueries({ queryKey: ['live', 'nodes'] }))
      .catch((cause) => onError?.(errorMessage(cause)))
  }
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
          value={system?.state ?? t('common.loading')}
          hint={system?.version}
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
          {nodes.length === 0 ? (
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
                    <div className="actions">
                      {maintenance === 'active' ? (
                        <>
                          <button onClick={() => setMaintenance(node, 'drain')}>{t('overview.drain')}</button>
                          <button onClick={() => setMaintenance(node, 'maintain')}>
                            {t('overview.maintain')}
                          </button>
                        </>
                      ) : (
                        <button onClick={() => setMaintenance(node, 'resume')}>{t('overview.resume')}</button>
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
            {Object.entries(metrics).length ? (
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
        {events.length ? (
          events
            .slice(0, 8)
            .map((event, index) => <EventRow event={event} key={`${event.occurred_at_ms}-${index}`} />)
        ) : (
          <p className="empty">{t('overview.noRecentEvents')}</p>
        )}
      </section>
    </>
  )
}
