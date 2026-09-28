import { api, errorMessage, formatTime } from '../api'
import type { ControlNode } from '../api'
import { useT } from '../i18n'
import { Card, EventRow, active, number } from './shared'
import type { Snapshot } from './types'

export function Overview({
  snapshot,
  onError,
  onNodesChanged,
}: {
  snapshot: Snapshot
  onError?: (message: string) => void
  onNodesChanged?: () => void
}) {
  const t = useT()
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
      .then(() => onNodesChanged?.())
      .catch((cause) => onError?.(errorMessage(cause)))
  }
  const running = snapshot.streams.filter((stream) => stream.state === 'running').length
  const failed = snapshot.streams.filter((stream) => stream.state === 'failed').length
  const online = snapshot.nodes.filter((node) => node.state === 'online').length
  const stale = snapshot.nodes.filter((node) => node.state !== 'online').length
  const activeOperations = snapshot.operations.filter((operation) => active(operation.state)).length
  const metrics = snapshot.metrics?.aggregate ?? {}
  return (
    <>
      <section className="cards">
        <Card
          label={t('overview.cardControlPlane')}
          value={snapshot.system?.state ?? t('common.loading')}
          hint={snapshot.system?.version}
        />
        <Card
          label={t('overview.cardNodesOnline')}
          value={`${online}/${snapshot.nodes.length || snapshot.system?.node_count || 0}`}
          hint={stale ? t('overview.nodesNeedAttention', { count: stale }) : t('overview.allNodesHealthy')}
        />
        <Card
          label={t('overview.cardStreamsRunning')}
          value={`${running}/${snapshot.streams.length || snapshot.status?.streams_total || 0}`}
          hint={failed ? t('overview.streamsFailed', { count: failed }) : t('overview.noFailures')}
        />
        <Card
          label={t('overview.cardActiveOperations')}
          value={activeOperations || snapshot.system?.active_operations || 0}
          hint={t('overview.queuedAndRunning')}
        />
      </section>
      <section className="overview-grid">
        <section className="panel">
          <div className="panel-title">
            <h3>{t('overview.fleetHealth')}</h3>
            <span>{t('common.registered', { count: snapshot.nodes.length })}</span>
          </div>
          {snapshot.nodes.length === 0 ? (
            <p className="empty">{t('overview.noNodes')}</p>
          ) : (
            <div className="node-grid">
              {snapshot.nodes.map((node) => {
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
          <span>{t('overview.eventsCount', { count: snapshot.events.length })}</span>
        </div>
        {snapshot.events.length ? (
          snapshot.events
            .slice(0, 8)
            .map((event, index) => <EventRow event={event} key={`${event.occurred_at_ms}-${index}`} />)
        ) : (
          <p className="empty">{t('overview.noRecentEvents')}</p>
        )}
      </section>
    </>
  )
}
