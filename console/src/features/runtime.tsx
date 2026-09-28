import { useEffect, useMemo, useState } from 'react'
import { useSearchParams } from 'react-router'
import { useQueryClient } from '@tanstack/react-query'
import { formatTime } from '../api'
import type { ControlEvent, Operation, StreamStatus } from '../api'
import { useT } from '../i18n'
import { useEvents, useOperations, useStreams } from '../queries'
import { EventRow, OperationRow, Pagination, active, number } from './shared'
import type { Command } from './types'

export function Runtime({
  command,
  canMutate = true,
  onError,
}: {
  command: Command
  canMutate?: boolean
  onError?: (message: string) => void
}) {
  const t = useT()
  const [searchParams] = useSearchParams()
  const nodeId = searchParams.get('node_id') ?? undefined
  const streamsQuery = useStreams(nodeId)
  const operationsQuery = useOperations(nodeId)
  const eventsQuery = useEvents(nodeId)
  const streams = streamsQuery.data?.items ?? []
  const operations = operationsQuery.data?.items ?? []
  const events = eventsQuery.data?.items ?? []
  const streamTotal = streamsQuery.data?.total
  const operationTotal = operationsQuery.data?.total
  const [filter, setFilter] = useState('')
  const [state, setState] = useState('all')
  const [page, setPage] = useState(1)
  const [selected, setSelected] = useState<string>()
  const filtered = useMemo(
    () =>
      streams.filter(
        (stream) =>
          (!filter ||
            stream.id.toLowerCase().includes(filter.toLowerCase()) ||
            (stream.node_id ?? '').toLowerCase().includes(filter.toLowerCase())) &&
          (state === 'all' || stream.state === state),
      ),
    [streams, filter, state],
  )
  const pageSize = 8
  const pages = Math.max(1, Math.ceil(filtered.length / pageSize))
  const visible = filtered.slice((page - 1) * pageSize, page * pageSize)
  const detail = streams.find((stream) => stream.id === selected)
  useEffect(() => {
    if (page > pages) setPage(pages)
  }, [page, pages])
  return (
    <>
      <section className="panel">
        <div className="panel-title">
          <h3>{t('runtime.title')}</h3>
          <span>
            {t('runtime.matchingResources', { count: filtered.length })}
            {streamTotal !== undefined && streamTotal > streams.length
              ? t('runtime.registeredServerSide', { count: streamTotal })
              : ''}
          </span>
        </div>
        <div className="toolbar">
          <input
            aria-label={t('runtime.filterLabel')}
            placeholder={t('runtime.filterPlaceholder')}
            value={filter}
            onChange={(event) => {
              setFilter(event.target.value)
              setPage(1)
            }}
          />
          <select
            aria-label={t('runtime.stateLabel')}
            value={state}
            onChange={(event) => {
              setState(event.target.value)
              setPage(1)
            }}
          >
            <option value="all">{t('runtime.allStates')}</option>
            {['running', 'starting', 'stopped', 'failed', 'restarting'].map((value) => (
              <option key={value}>{value}</option>
            ))}
          </select>
        </div>
        {visible.length === 0 ? (
          <p className="empty">{t('runtime.noMatches')}</p>
        ) : (
          <div className="table">
            {visible.map((stream) => (
              <div
                className={`row ${selected === stream.id ? 'selected' : ''}`}
                key={`${stream.node_id ?? 'local'}:${stream.id}`}
              >
                <button className="link-button" onClick={() => setSelected(stream.id)}>
                  <strong>{stream.id}</strong>
                  <small>
                    {stream.node_id ?? t('common.localNode')} ·{' '}
                    {t('runtime.desired', { state: stream.desired_state ?? t('common.unknown') })} ·{' '}
                    {t('common.generation', { value: stream.desired_generation ?? '—' })}
                  </small>
                </button>
                <div>
                  <span className={`state ${stream.state}`}>{stream.state}</span>
                  <small>
                    {t('runtime.convergence', { value: stream.convergence ?? t('common.unknown') })} ·{' '}
                    {t('runtime.observedGeneration', { value: stream.observed_generation ?? '—' })}
                  </small>
                  <small>
                    {t('runtime.inputMessages', { count: number(stream.metrics.input_messages) })} ·{' '}
                    {t('runtime.outputMessages', { count: number(stream.metrics.output_messages) })}
                  </small>
                </div>
                <div className="actions">
                  {(['start', 'stop', 'restart'] as const).map((action) => (
                    <button
                      disabled={!canMutate}
                      key={action}
                      onClick={() => {
                        if (
                          window.confirm(
                            t('runtime.confirmAction', { action: t(`common.${action}`), id: stream.id }),
                          )
                        )
                          void command(stream.id, action)
                      }}
                    >
                      {t(`common.${action}`)}
                    </button>
                  ))}
                </div>
              </div>
            ))}
          </div>
        )}
        <Pagination page={page} pages={pages} onChange={setPage} />
      </section>
      {detail && (
        <RuntimeDetail
          stream={detail}
          onError={onError}
          operations={operations.filter((operation) => operation.resource_id === detail.id)}
          events={(events ?? []).filter((event) => event.stream_id === detail.id)}
          onClose={() => setSelected(undefined)}
        />
      )}
      <section className="panel">
        <div className="panel-title">
          <h3>{t('runtime.adminOperations')}</h3>
          <span>
            {t('runtime.activeCount', {
              count: operations.filter((operation) => active(operation.state)).length,
            })}
            {operationTotal !== undefined && operationTotal > operations.length
              ? t('runtime.recordedServerSide', { count: operationTotal })
              : ''}
          </span>
        </div>
        {operations.length ? (
          operations
            .slice(0, 12)
            .map((operation) => <OperationRow operation={operation} onError={onError} key={operation.id} />)
        ) : (
          <p className="empty">{t('runtime.noOperations')}</p>
        )}
      </section>
    </>
  )
}

function RuntimeDetail({
  stream,
  operations,
  events,
  onClose,
  onError,
}: {
  stream: StreamStatus
  operations: Operation[]
  events: ControlEvent[]
  onClose: () => void
  onError?: (message: string) => void
}) {
  const t = useT()
  return (
    <section className="panel detail">
      <div className="panel-title">
        <div>
          <span className="eyebrow">{t('runtime.detailEyebrow')}</span>
          <h3>{stream.id}</h3>
        </div>
        <button onClick={onClose}>{t('common.close')}</button>
      </div>
      <div className="detail-grid">
        <div>
          <span className={`state ${stream.state}`}>{stream.state}</span>
          <p>
            {t('runtime.node')}: <strong>{stream.node_id ?? t('common.localNode')}</strong>
          </p>
          <p>
            {t('runtime.desiredLabel')}: <strong>{stream.desired_state ?? t('common.unknown')}</strong> ·{' '}
            {t('common.generation', { value: stream.desired_generation ?? '—' })}
          </p>
          <p>
            {t('runtime.observedLabel')}: <strong>{stream.state}</strong> ·{' '}
            {t('common.generation', { value: stream.observed_generation ?? '—' })}
          </p>
          <p>
            {t('runtime.convergenceLabel')}: <strong>{stream.convergence ?? t('common.unknown')}</strong>
          </p>
          <p>
            {t('runtime.config')}: {stream.desired_config_version ?? t('common.none')} →{' '}
            {stream.observed_config_version ?? t('common.unknown')}
          </p>
          <p>
            {t('runtime.retry')}: {stream.retry_count ?? 0}
            {stream.next_retry_at_ms
              ? t('runtime.nextRetry', { time: formatTime(stream.next_retry_at_ms) })
              : ''}
          </p>
          <p>
            {t('runtime.activeOperation')}: {stream.active_operation_id ?? t('common.none')}
          </p>
        </div>
        <div className="metric-list">
          <div className="metric">
            <span>{t('runtime.metricInput')}</span>
            <strong>{number(stream.metrics.input_messages)}</strong>
          </div>
          <div className="metric">
            <span>{t('runtime.metricOutput')}</span>
            <strong>{number(stream.metrics.output_messages)}</strong>
          </div>
          <div className="metric">
            <span>{t('runtime.metricErrors')}</span>
            <strong>{number(stream.metrics.processing_errors)}</strong>
          </div>
        </div>
      </div>
      {stream.last_error && (
        <div className="error-row">
          <strong>{stream.last_error.stage}</strong> {stream.last_error.message} ·{' '}
          {formatTime(stream.last_error.occurred_at_ms)}
        </div>
      )}
      <h4>{t('runtime.operationHistory')}</h4>
      {operations.length ? (
        operations.map((operation) => (
          <OperationRow operation={operation} onError={onError} key={operation.id} />
        ))
      ) : (
        <p className="empty">{t('runtime.noOperationHistory')}</p>
      )}
      <h4>{t('runtime.relatedEvents')}</h4>
      {events.length ? (
        events.map((event, index) => <EventRow event={event} key={`${event.occurred_at_ms}-${index}`} />)
      ) : (
        <p className="empty">{t('runtime.noRelatedEvents')}</p>
      )}
    </section>
  )
}
