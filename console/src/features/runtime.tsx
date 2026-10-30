import { useEffect, useMemo, useState } from 'react'
import { useSearchParams } from 'react-router'
import { useConfirm } from './confirm'
import { formatTime } from '../api'
import type { ControlEvent, Operation, StreamStatus } from '../api'
import { useT } from '../i18n'
import { useEvents, useOperations, useStreams } from '../queries'
import {
  EventRow,
  OperationRow,
  PAGE_SIZES,
  Pagination,
  SkeletonRows,
  active,
  number,
  pageSizeParam,
} from './shared'
import type { Command } from './types'

const OPERATION_STATES = ['queued', 'running', 'succeeded', 'failed', 'cancelled', 'timed_out'] as const

export function Runtime({
  command,
  canMutate = true,
  canOperate = true,
  onError,
}: {
  command: Command
  canMutate?: boolean
  canOperate?: boolean
  onError?: (message: string) => void
}) {
  const t = useT()
  const confirm = useConfirm()
  // Operation history is server-paginated: its filters and page live in the
  // URL so a link reproduces the exact query, matching the events/audit lists.
  const [searchParams, setSearchParams] = useSearchParams()
  const nodeId = searchParams.get('node_id') ?? undefined
  const operationName = searchParams.get('operation') ?? undefined
  const operationState = searchParams.get('state') ?? undefined
  const correlationId = searchParams.get('correlation_id') ?? undefined
  const operationPage = Math.max(1, Number(searchParams.get('page') ?? '1') || 1)
  const operationPageSize = pageSizeParam(searchParams)
  const streamsQuery = useStreams(nodeId)
  const operationsQuery = useOperations({
    node_id: nodeId,
    operation: operationName,
    state: operationState,
    correlation_id: correlationId,
    page: operationPage,
    page_size: operationPageSize,
  })
  const eventsQuery = useEvents({ node_id: nodeId })
  // While a node-scoped refetch is pending, placeholder rows still belong to
  // the previously selected node — hide them so commands can never be sent
  // against the wrong node.
  const staleScope = streamsQuery.isPlaceholderData || operationsQuery.isPlaceholderData
  const streams = staleScope ? [] : (streamsQuery.data?.items ?? [])
  const operations = staleScope ? [] : (operationsQuery.data?.items ?? [])
  const events = eventsQuery.data?.items ?? []
  const streamTotal = streamsQuery.data?.total
  const operationTotal = operationsQuery.data?.total
  const operationPages = Math.max(1, Math.ceil((operationTotal ?? 0) / operationPageSize))
  const setOperationParam = (key: string, value: string) => {
    const next = new URLSearchParams(searchParams)
    if (value) next.set(key, value)
    else next.delete(key)
    if (key !== 'page') next.delete('page') // any filter change restarts at page 1
    setSearchParams(next, { replace: true })
  }
  const [filter, setFilter] = useState('')
  const [state, setState] = useState('all')
  const [streamPage, setStreamPage] = useState(1)
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
  const visible = filtered.slice((streamPage - 1) * pageSize, streamPage * pageSize)
  const detail = streams.find((stream) => stream.id === selected)
  useEffect(() => {
    // Clamp the page when the filtered result shrinks: adjusting pagination
    // state on an external list change, not a render-derivable value.
    // eslint-disable-next-line react-hooks/set-state-in-effect
    if (streamPage > pages) setStreamPage(pages)
  }, [streamPage, pages])
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
              setStreamPage(1)
            }}
          />
          <select
            aria-label={t('runtime.stateLabel')}
            value={state}
            onChange={(event) => {
              setState(event.target.value)
              setStreamPage(1)
            }}
          >
            <option value="all">{t('runtime.allStates')}</option>
            {['running', 'starting', 'stopped', 'failed', 'restarting'].map((value) => (
              <option key={value}>{value}</option>
            ))}
          </select>
        </div>
        {streamsQuery.isPending || staleScope ? (
          <SkeletonRows rows={4} />
        ) : visible.length === 0 ? (
          <p className="empty">
            {t('runtime.noMatches')}
            {(filter || state !== 'all') && (
              <button
                onClick={() => {
                  setFilter('')
                  setState('all')
                }}
              >
                {t('common.clearFilters')}
              </button>
            )}
          </p>
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
                        void confirm({
                          title: t('runtime.confirmAction', { action: t(`common.${action}`), id: stream.id }),
                          confirmLabel: t(`common.${action}`),
                        }).then((accepted) => {
                          if (accepted) void command(stream.id, action)
                        })
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
        <Pagination page={streamPage} pages={pages} onChange={setStreamPage} />
      </section>
      {detail && (
        <RuntimeDetail
          stream={detail}
          onError={onError}
          canOperate={canOperate}
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
        <div className="toolbar">
          <input
            aria-label={t('runtime.operationFilter')}
            placeholder={t('runtime.operationFilter')}
            value={operationName ?? ''}
            onChange={(event) => setOperationParam('operation', event.target.value.trim())}
          />
          <select
            aria-label={t('runtime.operationState')}
            value={operationState ?? 'all'}
            onChange={(event) =>
              setOperationParam('state', event.target.value === 'all' ? '' : event.target.value)
            }
          >
            <option value="all">{t('runtime.allStates')}</option>
            {OPERATION_STATES.map((value) => (
              <option key={value}>{value}</option>
            ))}
          </select>
          <input
            aria-label={t('runtime.correlationFilter')}
            placeholder={t('runtime.correlationFilter')}
            value={correlationId ?? ''}
            onChange={(event) => setOperationParam('correlation_id', event.target.value.trim())}
          />
          <select
            aria-label={t('list.pageSize')}
            value={operationPageSize}
            onChange={(event) => setOperationParam('page_size', event.target.value)}
          >
            {PAGE_SIZES.map((value) => (
              <option key={value} value={value}>
                {t('list.pageSize')}: {value}
              </option>
            ))}
          </select>
        </div>
        {operations.length ? (
          operations.map((operation) => (
            <OperationRow operation={operation} onError={onError} canMutate={canOperate} key={operation.id} />
          ))
        ) : (
          <p className="empty">{t('runtime.noOperations')}</p>
        )}
        <Pagination
          page={operationPage}
          pages={operationPages}
          onChange={(next) => setOperationParam('page', String(next))}
        />
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
  canOperate,
}: {
  stream: StreamStatus
  operations: Operation[]
  events: ControlEvent[]
  onClose: () => void
  onError?: (message: string) => void
  canOperate: boolean
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
          <OperationRow operation={operation} onError={onError} canMutate={canOperate} key={operation.id} />
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
