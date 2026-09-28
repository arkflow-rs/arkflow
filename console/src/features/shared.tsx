import { useQueryClient } from '@tanstack/react-query'
import { api, formatTime } from '../api'
import type { ControlEvent, Operation } from '../api'
import { currentLocale, intlLocale, useT } from '../i18n'

export const number = (value: number | undefined) =>
  value === undefined ? '—' : new Intl.NumberFormat(intlLocale(currentLocale())).format(value)
export const active = (state: Operation['state']) =>
  ['queued', 'dispatched', 'acknowledged', 'running'].includes(state)

export function OperationRow({ operation }: { operation: Operation }) {
  const t = useT()
  const queryClient = useQueryClient()
  return (
    <div className="operation-row">
      <div>
        <strong>{operation.operation}</strong>
        <small>
          {operation.node_id ?? t('common.localNode')} · {operation.resource_id} ·{' '}
          {t('common.generation', { value: operation.generation ?? '—' })} · {operation.id}
          {operation.correlation_id ? ` · ${operation.correlation_id}` : ''}
        </small>
      </div>
      <div className="progress-wrap">
        <span className={`state ${operation.convergence_state ?? operation.state}`}>
          {operation.intent_state ?? operation.state}
        </span>
        <progress max="100" value={operation.progress} />
        <small>
          {operation.progress}%
          {operation.retry_count ? t('common.retryCount', { count: operation.retry_count }) : ''}
        </small>
      </div>
      <div>
        {active(operation.state) && (
          <button
            onClick={() =>
              void api
                .cancel(operation.id)
                .then(() => queryClient.invalidateQueries({ queryKey: ['live', 'operations'] }))
            }
          >
            {t('common.cancel')}
          </button>
        )}
        {operation.failure_class && <small className="error-text">{operation.failure_class}</small>}
        {operation.error && <small className="error-text">{operation.error}</small>}
      </div>
    </div>
  )
}
export function EventRow({ event }: { event: ControlEvent }) {
  const t = useT()
  return (
    <div className="event-row">
      <strong>{event.event_type}</strong>
      <span>
        {event.stream_id ?? t('common.system')} · {event.outcome}
      </span>
      <small>
        {formatTime(event.occurred_at_ms)}
        {event.operation_id ? ` · ${event.operation_id}` : ''}
      </small>
      {event.message && <p>{event.message}</p>}
    </div>
  )
}
export function Pagination({
  page,
  pages,
  onChange,
}: {
  page: number
  pages: number
  onChange: (page: number) => void
}) {
  const t = useT()
  return pages <= 1 ? null : (
    <div className="pagination">
      <button disabled={page === 1} onClick={() => onChange(page - 1)}>
        {t('common.previous')}
      </button>
      <span>{t('common.pageOf', { page, pages })}</span>
      <button disabled={page === pages} onClick={() => onChange(page + 1)}>
        {t('common.next')}
      </button>
    </div>
  )
}
export function Card({ label, value, hint }: { label: string; value: string | number; hint?: string }) {
  return (
    <div className="card">
      <span>{label}</span>
      <strong>{value}</strong>
      {hint && <small>{hint}</small>}
    </div>
  )
}
