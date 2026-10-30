import { useSearchParams } from 'react-router'
import { useT } from '../i18n'
import { useEvents } from '../queries'
import { EventRow, PAGE_SIZES, Pagination, SkeletonRows, pageSizeParam } from './shared'

export function Events() {
  const t = useT()
  // Filter and pagination state lives in the URL: the backend params are the
  // only filtering vocabulary, so a shareable link reproduces the exact view.
  const [searchParams, setSearchParams] = useSearchParams()
  const nodeId = searchParams.get('node_id') ?? undefined
  const eventType = searchParams.get('event_type') ?? undefined
  const streamId = searchParams.get('stream_id') ?? undefined
  const correlationId = searchParams.get('correlation_id') ?? undefined
  const page = Math.max(1, Number(searchParams.get('page') ?? '1') || 1)
  const pageSize = pageSizeParam(searchParams)

  const eventsQuery = useEvents({
    node_id: nodeId,
    event_type: eventType,
    stream_id: streamId,
    correlation_id: correlationId,
    page,
    page_size: pageSize,
  })
  const events = eventsQuery.data?.items ?? []
  const total = eventsQuery.data?.total
  const pages = Math.max(1, Math.ceil((total ?? 0) / pageSize))
  const hasFilter = Boolean(eventType || streamId || correlationId)

  const setParam = (key: string, value: string) => {
    const next = new URLSearchParams(searchParams)
    if (value) next.set(key, value)
    else next.delete(key)
    if (key !== 'page') next.delete('page') // any filter change restarts at page 1
    // Page changes stack in history so Back returns to the previous page;
    // filter typing replaces instead of flooding the history.
    setSearchParams(next, { replace: key !== 'page' })
  }

  return (
    <section className="panel">
      <div className="panel-title">
        <h3>{t('events.title')}</h3>
        <div className="actions">
          <input
            aria-label={t('events.filterEventType')}
            placeholder={t('events.filterEventType')}
            value={eventType ?? ''}
            onChange={(event) => setParam('event_type', event.target.value.trim())}
          />
          <input
            aria-label={t('events.filterStreamId')}
            placeholder={t('events.filterStreamId')}
            value={streamId ?? ''}
            onChange={(event) => setParam('stream_id', event.target.value.trim())}
          />
          <input
            aria-label={t('events.filterCorrelationId')}
            placeholder={t('events.filterCorrelationId')}
            value={correlationId ?? ''}
            onChange={(event) => setParam('correlation_id', event.target.value.trim())}
          />
          <select
            aria-label={t('list.pageSize')}
            value={pageSize}
            onChange={(event) => setParam('page_size', event.target.value)}
          >
            {PAGE_SIZES.map((value) => (
              <option key={value} value={value}>
                {t('list.pageSize')}: {value}
              </option>
            ))}
          </select>
          <span>{total !== undefined ? t('list.total', { count: total }) : ''}</span>
        </div>
      </div>
      {/* Page transitions keep the previous payload (keepPreviousData):
          render skeleton rows instead of flashing the empty state. */}
      {eventsQuery.isPending || eventsQuery.isPlaceholderData ? (
        <SkeletonRows rows={5} />
      ) : events.length ? (
        events.map((event, index) => (
          <EventRow event={event} key={`${event.occurred_at_ms}-${event.event_type}-${index}`} />
        ))
      ) : (
        <p className="empty">
          {t('events.noMatches')}
          {hasFilter && (
            <button
              onClick={() => {
                // Clear the event filters only: node_id is header state that
                // also scopes this page, and page_size is a view preference.
                const next = new URLSearchParams(searchParams)
                next.delete('event_type')
                next.delete('stream_id')
                next.delete('correlation_id')
                next.delete('page')
                setSearchParams(next)
              }}
            >
              {t('common.clearFilters')}
            </button>
          )}
        </p>
      )}
      <Pagination page={page} pages={pages} onChange={(next) => setParam('page', String(next))} />
    </section>
  )
}
