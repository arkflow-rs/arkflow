import { useState } from 'react'
import { useSearchParams } from 'react-router'
import { useT } from '../i18n'
import { useEvents } from '../queries'
import { EventRow, SkeletonRows } from './shared'

export function Events() {
  const [filter, setFilter] = useState('')
  const t = useT()
  const [searchParams] = useSearchParams()
  const nodeId = searchParams.get('node_id') ?? undefined
  const eventsQuery = useEvents(nodeId)
  const events = eventsQuery.isPlaceholderData ? [] : (eventsQuery.data?.items ?? [])
  const total = eventsQuery.data?.total
  const clearFilters = () => setFilter('')
  const visible = events.filter(
    (event) =>
      !filter ||
      `${event.event_type} ${event.stream_id ?? ''} ${event.outcome} ${event.message ?? ''}`
        .toLowerCase()
        .includes(filter.toLowerCase()),
  )
  return (
    <section className="panel">
      <div className="panel-title">
        <h3>{t('events.title')}</h3>
        <div className="actions">
          <input
            aria-label={t('events.filterLabel')}
            placeholder={t('events.filterPlaceholder')}
            value={filter}
            onChange={(event) => setFilter(event.target.value)}
          />
          <span>
            {t('events.matching', { count: visible.length })}
            {total !== undefined && total > events.length
              ? t('common.storedServerSide', { count: total })
              : ''}
          </span>
        </div>
      </div>
      {eventsQuery.isPending ? (
        <SkeletonRows rows={5} />
      ) : visible.length ? (
        visible.map((event, i) => <EventRow event={event} key={i} />)
      ) : (
        <p className="empty">
          {t('events.noMatches')}
          {filter && <button onClick={clearFilters}>{t('common.clearFilters')}</button>}
        </p>
      )}
    </section>
  )
}
