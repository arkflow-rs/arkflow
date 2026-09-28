import { useState } from 'react'
import type { ControlEvent } from '../api'
import { useT } from '../i18n'
import { EventRow } from './shared'

export function Events({ events, total }: { events: ControlEvent[]; total?: number }) {
  const [filter, setFilter] = useState('')
  const t = useT()
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
      {visible.length ? (
        visible.map((event, i) => <EventRow event={event} key={i} />)
      ) : (
        <p className="empty">{t('events.noMatches')}</p>
      )}
    </section>
  )
}
