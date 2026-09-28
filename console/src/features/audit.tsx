import { useEffect, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { api, AuditRecord, errorMessage, formatTime } from '../api'
import { useT } from '../i18n'

export function Audit({ onError }: { onError: (message: string) => void }) {
  const t = useT()
  const [filter, setFilter] = useState('')
  const auditQuery = useQuery({ queryKey: ['audit'], queryFn: () => api.audit(), staleTime: Infinity })
  const records = auditQuery.data?.items
  const total = auditQuery.data?.total
  useEffect(() => {
    if (auditQuery.isError) onError(errorMessage(auditQuery.error))
  }, [auditQuery.isError, auditQuery.error, onError])
  const visible = (records ?? []).filter(
    (record) =>
      !filter ||
      `${record.action} ${record.actor ?? ''} ${record.resource_type} ${record.resource_id ?? ''} ${
        record.node_id ?? ''
      } ${record.outcome} ${record.message ?? ''}`
        .toLowerCase()
        .includes(filter.toLowerCase()),
  )
  return (
    <section className="panel">
      <div className="panel-title">
        <h3>{t('audit.title')}</h3>
        <div className="actions">
          <input
            aria-label={t('audit.filterAriaLabel')}
            placeholder={t('audit.filterPlaceholder')}
            value={filter}
            onChange={(event) => setFilter(event.target.value)}
          />
          <span>
            {records === undefined
              ? t('audit.loading')
              : total !== undefined && total > records.length
                ? t('audit.matchingStored', { count: visible.length, total })
                : t('audit.matching', { count: visible.length })}
          </span>
        </div>
      </div>
      {visible.length ? (
        visible.map((record) => <AuditRow key={record.event_id} record={record} />)
      ) : (
        <p className="empty">{records === undefined ? t('audit.loadingRecords') : t('audit.noMatching')}</p>
      )}
    </section>
  )
}

function AuditRow({ record }: { record: AuditRecord }) {
  const t = useT()
  return (
    <div className="event-row">
      <strong>{record.action}</strong>
      <span>
        {record.outcome}
        {record.failure_code ? ` · ${record.failure_code}` : ''} · {record.actor ?? t('audit.unknownActor')}
      </span>
      <small>
        {formatTime(record.occurred_at_ms)}
        {record.resource_type ? ` · ${record.resource_type}` : ''}
        {record.resource_id ? ` ${record.resource_id}` : ''}
        {record.node_id ? ` · ${record.node_id}` : ''}
        {record.correlation_id ? ` · ${record.correlation_id}` : ''}
      </small>
      {record.message && <p>{record.message}</p>}
    </div>
  )
}
