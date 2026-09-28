import { useT } from '../i18n'
import { useStatus } from '../queries'
import { SNAPSHOT_INTERVAL_MS } from '../api'

export function Settings() {
  const t = useT()
  const status = useStatus().data ?? null
  return (
    <section className="panel">
      <div className="panel-title">
        <h3>{t('settings.title')}</h3>
        <span>{t('settings.subtitle')}</span>
      </div>
      <div className="settings-grid">
        <p>
          <small>{t('settings.apiVersion')}</small>
          <strong>v1</strong>
        </p>
        <p>
          <small>{t('settings.backend')}</small>
          <strong>{status?.version ?? t('common.loading')}</strong>
        </p>
        <p>
          <small>{t('settings.snapshotPolling')}</small>
          <strong>{t('settings.pollingValue', { seconds: SNAPSHOT_INTERVAL_MS / 1000 })}</strong>
        </p>
      </div>
      <p>{t('settings.credentialsNote')}</p>
    </section>
  )
}
