import { useEffect, useMemo, useState } from 'react'
import { api, errorMessage } from '../api'
import type { Component } from '../api'
import { useT } from '../i18n'
import { ComponentBrowserControls, ComponentKind, filterComponents } from './component-browser'

export function Components({ onError }: { onError: (message: string) => void }) {
  const [items, setItems] = useState<Component[]>([])
  const [kind, setKind] = useState<ComponentKind>('input')
  const [query, setQuery] = useState('')
  const [selected, setSelected] = useState<string>()
  const t = useT()
  useEffect(() => {
    api
      .components()
      .then(setItems)
      .catch((cause) => onError(errorMessage(cause)))
  }, [onError])
  const visible = useMemo(() => filterComponents(items, kind, query), [items, kind, query])
  useEffect(() => {
    if (!visible.some((item) => `${item.kind}:${item.name}` === selected))
      setSelected(visible[0] && `${visible[0].kind}:${visible[0].name}`)
  }, [visible, selected])
  const current = visible.find((item) => `${item.kind}:${item.name}` === selected)
  return (
    <section className="panel component-catalogue">
      <div className="panel-title">
        <div>
          <h3>{t('components.title')}</h3>
          <span>{t('common.registered', { count: items.length })}</span>
        </div>
      </div>
      <ComponentBrowserControls
        kind={kind}
        query={query}
        onKindChange={setKind}
        onQueryChange={setQuery}
        count={visible.length}
      />
      <div className="component-browser">
        <div className="component-list">
          {visible.length ? (
            visible.map((item) => (
              <button
                type="button"
                className={current === item ? 'selected' : ''}
                key={`${item.kind}-${item.name}`}
                onClick={() => setSelected(`${item.kind}:${item.name}`)}
              >
                <strong>{item.name}</strong>
                <small>{item.description ?? t('components.noDescription')}</small>
              </button>
            ))
          ) : (
            <p className="empty">{t('components.noMatches')}</p>
          )}
        </div>
        <div className="component-detail">
          {current ? (
            <>
              <span className="eyebrow">{current.kind}</span>
              <h4>{current.name}</h4>
              <p>{current.description ?? t('components.noDescription')}</p>
              {current.example !== undefined && (
                <details open>
                  <summary>{t('components.example')}</summary>
                  <pre className="schema">{JSON.stringify(current.example, null, 2)}</pre>
                </details>
              )}
              <details>
                <summary>{t('components.schema')}</summary>
                <pre className="schema">{JSON.stringify(current.schema ?? {}, null, 2)}</pre>
              </details>
            </>
          ) : (
            <p className="empty">{t('components.selectPrompt')}</p>
          )}
        </div>
      </div>
    </section>
  )
}
