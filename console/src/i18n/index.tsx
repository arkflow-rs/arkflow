import { createContext, useContext, useState, type ReactNode } from 'react'
import { en } from './en'
import { zh } from './zh'

export type Locale = 'zh' | 'en'
export type TKey = keyof typeof en
export type TParams = Record<string, string | number>

export const LOCALE_STORAGE_KEY = 'arkflow.console.locale'

export const dictionaries: Record<Locale, Partial<Record<TKey, string>>> = { en, zh }

export function resolveLocale(storage: string | null, language: string | undefined): Locale {
  if (storage === 'zh' || storage === 'en') return storage
  if (language?.toLowerCase().startsWith('zh')) return 'zh'
  return 'en'
}

export function intlLocale(locale: Locale): string {
  return locale === 'zh' ? 'zh-Hans' : 'en-US'
}

export function readStoredLocale(): string | null {
  try {
    return window.localStorage.getItem(LOCALE_STORAGE_KEY)
  } catch {
    return null
  }
}

export function storeLocale(locale: Locale) {
  try {
    window.localStorage.setItem(LOCALE_STORAGE_KEY, locale)
  } catch {
    // storage unavailable — locale stays session-only
  }
}

export function initialLocale(): Locale {
  return resolveLocale(readStoredLocale(), typeof navigator === 'undefined' ? undefined : navigator.language)
}

export function translate(locale: Locale, key: TKey, params?: TParams): string {
  const template = dictionaries[locale][key] ?? en[key]
  if (!params) return template
  return template.replace(/\{(\w+)\}/g, (match, name: string) =>
    name in params ? String(params[name]) : match,
  )
}

type LocaleContextValue = { locale: Locale; setLocale: (next: Locale) => void }

// Module-level mirror of the provider state so non-React modules (api.ts)
// can translate with the active locale.
let activeLocale: Locale = initialLocale()

export function currentLocale(): Locale {
  return activeLocale
}

const LocaleContext = createContext<LocaleContextValue>({
  locale: initialLocale(),
  setLocale: () => {},
})

export function LocaleProvider({ children }: { children: ReactNode }) {
  const [locale, setLocale] = useState<Locale>(() => {
    const resolved = initialLocale()
    activeLocale = resolved
    return resolved
  })
  const update = (next: Locale) => {
    activeLocale = next
    setLocale(next)
    storeLocale(next)
  }
  return <LocaleContext.Provider value={{ locale, setLocale: update }}>{children}</LocaleContext.Provider>
}

export function useLocale(): Locale {
  return useContext(LocaleContext).locale
}

export function useSetLocale(): (next: Locale) => void {
  return useContext(LocaleContext).setLocale
}

export function useT() {
  const locale = useLocale()
  return (key: TKey, params?: TParams) => translate(locale, key, params)
}
