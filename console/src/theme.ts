import { useEffect, useState } from 'react'

export const THEME_STORAGE_KEY = 'arkflow.console.theme'

export type ThemeSetting = 'dark' | 'light' | 'system'

export function readStoredTheme(): ThemeSetting {
  try {
    const value = window.localStorage.getItem(THEME_STORAGE_KEY)
    return value === 'dark' || value === 'light' || value === 'system' ? value : 'system'
  } catch {
    return 'system'
  }
}

export function storeTheme(setting: ThemeSetting) {
  try {
    window.localStorage.setItem(THEME_STORAGE_KEY, setting)
  } catch {
    // storage unavailable — theme stays session-only
  }
}

export function systemPrefersDark(): boolean {
  // jsdom (and very old browsers) lack matchMedia; default to dark like the
  // theme fallback in index.html.
  return typeof window.matchMedia === 'function'
    ? window.matchMedia('(prefers-color-scheme: dark)').matches
    : true
}

export function resolvedTheme(setting: ThemeSetting): 'dark' | 'light' {
  return setting === 'system' ? (systemPrefersDark() ? 'dark' : 'light') : setting
}

export function useTheme() {
  const [setting, setSetting] = useState<ThemeSetting>(readStoredTheme)
  useEffect(() => {
    const apply = () => {
      document.documentElement.dataset.theme = resolvedTheme(setting)
    }
    apply()
    storeTheme(setting)
    if (setting !== 'system' || typeof window.matchMedia !== 'function') return
    const media = window.matchMedia('(prefers-color-scheme: dark)')
    media.addEventListener('change', apply)
    return () => media.removeEventListener('change', apply)
  }, [setting])
  return { setting, setSetting }
}
