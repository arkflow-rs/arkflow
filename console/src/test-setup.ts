import '@testing-library/jest-dom/vitest'
import { LOCALE_STORAGE_KEY } from './i18n'

// Pin console tests to the en locale so text assertions stay independent of the
// environment's browser language or any persisted locale choice.
window.localStorage.removeItem(LOCALE_STORAGE_KEY)
Object.defineProperty(window.navigator, 'language', { value: 'en-US', configurable: true })

class ResizeObserverMock {
  observe() {}
  unobserve() {}
  disconnect() {}
}

Object.defineProperty(globalThis, 'ResizeObserver', { writable: true, value: ResizeObserverMock })

Object.defineProperty(window, 'matchMedia', {
  writable: true,
  value: (query: string) => ({
    matches: false,
    media: query,
    addEventListener() {},
    removeEventListener() {},
    addListener() {},
    removeListener() {},
  }),
})
