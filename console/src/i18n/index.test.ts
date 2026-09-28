import { afterEach, describe, expect, it } from 'vitest'
import { dictionaries, intlLocale, resolveLocale, translate } from './index'

describe('resolveLocale', () => {
  it('prefers a stored valid value over the browser language', () => {
    expect(resolveLocale('en', 'zh-CN')).toBe('en')
    expect(resolveLocale('zh', 'en-US')).toBe('zh')
  })

  it('detects zh-prefixed browser languages', () => {
    expect(resolveLocale(null, 'zh-CN')).toBe('zh')
    expect(resolveLocale(null, 'zh-TW')).toBe('zh')
  })

  it('falls back to en for non-zh browser languages', () => {
    expect(resolveLocale(null, 'en-US')).toBe('en')
    expect(resolveLocale(null, 'fr')).toBe('en')
  })

  it('treats invalid stored values as unset', () => {
    expect(resolveLocale('fr', 'zh-CN')).toBe('zh')
    expect(resolveLocale('garbage', 'en-US')).toBe('en')
    expect(resolveLocale(null, undefined)).toBe('en')
  })
})

describe('translate', () => {
  it('renders en text for en locale', () => {
    expect(translate('en', 'header.refresh')).toBe('Refresh')
  })

  it('renders zh text for zh locale', () => {
    expect(translate('zh', 'header.refresh')).toBe('刷新')
  })

  it('interpolates {name} placeholders', () => {
    expect(translate('en', 'warning.nodeUnavailable', { node: 'node-a', state: 'offline' })).toBe(
      'Node node-a is offline; mutating actions are disabled.',
    )
    expect(translate('zh', 'warning.nodeUnavailable', { node: 'node-a', state: 'offline' })).toBe(
      '节点 node-a 当前为 offline；变更操作已禁用。',
    )
  })

  it('falls back to en when the locale dictionary lacks a key', () => {
    const key = 'header.refresh'
    const removed = dictionaries.zh[key]
    delete dictionaries.zh[key]
    expect(translate('zh', key)).toBe('Refresh')
    dictionaries.zh[key] = removed
  })
})

describe('intlLocale', () => {
  it('maps locales to BCP 47 tags', () => {
    expect(intlLocale('zh')).toBe('zh-Hans')
    expect(intlLocale('en')).toBe('en-US')
  })
})

afterEach(() => {
  expect(dictionaries.zh['header.refresh']).toBe('刷新')
})
