import { cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { BrowserRouter } from 'react-router'
import type { ReactElement } from 'react'
import { Users } from './features/users'
import { ConfirmProvider } from './features/confirm'
import { App } from './app'
import type { CredentialView, User } from './api'

const LOCALE_STORAGE_KEY = 'arkflow.console.locale'

const json = (body: unknown, status = 200) =>
  Promise.resolve({
    ok: status < 400,
    status,
    headers: new Headers(),
    json: async () => body,
  })

const page = (items: unknown[]) => ({ items, page: 1, page_size: items.length || 50, total: items.length })

const renderWithQueries = (ui: ReactElement) =>
  render(
    <QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}>
      <ConfirmProvider>
        <BrowserRouter>{ui}</BrowserRouter>
      </ConfirmProvider>
    </QueryClientProvider>,
  )

const baseUser = (overrides: Partial<User> = {}): User => ({
  user_id: 'alice',
  display_name: 'Alice',
  role: 'admin',
  scopes: [{ resource_type: 'node', resource_id: 'node-a' }],
  disabled: false,
  oidc_subject: null,
  tokens_valid_after_ms: 0,
  created_at_ms: 1,
  updated_at_ms: 2,
  ...overrides,
})

const liveCredential = (overrides: Partial<CredentialView> = {}): CredentialView => ({
  credential_id: 'cred-1',
  label: 'ci',
  created_at_ms: 1,
  expires_at_ms: null,
  last_used_at_ms: null,
  revoked_at_ms: null,
  ...overrides,
})

beforeEach(() => {
  window.localStorage.removeItem(LOCALE_STORAGE_KEY)
})

afterEach(() => {
  cleanup()
  vi.unstubAllGlobals()
  window.localStorage.removeItem(LOCALE_STORAGE_KEY)
  window.history.replaceState({}, '', '/')
})

describe('user directory panel', () => {
  it('only renders for manage_users holders', async () => {
    const fetchMock = vi.fn()
    vi.stubGlobal('fetch', fetchMock)
    renderWithQueries(<Users onError={() => {}} canManageUsers={false} />)
    expect(screen.getByTestId('users-forbidden')).toBeInTheDocument()
    expect(screen.getByText('Requires the manage_users permission')).toBeInTheDocument()
    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('lists users and edits role/scopes through a PATCH', async () => {
    const users = [baseUser()]
    const fetchMock = vi.fn((url: string, init?: RequestInit) => {
      if (init?.method === 'PATCH' && url.endsWith('/users/alice')) {
        const body = JSON.parse(String(init.body)) as Partial<User>
        return json({ ...users[0], ...body, updated_at_ms: 3 })
      }
      if (url.endsWith('/users/alice/credentials')) return json([])
      if (url.endsWith('/users')) return json(users)
      throw new Error(`unexpected fetch: ${url}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    renderWithQueries(<Users onError={() => {}} />)
    expect(await screen.findByText('alice')).toBeInTheDocument()
    expect(screen.getByText('node:node-a')).toBeInTheDocument()

    fireEvent.click(screen.getByRole('button', { name: 'Edit' }))
    const role = await screen.findByLabelText('Role')
    fireEvent.change(role, { target: { value: 'viewer' } })
    fireEvent.click(screen.getByRole('button', { name: 'Add scope' }))
    const scopeTypes = screen.getAllByLabelText('Resource type')
    fireEvent.change(scopeTypes[scopeTypes.length - 1], { target: { value: 'job' } })
    fireEvent.click(screen.getByRole('button', { name: 'Save' }))

    await waitFor(() =>
      expect(fetchMock).toHaveBeenCalledWith(
        expect.stringContaining('/users/alice'),
        expect.objectContaining({ method: 'PATCH' }),
      ),
    )
    const patch = fetchMock.mock.calls.find(
      ([url, init]) => String(url).endsWith('/users/alice') && (init as RequestInit)?.method === 'PATCH',
    )
    expect(JSON.parse(String((patch?.[1] as RequestInit).body))).toMatchObject({
      role: 'viewer',
      scopes: [{ resource_type: 'node', resource_id: 'node-a' }, { resource_type: 'job' }],
    })
    expect(await screen.findByText('Updated user alice.')).toBeInTheDocument()
  })

  it('creates a user and shows the server problem code on a conflict', async () => {
    const users: User[] = []
    const fetchMock = vi.fn((url: string, init?: RequestInit) => {
      if (init?.method === 'POST' && url.endsWith('/users')) {
        const body = JSON.parse(String(init.body)) as {
          user_id: string
          role: string
          display_name?: string
          scopes?: User['scopes']
        }
        if (body.user_id === 'taken') {
          return json({ code: 'user_limit', message: 'user directory is full' }, 409)
        }
        const created = baseUser({
          ...body,
          scopes: body.scopes ?? [],
          display_name: body.display_name ?? '',
        })
        users.push(created)
        return json(created, 201)
      }
      if (url.endsWith('/users/alice/credentials')) return json([])
      if (url.endsWith('/users')) return json(users)
      throw new Error(`unexpected fetch: ${url}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    const onError = vi.fn()
    renderWithQueries(<Users onError={onError} />)

    fireEvent.click(await screen.findByRole('button', { name: 'Create user' }))
    fireEvent.change(screen.getByLabelText(/^User ID/), { target: { value: 'taken' } })
    const form = screen.getByTestId('user-form')
    fireEvent.click(within(form).getByRole('button', { name: 'Create user' }))
    // Server problem codes are surfaced verbatim, never mapped away.
    await waitFor(() =>
      expect(onError).toHaveBeenCalledWith(expect.stringContaining('user_limit: user directory is full')),
    )

    fireEvent.change(screen.getByLabelText(/^User ID/), { target: { value: 'bob' } })
    fireEvent.change(screen.getByLabelText('Display name'), { target: { value: 'Bob' } })
    fireEvent.change(screen.getByLabelText('Role'), { target: { value: 'viewer' } })
    fireEvent.click(within(screen.getByTestId('user-form')).getByRole('button', { name: 'Create user' }))
    expect(await screen.findByText('Created user bob.')).toBeInTheDocument()
    expect(await screen.findByTestId('user-row-bob')).toBeInTheDocument()
  })

  it('deletes a user only after confirmation', async () => {
    let deleted = false
    const fetchMock = vi.fn((url: string, init?: RequestInit) => {
      if (init?.method === 'DELETE' && url.endsWith('/users/alice')) {
        deleted = true
        return Promise.resolve({ ok: true, status: 204, headers: new Headers() })
      }
      if (url.endsWith('/users/alice/credentials')) return json([])
      if (url.endsWith('/users')) return json(deleted ? [] : [baseUser()])
      throw new Error(`unexpected fetch: ${url}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    renderWithQueries(<Users onError={() => {}} />)
    await screen.findByText('alice')
    fireEvent.click(screen.getByRole('button', { name: 'Delete' }))
    const dialog = await screen.findByRole('alertdialog')
    fireEvent.click(within(dialog).getByRole('button', { name: 'Cancel' }))
    await waitFor(() =>
      expect(
        fetchMock.mock.calls.some(
          ([url, init]) => String(url).endsWith('/users/alice') && (init as RequestInit)?.method === 'DELETE',
        ),
      ).toBe(false),
    )

    fireEvent.click(screen.getByRole('button', { name: 'Delete' }))
    const confirmDialog = await screen.findByRole('alertdialog')
    fireEvent.click(within(confirmDialog).getByRole('button', { name: 'Delete' }))
    expect(await screen.findByText('Deleted user alice.')).toBeInTheDocument()
    expect(deleted).toBe(true)
  })

  it('issues a credential and shows the plaintext token exactly once', async () => {
    const fetchMock = vi.fn((url: string, init?: RequestInit) => {
      if (init?.method === 'POST' && url.endsWith('/users/alice/credentials')) {
        expect(JSON.parse(String(init.body))).toMatchObject({ label: 'ci-deploy' })
        return json(
          { credential: liveCredential({ label: 'ci-deploy' }), token: 'ark_cred-1_topsecret' },
          201,
        )
      }
      if (url.endsWith('/users/alice/credentials')) return json([liveCredential()])
      if (url.endsWith('/users')) return json([baseUser()])
      throw new Error(`unexpected fetch: ${url}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    renderWithQueries(<Users onError={() => {}} />)
    const row = await screen.findByTestId('user-row-alice')
    fireEvent.click(within(row).getByText('Credentials'))
    await screen.findByTestId('credential-row-cred-1')

    fireEvent.change(screen.getByLabelText('Label'), { target: { value: 'ci-deploy' } })
    fireEvent.click(screen.getByRole('button', { name: 'Issue credential' }))

    expect(await screen.findByTestId('issued-token')).toHaveTextContent('ark_cred-1_topsecret')
    expect(screen.getByText(/only time the plaintext token is shown/i)).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'Dismiss' }))
    expect(screen.queryByTestId('issued-token')).not.toBeInTheDocument()
  })

  it('rotates and revokes credentials through confirmation dialogs', async () => {
    const fetchMock = vi.fn((url: string, init?: RequestInit) => {
      if (init?.method === 'POST' && url.endsWith('/credentials/cred-1/rotate'))
        return json({ credential: liveCredential(), token: 'ark_cred-1_rotated' })
      if (init?.method === 'DELETE' && url.endsWith('/credentials/cred-1'))
        return Promise.resolve({ ok: true, status: 200, headers: new Headers() })
      if (url.endsWith('/users/alice/credentials')) return json([liveCredential()])
      if (url.endsWith('/users')) return json([baseUser()])
      throw new Error(`unexpected fetch: ${url}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    renderWithQueries(<Users onError={() => {}} />)
    const row = await screen.findByTestId('user-row-alice')
    fireEvent.click(within(row).getByText('Credentials'))
    const credentialRow = await screen.findByTestId('credential-row-cred-1')

    fireEvent.click(within(credentialRow).getByRole('button', { name: 'Rotate' }))
    let dialog = await screen.findByRole('alertdialog')
    fireEvent.click(within(dialog).getByRole('button', { name: 'Rotate' }))
    expect(await screen.findByTestId('issued-token')).toHaveTextContent('ark_cred-1_rotated')
    fireEvent.click(screen.getByRole('button', { name: 'Dismiss' }))

    fireEvent.click(within(credentialRow).getByRole('button', { name: 'Revoke' }))
    dialog = await screen.findByRole('alertdialog')
    fireEvent.click(within(dialog).getByRole('button', { name: 'Revoke' }))
    await waitFor(() =>
      expect(
        fetchMock.mock.calls.some(
          ([url, init]) =>
            String(url).endsWith('/credentials/cred-1') && (init as RequestInit)?.method === 'DELETE',
        ),
      ).toBe(true),
    )
  })

  it('revokes all tokens for the selected user', async () => {
    const fetchMock = vi.fn((url: string, init?: RequestInit) => {
      if (init?.method === 'POST' && url.endsWith('/users/alice/revoke-tokens'))
        return json({ revoked_sessions: 2 })
      if (url.endsWith('/users/alice/credentials')) return json([])
      if (url.endsWith('/users')) return json([baseUser()])
      throw new Error(`unexpected fetch: ${url}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    renderWithQueries(<Users onError={() => {}} />)
    const row = await screen.findByTestId('user-row-alice')
    fireEvent.click(within(row).getByText('Credentials'))
    fireEvent.click(await screen.findByRole('button', { name: 'Revoke all tokens' }))
    const dialog = await screen.findByRole('alertdialog')
    fireEvent.click(within(dialog).getByRole('button', { name: 'Revoke all tokens' }))
    expect(await screen.findByText('Revoked 2 browser session(s).')).toBeInTheDocument()
  })
})

describe('user directory navigation gating', () => {
  const systemWith = (actions: string[]) => ({
    id: 'hub',
    version: 'hub',
    state: 'running',
    node_count: 0,
    stream_count: 0,
    active_operations: 0,
    capabilities: ['fleet_aggregation'],
    principal: { id: 'root', roles: ['admin'], actions },
  })

  it('shows the Users navigation entry only for manage_users holders', async () => {
    const fetchMock = vi.fn((url: string) => {
      if (url.endsWith('/system')) return json(systemWith(['read']))
      if (url.includes('/nodes?')) return json(page([]))
      if (url.endsWith('/users')) return json([])
      return json(page([]))
    })
    vi.stubGlobal('fetch', fetchMock)
    render(<App />)
    await screen.findByText('Fleet health')
    expect(screen.queryByText('Users', { selector: 'a' })).not.toBeInTheDocument()
  })

  it('renders the user directory page for an admin', async () => {
    const fetchMock = vi.fn((url: string) => {
      if (url.endsWith('/system')) return json(systemWith(['read', 'manage_users']))
      if (url.includes('/nodes?')) return json(page([]))
      if (url.endsWith('/users')) return json([])
      return json(page([]))
    })
    vi.stubGlobal('fetch', fetchMock)
    render(<App />)
    await screen.findByText('Fleet health')
    const nav = await screen.findByText('Users', { selector: 'a' })
    fireEvent.click(nav)
    expect(await screen.findByText('No managed users are configured.')).toBeInTheDocument()
  })
})
