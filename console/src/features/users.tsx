import { useState } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import {
  api,
  errorMessage,
  formatTime,
  isPermissionError,
  type CredentialView,
  type IssuedCredential,
  type ResourceScope,
  type User,
  type UserRole,
} from '../api'
import { useT } from '../i18n'
import { useMutationFailure, useUserCredentials, useUsers } from '../queries'
import { useConfirm } from './confirm'
import { SkeletonRows } from './shared'

const ROLES: UserRole[] = ['admin', 'operator', 'viewer']

/** Roles map to dictionary keys so the en/zh wording stays in the i18n
 * tables instead of the component. */
const ROLE_KEYS = {
  admin: 'users.role.admin',
  operator: 'users.role.operator',
  viewer: 'users.role.viewer',
} as const

/** Surface the server problem code verbatim (e.g. `last_admin_required`,
 * `oidc_subject_conflict`) so operators can act on the exact contract code.
 * 403 stays on the shared permission-degradation path instead. */
function userMutationError(cause: unknown): string {
  const code = (cause as { code?: unknown } | null)?.code
  const message = errorMessage(cause)
  return typeof code === 'string' && code !== '' && code !== 'request_failed'
    ? `${code}: ${message}`
    : message
}

const cleanScopes = (scopes: ScopeDraft[]): ResourceScope[] =>
  scopes
    .map((scope) => ({
      resource_type: scope.resourceType.trim(),
      ...(scope.resourceId.trim() ? { resource_id: scope.resourceId.trim() } : {}),
    }))
    .filter((scope) => scope.resource_type)

const scopesToDraft = (scopes: ResourceScope[]): ScopeDraft[] =>
  scopes.map((scope) => ({ resourceType: scope.resource_type, resourceId: scope.resource_id ?? '' }))

type ScopeDraft = { resourceType: string; resourceId: string }

type UserFormState =
  | {
      mode: 'create'
      userId: string
      displayName: string
      role: UserRole
      scopes: ScopeDraft[]
      oidcSubject: string
    }
  | {
      mode: 'edit'
      user: User
      displayName: string
      role: UserRole
      scopes: ScopeDraft[]
      oidcSubject: string
    }

export function Users({
  onError,
  canManageUsers = true,
}: {
  onError: (message: string) => void
  canManageUsers?: boolean
}) {
  const t = useT()
  if (!canManageUsers) {
    return (
      <section className="panel" data-testid="users-forbidden">
        <div className="panel-title">
          <h3>{t('users.title')}</h3>
        </div>
        <p className="empty">{t('perm.requires', { action: 'manage_users' })}</p>
      </section>
    )
  }
  return <UsersPanel onError={onError} />
}

function UsersPanel({ onError }: { onError: (message: string) => void }) {
  const t = useT()
  const queryClient = useQueryClient()
  const reportMutationFailure = useMutationFailure()
  const confirm = useConfirm()
  const usersQuery = useUsers()
  const [selectedId, setSelectedId] = useState<string>()
  const [form, setForm] = useState<UserFormState | null>(null)
  const [issued, setIssued] = useState<IssuedCredential | null>(null)
  const [notice, setNotice] = useState('')

  const users = usersQuery.data ?? []
  const selected = users.find((user) => user.user_id === selectedId)
  const credentials = useUserCredentials(selected?.user_id)

  const fail = (cause: unknown) => {
    if (isPermissionError(cause)) {
      reportMutationFailure(onError, cause)
      return
    }
    onError(userMutationError(cause))
  }

  const openCreate = () => {
    onError('')
    setNotice('')
    setForm({ mode: 'create', userId: '', displayName: '', role: 'operator', scopes: [], oidcSubject: '' })
  }

  const openEdit = (user: User) => {
    onError('')
    setNotice('')
    setForm({
      mode: 'edit',
      user,
      displayName: user.display_name,
      role: ROLES.includes(user.role as UserRole) ? (user.role as UserRole) : 'operator',
      scopes: scopesToDraft(user.scopes),
      oidcSubject: user.oidc_subject ?? '',
    })
  }

  const submitForm = async (state: UserFormState) => {
    onError('')
    try {
      if (state.mode === 'create') {
        const created = await api.createUser({
          user_id: state.userId.trim(),
          display_name: state.displayName.trim() || undefined,
          role: state.role,
          scopes: cleanScopes(state.scopes),
          oidc_subject: state.oidcSubject.trim() || undefined,
        })
        setSelectedId(created.user_id)
        setNotice(t('users.createdNotice', { userId: created.user_id }))
      } else {
        await api.updateUser(state.user.user_id, {
          display_name: state.displayName.trim(),
          role: state.role,
          scopes: cleanScopes(state.scopes),
          // An empty string clears the OIDC binding on the Hub contract.
          oidc_subject: state.oidcSubject.trim(),
        })
        setNotice(t('users.updatedNotice', { userId: state.user.user_id }))
      }
      setForm(null)
      await queryClient.invalidateQueries({ queryKey: ['users'] })
      if (state.mode === 'edit')
        await queryClient.invalidateQueries({ queryKey: ['user-credentials', state.user.user_id] })
    } catch (cause) {
      fail(cause)
    }
  }

  const toggleDisabled = async (user: User) => {
    onError('')
    try {
      await api.updateUser(user.user_id, { disabled: !user.disabled })
      setNotice(
        user.disabled
          ? t('users.enabledNotice', { userId: user.user_id })
          : t('users.disabledNotice', { userId: user.user_id }),
      )
      await queryClient.invalidateQueries({ queryKey: ['users'] })
    } catch (cause) {
      fail(cause)
    }
  }

  const removeUser = async (user: User) => {
    onError('')
    const accepted = await confirm({
      title: t('users.deleteTitle'),
      body: t('users.confirmDelete', { userId: user.user_id }),
      confirmLabel: t('users.delete'),
    })
    if (!accepted) return
    try {
      await api.deleteUser(user.user_id)
      if (selectedId === user.user_id) {
        setSelectedId(undefined)
        setIssued(null)
      }
      setNotice(t('users.deletedNotice', { userId: user.user_id }))
      await queryClient.invalidateQueries({ queryKey: ['users'] })
    } catch (cause) {
      fail(cause)
    }
  }

  const issueCredential = async (input: { label: string; expiresAt: string }) => {
    if (!selected) return
    onError('')
    try {
      const expiresAtMs = input.expiresAt ? Date.parse(input.expiresAt) : undefined
      const response = await api.issueCredential(selected.user_id, {
        label: input.label.trim() || undefined,
        expires_at_ms: Number.isFinite(expiresAtMs) ? expiresAtMs : undefined,
      })
      setIssued(response)
      setNotice('')
      await queryClient.invalidateQueries({ queryKey: ['user-credentials', selected.user_id] })
    } catch (cause) {
      fail(cause)
    }
  }

  const rotateCredential = async (credential: CredentialView) => {
    if (!selected) return
    onError('')
    const accepted = await confirm({
      title: t('users.credentials.rotateTitle'),
      body: t('users.credentials.confirmRotate', { credentialId: credential.credential_id }),
      confirmLabel: t('users.credentials.rotate'),
    })
    if (!accepted) return
    try {
      const response = await api.rotateCredential(credential.credential_id)
      setIssued(response)
      await queryClient.invalidateQueries({ queryKey: ['user-credentials', selected.user_id] })
    } catch (cause) {
      fail(cause)
    }
  }

  const revokeCredential = async (credential: CredentialView) => {
    if (!selected) return
    onError('')
    const accepted = await confirm({
      title: t('users.credentials.revokeTitle'),
      body: t('users.credentials.confirmRevoke', { credentialId: credential.credential_id }),
      confirmLabel: t('users.credentials.revoke'),
    })
    if (!accepted) return
    try {
      await api.revokeCredential(credential.credential_id)
      await queryClient.invalidateQueries({ queryKey: ['user-credentials', selected.user_id] })
    } catch (cause) {
      fail(cause)
    }
  }

  const revokeTokens = async () => {
    if (!selected) return
    onError('')
    const accepted = await confirm({
      title: t('users.revokeTokensTitle'),
      body: t('users.confirmRevokeTokens', { userId: selected.user_id }),
      confirmLabel: t('users.revokeTokens'),
    })
    if (!accepted) return
    try {
      const response = await api.revokeUserTokens(selected.user_id)
      setNotice(t('users.revokeTokensDone', { count: response.revoked_sessions }))
      await queryClient.invalidateQueries({ queryKey: ['user-credentials', selected.user_id] })
    } catch (cause) {
      fail(cause)
    }
  }

  return (
    <>
      <section className="panel">
        <div className="panel-title">
          <h3>{t('users.title')}</h3>
          <span>{t('users.subtitle')}</span>
        </div>
        <p>{t('users.directoryNote')}</p>
        {usersQuery.isError ? (
          <p className="error-text">{t('users.unavailable', { message: errorMessage(usersQuery.error) })}</p>
        ) : usersQuery.isLoading ? (
          <SkeletonRows rows={2} />
        ) : (
          <table className="table">
            <thead>
              <tr>
                <th>{t('users.id')}</th>
                <th>{t('users.displayName')}</th>
                <th>{t('users.role')}</th>
                <th>{t('users.status')}</th>
                <th>{t('users.scopes')}</th>
                <th>{t('users.oidcSubject')}</th>
                <th>{t('users.actions')}</th>
              </tr>
            </thead>
            <tbody>
              {users.length === 0 ? (
                <tr>
                  <td colSpan={7}>{t('users.empty')}</td>
                </tr>
              ) : (
                users.map((user) => (
                  <tr
                    key={user.user_id}
                    className={user.user_id === selectedId ? 'selected' : undefined}
                    data-testid={`user-row-${user.user_id}`}
                  >
                    <td>
                      <code>{user.user_id}</code>
                    </td>
                    <td>{user.display_name || '—'}</td>
                    <td>{t(ROLE_KEYS[user.role as UserRole] ?? 'users.role.operator')}</td>
                    <td>
                      <span className={`state ${user.disabled ? 'blocked' : 'running'}`}>
                        {user.disabled ? t('users.disabled') : t('users.enabled')}
                      </span>
                    </td>
                    <td>
                      {user.scopes.length === 0
                        ? t('common.none')
                        : user.scopes
                            .map((scope) =>
                              scope.resource_id
                                ? `${scope.resource_type}:${scope.resource_id}`
                                : scope.resource_type,
                            )
                            .join(', ')}
                    </td>
                    <td>{user.oidc_subject ?? '—'}</td>
                    <td>
                      <div className="toolbar">
                        <button type="button" onClick={() => openEdit(user)}>
                          {t('users.edit')}
                        </button>
                        <button
                          type="button"
                          onClick={() => {
                            setSelectedId(user.user_id)
                            // A one-time token belongs to the previously
                            // selected user; never carry it to another.
                            setIssued(null)
                          }}
                        >
                          {t('users.credentials.manage')}
                        </button>
                        <button type="button" onClick={() => void toggleDisabled(user)}>
                          {user.disabled ? t('users.enable') : t('users.disable')}
                        </button>
                        <button type="button" onClick={() => void removeUser(user)}>
                          {t('users.delete')}
                        </button>
                      </div>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        )}
        <div className="toolbar">
          <button type="button" onClick={openCreate}>
            {t('users.create')}
          </button>
        </div>
        {notice ? <p className="muted">{notice}</p> : null}
        {form ? (
          <UserForm
            state={form}
            onChange={setForm}
            onCancel={() => setForm(null)}
            onSubmit={() => void submitForm(form)}
          />
        ) : null}
      </section>
      {selected ? (
        <CredentialsPanel
          user={selected}
          credentials={credentials.data ?? []}
          loading={credentials.isLoading}
          error={credentials.isError ? errorMessage(credentials.error) : undefined}
          issued={issued}
          onDismissIssued={() => setIssued(null)}
          onIssue={(input) => void issueCredential(input)}
          onRotate={(credential) => void rotateCredential(credential)}
          onRevoke={(credential) => void revokeCredential(credential)}
          onRevokeTokens={() => void revokeTokens()}
          onError={onError}
        />
      ) : (
        <section className="panel">
          <p className="empty">{t('users.selectHint')}</p>
        </section>
      )}
    </>
  )
}

function UserForm({
  state,
  onChange,
  onCancel,
  onSubmit,
}: {
  state: UserFormState
  onChange: (next: UserFormState) => void
  onCancel: () => void
  onSubmit: () => void
}) {
  const t = useT()
  // The discriminated union keeps the fields shared; `userId` exists only in
  // create mode and is read from the narrowed branch below.
  const scopes = state.scopes
  const setScopes = (next: ScopeDraft[]) => onChange({ ...state, scopes: next })
  return (
    <form
      className="stack"
      data-testid="user-form"
      onSubmit={(event) => {
        event.preventDefault()
        onSubmit()
      }}
    >
      <div className="panel-title">
        <h4>
          {state.mode === 'create'
            ? t('users.createTitle')
            : t('users.editTitle', { userId: state.user.user_id })}
        </h4>
      </div>
      {state.mode === 'create' ? (
        <label>
          <span>{t('users.id')}</span>
          <input
            value={state.userId}
            onChange={(event) => onChange({ ...state, userId: event.target.value })}
            pattern="[A-Za-z0-9][A-Za-z0-9._-]{0,63}"
            required
          />
          <small>{t('users.idHint')}</small>
        </label>
      ) : (
        <p>
          <small>{t('users.id')}</small>
          <code>{state.user.user_id}</code>
        </p>
      )}
      <label>
        <span>{t('users.displayName')}</span>
        <input
          value={state.displayName}
          onChange={(event) => onChange({ ...state, displayName: event.target.value })}
        />
      </label>
      <label>
        <span>{t('users.role')}</span>
        <select
          value={state.role}
          onChange={(event) => onChange({ ...state, role: event.target.value as UserRole })}
        >
          {ROLES.map((role) => (
            <option key={role} value={role}>
              {t(ROLE_KEYS[role])}
            </option>
          ))}
        </select>
      </label>
      <label>
        <span>{t('users.oidcSubject')}</span>
        <input
          value={state.oidcSubject}
          onChange={(event) => onChange({ ...state, oidcSubject: event.target.value })}
        />
        <small>{t('users.oidcSubjectHint')}</small>
      </label>
      <div>
        <small>{t('users.scopes')}</small>
        {scopes.map((scope, index) => (
          <div className="toolbar" key={index}>
            <input
              aria-label={t('users.scopeType')}
              value={scope.resourceType}
              placeholder={t('users.scopeTypePlaceholder')}
              onChange={(event) =>
                setScopes(
                  scopes.map((entry, position) =>
                    position === index ? { ...entry, resourceType: event.target.value } : entry,
                  ),
                )
              }
            />
            <input
              aria-label={t('users.scopeId')}
              value={scope.resourceId}
              placeholder={t('users.scopeIdPlaceholder')}
              onChange={(event) =>
                setScopes(
                  scopes.map((entry, position) =>
                    position === index ? { ...entry, resourceId: event.target.value } : entry,
                  ),
                )
              }
            />
            <button
              type="button"
              onClick={() => setScopes(scopes.filter((_, position) => position !== index))}
            >
              {t('users.removeScope')}
            </button>
          </div>
        ))}
        <button type="button" onClick={() => setScopes([...scopes, { resourceType: '', resourceId: '' }])}>
          {t('users.addScope')}
        </button>
      </div>
      <div className="toolbar">
        <button type="submit">{state.mode === 'create' ? t('users.create') : t('users.save')}</button>
        <button type="button" onClick={onCancel}>
          {t('common.cancel')}
        </button>
      </div>
    </form>
  )
}

function CredentialsPanel({
  user,
  credentials,
  loading,
  error,
  issued,
  onDismissIssued,
  onIssue,
  onRotate,
  onRevoke,
  onRevokeTokens,
  onError,
}: {
  user: User
  credentials: CredentialView[]
  loading: boolean
  error?: string
  issued: IssuedCredential | null
  onDismissIssued: () => void
  onIssue: (input: { label: string; expiresAt: string }) => void
  onRotate: (credential: CredentialView) => void
  onRevoke: (credential: CredentialView) => void
  onRevokeTokens: () => void
  onError: (message: string) => void
}) {
  const t = useT()
  const [label, setLabel] = useState('')
  const [expiresAt, setExpiresAt] = useState('')
  const [copied, setCopied] = useState(false)

  const copyToken = async () => {
    if (!issued) return
    try {
      await navigator.clipboard.writeText(issued.token)
      setCopied(true)
    } catch {
      onError(t('users.credentials.copyFailed'))
    }
  }

  return (
    <section className="panel">
      <div className="panel-title">
        <h3>{t('users.credentials.title')}</h3>
        <span>{t('users.credentials.subtitle')}</span>
      </div>
      <p>
        <strong>{user.user_id}</strong> · {t('users.credentials.metadataNote')}
      </p>
      {issued ? (
        <div className="issued-credential" data-testid="issued-credential">
          <h4>{t('users.credentials.oneTimeTitle')}</h4>
          <p className="warning-text">{t('users.credentials.oneTimeHint')}</p>
          <p>
            <code>{issued.credential.credential_id}</code>
            {issued.credential.label ? ` · ${issued.credential.label}` : ''}
          </p>
          <p>
            <code className="token-value" data-testid="issued-token">
              {issued.token}
            </code>
          </p>
          <div className="toolbar">
            <button type="button" onClick={() => void copyToken()}>
              {copied ? t('users.credentials.copied') : t('users.credentials.copy')}
            </button>
            <button
              type="button"
              onClick={() => {
                setCopied(false)
                onDismissIssued()
              }}
            >
              {t('users.credentials.dismiss')}
            </button>
          </div>
        </div>
      ) : null}
      {error ? <p className="error-text">{t('users.credentials.unavailable', { message: error })}</p> : null}
      <form
        className="toolbar"
        onSubmit={(event) => {
          event.preventDefault()
          onIssue({ label, expiresAt })
          setLabel('')
          setExpiresAt('')
        }}
      >
        <input
          aria-label={t('users.credentials.label')}
          value={label}
          placeholder={t('users.credentials.labelPlaceholder')}
          onChange={(event) => setLabel(event.target.value)}
        />
        <input
          aria-label={t('users.credentials.expires')}
          type="datetime-local"
          value={expiresAt}
          onChange={(event) => setExpiresAt(event.target.value)}
        />
        <button type="submit" disabled={loading}>
          {t('users.credentials.issue')}
        </button>
      </form>
      <table className="table">
        <thead>
          <tr>
            <th>{t('users.credentials.credentialId')}</th>
            <th>{t('users.credentials.label')}</th>
            <th>{t('users.credentials.issuedAt')}</th>
            <th>{t('users.credentials.expiresAt')}</th>
            <th>{t('users.credentials.lastUsed')}</th>
            <th>{t('users.credentials.status')}</th>
            <th>{t('users.credentials.actions')}</th>
          </tr>
        </thead>
        <tbody>
          {loading ? (
            <tr>
              <td colSpan={7}>{t('common.loading')}</td>
            </tr>
          ) : credentials.length === 0 ? (
            <tr>
              <td colSpan={7}>{t('users.credentials.empty')}</td>
            </tr>
          ) : (
            credentials.map((credential) => {
              const revoked = credential.revoked_at_ms != null
              return (
                <tr key={credential.credential_id} data-testid={`credential-row-${credential.credential_id}`}>
                  <td>
                    <code>{credential.credential_id}</code>
                  </td>
                  <td>{credential.label || '—'}</td>
                  <td>{formatTime(credential.created_at_ms)}</td>
                  <td>{formatTime(credential.expires_at_ms ?? undefined)}</td>
                  <td>{formatTime(credential.last_used_at_ms ?? undefined)}</td>
                  <td>
                    <span className={`state ${revoked ? 'blocked' : 'running'}`}>
                      {revoked ? t('users.credentials.revoked') : t('users.credentials.active')}
                    </span>
                  </td>
                  <td>
                    <div className="toolbar">
                      <button
                        type="button"
                        disabled={revoked}
                        title={revoked ? t('users.credentials.alreadyRevoked') : undefined}
                        onClick={() => onRotate(credential)}
                      >
                        {t('users.credentials.rotate')}
                      </button>
                      <button
                        type="button"
                        disabled={revoked}
                        title={revoked ? t('users.credentials.alreadyRevoked') : undefined}
                        onClick={() => onRevoke(credential)}
                      >
                        {t('users.credentials.revoke')}
                      </button>
                    </div>
                  </td>
                </tr>
              )
            })
          )}
        </tbody>
      </table>
      <div className="toolbar">
        <button type="button" onClick={onRevokeTokens}>
          {t('users.revokeTokens')}
        </button>
        <small>{t('users.credentials.revokeTokensNote')}</small>
      </div>
    </section>
  )
}
