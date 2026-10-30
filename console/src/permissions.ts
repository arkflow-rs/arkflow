import { useCallback } from 'react'
import { useSystem } from './queries'
import type { OperatorAction, PrincipalEcho } from './api'

/** The Hub plane advertises `fleet_aggregation`; the local standalone plane
 * does not. Drives mode-aware navigation (Hub-only pages). */
const HUB_CAPABILITY = 'fleet_aggregation'

export type Permissions = {
  /** Echoed principal, or null when the control plane predates the echo. */
  principal: PrincipalEcho | null
  /** Action gate for mutating controls. Lenient (always true) while the
   * echo is absent: an older control plane cannot be asked for roles, and
   * locking every button would be worse than showing them. */
  can: (action: OperatorAction) => boolean
  /** False until `/system` resolves — the identity is unknown, not denied. */
  identityKnown: boolean
}

export function usePermissions(): Permissions {
  const system = useSystem()
  const principal = system.data?.principal ?? null
  const actions = principal?.actions
  const can = useCallback((action: OperatorAction) => (actions ? actions.includes(action) : true), [actions])
  return { principal, can, identityKnown: system.isSuccess && principal != null }
}

/** `true` when connected to a Hub, `false` on a local plane, and
 * `undefined` while the system identity is still loading (the shell then
 * keeps the full navigation rather than flashing pages away). */
export function useHubMode(): boolean | undefined {
  const system = useSystem()
  const capabilities = system.data?.capabilities
  return capabilities == null ? undefined : capabilities.includes(HUB_CAPABILITY)
}
