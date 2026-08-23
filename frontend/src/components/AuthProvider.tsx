import type { ReactNode } from 'react'
import { AuthContext } from '../auth-context'
import type { AuthUser } from '../types'

export function AuthProvider({ user, children }: { user: AuthUser; children: ReactNode }) {
  return <AuthContext.Provider value={user}>{children}</AuthContext.Provider>
}
