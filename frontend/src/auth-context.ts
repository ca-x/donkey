import { createContext } from 'react'
import type { AuthUser } from './types'

export const AuthContext = createContext<AuthUser | null>(null)
