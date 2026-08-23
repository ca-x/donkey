import { lazy, Suspense } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Navigate, Route, Routes, useLocation } from 'react-router-dom'
import { api, ApiError } from './api'
import { AuthProvider } from './components/AuthProvider'
import { AppShell } from './components/AppShell'
import { ErrorState, LoadingState } from './components/States'

const DashboardPage = lazy(() => import('./pages/DashboardPage').then((module) => ({ default: module.DashboardPage })))
const NodesPage = lazy(() => import('./pages/NodesPage').then((module) => ({ default: module.NodesPage })))
const CachePage = lazy(() => import('./pages/CachePage').then((module) => ({ default: module.CachePage })))
const DomainFoldPage = lazy(() => import('./pages/DomainFoldPage').then((module) => ({ default: module.DomainFoldPage })))
const SettingsPage = lazy(() => import('./pages/SettingsPage').then((module) => ({ default: module.SettingsPage })))
const ImageToolsPage = lazy(() => import('./pages/ImageToolsPage').then((module) => ({ default: module.ImageToolsPage })))
const LoginPage = lazy(() => import('./pages/LoginPage').then((module) => ({ default: module.LoginPage })))

export function App() {
  const location = useLocation()
  const session = useQuery({ queryKey: ['auth-me'], queryFn: api.me, retry: false, staleTime: 30_000 })
  if (location.pathname === '/login') {
    if (session.data) return <Navigate to="/" replace />
    return <Suspense fallback={<LoadingState />}><LoginPage /></Suspense>
  }
  if (session.isLoading) return <LoadingState />
  if (session.error instanceof ApiError && session.error.status === 401) {
    return <Navigate to="/login" replace state={{ from: `${location.pathname}${location.search}` }} />
  }
  if (session.error) return <ErrorState error={session.error} retry={() => void session.refetch()} />
  return (
    <AuthProvider user={session.data!}>
      <AppShell>
        <Suspense fallback={<LoadingState />}>
          <Routes>
            <Route path="/" element={<DashboardPage />} />
            <Route path="/nodes" element={<NodesPage />} />
            <Route path="/cache" element={<CachePage />} />
            <Route path="/domainfold" element={<DomainFoldPage />} />
            <Route path="/settings" element={<SettingsPage />} />
            <Route path="/image-tools" element={<ImageToolsPage />} />
            <Route path="*" element={<Navigate to="/" replace />} />
          </Routes>
        </Suspense>
      </AppShell>
    </AuthProvider>
  )
}
