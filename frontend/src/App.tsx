import { lazy, Suspense, useEffect } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Navigate, Route, Routes, useLocation } from 'react-router-dom'
import { useTranslation } from 'react-i18next'
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
const AboutPage = lazy(() => import('./pages/AboutPage').then((module) => ({ default: module.AboutPage })))
const DeploymentPage = lazy(() => import('./pages/DeploymentPage').then((module) => ({ default: module.DeploymentPage })))
const PullHistoryPage = lazy(() => import('./pages/PullHistoryPage').then((module) => ({ default: module.PullHistoryPage })))

export function App() {
  const location = useLocation()
  const { t, i18n } = useTranslation()
  const session = useQuery({ queryKey: ['auth-me'], queryFn: api.me, retry: false, staleTime: 30_000 })
  useEffect(() => {
    const titleKey: Record<string, string> = {
      '/': 'dashboard.title',
      '/nodes': 'nodes.title',
      '/cache': 'cache.title',
      '/domainfold': 'domain.title',
      '/image-tools': 'imageTools.title',
      '/settings': 'ui.settingsTitle',
      '/login': 'login.title',
      '/about': 'about.title',
      '/deployment': 'deployment.title',
      '/pull-history': 'pulls.title',
    }
    document.title = `Donkey · ${t(titleKey[location.pathname] ?? 'dashboard.title')}`
  }, [i18n.resolvedLanguage, location.pathname, t])
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
            <Route path="/about" element={<AboutPage />} />
            <Route path="/deployment" element={<DeploymentPage />} />
            <Route path="/pull-history" element={<PullHistoryGate />} />
            <Route path="*" element={<Navigate to="/" replace />} />
          </Routes>
        </Suspense>
      </AppShell>
    </AuthProvider>
  )
}

function PullHistoryGate() {
  const runtime = useQuery({ queryKey: ['runtime'], queryFn: api.runtime, staleTime: 30_000 })
  if (runtime.isLoading) return <LoadingState />
  if (runtime.error) return <ErrorState error={runtime.error} retry={() => void runtime.refetch()} />
  if (!runtime.data?.pull_logging_enabled) return <Navigate to="/" replace />
  return <PullHistoryPage />
}
