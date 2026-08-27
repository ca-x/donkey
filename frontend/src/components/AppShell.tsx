import { useEffect, type ReactNode } from 'react'
import {
  ActionIcon,
  Box,
  Drawer,
  Group,
  NavLink,
  Stack,
  Text,
  Tooltip,
  UnstyledButton,
} from '@mantine/core'
import { useDisclosure } from '@mantine/hooks'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useMantineColorScheme } from '@mantine/core'
import { notifications } from '@mantine/notifications'
import {
  IconActivityHeartbeat,
  IconArrowsShuffle,
  IconDatabase,
  IconLayoutDashboard,
  IconSettings,
  IconLanguage,
  IconMoon,
  IconSun,
  IconTool,
  IconLogout,
  IconInfoCircle,
  IconTerminal2,
  IconHistory,
  IconDots,
} from '@tabler/icons-react'
import { NavLink as RouterNavLink, useLocation, useNavigate } from 'react-router-dom'
import { useTranslation } from 'react-i18next'
import { api } from '../api'
import { adminUrl } from '../basePath'
import { useAuth } from '../useAuth'

const navigation = [
  { path: '/', label: 'nav.overview', short: 'nav.overview', icon: IconLayoutDashboard },
  { path: '/nodes', label: 'nav.nodes', short: 'nav.nodesShort', icon: IconActivityHeartbeat },
  { path: '/cache', label: 'nav.cache', short: 'nav.cache', icon: IconDatabase },
  { path: '/pull-history', label: 'pulls.title', short: 'pulls.short', icon: IconHistory },
  { path: '/domainfold', label: 'nav.domain', short: 'nav.domainShort', icon: IconArrowsShuffle },
  { path: '/image-tools', label: 'nav.imageTools', short: 'nav.imageToolsShort', icon: IconTool },
  { path: '/settings', label: 'nav.settings', short: 'nav.settingsShort', icon: IconSettings },
  { path: '/deployment', label: 'nav.deployment', short: 'nav.deploymentShort', icon: IconTerminal2 },
  { path: '/about', label: 'nav.about', short: 'nav.about', icon: IconInfoCircle },
]
const mobilePrimaryPaths = new Set(['/', '/nodes', '/cache', '/image-tools'])

export function AppShell({ children }: { children: ReactNode }) {
  const location = useLocation()
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const user = useAuth()
  const runtime = useQuery({ queryKey: ['runtime'], queryFn: api.runtime, staleTime: 30_000 })
  const [moreOpened, more] = useDisclosure(false)
  const { t, i18n } = useTranslation()
  const { colorScheme, setColorScheme } = useMantineColorScheme()
  const dark = colorScheme === 'dark'
  const toggleTheme = () => setColorScheme(dark ? 'light' : 'dark')
  const toggleLanguage = () => void i18n.changeLanguage(i18n.resolvedLanguage === 'zh' ? 'en' : 'zh')
  const logout = useMutation({
    mutationFn: api.logout,
    onSuccess: () => { queryClient.removeQueries({ queryKey: ['auth-me'] }); void navigate('/login', { replace: true }) },
    onError: (error: Error) => notifications.show({ color: 'red', title: t('shell.logoutFailed'), message: error.message }),
  })
  useEffect(() => {
    document.getElementById('main-content')?.focus({ preventScroll: true })
  }, [location.pathname])
  const visibleNavigation = navigation.filter((item) => item.path !== '/pull-history' || runtime.data?.pull_logging_enabled)
  const mobilePrimary = visibleNavigation.filter((item) => mobilePrimaryPaths.has(item.path))
  const mobileSecondary = visibleNavigation.filter((item) => !mobilePrimaryPaths.has(item.path))
  const moreActive = mobileSecondary.some((item) => item.path === location.pathname)
  return (
    <div className="app-frame">
      <a className="skip-link" href="#main-content">
        {t('shell.skip')}
      </a>
      <aside className="desktop-sidebar" aria-label={t('shell.mainNav')}>
        <Brand />
        <Stack gap={4} mt={28}>
          {visibleNavigation.map((item) => (
            <NavLink
              key={item.path}
              component={RouterNavLink}
              to={item.path}
              active={location.pathname === item.path}
              label={t(item.label)}
              leftSection={<item.icon size={19} stroke={1.8} />}
              className="nav-item pressable"
            />
          ))}
        </Stack>
        <div className="sidebar-status">
          <Box className="sidebar-status-copy">
            <Text size="xs" fw={650} truncate>{user.display_name}</Text>
            <Text size="xs" c="dimmed">{t(`login.${user.role}`)}</Text>
          </Box>
          <Group gap={2} ml="auto" wrap="nowrap">
            <Tooltip label={t('shell.language')}><ActionIcon variant="subtle" size="lg" aria-label={t('shell.language')} onClick={toggleLanguage}><IconLanguage size={18} /></ActionIcon></Tooltip>
            <Tooltip label={t(dark ? 'shell.light' : 'shell.dark')}><ActionIcon variant="subtle" size="lg" aria-label={t(dark ? 'shell.light' : 'shell.dark')} onClick={toggleTheme}>{dark ? <IconSun size={18} /> : <IconMoon size={18} />}</ActionIcon></Tooltip>
            <Tooltip label={t('login.logout')}><ActionIcon variant="subtle" size="lg" aria-label={t('login.logout')} loading={logout.isPending} onClick={() => logout.mutate()}><IconLogout size={18} /></ActionIcon></Tooltip>
          </Group>
        </div>
      </aside>

      <header className="mobile-header">
        <Brand compact />
        <Group gap={2} wrap="nowrap">
          <ActionIcon variant="subtle" size={44} aria-label={t('shell.language')} onClick={toggleLanguage}><IconLanguage size={19} /></ActionIcon>
          <ActionIcon variant="subtle" size={44} aria-label={t(dark ? 'shell.light' : 'shell.dark')} onClick={toggleTheme}>{dark ? <IconSun size={19} /> : <IconMoon size={19} />}</ActionIcon>
          <ActionIcon variant="subtle" size={44} aria-label={t('login.logout')} loading={logout.isPending} onClick={() => logout.mutate()}><IconLogout size={19} /></ActionIcon>
        </Group>
      </header>

      <main id="main-content" className="app-content" tabIndex={-1}>
        {children}
      </main>

      <nav className="mobile-nav" aria-label={t('shell.mobileNav')}>
        {mobilePrimary.map((item) => {
          const active = location.pathname === item.path
          return (
            <UnstyledButton
              key={item.path}
              component={RouterNavLink}
              to={item.path}
              className="mobile-nav-item pressable"
              data-active={active || undefined}
              aria-label={t(item.label)}
            >
              <item.icon size={21} stroke={active ? 2.2 : 1.7} />
              <span>{t(item.short)}</span>
            </UnstyledButton>
          )
        })}
        <UnstyledButton
          className="mobile-nav-item pressable"
          data-active={moreActive || undefined}
          aria-label={t('ui.more')}
          aria-expanded={moreOpened}
          aria-controls="mobile-more-navigation"
          onClick={more.open}
        >
          <IconDots size={21} stroke={moreActive ? 2.2 : 1.7} />
          <span>{t('ui.more')}</span>
        </UnstyledButton>
      </nav>

      <Drawer
        id="mobile-more-navigation"
        opened={moreOpened}
        onClose={more.close}
        title={t('ui.moreNav')}
        position="bottom"
        size="min(70dvh, 380px)"
        radius="lg"
        closeButtonProps={{ 'aria-label': t('ui.close') }}
        classNames={{ content: 'polished-modal', overlay: 'polished-overlay' }}
      >
        <Stack gap={4} pb="md">
          {mobileSecondary.map((item) => (
            <NavLink
              key={item.path}
              component={RouterNavLink}
              to={item.path}
              active={location.pathname === item.path}
              label={t(item.label)}
              leftSection={<item.icon size={20} stroke={1.8} />}
              className="mobile-more-item pressable"
              onClick={more.close}
            />
          ))}
        </Stack>
      </Drawer>
    </div>
  )
}

function Brand({ compact = false }: { compact?: boolean }) {
  const { t } = useTranslation()
  return (
    <Group gap={compact ? 8 : 10} wrap="nowrap">
      <img
        className={compact ? 'brand-logo brand-logo--compact' : 'brand-logo'}
        src={adminUrl('/donkey-logo.webp')}
        width={compact ? 36 : 48}
        height={compact ? 36 : 48}
        alt=""
      />
      <Box>
        <Text className="brand-name">DONKEY</Text>
        {!compact && <Text className="brand-caption">{t('ui.brandCaption')}</Text>}
      </Box>
    </Group>
  )
}
