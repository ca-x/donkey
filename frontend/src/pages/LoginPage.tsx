import {
  ActionIcon,
  Box,
  Button,
  Divider,
  Group,
  Loader,
  Paper,
  PasswordInput,
  Stack,
  Text,
  TextInput,
  Title,
  Tooltip,
  useMantineColorScheme,
} from '@mantine/core'
import { useForm } from '@mantine/form'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { IconLanguage, IconLogin2, IconMoon, IconSun } from '@tabler/icons-react'
import { useEffect, useRef } from 'react'
import { useTranslation } from 'react-i18next'
import { useLocation, useNavigate } from 'react-router-dom'
import { api, ApiError } from '../api'
import { adminUrl } from '../basePath'
import type { AuthUser } from '../types'

export function LoginPage() {
  const { t, i18n } = useTranslation()
  const { colorScheme, setColorScheme } = useMantineColorScheme()
  const queryClient = useQueryClient()
  const navigate = useNavigate()
  const location = useLocation()
  const usernameRef = useRef<HTMLInputElement>(null)
  const dark = colorScheme === 'dark'
  const config = useQuery({ queryKey: ['auth-config'], queryFn: api.authConfig, retry: false })
  const form = useForm({ initialValues: { username: '', password: '' } })
  const returnTo = safeReturnTo((location.state as { from?: string } | null)?.from)
  const oidcError = new URLSearchParams(location.search).get('error') === 'oidc'
  const login = useMutation({
    mutationFn: ({ username, password }: { username: string; password: string }) => api.login(username, password),
    onSuccess: (user: AuthUser) => {
      queryClient.setQueryData(['auth-me'], user)
      void navigate(returnTo, { replace: true })
    },
  })

  useEffect(() => {
    if (config.data?.local_enabled) usernameRef.current?.focus()
  }, [config.data?.local_enabled])

  const error = login.error instanceof ApiError
    ? t(login.error.status === 429 ? 'login.rateLimited' : 'login.invalid')
    : oidcError ? t('login.oidcFailed') : null

  return (
    <main className="login-page">
      <div className="login-controls">
        <Tooltip label={t('shell.language')}>
          <ActionIcon variant="subtle" size={44} aria-label={t('shell.language')} onClick={() => void i18n.changeLanguage(i18n.resolvedLanguage === 'zh' ? 'en' : 'zh')}>
            <IconLanguage size={19} />
          </ActionIcon>
        </Tooltip>
        <Tooltip label={t(dark ? 'shell.light' : 'shell.dark')}>
          <ActionIcon variant="subtle" size={44} aria-label={t(dark ? 'shell.light' : 'shell.dark')} onClick={() => setColorScheme(dark ? 'light' : 'dark')}>
            {dark ? <IconSun size={19} /> : <IconMoon size={19} />}
          </ActionIcon>
        </Tooltip>
      </div>

      <section className="login-layout" aria-labelledby="login-title">
        <div className="login-identity">
          <img className="login-logo" src={adminUrl('/donkey-logo.webp')} width="112" height="112" alt="Donkey" />
          <Text className="login-wordmark">DONKEY</Text>
          <Text c="dimmed" size="sm">{t('ui.loginSubtitle')}</Text>
        </div>

        <Paper className="login-panel">
          <Stack gap="lg">
            <Box>
              <Title id="login-title" order={1}>{t('login.title')}</Title>
              <Text c="dimmed" size="sm" mt={7}>{t('login.description')}</Text>
            </Box>

            {config.isLoading && <Group justify="center" py="xl"><Loader size="sm" aria-label={t('common.loading')} /></Group>}
            {config.error && <Text role="alert" c="red.5" size="sm">{t('login.configFailed')}</Text>}

            {config.data?.local_enabled && (
              <form onSubmit={form.onSubmit((values) => login.mutate(values))}>
                <Stack>
                  <TextInput
                    ref={usernameRef}
                    required
                    label={t('login.username')}
                    autoComplete="username"
                    maxLength={80}
                    {...form.getInputProps('username')}
                  />
                  <PasswordInput
                    required
                    label={t('login.password')}
                    autoComplete="current-password"
                    maxLength={1024}
                    visibilityToggleButtonProps={{ 'aria-label': t('ui.togglePassword') }}
                    {...form.getInputProps('password')}
                  />
                  {error && <Text className="login-error" role="alert" c="red.5" size="sm">{error}</Text>}
                  <Button type="submit" loading={login.isPending} leftSection={<IconLogin2 size={17} />}>
                    {t('login.submit')}
                  </Button>
                </Stack>
              </form>
            )}

            {config.data?.local_enabled && config.data.oidc_enabled && <Divider label={t('login.or')} labelPosition="center" />}
            {config.data?.oidc_enabled && (
              <Button
                component="a"
                href={`${adminUrl('/api/auth/oidc/start')}?return_to=${encodeURIComponent(returnTo)}`}
                variant="default"
              >
                {t('login.continueWith', { provider: config.data.oidc_name })}
              </Button>
            )}
            {config.data && !config.data.local_enabled && !config.data.oidc_enabled && (
              <Text role="alert" size="sm" c="dimmed">{t('login.notConfigured')}</Text>
            )}
          </Stack>
        </Paper>
      </section>
    </main>
  )
}

function safeReturnTo(value?: string) {
  return value?.startsWith('/') && !value.startsWith('//') ? value : '/'
}
