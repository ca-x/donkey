import type { ReactNode } from 'react'
import { Alert, Button, Center, Loader, Stack, Text } from '@mantine/core'
import { IconAlertTriangle, IconDatabaseOff } from '@tabler/icons-react'
import { useTranslation } from 'react-i18next'

export function LoadingState({ label }: { label?: string }) {
  const { t } = useTranslation()
  const copy = label ?? t('common.loading')
  return (
    <Center mih={260}>
      <Stack align="center" gap="sm" role="status" aria-label={copy}>
        <Loader size={26} type="dots" aria-hidden="true" />
        <Text size="sm" c="dimmed">{copy}</Text>
      </Stack>
    </Center>
  )
}

export function ErrorState({ error, retry }: { error: Error; retry?: () => void }) {
  const { t } = useTranslation()
  return (
    <Alert color="red" icon={<IconAlertTriangle size={18} />} title={t('errors.loadTitle')}>
      <Text size="sm">{error.message}</Text>
      {retry && <Button size="xs" variant="light" color="red" mt="md" onClick={retry}>{t('common.retry')}</Button>}
    </Alert>
  )
}

export function EmptyState({ title, description, action }: { title: string; description: string; action?: ReactNode }) {
  return (
    <Center className="empty-state">
      <Stack align="center" gap="sm" ta="center">
        <IconDatabaseOff size={24} color="var(--muted)" aria-hidden="true" />
        <div>
          <Text fw={650}>{title}</Text>
          <Text size="sm" c="dimmed" mt={4}>{description}</Text>
        </div>
        {action}
      </Stack>
    </Center>
  )
}
