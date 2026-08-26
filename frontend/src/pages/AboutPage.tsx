import { Box, Group, Paper, SimpleGrid, Stack, Text, Title } from '@mantine/core'
import { IconBrandGithub, IconInfoCircle, IconShieldCheck } from '@tabler/icons-react'
import { useQuery } from '@tanstack/react-query'
import { useTranslation } from 'react-i18next'
import { api } from '../api'
import { PageHeader } from '../components/PageHeader'
import { ErrorState, LoadingState } from '../components/States'

export function AboutPage() {
  const { t } = useTranslation()
  const health = useQuery({ queryKey: ['health'], queryFn: api.health, staleTime: 300_000 })
  if (health.isLoading) return <LoadingState />
  if (health.error) return <ErrorState error={health.error} retry={() => void health.refetch()} />
  return <Stack gap={24}>
    <PageHeader title={t('about.title')} description={t('about.description')} />
    <Paper className="panel about-hero">
      <Group gap="md" align="flex-start"><Box className="about-icon" aria-hidden="true"><IconInfoCircle size={26} /></Box><Box><Title order={2}>Donkey</Title><Text c="dimmed" mt={5}>{t('about.tagline')}</Text></Box></Group>
      <SimpleGrid cols={{ base: 1, sm: 2 }} mt="xl"><Info label={t('about.version')} value={health.data?.version ?? '—'} /><Info label={t('about.status')} value={health.data?.status ?? '—'} /></SimpleGrid>
    </Paper>
    <SimpleGrid cols={{ base: 1, sm: 2 }}>
      <Paper className="panel"><Group gap="sm"><IconShieldCheck size={20} /><Text fw={680}>{t('about.securityTitle')}</Text></Group><Text size="sm" c="dimmed" mt="md">{t('about.securityDescription')}</Text></Paper>
      <Paper className="panel"><Group gap="sm"><IconBrandGithub size={20} /><Text fw={680}>{t('about.projectTitle')}</Text></Group><Text component="a" href="https://github.com/ca-x/donkey" target="_blank" rel="noreferrer" size="sm" c="blue" mt="md">github.com/ca-x/donkey</Text></Paper>
    </SimpleGrid>
  </Stack>
}

function Info({ label, value }: { label: string; value: string }) { return <Box><Text size="xs" c="dimmed">{label}</Text><Text fw={650} mt={4} ff="monospace">{value}</Text></Box> }

