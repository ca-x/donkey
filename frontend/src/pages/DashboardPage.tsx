import { Box, Group, Paper, SimpleGrid, Stack, Text, Title } from '@mantine/core'
import { useQuery } from '@tanstack/react-query'
import {
  IconBolt,
  IconCloudCheck,
  IconDatabase,
  IconActivityHeartbeat,
  IconRoute,
} from '@tabler/icons-react'
import {
  Bar,
  BarChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import { api, formatBytes, formatRate } from '../api'
import { useTranslation } from 'react-i18next'
import { MetricCard } from '../components/MetricCard'
import { PageHeader } from '../components/PageHeader'
import { ErrorState, LoadingState } from '../components/States'

export function DashboardPage() {
  const { t } = useTranslation()
  const dashboard = useQuery({
    queryKey: ['dashboard'],
    queryFn: api.dashboard,
    refetchInterval: 15_000,
  })

  if (dashboard.isLoading) return <LoadingState />
  if (dashboard.error) return <ErrorState error={dashboard.error} retry={() => void dashboard.refetch()} />
  const data = dashboard.data!
  const enabledNodes = data.nodes.filter((item) => item.node.enabled)
  const chart = enabledNodes.slice(0, 8).map((item) => ({
    name: item.node.name.length > 12 ? `${item.node.name.slice(0, 11)}…` : item.node.name,
    speed: Math.round(item.metric.speed_bps / 1024),
    healthy: item.metric.healthy,
  }))

  return (
    <Stack gap={24}>
      <PageHeader
        title={t('dashboard.title')}
        description={t('dashboard.description')}
        action={<Group gap={7} wrap="nowrap"><span className="badge-dot" data-healthy={data.healthy_nodes > 0 || undefined} aria-hidden="true" /><Text size="sm" fw={620}>{t(data.healthy_nodes > 0 ? 'dashboard.online' : 'dashboard.waiting')}</Text></Group>}
      />

      <SimpleGrid cols={{ base: 1, xs: 2, lg: 4 }} spacing="md">
        <MetricCard index={0} label={t('dashboard.healthyNodes')} value={`${data.healthy_nodes}/${enabledNodes.length}`} detail={t('dashboard.activeRouting')} icon={<IconCloudCheck size={18} />} />
        <MetricCard index={1} label={t('dashboard.cacheSize')} value={formatBytes(data.cache_bytes)} detail={t('dashboard.objects', { count: data.cache_entries })} icon={<IconDatabase size={18} />} />
        <MetricCard index={2} label={t('dashboard.hits')} value={data.cache_hits.toLocaleString()} detail={t('dashboard.hitsDetail')} icon={<IconBolt size={18} />} />
        <MetricCard index={3} label={t('dashboard.mode')} value={t(enabledNodes.length > 1 ? 'dashboard.concurrent' : 'dashboard.fallback')} detail={t('dashboard.integrity')} icon={<IconRoute size={18} />} />
        <MetricCard index={4} label={t('dashboard.registryRequests')} value={data.registry_requests.toLocaleString()} detail={t('dashboard.registryRequestsDetail')} icon={<IconActivityHeartbeat size={18} />} />
        <MetricCard index={5} label={t('dashboard.registryBytes')} value={formatBytes(data.registry_bytes)} detail={t('dashboard.registryBytesDetail')} icon={<IconDatabase size={18} />} />
        <MetricCard index={6} label={t('dashboard.concurrent')} value={data.parallel_blobs.toLocaleString()} detail={`${formatBytes(data.last_chunk_size)} · ${data.cooling_nodes} ${t('dashboard.waiting')}`} icon={<IconRoute size={18} />} />
        <MetricCard index={7} label={t('imageTools.retry')} value={data.retry_attempts.toLocaleString()} detail={`${data.resume_attempts.toLocaleString()} · ${t('settings.resumableThreshold')}`} icon={<IconActivityHeartbeat size={18} />} />
      </SimpleGrid>

      <div className="dashboard-grid">
        <Paper className="panel chart-panel">
          <Group justify="space-between" align="flex-start" mb="lg">
            <Box>
              <Title order={2}>{t('dashboard.throughput')}</Title>
              <Text size="sm" c="dimmed" mt={4}>{t('dashboard.throughputDesc')}</Text>
            </Box>
          </Group>
          {chart.length > 0 ? (
            <Box h={290} role="img" aria-label={chart.map((item) => `${item.name} ${item.speed} KB/s`).join(', ')}>
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={chart} margin={{ top: 8, right: 4, bottom: 4, left: -20 }}>
                  <CartesianGrid stroke="rgba(255,255,255,0.06)" vertical={false} />
                  <XAxis dataKey="name" tick={{ fill: '#8290a7', fontSize: 11 }} axisLine={false} tickLine={false} />
                  <YAxis tick={{ fill: '#8290a7', fontSize: 11 }} axisLine={false} tickLine={false} />
                  <Tooltip
                    cursor={{ fill: 'rgba(38, 169, 255, 0.06)' }}
                    contentStyle={{ background: '#121c2e', border: '1px solid rgba(255,255,255,.1)', borderRadius: 10 }}
                    formatter={(value) => [`${Number(value).toLocaleString()} KB/s`, t('dashboard.speed')]}
                  />
                  <Bar dataKey="speed" fill="#26a9ff" radius={[5, 5, 0, 0]} maxBarSize={38} isAnimationActive={false} />
                </BarChart>
              </ResponsiveContainer>
            </Box>
          ) : (
            <Text c="dimmed" py={80} ta="center">{t('dashboard.chartEmpty')}</Text>
          )}
        </Paper>

        <Paper className="panel routing-panel">
          <Title order={2}>{t('dashboard.currentRoute')}</Title>
          <Text size="sm" c="dimmed" mt={4} mb="lg">{t('dashboard.routeDesc')}</Text>
          <Stack gap={8}>
            {data.nodes.slice(0, 5).map((item, index) => (
              <Group key={item.node.id} className="route-row" wrap="nowrap">
                <Text className="route-rank">{String(index + 1).padStart(2, '0')}</Text>
                <Box c={item.metric.healthy ? 'green.5' : 'red.5'} aria-hidden="true"><IconRoute size={18} /></Box>
                <Box className="route-copy">
                  <Text size="sm" fw={650} truncate>{item.node.name}</Text>
                  <Text size="xs" c="dimmed">{formatRate(item.metric.speed_bps)} · {item.metric.latency_ms}ms</Text>
                </Box>
                <Text size="xs" fw={620} c={item.metric.healthy ? 'green.5' : 'red.5'}>
                  {t(item.metric.healthy ? 'dashboard.available' : 'common.error')}
                </Text>
              </Group>
            ))}
          </Stack>
        </Paper>
      </div>
    </Stack>
  )
}
