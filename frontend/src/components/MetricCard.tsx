import type { ReactNode } from 'react'
import { Box, Group, Paper, Text } from '@mantine/core'

export function MetricCard({
  label,
  value,
  detail,
  icon,
  index = 0,
}: {
  label: string
  value: string
  detail: string
  icon: ReactNode
  index?: number
}) {
  return (
    <Paper className="metric-card entrance-card" data-index={index}>
      <Group gap={7} c="dimmed" wrap="nowrap">
        <Box component="span" className="metric-icon" aria-hidden="true">{icon}</Box>
        <Text size="sm" fw={620}>{label}</Text>
      </Group>
      <Text className="metric-value" mt={12}>{value}</Text>
      <Text size="xs" c="dimmed" mt={7}>{detail}</Text>
    </Paper>
  )
}
