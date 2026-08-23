import type { ReactNode } from 'react'
import { Box, Group, Text, Title } from '@mantine/core'

export function PageHeader({
  title,
  description,
  action,
}: {
  title: string
  description: string
  action?: ReactNode
}) {
  return (
    <Group justify="space-between" align="flex-end" gap="lg" wrap="nowrap" className="page-header">
      <Box maw={720}>
        <Title order={1}>{title}</Title>
        <Text c="dimmed" mt={8} className="page-description">{description}</Text>
      </Box>
      {action}
    </Group>
  )
}
