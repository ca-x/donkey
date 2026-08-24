import { Button, Group, Modal, Stack, Text } from '@mantine/core'
import { useEffect, useRef } from 'react'
import { useTranslation } from 'react-i18next'

type NamedConfirmDialogProps = {
  opened: boolean
  title: string
  name: string
  consequence: string
  loading: boolean
  onCancel: () => void
  onConfirm: () => void
  confirmLabel?: string
}

export function NamedConfirmDialog({
  opened,
  title,
  name,
  consequence,
  loading,
  onCancel,
  onConfirm,
  confirmLabel,
}: NamedConfirmDialogProps) {
  const { t } = useTranslation()
  const confirming = useRef(false)
  useEffect(() => {
    if (!opened || !loading) confirming.current = false
  }, [loading, opened])
  const cancel = () => {
    if (!loading) onCancel()
  }
  const confirm = () => {
    if (loading || confirming.current) return
    confirming.current = true
    onConfirm()
  }

  return (
    <Modal
      opened={opened}
      onClose={cancel}
      title={title}
      centered
      closeOnClickOutside={!loading}
      closeOnEscape={!loading}
      classNames={{ content: 'polished-modal', overlay: 'polished-overlay' }}
      transitionProps={{ transition: 'pop', duration: 180, timingFunction: 'cubic-bezier(0.23, 1, 0.32, 1)' }}
    >
      <Stack gap="md">
        <div>
          <Text fw={680} style={{ overflowWrap: 'anywhere' }}>{name}</Text>
          <Text c="dimmed" size="sm" mt={6}>{consequence}</Text>
        </div>
        <Group justify="flex-end">
          <Button variant="default" data-autofocus disabled={loading} onClick={cancel}>{t('common.cancel')}</Button>
          <Button color="red" loading={loading} onClick={confirm}>{confirmLabel ?? t('common.delete')}</Button>
        </Group>
      </Stack>
    </Modal>
  )
}
