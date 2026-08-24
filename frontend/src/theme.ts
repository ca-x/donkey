import { createTheme, rem } from '@mantine/core'

export const theme = createTheme({
  respectReducedMotion: true,
  primaryColor: 'blue',
  primaryShade: { light: 8, dark: 8 },
  fontFamily: 'Inter, ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif',
  fontFamilyMonospace: '"SFMono-Regular", Consolas, "Liberation Mono", monospace',
  defaultRadius: 'md',
  cursorType: 'pointer',
  headings: {
    fontFamily: 'Inter, ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif',
    fontWeight: '680',
    sizes: {
      h1: { fontSize: rem(30), lineHeight: '1.15' },
      h2: { fontSize: rem(21), lineHeight: '1.25' },
      h3: { fontSize: rem(17), lineHeight: '1.3' },
    },
  },
  colors: {
    donkey: [
      '#edf8ff',
      '#d8efff',
      '#abdfff',
      '#78ceff',
      '#4cbeff',
      '#2eb5ff',
      '#13afff',
      '#0099e5',
      '#0088ce',
      '#0076b7',
    ],
  },
})
