function normalizeBasePath(path: string): string {
  const normalized = path.replace(/\/+$/, '')
  return normalized === '/' ? '' : normalized
}

export function basePathFromModuleUrl(moduleUrl: string): string {
  const pathname = new URL(moduleUrl).pathname
  for (const marker of ['/assets/', '/src/']) {
    const index = pathname.lastIndexOf(marker)
    if (index >= 0) return normalizeBasePath(pathname.slice(0, index))
  }
  return ''
}

export const adminBasePath = basePathFromModuleUrl(import.meta.url)

export function adminUrl(path: string): string {
  const normalized = path.startsWith('/') ? path : `/${path}`
  return `${adminBasePath}${normalized}`
}
