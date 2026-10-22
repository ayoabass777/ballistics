const BACKEND_API_BASE_URL = process.env.BACKEND_API_BASE_URL?.replace(/\/$/, "")

function normalizeFileId(value: string | undefined | null): string | null {
  const normalized = value?.trim()
  return normalized ? normalized : null
}

export function frontendLogoAssetPath(fileId: string | undefined | null): string | undefined {
  const normalized = normalizeFileId(fileId)
  if (!normalized) return undefined
  return `/assets/logos/${normalized}`
}

export function backendLogoAssetUrl(fileId: string | undefined | null): string | undefined {
  const normalized = normalizeFileId(fileId)
  if (!normalized || !BACKEND_API_BASE_URL) return undefined
  return `${BACKEND_API_BASE_URL}/assets/logos/${normalized}`
}

export function rewriteAssetUrlToFrontend(url: string | undefined | null): string | undefined {
  const value = url?.trim()
  if (!value) return undefined

  try {
    const parsed = new URL(value)
    if (parsed.pathname.startsWith("/assets/logos/")) {
      return parsed.pathname
    }
  } catch {
    if (value.startsWith("/assets/logos/")) {
      return value
    }
  }

  return value
}
