type RedisRestResponse<T = unknown> = {
  result?: T
  error?: string
}

export type RedisCacheReadStatus = "disabled" | "hit" | "miss" | "error"
export type RedisCacheWriteStatus = "skipped" | "stored" | "error"

export type RedisCacheMeta = {
  status: RedisCacheReadStatus
  storeStatus: RedisCacheWriteStatus
}

type RedisCacheResult<T> = {
  value: T
  meta: RedisCacheMeta
}

const DEFAULT_NAMESPACE = "ballistics:frontend:v1"

function redisRestUrl(): string | undefined {
  return (process.env.UPSTASH_REDIS_REST_URL || process.env.REDIS_REST_URL || process.env.FRONTEND_REDIS_REST_URL)
    ?.trim()
    .replace(/\/$/, "")
}

function redisRestToken(): string | undefined {
  return (
    process.env.UPSTASH_REDIS_REST_TOKEN ||
    process.env.REDIS_REST_TOKEN ||
    process.env.FRONTEND_REDIS_REST_TOKEN
  )?.trim()
}

function redisEnabled(): boolean {
  const enabled = String(process.env.FRONTEND_REDIS_CACHE_ENABLED || "true").trim().toLowerCase()
  return enabled !== "false" && enabled !== "0" && enabled !== "off"
}

function redisConfigured(): { url: string; token: string } | null {
  if (!redisEnabled()) return null
  const url = redisRestUrl()
  const token = redisRestToken()
  if (!url || !token) return null
  return { url, token }
}

function cacheNamespace(): string {
  return (process.env.FRONTEND_REDIS_CACHE_NAMESPACE || DEFAULT_NAMESPACE).trim() || DEFAULT_NAMESPACE
}

function normalizeTtlSeconds(ttlSeconds: number): number {
  if (!Number.isFinite(ttlSeconds)) return 0
  return Math.max(0, Math.trunc(ttlSeconds))
}

function parseEnvInt(name: string, fallback: number): number {
  const parsed = Number(process.env[name])
  if (!Number.isFinite(parsed)) return fallback
  return Math.max(0, Math.trunc(parsed))
}

async function redisCommand<T>(command: unknown[]): Promise<RedisRestResponse<T>> {
  const config = redisConfigured()
  if (!config) return { error: "redis_disabled" }

  const response = await fetch(`${config.url}/pipeline`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${config.token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify([command]),
    cache: "no-store",
  })

  if (!response.ok) {
    return { error: `redis_http_${response.status}` }
  }

  const payload = (await response.json()) as Array<RedisRestResponse<T>>
  return payload[0] || { error: "redis_empty_response" }
}

async function readJsonFromRedis<T>(key: string): Promise<{ status: RedisCacheReadStatus; value?: T }> {
  if (!redisConfigured()) return { status: "disabled" }

  try {
    const payload = await redisCommand<string | null>(["GET", key])
    if (payload.error) return { status: "error" }
    if (payload.result === null || payload.result === undefined) return { status: "miss" }
    return { status: "hit", value: JSON.parse(payload.result) as T }
  } catch (error) {
    console.warn("Redis cache read failed:", error)
    return { status: "error" }
  }
}

async function writeJsonToRedis(key: string, value: unknown, ttlSeconds: number): Promise<RedisCacheWriteStatus> {
  if (!redisConfigured() || normalizeTtlSeconds(ttlSeconds) <= 0) return "skipped"

  try {
    const payload = await redisCommand<"OK">(["SET", key, JSON.stringify(value), "EX", normalizeTtlSeconds(ttlSeconds)])
    return payload.error ? "error" : "stored"
  } catch (error) {
    console.warn("Redis cache write failed:", error)
    return "error"
  }
}

export function redisCacheKey(...parts: Array<string | number | boolean | null | undefined>): string {
  const normalized = parts
    .filter((part) => part !== null && part !== undefined && String(part).trim().length > 0)
    .map((part) => encodeURIComponent(String(part)))
  return [cacheNamespace(), ...normalized].join(":")
}

export function redisCacheTtl(envName: string, fallbackSeconds: number): number {
  return parseEnvInt(envName, fallbackSeconds)
}

export function redisCacheHeader(meta: RedisCacheMeta): string {
  if (meta.status === "hit") return "hit"
  if (meta.status === "miss" && meta.storeStatus === "stored") return "miss-stored"
  if (meta.status === "disabled") return "disabled"
  if (meta.status === "error" || meta.storeStatus === "error") return "error"
  return "miss"
}

export async function getOrSetRedisJson<T>(
  key: string,
  ttlSeconds: number,
  loader: () => Promise<T>,
): Promise<RedisCacheResult<T>> {
  const read = await readJsonFromRedis<T>(key)
  if (read.status === "hit" && read.value !== undefined) {
    return {
      value: read.value,
      meta: { status: "hit", storeStatus: "skipped" },
    }
  }

  const value = await loader()
  const storeStatus =
    read.status === "disabled" || normalizeTtlSeconds(ttlSeconds) <= 0
      ? "skipped"
      : await writeJsonToRedis(key, value, ttlSeconds)

  return {
    value,
    meta: {
      status: read.status,
      storeStatus,
    },
  }
}
