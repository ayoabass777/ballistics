/**
 * Shared API response structures
 * These types represent common patterns in API responses
 */

/**
 * Standard pagination for list endpoints
 */
export interface ApiPagination {
  offset: number
  limit: number
  total: number
  hasMore: boolean
}

/**
 * Standard metadata for all API responses
 */
export interface ApiMeta {
  generatedAtIso: string // When streak engine computed this data
  cacheExpiresAtIso?: string // When cached response expires
}

/**
 * Standard error response
 */
export interface ApiError {
  error: string
  code: string
  details?: string
}

/**
 * Generic wrapper for paginated responses
 */
export interface ApiPaginatedResponse<T> {
  data: T[]
  pagination: ApiPagination
  meta: ApiMeta
}

/**
 * Generic wrapper for single-item responses
 */
export interface ApiResponse<T> {
  data: T
  meta: ApiMeta
}
