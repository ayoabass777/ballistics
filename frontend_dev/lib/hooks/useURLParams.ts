import { useCallback, useEffect } from "react"
import { useRouter, useSearchParams } from "next/navigation"

interface URLParamsConfig {
  search?: string
  league?: string
}

interface UseURLParamsReturn {
  updateURLParams: (params: URLParamsConfig) => void
  getURLParam: (key: string) => string | null
}

/**
 * Custom hook for managing URL search parameters
 * Enables shareable URLs with search and filter state
 */
export function useURLParams(): UseURLParamsReturn {
  const router = useRouter()
  const searchParams = useSearchParams()

  const updateURLParams = useCallback(
    (params: URLParamsConfig) => {
      const current = new URLSearchParams(Array.from(searchParams.entries()))

      // Update or remove search param
      if (params.search !== undefined) {
        if (params.search) {
          current.set("search", params.search)
        } else {
          current.delete("search")
        }
      }

      // Update or remove league param
      if (params.league !== undefined) {
        if (params.league && params.league !== "all") {
          current.set("league", params.league)
        } else {
          current.delete("league")
        }
      }

      // Update URL without full page reload
      const search = current.toString()
      const query = search ? `?${search}` : ""
      router.push(`/${query}`, { scroll: false })
    },
    [router, searchParams],
  )

  const getURLParam = useCallback(
    (key: string) => {
      return searchParams.get(key)
    },
    [searchParams],
  )

  return {
    updateURLParams,
    getURLParam,
  }
}
