"use client"

import { RefreshCw } from "lucide-react"

interface RefreshIndicatorProps {
  isRefreshing: boolean
}

/**
 * Small indicator shown when data is being revalidated in the background
 */
export function RefreshIndicator({ isRefreshing }: RefreshIndicatorProps) {
  if (!isRefreshing) return null

  return (
    <div
      className="fixed top-20 right-4 z-40 px-3 py-2 rounded-xl bg-blue-500/90 backdrop-blur-xl border border-blue-400/50 shadow-lg shadow-blue-500/20 animate-fade-in"
      role="status"
      aria-live="polite"
    >
      <div className="flex items-center gap-2">
        <RefreshCw className="h-3.5 w-3.5 text-white animate-spin" />
        <span className="text-xs font-medium text-white">Updating...</span>
      </div>
    </div>
  )
}
