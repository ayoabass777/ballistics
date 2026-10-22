"use client"

import { AlertCircle, RefreshCw } from "lucide-react"

interface ErrorStateProps {
  error: string
  onRetry?: () => void
}

/**
 * Error state component for displaying API errors with retry option
 */
export function ErrorState({ error, onRetry }: ErrorStateProps) {
  return (
    <div className="flex flex-col items-center justify-center py-16 sm:py-20 px-4 text-center animate-fade-in-up">
      <div className="relative mb-6">
        <div className="absolute inset-0 bg-gradient-to-br from-rose-500/20 to-red-500/20 rounded-full blur-2xl animate-pulse" />
        <div className="relative p-6 rounded-2xl bg-gradient-to-br from-rose-500/10 to-red-500/10 border border-rose-400/20">
          <AlertCircle className="h-12 w-12 text-rose-400" strokeWidth={1.5} />
        </div>
      </div>

      <h3 className="text-xl sm:text-2xl font-bold text-slate-900 dark:text-white mb-3">Failed to load streaks</h3>

      <p className="text-sm sm:text-base text-slate-600 dark:text-slate-400 max-w-md mb-2">
        We encountered an error while fetching the data.
      </p>

      {error && (
        <p className="text-xs text-slate-500 dark:text-slate-500 max-w-md mb-6 font-mono">{error}</p>
      )}

      {onRetry && (
        <button
          onClick={onRetry}
          className="inline-flex items-center gap-2 px-5 py-2.5 rounded-xl bg-gradient-to-r from-blue-500 to-cyan-500 text-white font-semibold text-sm hover:scale-105 hover:shadow-xl hover:shadow-blue-500/50 active:scale-95 transition-all duration-200 focus:outline-none focus:ring-2 focus:ring-blue-400 focus:ring-offset-2"
        >
          <RefreshCw className="h-4 w-4" />
          Try Again
        </button>
      )}
    </div>
  )
}
