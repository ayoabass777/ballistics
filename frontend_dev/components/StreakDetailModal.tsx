"use client"

import { useEffect } from "react"
import { X, TrendingUp, Calendar, MapPin, Home, Plane } from "lucide-react"
import type { StreakData } from "@/types"

interface StreakDetailModalProps {
  isOpen: boolean
  onClose: () => void
  streak: StreakData | null
}

/**
 * Modal component for displaying detailed streak information
 * Shows comprehensive stats without leaving the page
 */
export function StreakDetailModal({ isOpen, onClose, streak }: StreakDetailModalProps) {
  // Handle ESC key to close modal
  useEffect(() => {
    const handleEsc = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose()
    }
    if (isOpen) {
      document.addEventListener("keydown", handleEsc)
      document.body.style.overflow = "hidden"
    }
    return () => {
      document.removeEventListener("keydown", handleEsc)
      document.body.style.overflow = "unset"
    }
  }, [isOpen, onClose])

  if (!isOpen || !streak) return null

  const scopeConfig = {
    overall: { icon: TrendingUp, label: "Overall Streak", color: "text-blue-400" },
    home: { icon: Home, label: "Home Streak", color: "text-blue-400" },
    away: { icon: Plane, label: "Away Streak", color: "text-cyan-400" },
  }

  const config = scopeConfig[streak.scope || "overall"]
  const ScopeIcon = config.icon

  return (
    <div
      className="fixed inset-0 z-[100] flex items-center justify-center p-4 animate-fade-in"
      onClick={onClose}
      role="dialog"
      aria-modal="true"
      aria-labelledby="modal-title"
    >
      {/* Backdrop */}
      <div className="absolute inset-0 bg-black/60 backdrop-blur-sm" />

      {/* Modal Content */}
      <div
        className="relative max-w-2xl w-full max-h-[90vh] overflow-y-auto bg-amber-50/95 dark:bg-slate-900/95 backdrop-blur-xl border border-stone-300/50 dark:border-white/10 rounded-2xl shadow-2xl animate-scale-in"
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div className="sticky top-0 z-10 flex items-center justify-between p-6 border-b border-stone-300/30 dark:border-white/10 bg-amber-50/95 dark:bg-slate-900/95 backdrop-blur-xl">
          <div className="flex items-center gap-4">
            {streak.team_logo_url && (
              <img
                src={streak.team_logo_url}
                alt={`${streak.team_name} logo`}
                className="h-12 w-12 rounded-full bg-gradient-to-br from-blue-500/5 to-cyan-500/5 p-2 ring-2 ring-blue-400/20"
              />
            )}
            <div>
              <h2 id="modal-title" className="text-xl font-bold text-slate-900 dark:text-white">
                {streak.team_name}
              </h2>
              <p className="text-sm text-slate-600 dark:text-slate-400">{streak.league_name}</p>
            </div>
          </div>
          <button
            onClick={onClose}
            className="p-2 rounded-lg hover:bg-stone-200/50 dark:hover:bg-white/10 transition-colors focus:outline-none focus:ring-2 focus:ring-blue-400"
            aria-label="Close modal"
          >
            <X className="h-5 w-5 text-slate-700 dark:text-slate-300" />
          </button>
        </div>

        {/* Content */}
        <div className="p-6 space-y-6">
          {/* Streak Headline */}
          <div className="relative bg-gradient-to-br from-blue-500/15 via-cyan-500/10 to-blue-500/15 border-2 border-blue-400/30 rounded-xl p-6">
            <div className="absolute top-0 right-0 w-32 h-32 bg-cyan-500/10 rounded-full blur-3xl" />
            <div className="relative flex items-start gap-3 mb-3">
              <ScopeIcon className={`h-6 w-6 ${config.color} flex-shrink-0 mt-1`} />
              <div>
                <p className="text-xs font-bold text-blue-400 uppercase tracking-wider mb-1">{config.label}</p>
                <p className="text-lg font-extrabold text-slate-900 dark:text-white leading-snug">
                  {streak.headline}
                </p>
              </div>
            </div>
            {streak.secondary_streak && (
              <p className="text-sm text-slate-700 dark:text-slate-300 mt-2 pl-9">{streak.secondary_streak}</p>
            )}
          </div>

          {/* Recent Form */}
          {streak.recent_form && streak.recent_form.length > 0 && (
            <div className="space-y-3">
              <h3 className="text-sm font-bold text-slate-900 dark:text-white uppercase tracking-wider">
                Recent Form
              </h3>
              <div className="flex items-center gap-2">
                {streak.recent_form.map((result, idx) => {
                  const styles = {
                    W: "bg-emerald-400 text-white shadow-emerald-400/50",
                    D: "bg-amber-400 text-white shadow-amber-400/50",
                    L: "bg-rose-400 text-white shadow-rose-400/50",
                  }
                  return (
                    <div
                      key={idx}
                      className={`h-10 w-10 rounded-lg ${styles[result]} shadow-lg flex items-center justify-center font-bold text-sm`}
                    >
                      {result}
                    </div>
                  )
                })}
              </div>
            </div>
          )}

          {/* Next Match */}
          {streak.next_opponent && (
            <div className="space-y-3">
              <h3 className="text-sm font-bold text-slate-900 dark:text-white uppercase tracking-wider flex items-center gap-2">
                <Calendar className="h-4 w-4 text-blue-400" />
                Next Match
              </h3>
              <div className="flex items-center justify-between p-4 rounded-xl bg-gradient-to-r from-blue-500/5 to-cyan-500/5 border border-blue-400/20">
                <div className="flex items-center gap-3">
                  <div className="h-10 w-10 rounded-full bg-gradient-to-br from-blue-500/20 to-cyan-500/20 flex items-center justify-center">
                    <span className="text-sm font-bold text-blue-400">VS</span>
                  </div>
                  <div>
                    <p className="font-bold text-slate-900 dark:text-white">{streak.next_opponent}</p>
                    {streak.next_match_date && (
                      <p className="text-xs text-slate-600 dark:text-slate-400">
                        {new Date(streak.next_match_date).toLocaleString(undefined, {
                          month: "short",
                          day: "numeric",
                          hour: "numeric",
                          minute: "2-digit",
                        })}
                      </p>
                    )}
                  </div>
                </div>
              </div>
            </div>
          )}

          {/* League Info */}
          <div className="flex items-center gap-4 p-4 rounded-xl bg-stone-100/50 dark:bg-white/5 border border-stone-300/30 dark:border-white/10">
            {streak.league_logo_url && (
              <img src={streak.league_logo_url} alt="" className="h-8 w-8 object-contain" />
            )}
            <div className="flex-1">
              <p className="text-sm font-bold text-slate-900 dark:text-white">{streak.league_name}</p>
              {streak.country_name && (
                <div className="flex items-center gap-1.5 mt-1">
                  {streak.country_logo_url && (
                    <img src={streak.country_logo_url} alt="" className="h-3 w-3 object-contain" />
                  )}
                  <p className="text-xs text-slate-600 dark:text-slate-400">{streak.country_name}</p>
                </div>
              )}
            </div>
          </div>
        </div>

        {/* Footer */}
        <div className="sticky bottom-0 p-6 border-t border-stone-300/30 dark:border-white/10 bg-amber-50/95 dark:bg-slate-900/95 backdrop-blur-xl">
          <button
            onClick={onClose}
            className="w-full px-4 py-3 rounded-xl bg-gradient-to-r from-blue-500 to-cyan-500 text-white font-bold text-sm shadow-lg shadow-blue-500/30 hover:shadow-xl hover:shadow-blue-500/50 hover:scale-[1.02] active:scale-[0.98] transition-all focus:outline-none focus:ring-2 focus:ring-blue-400 focus:ring-offset-2"
          >
            Close
          </button>
        </div>
      </div>
    </div>
  )
}
