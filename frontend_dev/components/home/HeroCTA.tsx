"use client"

import Link from "next/link"
import { TrendingUp, ArrowRight, Sparkles } from "lucide-react"

export function HeroCTA() {
  return (
    <div className="relative overflow-hidden rounded-3xl bg-gradient-to-br from-blue-500/15 via-cyan-500/10 to-blue-500/15 border border-blue-400/30 p-8 sm:p-10 hover:border-blue-400/50 transition-all duration-300 group shadow-2xl shadow-blue-500/10 animate-fade-in">
      {/* Animated background glow */}
      <div className="absolute inset-0 bg-gradient-to-r from-blue-500/0 via-blue-400/10 to-cyan-500/0 opacity-0 group-hover:opacity-100 transition-opacity duration-500" />

      <div className="relative z-10 text-center">
        <div className="inline-flex items-center gap-2 px-4 py-2 rounded-full bg-blue-500/20 border border-blue-400/30 mb-4 sm:mb-5">
          <Sparkles className="h-4 w-4 text-blue-400 animate-pulse" />
          <span className="text-xs sm:text-sm font-semibold text-blue-300">Discover Match-Winning Patterns</span>
        </div>

        <h2 className="text-2xl sm:text-3xl md:text-4xl font-bold mb-3 sm:mb-4 text-slate-900 dark:text-white leading-tight">
          Ready to Explore Winning Streaks?
        </h2>

        <p className="text-sm sm:text-base text-slate-700 dark:text-slate-300/90 mb-6 sm:mb-8 max-w-2xl mx-auto leading-relaxed">
          Dive deep into comprehensive streak analytics across all major leagues. Filter by category, track team
          form, and discover patterns that predict outcomes.
        </p>

        <Link
          href="/explore"
          className="inline-flex items-center gap-2 px-6 sm:px-8 py-3 sm:py-4 rounded-xl sm:rounded-2xl bg-gradient-to-r from-blue-500 to-cyan-500 text-white font-bold text-sm sm:text-base shadow-2xl shadow-blue-500/40 hover:shadow-blue-500/60 hover:scale-105 active:scale-95 transition-all duration-200 group/btn"
        >
          <TrendingUp className="h-5 w-5 transition-transform group-hover/btn:rotate-12" />
          <span>Explore All Streaks</span>
          <ArrowRight className="h-5 w-5 transition-transform group-hover/btn:translate-x-1" />
        </Link>

        <div className="mt-6 sm:mt-8 flex flex-wrap justify-center gap-4 sm:gap-6 text-xs sm:text-sm text-slate-400">
          <div className="flex items-center gap-2">
            <div className="w-2 h-2 rounded-full bg-blue-400" />
            <span>500+ Active Streaks</span>
          </div>
          <div className="flex items-center gap-2">
            <div className="w-2 h-2 rounded-full bg-cyan-400" />
            <span>Real-time Updates</span>
          </div>
          <div className="flex items-center gap-2">
            <div className="w-2 h-2 rounded-full bg-blue-400" />
            <span>Advanced Filters</span>
          </div>
        </div>
      </div>

      {/* Decorative corner accents */}
      <div className="absolute top-0 right-0 w-32 sm:w-40 h-32 sm:h-40 bg-gradient-to-br from-blue-400/10 to-transparent rounded-full blur-3xl" />
      <div className="absolute bottom-0 left-0 w-32 sm:w-40 h-32 sm:h-40 bg-gradient-to-tr from-cyan-400/10 to-transparent rounded-full blur-3xl" />
    </div>
  )
}
