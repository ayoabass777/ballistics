"use client"

import Link from "next/link"
import { TrendingUp, ArrowRight } from "lucide-react"

export function ExploreMoreCTA() {
  return (
    <div className="flex justify-center mt-10 sm:mt-12 animate-fade-in">
      <Link
        href="/explore"
        className="inline-flex items-center gap-2 px-6 sm:px-8 py-3 sm:py-4 rounded-xl bg-gradient-to-r from-blue-500/20 to-cyan-500/20 border border-blue-400/30 hover:border-blue-400/50 text-white font-semibold text-sm sm:text-base hover:scale-105 hover:shadow-xl hover:shadow-blue-500/20 active:scale-95 transition-all duration-200 group/btn backdrop-blur-sm focus:outline-none focus:ring-2 focus:ring-blue-400 focus:ring-offset-2"
      >
        <TrendingUp className="h-5 w-5 transition-transform group-hover/btn:rotate-12" />
        <span>Explore More Streaks</span>
        <ArrowRight className="h-5 w-5 transition-transform group-hover/btn:translate-x-1" />
      </Link>
    </div>
  )
}
