"use client"

import { useRouter } from "next/navigation"
import { ArrowLeft, Crosshair, ChevronRight } from "lucide-react"
import Link from "next/link"
import { ThemeToggle } from "@/components/theme-toggle"

interface FixtureHeaderProps {
  returnTo?: string | null
}

export function FixtureHeader({ returnTo }: FixtureHeaderProps) {
  const router = useRouter()

  const handleBack = () => {
    if (returnTo && returnTo.length > 0) {
      router.push(returnTo)
    } else {
      // Check if we can go back in history, otherwise go to home
      if (typeof window !== "undefined" && window.history.length > 1) {
        router.back()
      } else {
        router.push("/")
      }
    }
  }

  // Determine the label based on returnTo
  const getBackLabel = () => {
    if (!returnTo) return "Back"
    if (returnTo.startsWith("/explore")) return "Back to Explore"
    if (returnTo === "/") return "Back to Home"
    return "Back"
  }
  const backLabel = getBackLabel()

  return (
    <header className="sticky top-0 z-50 w-full border-b border-border/40">
      <div className="absolute inset-0 bg-background/80 backdrop-blur-xl" />
      <div className="absolute inset-x-0 bottom-0 h-px bg-gradient-to-r from-transparent via-primary/30 to-transparent" />
      <div className="relative max-w-7xl mx-auto px-4 sm:px-6 py-3 grid grid-cols-3 items-center">
        {/* Left column - Back button */}
        <div className="flex justify-start">
          <button onClick={handleBack} className="flex items-center gap-2.5 hover:opacity-70 transition-all group">
            <div className="w-10 h-10 rounded-xl bg-primary/10 dark:bg-primary/20 border border-primary/20 dark:border-primary/30 flex items-center justify-center group-hover:border-primary/40 transition-all">
              <ArrowLeft className="w-5 h-5 text-primary" />
            </div>
            <span className="font-bold text-base text-foreground hidden sm:block">{backLabel}</span>
          </button>
        </div>

        {/* Center column - Ballistics icon */}
        <div className="flex justify-center">
          <Link href="/" className="relative p-1.5 sm:p-2 rounded-lg bg-primary/10 dark:bg-primary/20 border border-primary/20 dark:border-primary/30 shadow-lg shadow-primary/10 hover:scale-105 transition-transform">
            <Crosshair className="h-4 w-4 sm:h-5 sm:w-5 text-primary" />
          </Link>
        </div>

        {/* Right column - Explore Streaks button (hidden when coming from explore) and Theme Toggle */}
        <div className="flex items-center justify-end gap-2 sm:gap-3">
          {!returnTo?.startsWith("/explore") && (
            <Link
              href="/explore"
              className="group flex items-center gap-2 px-3 sm:px-4 py-2 rounded-lg text-xs sm:text-sm font-medium text-foreground bg-secondary/60 dark:bg-secondary/40 hover:bg-secondary/80 dark:hover:bg-secondary/60 border border-border/50 hover:border-primary/30 transition-all duration-200"
            >
              <span className="hidden sm:inline">Explore Streaks</span>
              <span className="sm:hidden">Explore</span>
              <ChevronRight className="h-3.5 w-3.5 sm:h-4 sm:w-4 group-hover:translate-x-1 transition-transform" />
            </Link>
          )}
          <ThemeToggle />
        </div>
      </div>
    </header>
  )
}
