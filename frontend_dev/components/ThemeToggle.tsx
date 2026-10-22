"use client"

import { Sun, Moon } from 'lucide-react'
import { useTheme } from "next-themes"
import { GlassButton } from "@/components/GlassButton"

export function ThemeToggle() {
  const { theme, setTheme } = useTheme()

  return (
    <GlassButton
      variant="secondary"
      size="sm"
      onClick={() => setTheme(theme === "dark" ? "light" : "dark")}
      className="p-2 rounded-full hover:rotate-12 transition-transform duration-300"
      aria-label="Toggle theme"
    >
      {theme === "dark" ? (
        <Sun className="h-5 w-5 text-amber-400" />
      ) : (
        <Moon className="h-5 w-5 text-blue-400" />
      )}
    </GlassButton>
  )
}
