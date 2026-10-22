"use client"

import { ReactNode } from "react"
import { ThemeProvider } from "@/components/theme-provider"
import { ToastProvider } from "@/components/Toast"
import { ErrorBoundary } from "@/components/ErrorBoundary"
import { SWRConfig } from "swr"
import { swrConfig } from "@/lib/swr-config"

/**
 * Client-side providers wrapper
 * Combines all context providers for the application
 */
export function Providers({ children }: { children: ReactNode }) {
  return (
    <ErrorBoundary>
      <ThemeProvider attribute="class" defaultTheme="dark" enableSystem disableTransitionOnChange>
        <SWRConfig value={swrConfig}>
          <ToastProvider>{children}</ToastProvider>
        </SWRConfig>
      </ThemeProvider>
    </ErrorBoundary>
  )
}
