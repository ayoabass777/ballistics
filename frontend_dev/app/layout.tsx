import type React from "react"
import type { Metadata } from "next"
import { ClerkProvider } from "@clerk/nextjs"
import localFont from "next/font/local"
import "./globals.css"
import { PreferencesProvider } from "@/components/research/PreferencesProvider"
import { Providers } from "@/components/Providers"
import { AccountProfileSync } from "@/components/account/AccountProfileSync"

const inter = localFont({ src: "./fonts/Inter-variable.ttf", weight: "100 900", style: "normal", display: "swap" })

export const metadata: Metadata = {
  title: "Ballistics - Football Analytics",
  description: "Real-time football streak analysis and match insights powered by advanced analytics.",
  generator: "v0.dev",
}

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode
}>) {
  return (
    <html lang="en" suppressHydrationWarning>
      <body className={inter.className}>
        <ClerkProvider afterSignOutUrl="/welcome?signed_out=1">
          <Providers><PreferencesProvider><AccountProfileSync />{children}</PreferencesProvider></Providers>
        </ClerkProvider>
      </body>
    </html>
  )
}
