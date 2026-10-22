import Link from "next/link"
import { ArrowLeft } from "lucide-react"
import { ThemeToggle } from "@/components/theme-toggle"

type TeamUnavailableStateProps = {
  teamSlug: string
  returnTo?: string | null
  message?: string
}

function backLabelFor(returnTo?: string | null): string {
  if (!returnTo) return "Back"
  if (returnTo.startsWith("/fixture/")) return "Back to Fixture"
  if (returnTo.startsWith("/explore")) return "Back to Explore"
  if (returnTo.startsWith("/league/")) return "Back to League"
  if (returnTo === "/") return "Back to Home"
  return "Back"
}

export function TeamUnavailableState({ teamSlug, returnTo, message }: TeamUnavailableStateProps) {
  const backHref = returnTo && returnTo.length > 0 ? returnTo : "/"

  return (
    <div className="min-h-screen font-sans text-foreground bg-background">
      <header className="sticky top-0 z-50 w-full border-b border-border/40">
        <div className="absolute inset-0 bg-background/80 backdrop-blur-xl" />
        <div className="absolute inset-x-0 bottom-0 h-px bg-gradient-to-r from-transparent via-primary/30 to-transparent" />
        <div className="relative max-w-7xl mx-auto px-4 sm:px-6 py-3 flex items-center justify-between">
          <Link href={backHref} className="flex items-center gap-2.5 hover:opacity-70 transition-all group">
            <div className="w-10 h-10 rounded-xl bg-primary/10 border border-primary/20 flex items-center justify-center group-hover:border-primary/40 transition-all">
              <ArrowLeft className="w-5 h-5 text-primary" />
            </div>
            <span className="font-bold text-base text-foreground hidden sm:block">{backLabelFor(returnTo)}</span>
          </Link>
          <ThemeToggle />
        </div>
      </header>

      <main className="px-4 sm:px-6 py-12">
        <div className="max-w-xl mx-auto text-center glass-panel-elevated p-8 sm:p-10 space-y-4">
          <div className="w-14 h-14 rounded-full bg-amber-500/10 border border-amber-500/20 mx-auto flex items-center justify-center">
            <span className="text-2xl text-amber-400">i</span>
          </div>
          <h1 className="text-2xl sm:text-3xl font-bold">Team analysis not available yet</h1>
          <p className="text-muted-foreground">
            {message || "We found the team route, but the analysis data for this team is not available yet."}
          </p>
          <p className="text-xs text-muted-foreground/80 font-mono">team_slug: {teamSlug}</p>
          <div className="pt-2">
            <Link
              href={backHref}
              className="inline-flex items-center gap-2 px-5 py-3 rounded-xl bg-blue-500 hover:bg-blue-600 text-white font-medium transition-colors"
            >
              <ArrowLeft className="w-4 h-4" />
              {backLabelFor(returnTo)}
            </Link>
          </div>
        </div>
      </main>
    </div>
  )
}
