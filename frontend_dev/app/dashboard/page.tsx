import Link from "next/link"
import { UserButton } from "@clerk/nextjs"
import { auth } from "@clerk/nextjs/server"
import { redirect } from "next/navigation"

type BackendMePayload = {
  data: {
    userId: string
    email: string | null
    firstName: string | null
    lastName: string | null
    imageUrl: string | null
    plan: string
    onboardingCompleted: boolean
  }
  meta: {
    generatedAtIso: string
    authProvider: string
    source?: string
  }
}

async function fetchBackendMe(token: string): Promise<BackendMePayload> {
  const rawBase = process.env.BACKEND_API_BASE_URL || process.env.NEXT_PUBLIC_API_BASE_URL
  if (!rawBase) {
    throw new Error("Backend API base URL is not configured")
  }
  const baseUrl = rawBase.replace(/\/$/, "")

  const response = await fetch(`${baseUrl}/me`, {
    method: "GET",
    headers: {
      Authorization: `Bearer ${token}`,
      Accept: "application/json",
    },
    cache: "no-store",
  })

  if (!response.ok) {
    const bodyText = await response.text()
    throw new Error(`Backend /me failed (${response.status}): ${bodyText}`)
  }

  return response.json()
}

export default async function DashboardPage() {
  const { userId, getToken } = await auth()

  if (!userId) {
    redirect("/sign-in")
  }

  const token = await getToken()
  let backendMe: BackendMePayload | null = null
  let backendError: string | null = null

  if (!token) {
    backendError = "No Clerk session token found for backend sync."
  } else {
    try {
      backendMe = await fetchBackendMe(token)
    } catch (error) {
      backendError = error instanceof Error ? error.message : "Failed to fetch backend user profile"
    }
  }

  return (
    <main className="min-h-screen p-6 sm:p-10">
      <div className="mx-auto max-w-3xl rounded-2xl border border-border/60 bg-background/80 p-6 shadow-sm">
        <div className="flex items-center justify-between gap-4">
          <div>
            <h1 className="text-2xl font-semibold tracking-tight">Dashboard (Protected)</h1>
            <p className="text-sm text-muted-foreground mt-1">
              Clerk is wired on the frontend and synced with API `/me`.
            </p>
          </div>
          <UserButton afterSignOutUrl="/welcome?signed_out=1" />
        </div>

        <div className="mt-6 grid gap-3 text-sm">
          <p>
            Signed in user ID: <code className="rounded bg-muted px-1.5 py-0.5">{userId}</code>
          </p>
          {backendMe && (
            <div className="rounded-xl border border-border/60 bg-secondary/30 p-4 space-y-2">
              <p className="font-medium">Backend profile sync</p>
              <p>
                API user ID: <code className="rounded bg-muted px-1.5 py-0.5">{backendMe.data.userId}</code>
              </p>
              <p>Email: {backendMe.data.email || "N/A"}</p>
              <p>Plan: {backendMe.data.plan}</p>
              <p>Onboarding completed: {backendMe.data.onboardingCompleted ? "Yes" : "No"}</p>
            </div>
          )}
          {backendError && (
            <div className="rounded-xl border border-amber-500/40 bg-amber-500/10 p-3 text-amber-700 dark:text-amber-300">
              Backend sync error: {backendError}
            </div>
          )}
          <div className="flex gap-3">
            <Link className="underline underline-offset-4" href="/">
              Go home
            </Link>
            <Link className="underline underline-offset-4" href="/sign-in">
              Sign in page
            </Link>
            <Link className="underline underline-offset-4" href="/sign-up">
              Sign up page
            </Link>
          </div>
        </div>
      </div>
    </main>
  )
}
