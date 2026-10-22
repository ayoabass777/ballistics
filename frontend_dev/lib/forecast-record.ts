import { z } from "zod"

export const statusSchema = z.enum(["pending", "hit", "miss", "void", "unavailable", "excluded"])
const time = z.string().datetime({ offset: true })
const evidence = z.object({ fixture: z.object({
  home_team_fulltime_goal: z.number().nullable().optional(),
  away_team_fulltime_goal: z.number().nullable().optional(),
  fixture_status: z.string().nullable().optional(),
}).passthrough().nullable().optional() }).passthrough().nullable()
export const forecastSchema = z.object({
  entry_id: z.number().int().positive(), event_key: z.string(), event_label: z.string().nullable(),
  probability: z.number().min(0).max(1), captured_at: time, captured_kickoff: time,
  status: statusSchema, reason: z.string(), outcome_observed_at: time.nullable(),
  evidence, result_observation_state: z.enum(["observed", "unobserved"]),
})
export const fixtureSchema = z.object({
  fixture_id: z.number().int().positive(), home: z.string().nullable().optional(),
  away: z.string().nullable().optional(), league: z.string().nullable().optional(),
  country: z.string().nullable().optional(), kickoff: time, selection_cutoff: time,
  selection_state: z.enum(["provisional", "final"]), forecasts: z.array(forecastSchema),
})
const count = z.number().int().nonnegative()
export const recordSchema = z.object({
  as_of: time, policy: z.string(), totals: z.object({ fixtures: count, forecasts: count }),
  cohort: z.object({ fixtures: count, forecasts: count, status_counts: z.object({
    pending: count, hit: count, miss: count, void: count, unavailable: count, excluded: count,
  }) }),
  pagination: z.object({ limit: count, offset: count, next_offset: count.nullable() }),
  fixtures: z.array(fixtureSchema),
})
export const directorySchema = z.object({ as_of: time,
  leagues: z.array(z.object({ country: z.string().nullable(), league: z.string() })),
})
export const historySchema = z.object({ as_of: time,
  forecast: z.object({ id: count, probability: z.number(), captured_at: time }),
  observations: z.array(z.object({ id: count, observed_at: time, status: statusSchema,
    reason: z.string(), evidence })), next_after_id: count.nullable(),
})
export type Forecast = z.infer<typeof forecastSchema>
export type Fixture = z.infer<typeof fixtureSchema>
export type ForecastRecord = z.infer<typeof recordSchema>
export type History = z.infer<typeof historySchema>
export const statusLabel: Record<Forecast["status"], string> = {
  pending: "Pending", hit: "Hit", miss: "Miss", void: "Void", unavailable: "Unavailable", excluded: "Excluded",
}
export function score(value: Forecast["evidence"]): string | null {
  const f = value?.fixture
  return f && typeof f.home_team_fulltime_goal === "number" && typeof f.away_team_fulltime_goal === "number"
    ? `${f.home_team_fulltime_goal}–${f.away_team_fulltime_goal}` : null
}
export function reasonLabel(reason: string) {
  const labels: Record<string, string> = {
    not_yet_checked: "No result observation recorded yet.",
    awaiting_regulation_result: "Awaiting a regulation-time result.",
    regulation_time_score: "Graded against the regulation-time score.",
    fixture_cancelled: "The fixture was cancelled.",
    corrected_kickoff_precedes_capture: "The corrected kickoff is at or before capture; excluded from evaluation.",
    unsupported_status_manual_review: "This result needs review before it can be graded.",
    invalid_or_missing_score: "A valid regulation-time score is not available.",
    fixture_or_kickoff_unavailable: "Fixture or kickoff information is unavailable.",
  }
  return labels[reason] || "Result observation requires review."
}
