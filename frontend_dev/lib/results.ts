import { z } from 'zod';
// Results page: one local day of played games (engine/api/results.py).
const id = z.number().int().positive();
const goals = z.number().int().nullable();
const day = z.string().regex(/^\d{4}-\d{2}-\d{2}$/);
export const resultSchema = z.object({ fixture_id: id, status: z.string(), home: z.string(), away: z.string(), home_goals: goals, away_goals: goals, home_ht: goals, away_ht: goals, league_id: id.nullable(), league: z.string(), country: z.string().nullable(), kickoff: z.string(), home_crest_url: z.string().url().nullable(), away_crest_url: z.string().url().nullable(), league_logo_url: z.string().url().nullable() });
export type Result = z.infer<typeof resultSchema>;
export const resultsSchema = z.object({ day: day.nullable(), zone: z.string(), previous_day: day.nullable(), next_day: day.nullable(), matches: z.array(resultSchema) });
export type Results = z.infer<typeof resultsSchema>;
/** FT / AET / Pens, as the live list shows a finished match. */
export const finalLabel = (status: string) => status === 'AET' ? 'AET' : status === 'PEN' ? 'Pens' : status === 'AWD' ? 'Awarded' : status === 'WO' ? 'W/O' : 'FT';
/** 'home' | 'away' | 'draw' | null — who won, for bolding the winner. */
export function winner(r: Pick<Result, 'home_goals' | 'away_goals'>) {
    if (r.home_goals == null || r.away_goals == null) return null;
    return r.home_goals > r.away_goals ? 'home' : r.away_goals > r.home_goals ? 'away' : 'draw';
}
