import { z } from 'zod';
const id = z.number().int().positive();
const nullableNumber = z.number().nullable();
export const kindSchema = z.enum(['strength', 'goals', 'patterns']);
export type Kind = z.infer<typeof kindSchema>;
export const labels: Record<Kind, string> = { strength: 'Team strength', goals: 'Goals outlook', patterns: 'Team patterns' };
const pickSchema = z.object({ list: kindSchema, favourite: z.string().nullish(), underdog: z.string().nullish(), team: z.string().nullish(), event: z.string().nullish(), context: z.string().nullish(), k: nullableNumber.optional(), n: nullableNumber.optional(), market: z.string().nullish(), probability: z.number().min(0).max(1).nullish(), big_dog: z.string().nullish() });
export const cardSchema = z.object({ fixture_id: id, home: z.string(), away: z.string(), league_id: id.nullable(), league: z.string(), country: z.string().nullable(), kickoff: z.string().datetime({ offset: true }), home_crest_url: z.string().url().nullable().optional(), away_crest_url: z.string().url().nullable().optional(), league_logo_url: z.string().url().nullable().optional(), pick: pickSchema.nullable(), preview: pickSchema.nullable().optional() });
export type Card = z.infer<typeof cardSchema>;
export const discoverSchema = z.object({ run_ds: z.string().nullable(), run_finished_at: z.string().nullable(), coverage: z.enum(['ready', 'needs_refresh']), timezone: z.string(), window: z.object({ from: z.string(), to: z.string() }), leagues: z.array(z.object({ league_id: id, league: z.string(), country: z.string().nullable(), league_logo_url: z.string().url().nullable().optional() })), matches: z.array(cardSchema).optional(), lists: z.object({ standouts: z.array(cardSchema), strength: z.array(cardSchema), goals: z.array(cardSchema), patterns: z.array(cardSchema) }) });
export type Discover = z.infer<typeof discoverSchema>;
const form = z.object({ attack_rate: nullableNumber, defense_rate: nullableNumber, attack_rank: nullableNumber, defense_rank: nullableNumber, league_size: nullableNumber }).nullable();
const views = z.object({ home: form, away: form, overall: form });
const position = z.object({ rank: nullableNumber, group_size: z.number(), points: nullableNumber, group_name: z.string(), show_group_name: z.boolean(), ranks_repeat: z.boolean(), source: z.enum(['official', 'calculated']).optional() }).nullable();
const venuePosition = z.object({ rank: z.number(), group_size: z.number(), points: z.number(), games_played: z.number(), group_name: z.string(), show_group_name: z.boolean(), source: z.enum(['provider_split', 'calculated']).optional() }).nullable();
const pattern = z.object({ team: z.string(), side: z.string(), event: z.string(), label: z.string(), k: z.number(), n: z.number().positive(), rate: z.number(), league_rate: nullableNumber, league_comparison: z.string().nullable(), streak: nullableNumber, context: z.string() });
const patternView = z.object({ top_results: z.array(pattern), top_attack_defence: z.array(pattern), also: z.array(pattern), see_all: z.array(pattern) }).nullable();
export const matchSchema = z.object({
    fixture: z.object({ fixture_id: id, home: z.string(), away: z.string(), league_id: id.nullable(), league: z.string(), country: z.string().nullable(), kickoff_utc: z.string(), fixture_status: z.string().nullable().optional(), home_team_id: id.nullable(), away_team_id: id.nullable(), league_logo_url: z.string().url().nullable().optional(), home_crest_url: z.string().nullable(), away_crest_url: z.string().nullable(), run_ds: z.string().nullable(), run_finished_at: z.string().nullable(), standings_updated_at: z.string().nullable() }),
    context: z.object({ form: z.object({ home_team: views, away_team: views }).nullable(), position: z.object({ home: position, away: position, venue: z.object({ home: venuePosition, away: venuePosition }).optional(), points_gap: nullableNumber }), table: z.object({ available: z.boolean(), groups: z.array(z.object({ group_name: z.string(), home_available: z.boolean().optional(), away_available: z.boolean().optional(), rows: z.array(z.object({ home: z.any().optional(), away: z.any().optional(), rank: nullableNumber, team_id: id, team_name: z.string(), games_played: nullableNumber, wins: nullableNumber.optional(), draws: nullableNumber.optional(), losses: nullableNumber.optional(), goals_for: nullableNumber.optional(), goals_against: nullableNumber.optional(), goal_diff: nullableNumber, points: nullableNumber, form: z.string().nullable().optional() })) })) }), h2h: z.object({ meetings: z.number(), tally: z.object({ home_wins: z.number(), draws: z.number(), away_wins: z.number() }).optional(), games: z.array(z.object({ fixture_id: id, kickoff_utc: z.string(), home: z.string(), away: z.string(), home_goals: z.number(), away_goals: z.number() })), stats: z.object({ goals_per_game: z.number(), over_2_5: z.number(), both_scored: z.number() }).nullish() }), recent: z.object({ home: z.array(z.object({ fixture_id: id, kickoff_utc: z.string(), league: z.string(), venue: z.enum(['home', 'away']), opponent: z.string(), scored: z.number(), conceded: z.number(), result: z.enum(['W', 'D', 'L']) })), away: z.array(z.object({ fixture_id: id, kickoff_utc: z.string(), league: z.string(), venue: z.enum(['home', 'away']), opponent: z.string(), scored: z.number(), conceded: z.number(), result: z.enum(['W', 'D', 'L']) })) }).optional() }),
    strength: z.object({ state: z.enum(['big_dog', 'slight_favourite', 'no_label', 'close']), favourite: z.string().nullish(), underdog: z.string().nullish(), reason: z.string().nullish(), level: z.enum(['slight_edge', 'favourite']).optional(), tiers: z.object({ home: z.enum(['Top', 'Middle', 'Bottom']), away: z.enum(['Top', 'Middle', 'Bottom']) }).nullish() }).nullable(),
    patterns: z.object({ default_view: z.enum(['venue', 'all']), venue: patternView, all: patternView }).nullable(),
    goals: z.object({ nothing_stands_out: z.boolean(), rows: z.array(z.object({ market: z.string(), probability: z.number().min(0).max(1), league_rate: nullableNumber, league_comparison: z.string().nullable(), league_rate_context: z.string().nullable() })) }).nullable(), discover: z.object({ list: kindSchema, team: z.string().nullish(), event: z.string().nullish() }).nullable(),
    // D148 data decorators: issues the API found while building this page.
    data_quality: z.object({ ok: z.boolean(), issues: z.array(z.object({ code: z.string(), section: z.string(), severity: z.string(), message: z.string() })) }).optional()
});
export type Match = z.infer<typeof matchSchema>;
export type Pattern = z.infer<typeof pattern>;
export const savedSchema = z.array(z.object({ card: cardSchema, saved_at: z.string().datetime({ offset: true }), run_ds: z.string().nullable(), snapshot_id: z.number().optional(), status: z.enum(["upcoming", "live", "finished", "postponed", "cancelled", "abandoned", "suspended", "interrupted", "awarded", "walkover", "unknown"]).optional(), current_kickoff: z.string().nullable().optional(), card_version: z.number().nullable().optional(), latest_version: z.number().nullable().optional(), content_hash: z.string().optional(), updated_since_saved: z.boolean().optional() }));
export type Saved = z.infer<typeof savedSchema>;
export const eventLabels: Record<string, string> = { win: 'Won', loss: 'Lost', draw: 'Drew', unbeaten: 'Unbeaten', winless: 'Winless', goalless: 'Failed to score', clean_sheet: 'Clean sheet', score_1goal: 'Scored 1+', score_2goals: 'Scored 2+', concede_1: 'Conceded 1+', concede_2: 'Conceded 2+', BTTS: 'Both teams scored' };
export function eventLabel(event: string) { return eventLabels[event] || event.replace(/^(Over|Under)_(\d)_(\d)$/, '$1 $2.$3 goals'); }
export function headline(card: Card) { const p = card.pick; if (!p)
    return `${card.home} vs ${card.away}`; if (p.list === 'strength')
    return `${p.favourite} · Big dog`; if (p.list === 'goals')
    return `${p.market} · ${Math.round((p.probability ?? 0) * 100)}%`; return `${p.team} · ${eventLabel(p.event || '')} ${p.k}/${p.n}${p.context && p.context !== 'all' ? ` · ${p.context}` : ''}`; }
export function reason(card: Card) { return card.pick?.list === 'strength' ? `Clear strength advantage for ${card.pick.favourite}.` : headline(card); }
export function stamp(value: string, timeZone?: string) { return new Intl.DateTimeFormat('en', { weekday: 'short', day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit', timeZoneName: 'short', timeZone }).format(new Date(value)); }
/** Only internal research lists can be a match's return destination. */
export function matchReturnTo(value: string | null | undefined) {
    if (!value || !value.startsWith('/') || value.startsWith('//') || value.includes('\\')) return '/discover';
    try {
        const url = new URL(value, 'https://ballistics.invalid');
        if (url.origin !== 'https://ballistics.invalid' || !['/discover', '/matches', '/standouts', '/saved'].includes(url.pathname)) return '/discover';
        return `${url.pathname}${url.search}`;
    } catch { return '/discover'; }
}
export function matchHref(card: Card, entry = true, returnTo?: string) {
    const p = card.pick;
    const params = new URLSearchParams(entry && p ? { section: p.list, team: p.team || '', event: p.event || p.market || '', context: p.context || '' } : {});
    if (returnTo) params.set('returnTo', matchReturnTo(returnTo));
    return `/match/${card.fixture_id}${params.size ? `?${params}` : ''}`;
}
export function toggleSaved(items: Saved, card: Card, run: string | null, now = new Date().toISOString()): Saved { return items.some(x => x.card.fixture_id === card.fixture_id) ? items.filter(x => x.card.fixture_id !== card.fixture_id) : [...items, { card: structuredClone(card), saved_at: now, run_ds: run }]; }
