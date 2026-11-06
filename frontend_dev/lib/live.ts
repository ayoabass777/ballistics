import { z } from 'zod';
import { pickSchema, eventLabel as patternLabel } from './research';
// Live scores (docs/live_scores.md, phase 1). The forecast never changes after
// kickoff (D152); this file compares it with the live score.
const id = z.number().int().positive();
const count = z.number().int().nullable();
export const liveStateSchema = z.object({ fixture_id: id, status: z.string(), minute: count, extra: count, home_goals: count, away_goals: count, home_red_cards: z.number().int(), away_red_cards: z.number().int(), finished: z.boolean(), changed_at: z.string().nullable(), polled_at: z.string().nullable() });
export type LiveState = z.infer<typeof liveStateSchema>;
export const liveCardSchema = liveStateSchema.extend({ pick: pickSchema.nullable(), home: z.string(), away: z.string(), league_id: id.nullable(), league: z.string(), country: z.string().nullable(), kickoff: z.string(), home_crest_url: z.string().url().nullable(), away_crest_url: z.string().url().nullable(), league_logo_url: z.string().url().nullable() });
export type LiveCard = z.infer<typeof liveCardSchema>;
export const liveListSchema = z.object({ matches: z.array(liveCardSchema) });
export const liveEventSchema = z.object({ minute: count, extra: count, side: z.enum(['home', 'away']).nullable(), type: z.string(), detail: z.string().nullable(), player: z.string().nullable(), assist: z.string().nullable() });
export type LiveEvent = z.infer<typeof liveEventSchema>;
export const liveFixtureSchema = z.object({ state: liveStateSchema.nullable(), events: z.array(liveEventSchema) });
export type LiveFixture = z.infer<typeof liveFixtureSchema>;

export const IN_PLAY = new Set(['1H', 'HT', '2H', 'ET', 'BT', 'P', 'SUSP', 'INT', 'LIVE']);
export const FINAL = new Set(['FT', 'AET', 'PEN']);
const EXTRA_TIME = new Set(['ET', 'BT', 'P', 'AET', 'PEN']);
const STOPPED: Record<string, string> = { SUSP: 'Suspended', INT: 'Interrupted', PST: 'Postponed', CANC: 'Cancelled', ABD: 'Abandoned', AWD: 'Awarded', WO: 'Walkover' };
/** Poll cadence: the provider refreshes live data about every 15 s. */
export const LIVE_POLL_MS = 15000;
/** Live data older than this is shown as paused, not as the current score. */
export const STALE_AFTER_MS = 90000;

export function clockLabel(s: Pick<LiveState, 'status' | 'minute' | 'extra'>) {
    if (s.status === 'HT') return 'HT';
    if (s.status === 'BT') return 'ET break';
    if (s.status === 'P') return 'Penalties';
    if (FINAL.has(s.status)) return s.status === 'FT' ? 'FT' : s.status === 'AET' ? 'AET' : 'FT (pens)';
    if (STOPPED[s.status]) return STOPPED[s.status];
    if (s.minute == null) return 'Live';
    return s.extra ? `${s.minute}+${s.extra}'` : `${s.minute}'`;
}
export const isLive = (s: Pick<LiveState, 'status'>) => IN_PLAY.has(s.status);
export const isStale = (s: Pick<LiveState, 'polled_at' | 'finished'>, now = Date.now()) => !s.finished && s.polled_at != null && now - Date.parse(s.polled_at) > STALE_AFTER_MS;
export function eventMinute(e: Pick<LiveEvent, 'minute' | 'extra'>) { return e.minute == null ? '' : e.extra ? `${e.minute}+${e.extra}'` : `${e.minute}'`; }
export function eventLabel(e: LiveEvent) {
    if (e.type === 'Goal') return e.detail === 'Own Goal' ? 'Own goal' : e.detail === 'Penalty' ? 'Penalty goal' : e.detail === 'Missed Penalty' ? 'Missed penalty' : 'Goal';
    if (e.type === 'Card') return e.detail === 'Red Card' ? 'Red card' : e.detail === 'Second Yellow card' ? 'Second yellow' : 'Yellow card';
    if (e.type === 'Var') return `VAR · ${e.detail || 'review'}`;
    return e.detail || e.type;
}

// ---- Forecast tracker ------------------------------------------------------
/** landed / missed cannot change; on_track / off_track describe the score now. */
export type TrackStatus = 'landed' | 'missed' | 'on_track' | 'off_track';
export type TrackRow = { key: string; kind: 'goals' | 'strength' | 'patterns'; title: string; status: TrackStatus; detail: string };
type MatchLike = {
    fixture: { home: string; away: string };
    strength: { state: string; favourite?: string | null; level?: string } | null;
    goals: { rows: { market: string; probability: number }[] } | null;
    patterns: { default_view: 'venue' | 'all'; venue: PatternGroups | null; all: PatternGroups | null } | null;
};
type PatternLike = { team: string; side: string; event: string; label: string; k: number; n: number; context: string };
type PatternGroups = { top_results: PatternLike[]; top_attack_defence: PatternLike[] };
type Score = { home: number; away: number; final: boolean };
const plural = (n: number, word: string) => `${n} ${word}${n === 1 ? '' : 's'}`;

function over(goals: number, line: number, final: boolean): [TrackStatus, string] {
    const need = Math.floor(line) + 1;
    if (goals >= need) return ['landed', plural(goals, 'goal')];
    if (final) return ['missed', `Finished with ${plural(goals, 'goal')}`];
    return ['off_track', `${plural(goals, 'goal')} so far · needs ${need - goals} more`];
}
function under(goals: number, line: number, final: boolean): [TrackStatus, string] {
    const limit = Math.floor(line);
    if (goals > limit) return ['missed', `${plural(goals, 'goal')} · over the line`];
    if (final) return ['landed', `Finished with ${plural(goals, 'goal')}`];
    return ['on_track', limit - goals ? `${plural(goals, 'goal')} so far · room for ${limit - goals} more` : `${plural(goals, 'goal')} · no more allowed`];
}
/** One Goals outlook market, e.g. "Match Over 1.5", "Molde Over 0.5", "Match Under 3.5", "Match Goals 1-4". */
export function trackMarket(market: string, home: string, away: string, sc: Score): [TrackStatus, string] | null {
    const m = market.match(/^(.*) (Over|Under) (\d+(?:\.\d)?)$/);
    const range = market.match(/^(.*) Goals (\d+)-(\d+)$/);
    const who = (m || range)?.[1];
    if (!who) return null;
    const goals = who === 'Match' ? sc.home + sc.away : who === home ? sc.home : who === away ? sc.away : null;
    if (goals == null) return null;
    if (m) return m[2] === 'Over' ? over(goals, Number(m[3]), sc.final) : under(goals, Number(m[3]), sc.final);
    const lo = Number(range![2]), hi = Number(range![3]);
    if (goals > hi) return ['missed', `${plural(goals, 'goal')} · above ${hi}`];
    if (goals < lo) return sc.final ? ['missed', `Finished with ${plural(goals, 'goal')}`] : ['off_track', `${plural(goals, 'goal')} so far · needs ${lo - goals} more`];
    return sc.final ? ['landed', `Finished with ${plural(goals, 'goal')}`] : ['on_track', `${plural(goals, 'goal')} · in range`];
}
/** A team pattern (win, clean_sheet, Over_1_5…) checked against this match. */
export function trackPattern(event: string, gf: number, ga: number, final: boolean): [TrackStatus, string] | null {
    const now = (ok: boolean, yes: string, no: string): [TrackStatus, string] => final ? [ok ? 'landed' : 'missed', ok ? 'Happened' : 'Did not happen'] : [ok ? 'on_track' : 'off_track', ok ? yes : no];
    // Reached once and can't be undone (scored, conceded), or broken once and can't recover (clean sheet).
    const reached = (ok: boolean, need: number): [TrackStatus, string] => ok ? ['landed', 'Happened'] : final ? ['missed', 'Did not happen'] : ['off_track', `Needs ${need} more`];
    const holding = (ok: boolean, yes: string): [TrackStatus, string] => !ok ? ['missed', 'Broken'] : final ? ['landed', 'Happened'] : ['on_track', yes];
    const total = gf + ga;
    const line = event.match(/^(Over|Under)_(\d)_(\d)$/);
    if (line) { const v = Number(`${line[2]}.${line[3]}`); const [st, d] = line[1] === 'Over' ? over(total, v, final) : under(total, v, final); return [st, d]; }
    switch (event) {
        case 'win': return now(gf > ga, 'Leading', gf === ga ? 'Level' : 'Trailing');
        case 'loss': return now(gf < ga, 'Trailing', gf === ga ? 'Level' : 'Leading');
        case 'draw': return now(gf === ga, 'Level', 'Not level');
        case 'unbeaten': return now(gf >= ga, gf > ga ? 'Leading' : 'Level', 'Trailing');
        case 'winless': return now(gf <= ga, gf < ga ? 'Trailing' : 'Level', 'Leading');
        case 'score_1goal': return reached(gf >= 1, 1 - gf);
        case 'score_2goals': return reached(gf >= 2, 2 - gf);
        case 'concede_1': return reached(ga >= 1, 1 - ga);
        case 'concede_2': return reached(ga >= 2, 2 - ga);
        case 'BTTS': return reached(gf >= 1 && ga >= 1, (gf ? 0 : 1) + (ga ? 0 : 1));
        case 'clean_sheet': return holding(ga === 0, 'No goals conceded');
        case 'goalless': return holding(gf === 0, 'Not scored yet');
        default: return null;   // halftime_win and unknown events need more than the score
    }
}
/** The pre-match research, checked against the live score. Goals markets are
 *  90-minute markets, so they are not graded once a match goes to extra time. */
export function trackForecast(match: MatchLike, state: LiveState, patternLimit = 4): TrackRow[] {
    if (state.home_goals == null || state.away_goals == null || (!IN_PLAY.has(state.status) && !FINAL.has(state.status))) return [];
    const { home, away } = match.fixture;
    const final = FINAL.has(state.status);
    const sc = { home: state.home_goals, away: state.away_goals, final };
    const rows: TrackRow[] = [];
    const st = match.strength;
    if (st?.favourite && (st.state === 'big_dog' || st.state === 'slight_favourite')) {
        const fav = st.favourite === home ? sc.home : st.favourite === away ? sc.away : null;
        const opp = st.favourite === home ? sc.away : sc.home;
        if (fav != null) {
            const label = st.state === 'big_dog' ? 'Big dog' : st.level === 'slight_edge' ? 'Slight edge' : 'Favourite';
            const [status, detail] = trackPattern('win', fav, opp, final)!;
            rows.push({ key: 'strength', kind: 'strength', title: `${st.favourite} · ${label}`, status, detail: final ? (fav > opp ? 'Won' : fav === opp ? 'Drew' : 'Lost') : detail });
        }
    }
    if (!EXTRA_TIME.has(state.status)) for (const g of match.goals?.rows || []) {
        const t = trackMarket(g.market, home, away, sc);
        if (t) rows.push({ key: `goals:${g.market}`, kind: 'goals', title: `${g.market} · ${Math.round(g.probability * 100)}%`, status: t[0], detail: t[1] });
    }
    const view = match.patterns?.[match.patterns.default_view] || match.patterns?.all;
    const patterns = view ? [...view.top_results, ...view.top_attack_defence] : [];
    for (const p of patterns.slice(0, patternLimit)) {
        const mine = p.team === home ? [sc.home, sc.away] : p.team === away ? [sc.away, sc.home] : null;
        const t = mine && !EXTRA_TIME.has(state.status) ? trackPattern(p.event, mine[0], mine[1], final) : null;
        if (t) rows.push({ key: `patterns:${p.team}:${p.event}:${p.context}`, kind: 'patterns', title: `${p.team} · ${p.label} ${p.k}/${p.n}`, status: t[0], detail: t[1] });
    }
    return rows;
}
/** Short status for one forecast on a list card. */
export function trackChip(row: TrackRow) { return row.status === 'landed' ? 'Landed' : row.status === 'missed' ? 'Missed' : row.status === 'on_track' ? 'On track' : 'Not yet'; }
/** The list card's one forecast: the match's leading pre-kickoff insight, checked live. */
export function trackPick(card: LiveCard): TrackRow | null {
    const p = card.pick;
    if (!p) return null;
    const rows = trackForecast({
        fixture: { home: card.home, away: card.away },
        // Discover's strength picks are Big dogs (headline: "favourite · Big dog").
        strength: p.list === 'strength' ? { state: 'big_dog', favourite: p.favourite || p.big_dog } : null,
        goals: p.list === 'goals' && p.market ? { rows: [{ market: p.market, probability: p.probability ?? 0 }] } : null,
        patterns: p.list === 'patterns' && p.team && p.event ? { default_view: 'all', venue: null, all: { top_results: [{ team: p.team, side: '', event: p.event, label: patternLabel(p.event), k: p.k ?? 0, n: p.n ?? 0, context: p.context || 'all' }], top_attack_defence: [] } } : null,
    }, card, 1);
    return rows[0] ?? null;
}
