import { eventLabel, type Card, type Discover } from './research';

export type EvidenceOption = { key: string; label: string; group: string; count: number };
type Pick = NonNullable<Card['pick']>;
export function finding(card: Card, pick: Pick = card.pick!): Omit<EvidenceOption, 'count'> | null {
    if (!pick) return null;
    if (pick.list === 'strength') return { key: 'strength:big_dog', label: 'Big dog', group: 'Team strength' };
    if (pick.list === 'patterns') {
        const event = pick.event || '';
        if (!event) return null;
        const group = /^(win|loss|draw|unbeaten|winless|halftime_win)$/.test(event) ? 'Results'
            : /^(clean_sheet|concede_)/.test(event) ? 'Defending'
            : /^(score_|goalless)/.test(event) ? 'Scoring' : 'Match goals';
        return { key: `patterns:${event}`, label: event === 'halftime_win' ? 'Led at half-time' : eventLabel(event), group: `Team patterns · ${group}` };
    }
    let label = (pick.market || '').trim();
    if (!label) return null;
    let teamMarket = false;
    for (const team of [card.home, card.away].sort((a, b) => b.length - a.length)) {
        if (label.toLowerCase().startsWith(`${team.toLowerCase()} `)) { label = `Team ${label.slice(team.length + 1)}`; teamMarket = true; break; }
    }
    // The API uses both team Over 0.5 and team scores 1+ presentation labels.
    label = label.replace(/^Team over 0\.5$/i, 'Team scores 1+').replace(/^Team over 1\.5$/i, 'Team scores 2+');
    return { key: `goals:${label.toLowerCase()}`, label, group: teamMarket ? 'Goals outlook · Team goals' : 'Goals outlook · Match goals' };
}

export function evidenceOptions(cards: Card[]): EvidenceOption[] {
    const options = new Map<string, EvidenceOption & { ids: Set<number> }>();
    for (const card of cards) {
        const item = finding(card); if (!item) continue;
        const entry = options.get(item.key) || { ...item, count: 0, ids: new Set<number>() };
        entry.ids.add(card.fixture_id); entry.count = entry.ids.size; options.set(item.key, entry);
    }
    return [...options.values()].sort((a, b) => a.group.localeCompare(b.group) || a.label.localeCompare(b.label, undefined, { numeric: true }));
}

export function highestFirst<T>(items: T[], score: (item: T) => number | null | undefined): T[] {
    const value = (item: T) => { const n = score(item); return n != null && Number.isFinite(n) ? n : -Infinity; };
    return [...items].sort((a, b) => value(b) - value(a));
}
export function cardRate(card: Card): number | null {
    const p = card.pick;
    return p?.list === 'goals' ? p.probability ?? null : p?.k != null && p.n != null && p.n > 0 ? p.k / p.n : null;
}
export function categoryResults(cards: Card[], key: string, descending: boolean): Card[] {
    const filtered = key ? cards.filter(card => finding(card)?.key === key) : cards;
    return descending ? highestFirst(filtered, cardRate) : filtered;
}

/** Available qualifying findings, not an assertion about every market in a match. */
export function matchEvidence(data: Discover | null, matches: Card[]): Card[] {
    if (!data) return [];
    const ids = new Set(matches.map(card => card.fixture_id));
    return [...data.lists.strength, ...data.lists.goals, ...data.lists.patterns,
        ...matches.filter(card => card.preview).map(card => ({ ...card, pick: card.preview! }))].filter(card => ids.has(card.fixture_id));
}
export function filterMatches(matches: Card[], evidence: Card[], key: string): Card[] {
    if (!key) return matches;
    const picks = new Map(evidence.filter(card => finding(card)?.key === key).map(card => [card.fixture_id, card.pick]));
    return matches.filter(card => picks.has(card.fixture_id)).map(card => ({ ...card, preview: picks.get(card.fixture_id)! }));
}
