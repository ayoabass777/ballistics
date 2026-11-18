'use client';
import { Bookmark, ChevronDown, Circle } from 'lucide-react';
import { useState, useLayoutEffect } from 'react';
import { useUser } from '@clerk/nextjs';
import { useListPosition } from './useListPosition';
import { MatchesControls } from './MatchesControls';
import { useFollowedLeagues } from './useFollowedLeagues';
import Link from 'next/link';
import { usePathname, useRouter, useSearchParams } from 'next/navigation';
import { type Discover, type Card, headline } from '@/lib/research';
import { matchEvidence, evidenceOptions, filterMatches } from '@/lib/evidence-controls';
import { EvidenceFilter } from './EvidenceControls';
import { resultsSchema, type Result } from '@/lib/results';
import { IN_PLAY, clockLabel, liveListSchema } from '@/lib/live';
import { dayInZone } from '@/lib/preferences';
import { useLive } from './Live';
import { Button, Crest, ResourceState, useResource, useSaved, Toast } from './shared';
import s from './research.module.css';
import l from './live.module.css';

// The adjacent team rows identify Home (top) and Away (bottom).
function compactFinding(card: Card) {
    const pick = card.preview;
    if (!pick) return '';
    let text = headline({...card,pick});
    for (const [team, side] of [[card.home,'Home'],[card.away,'Away']]) {
        if (text.startsWith(`${team} · `)) { text = `${side} · ${text.slice(team.length + 3)}`; break; }
        if (text.startsWith(`${team} `)) { text = `${side} · ${text.slice(team.length + 1)}`; break; }
    }
    return text.replace(/ (\d+)\/(\d+)/, ' $1/$2').replace(/ · home$/, ' · at home').replace(/ · away$/, ' · away games');
}

// Country is part of the key because several leagues share a name.
const priorityLeagues = ['England:Premier League', 'Spain:La Liga', 'Italy:Serie A', 'Germany:Bundesliga', 'France:Ligue 1'];

export function UnifiedMatches({ zone, leagues, team, data, followedLeagues, follows }: { follows: ReturnType<typeof useFollowedLeagues>; zone: string; leagues: string[]; team: string; data: Discover | null; followedLeagues: number[] }) {
    const search = useSearchParams(), router = useRouter(), path = usePathname();
    const scope = search.get('scope') || 'today';
    const today = dayInZone(zone);
    const start = scope === 'date' || scope === 'range' ? search.get('day') || today : dayInZone(zone, scope === 'tomorrow' ? 1 : 0);
    const end = scope === 'range' ? search.get('end_day') || start : scope === 'week' ? dayInZone(zone, 6) : start;
    const saved = useSaved();
    const { user } = useUser();
    const navigationQuery = new URLSearchParams(search.toString());
    navigationQuery.sort();
    const browseKey = `${user?.id || 'guest'}:${path}?${navigationQuery}:${zone}:${start}:${end}`;
    const storageKey = `ballistics:match-leagues:${browseKey}`;
    const [expansion, setExpansion] = useState<{key:string; closed:string[]}>({key:'',closed:[]});
    useLayoutEffect(() => {
        let closed: string[] = [];
        try { const value = JSON.parse(sessionStorage.getItem(storageKey) || '[]'); if (Array.isArray(value)) closed = value.filter(x => typeof x === 'string'); } catch { /* Browsing works without storage. */ }
        setExpansion({key:storageKey,closed});
    }, [storageKey]);
    function toggleLeague(key: string) {
        setExpansion(current => {
            const closed = current.closed.includes(key) ? current.closed.filter(x => x !== key) : [...current.closed,key];
            try { sessionStorage.setItem(storageKey,JSON.stringify(closed)); } catch { /* Optional persistence. */ }
            return {key:storageKey,closed};
        });
    }
    const [filtersOpen, setFiltersOpen] = useState(false);
    const cards = data?.matches || [];
    const cardsById = new Map(cards.map(card => [card.fixture_id, card]));
    const findings = matchEvidence(data, cards);
    const finding = search.get('finding') || '';
    const eligible = new Set(filterMatches(cards, findings, finding).map(card => card.fixture_id));
    const onlyLive = search.get('status') === 'live';
    const query = new URLSearchParams({ zone, date_from: start, date_to: end });
    const fixtures = useResource(`/api/research/fixtures?${query}`, resultsSchema);
    const position = useListPosition(`owner-matches:${browseKey}`, Boolean(fixtures.data) && expansion.key === storageKey, Boolean(user));
    const live = useLive('/api/research/live', liveListSchema, 'poll');
    const localDate = (kickoff: string) => dayInZone(zone, 0, new Date(kickoff));
    const liveById = new Map((live.data?.matches || []).map(m => [m.fixture_id, m]));
    const merged = new Map<number, Result>((fixtures.data?.matches || []).map(m => [m.fixture_id, m]));
    for (const m of live.data?.matches || []) {
        const day = localDate(m.kickoff);
        if (day >= start && day <= end) merged.set(m.fixture_id, { ...merged.get(m.fixture_id), ...m, home_ht: null, away_ht: null });
    }
    const followed = new Set(followedLeagues);
    const leaguePriority = (fixture: Result) => {
        if (fixture.league_id != null && followed.has(fixture.league_id)) return -1;
        const index = priorityLeagues.indexOf(`${fixture.country}:${fixture.league}`);
        return index < 0 ? priorityLeagues.length : index;
    };
    const shown = [...merged.values()].filter(m =>
        (!onlyLive || IN_PLAY.has(m.status)) &&
        (!finding || eligible.has(m.fixture_id)) &&
        (!leagues.length || leagues.includes(String(m.league_id))) &&
        (!team || `${m.home} ${m.away}`.toLocaleLowerCase().includes(team))
    ).sort((a, b) =>
        localDate(a.kickoff).localeCompare(localDate(b.kickoff)) ||
        leaguePriority(a) - leaguePriority(b) ||
        (a.country || 'Other competitions').localeCompare(b.country || 'Other competitions') ||
        a.league.localeCompare(b.league) ||
        (a.league_id ?? 0) - (b.league_id ?? 0) ||
        Date.parse(a.kickoff) - Date.parse(b.kickoff) ||
        a.fixture_id - b.fixture_id
    );
    const days = new Map<string, Map<string, Result[]>>();
    for (const m of shown) {
        const day = localDate(m.kickoff);
        if (!days.has(day)) days.set(day, new Map());
        const groups = days.get(day)!;
        const key = `${m.country}:${m.league_id ?? m.league}`;
        if (!groups.has(key)) groups.set(key, []);
        groups.get(key)!.push(m);
    }
    const returnTo = `${path}?${search}`;
    return <div className={l.matchesBrowser} style={position.restoringHeight ? {minHeight:position.restoringHeight} : undefined}>
        <div className={s.controls}><MatchesControls leagueRows data={data} zone={zone} follows={follows} secondaryFilters={<Button aria-expanded={filtersOpen} aria-controls="match-extra-filters" onClick={() => setFiltersOpen(!filtersOpen)}>Filters{finding ? ' · 1' : ''}</Button>}/></div>
        <section className={`${s.section} ${l.live}`} aria-label="Fixtures">
        {filtersOpen && <div id="match-extra-filters" className={s.leaguePicker}>
        <EvidenceFilter label="Finding" allLabel="All findings" options={evidenceOptions(findings)} value={finding} total={merged.size} onChange={value => {
            const next = new URLSearchParams(search.toString());
            if (value) next.set('finding', value); else next.delete('finding');
            router.replace(`${path}?${next}`, { scroll: false });
        }}/>
        </div>}
        <ResourceState busy={fixtures.busy} error={fixtures.error} retry={fixtures.retry}/>
        {live.failed && <p role="status">Live updates are temporarily unavailable. Retrying.</p>}
        <h2 aria-live="polite">{shown.length} {onlyLive ? 'live matches' : 'fixtures'}</h2>
        {[...days].map(([day, groups]) => <div key={day} className={l.block}>
            {start !== end && <h3>{new Date(`${day}T12:00:00Z`).toLocaleDateString('en-GB',{weekday:'short',day:'numeric',month:'short',timeZone:'UTC'})}</h3>}
            {[...groups].map(([key, matches]) => <details key={key} className={l.league} open={!expansion.closed.includes(`${day}:${key}`)}>
                <summary className={l.leagueToggle} onClick={event => { event.preventDefault(); toggleLeague(`${day}:${key}`); }}><span className={s.competition}><Crest name={matches[0].league} url={matches[0].league_logo_url} league/><span>{matches[0].country} · {matches[0].league}</span></span><span className={l.fixtureCount}>{matches.length}</span><ChevronDown size={16} className={l.leagueChevron} aria-hidden="true"/></summary>
                <ul>{matches.map(m => {
                    const card = cardsById.get(m.fixture_id);
                    const state = liveById.get(m.fixture_id);
                    const time = new Intl.DateTimeFormat('en-GB', { timeZone: zone, hour: '2-digit', minute: '2-digit' }).format(new Date(m.kickoff));
                    const inPlay = IN_PLAY.has(m.status);
                    const label = state ? clockLabel(state) : ['NS', 'TBD'].includes(m.status) ? time : m.status;
                    return <li key={m.fixture_id} className={l.fixtureItem}><Link className={l.liveRow} href={`/match/${m.fixture_id}?returnTo=${encodeURIComponent(returnTo)}`}>
                        <span className={`${l.clock} ${inPlay ? l.inPlay : ''}`} aria-label={inPlay ? `Live: ${label}` : undefined}>{inPlay && <span className={l.dot} aria-hidden="true"/>}{label}</span>
                        <span className={l.teams}><span className={l.team}><Crest name={m.home} url={m.home_crest_url}/><span className={l.teamName}>{m.home}</span></span><span className={l.team}><Crest name={m.away} url={m.away_crest_url}/><span className={l.teamName}>{m.away}</span></span></span>
                        {!['NS', 'TBD'].includes(m.status) && <strong className={`${l.score} ${inPlay ? l.liveScore : ''}`}><span>{m.home_goals ?? '–'}</span><span>{m.away_goals ?? '–'}</span></strong>}
                    </Link>
                    {card?.preview && <p className={`${s.caption} ${l.fixtureEvidence}`}>{compactFinding(card)}</p>}
                    {card && <Button className={l.saveBookmark} aria-label={`${saved.isSaved(m.fixture_id) ? 'Unsave' : 'Save'} ${m.home} vs ${m.away}`} title={saved.isSaved(m.fixture_id) ? 'Remove from Saved' : 'Save fixture'} aria-pressed={saved.isSaved(m.fixture_id)} onClick={() => saved.toggle(card, data?.run_ds || null)}><Bookmark size={19} fill={saved.isSaved(m.fixture_id) ? 'currentColor' : 'none'} aria-hidden="true"/></Button>}
                    </li>;
                })}</ul>
            </details>)}
        </div>)}
        {!fixtures.busy && !fixtures.error && !shown.length && <p className={s.empty}>{onlyLive ? 'No live matches for these dates, leagues and team search.' : 'No fixtures for these dates, leagues and team search.'}</p>}
        <Toast message={saved.message}/>
    </section></div>;
}
