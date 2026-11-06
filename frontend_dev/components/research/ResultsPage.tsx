'use client';
// Matches on a past day: played games in the live-score list format (league
// groups, crests, full-time score). Results used to be its own page; it now
// lives in Matches so past and upcoming games share one date picker.
import { resultsSchema, finalLabel, winner, type Result } from '@/lib/results';
import { Crest, ResourceState, useResource } from './shared';
import Link from 'next/link';
import { usePathname, useSearchParams } from 'next/navigation';
import s from './research.module.css';
import l from './live.module.css';
function ResultRow({ m, returnTo }: { m: Result; returnTo: string }) {
    const won = winner(m);
    const cls = (side: 'home' | 'away') => won === side ? l.won : won && won !== 'draw' ? l.lost : '';
    return <li><Link className={l.liveRow} href={`/match/${m.fixture_id}?returnTo=${encodeURIComponent(returnTo)}`} aria-label={`${m.home} ${m.home_goals ?? ''} ${m.away} ${m.away_goals ?? ''}, ${finalLabel(m.status)}`}>
        <span className={l.clock}>{finalLabel(m.status)}</span>
        <span className={l.teams}><span className={`${l.team} ${cls('home')}`}><Crest name={m.home} url={m.home_crest_url}/>{m.home}</span><span className={`${l.team} ${cls('away')}`}><Crest name={m.away} url={m.away_crest_url}/>{m.away}</span></span>
        <strong className={l.score}><span className={cls('home')}>{m.home_goals ?? '–'}</span><span className={cls('away')}>{m.away_goals ?? '–'}</span></strong>
        {m.home_ht != null && m.away_ht != null && <span className={l.ht}>HT {m.home_ht}–{m.away_ht}</span>}
    </Link></li>;
}

/** One past local day of played games, filtered like the upcoming list. */
export function DayResults({ day, zone, leagues, team, returnTo }: { day: string; zone: string; leagues: string[]; team: string; returnTo: string }) {
    const { data, busy, error, retry } = useResource(`/api/research/results?${new URLSearchParams({ zone, day })}`, resultsSchema);
    const search = useSearchParams(), path = usePathname();
    const dayHref = (next: string) => { const q = new URLSearchParams(search.toString()); q.set('scope', 'date'); q.set('day', next); q.delete('end_day'); return `${path}?${q}`; };
    const short = (d: string) => new Intl.DateTimeFormat('en-GB', { timeZone: 'UTC', weekday: 'short', day: 'numeric', month: 'short' }).format(new Date(`${d}T12:00:00Z`));
    if (!data) return <ResourceState busy={busy} error={error} retry={retry}/>;
    const shown = data.matches.filter(m => (!leagues.length || (m.league_id != null && leagues.includes(String(m.league_id))))
        && (!team || `${m.home} ${m.away}`.toLocaleLowerCase().includes(team)));
    const groups = new Map<string, Result[]>();
    for (const m of shown) groups.set(`${m.league_id ?? m.league}`, [...(groups.get(`${m.league_id ?? m.league}`) || []), m]);
    return <section className={`${s.section} ${l.live}`} aria-label="Results">
        <h2 aria-live="polite">Results · {shown.length}</h2>
        {shown.length ? <div className={l.block}>{[...groups.values()].map(g => <div key={g[0].league_id ?? g[0].league} className={l.league}>
            <h3 className={s.competition}><Crest name={g[0].league} url={g[0].league_logo_url} league/><span>{g[0].league}{g[0].country && ` · ${g[0].country}`}</span></h3>
            <ul>{g.map(m => <ResultRow key={m.fixture_id} m={m} returnTo={returnTo}/>)}</ul>
        </div>)}</div>
            : <p className={s.empty}>{data.matches.length ? 'No played games match these leagues and team search on this day.' : 'No played games on this day.'}{!data.matches.length && data.previous_day && <> <Link className={s.link} href={dayHref(data.previous_day)}>Latest earlier results: {short(data.previous_day)} →</Link></>}</p>}
        <p className={s.caption}>Final scores from the nightly update. Open a match for its pre-match research.</p>
        {error && <p role="alert">{error}</p>}
    </section>;
}
