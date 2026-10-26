'use client';
import Link from 'next/link';
import { usePreferences, useStamp } from './PreferencesProvider';
import { useSearchParams, useRouter } from 'next/navigation';
import { useEffect, useRef, useState } from 'react';
import { useUser } from '@clerk/nextjs';
import { matchSchema, type Match, type Pattern, type Card } from '@/lib/research';
import { Shell, Button, Crest, FixtureIdentity, ResourceState, useResource, useSaved, Toast } from './shared';
import s from './research.module.css';
import { matchGuideKey } from './FunctionalityTip';
import { track } from '@/lib/usage-events';
// Mirrors FORECAST_STATUSES in engine/api/match_detail.py: not started, in play and finished keep the forecast.
const forecastStatuses = new Set(['NS', '1H', 'HT', '2H', 'ET', 'BT', 'P', 'LIVE', 'FT', 'AET', 'PEN']);
const statusNotices: Record<string, string> = { PST: 'This match has been postponed.', CANC: 'This match has been cancelled.', ABD: 'This match was abandoned.', SUSP: 'This match has been suspended.', INT: 'This match has been interrupted.', AWD: 'This match was awarded to one team.', WO: 'This match was a walkover.', TBD: 'The kickoff time has not been confirmed.' };
const comparisons: Record<string, string> = { well_above: 'Much more often than usual', above: 'More often than usual', typical: 'Typical for this league', below: 'Less often than usual', well_below: 'Much less often than usual' };
function Segment({ value, onChange, disabled = false }: {
    value: string;
    onChange: (s: 'venue' | 'all') => void;
    disabled?: boolean;
}) { return <div className={s.segment}><Button className={value === 'venue' ? s.active : ''} aria-pressed={value === 'venue'} disabled={disabled} onClick={() => onChange('venue')}>Home / Away</Button><Button className={value === 'all' ? s.active : ''} aria-pressed={value === 'all'} onClick={() => onChange('all')}>All venues</Button></div>; }
function PatternRow({ p, highlight }: {
    p: Pattern;
    highlight: boolean;
}) { return <div className={`${s.evidence} ${highlight ? s.highlight : ''}`}><div><h3>{p.team} · {p.label}</h3><p className={s.caption}>{p.context === 'all' ? 'All venues' : p.context} · This season{p.streak != null && p.streak > 0 ? ` · Current run ${p.streak}` : ''}</p>{p.league_rate != null && comparisons[p.league_comparison || ''] && <p className={s.caption}>{comparisons[p.league_comparison!]} · {Math.round(p.k / p.n * 100)}% vs league {Math.round(p.league_rate * 100)}%</p>}</div><strong>{p.k} / {p.n}</strong></div>; }
function Details({ data }: {
    data: Match;
}) {
    const stamp = useStamp();
    const { zone } = usePreferences();
    const { fixture: f, context: c } = data;
    const { user, isLoaded } = useUser();
    const guideSections = (['strength', 'goals', 'patterns'] as const).filter(section => Boolean(data[section]));
    const [guideIndex, setGuideIndex] = useState<number | null>(null);
    const opened = useRef<number | null>(null);
    useEffect(() => {
        // Strict Mode runs effects twice in development; record one open per fixture view.
        if (opened.current === f.fixture_id) return;
        opened.current = f.fixture_id;
        track('match_opened', { fixture_id: f.fixture_id, after_kickoff: Date.now() >= new Date(f.kickoff_utc).getTime() });
    }, [f.fixture_id]);
    useEffect(() => {
        if (!isLoaded || !user?.id || !guideSections.length) return;
        try { setGuideIndex(localStorage.getItem(matchGuideKey(user.id)) !== '1' ? 0 : null); }
        catch { setGuideIndex(0); }
    }, [isLoaded, user?.id, guideSections.length]);
    useEffect(() => {
        if (guideIndex == null) return;
        document.querySelector('[aria-label^="Match details guide"]')?.scrollIntoView({ behavior: 'smooth', block: 'center' });
    }, [guideIndex, guideSections.join(',')]);
    const closeGuide = (action: 'done' | 'skip') => {
        track('tutorial_dismissed', { tutorial_id: 'match', action });
        if (user?.id) try { localStorage.setItem(matchGuideKey(user.id), '1'); } catch { /* Dismiss for this visit. */ }
        setGuideIndex(null);
    };
    const strengthLine = !data.strength ? '' : data.strength.state === 'big_dog'
        ? 'A Big dog has a clear ratings edge in a league where similar favourites have a strong past record. The other team is the Underdog.'
        : data.strength.state === 'slight_favourite' && data.strength.level === 'favourite'
        ? 'There is a clear strength edge, but this match does not meet the extra league-history rule for a Big dog label.'
        : data.strength.state === 'slight_favourite' && data.strength.level === 'slight_edge'
        ? 'One team has a small edge. Treat it as context, not a strong verdict.'
        : data.strength.state === 'slight_favourite'
        ? 'One team has an edge, but the gap is smaller.'
        : data.strength.state === 'close'
        ? 'The teams are close on this strength measure.'
        : 'There is not enough qualifying evidence for a Big dog label here. That alone does not mean the teams are equal.';
    const guide = (section: 'strength' | 'goals' | 'patterns') => {
        if (guideIndex == null || guideSections[guideIndex] !== section) return null;
        const last = guideIndex === guideSections.length - 1;
        return <aside className={s.tutorialTip} aria-label={`Match details guide, step ${guideIndex + 1} of ${guideSections.length}`}>
            <p className={s.caption}>{guideSections.length === 3 ? 'Three ways to understand this match' : 'Understand this match'} · {guideIndex + 1} of {guideSections.length}</p>
            {section === 'strength' ? <><strong>Who has the strength edge?</strong><p>This compares the teams’ strength using ratings and past league results. It shows an expectation for the matchup, not a certain winner.</p><p>{strengthLine}</p>{data.strength?.tiers && (data.strength.state === 'slight_favourite' || data.strength.state === 'close') && <p>Top, Middle and Bottom tiers group team ratings within this league; they are not table positions.</p>}</>
                : section === 'goals' ? <><strong>What goal outcomes look likely?</strong><p>Each row shows a goal outcome and the model’s estimated chance of it happening in this match. The estimate draws on scoring and conceding evidence, but it is not a promised score or an exact goal range.</p><p>Related goal outcomes can overlap, so several high percentages are not separate confirmations.</p>{data.goals?.nothing_stands_out && <p>The available estimates do not identify a standout goal outcome here.</p>}</>
                : <><strong>What has each team been doing this season?</strong><p>These are observed results and goal patterns from this season. A count such as 6/8 means that event happened in six of eight relevant matches. The league comparison uses the same event and home/away or all-venue setting.</p><p>Patterns describe what has happened, not the chance of it happening next time. Smaller samples can change quickly.</p></>}
            <div className={s.row}>{guideIndex > 0 && <Button onClick={() => setGuideIndex(guideIndex - 1)}>Back</Button>}<Button className={s.active} onClick={() => last ? closeGuide('done') : setGuideIndex(guideIndex + 1)}>{last ? 'Done' : `Look at ${guideSections[guideIndex + 1]}`}</Button><Button onClick={() => closeGuide('skip')}>Skip guide</Button></div>
            <a className={s.link} href="#methodology" onClick={() => { track('methodology_opened', { from_section: 'match_guide' }); const details = document.getElementById('methodology') as HTMLDetailsElement | null; if (details) details.open = true; }}>{section === 'goals' ? 'How the model works' : 'Read how the numbers work'} →</a>
        </aside>;
    };
    const search = useSearchParams(), router = useRouter(), saved = useSaved();
    const [contextView, setContextView] = useState<'venue' | 'all'>('venue');
    const [patternView, setPatternView] = useState<'venue' | 'all'>(search.get('section') === 'patterns' && search.get('context') ? (search.get('context') === 'all' || !data.patterns?.venue ? 'all' : 'venue') : data.patterns?.default_view || 'all');
    const [more, setMore] = useState(search.get('section') === 'patterns');
    const entry = ['strength', 'patterns', 'goals'].includes(search.get('section') || '') ? search.get('section') : null;
    const [active, setActive] = useState(entry || 'context');
    useEffect(() => { if (entry)
        document.getElementById(entry)?.scrollIntoView({ block: 'start' }); const observer = new IntersectionObserver(entries => { const visible = entries.filter(x => x.isIntersecting); if (visible.length)
        setActive(visible[0].target.id); }, { rootMargin: '-130px 0px -45% 0px' }); document.querySelectorAll('[data-match-section]').forEach(el => observer.observe(el)); return () => observer.disconnect(); }, [entry]);
    const card: Card = { fixture_id: f.fixture_id, home: f.home, away: f.away, league_id: f.league_id, league: f.league, country: f.country, kickoff: f.kickoff_utc, home_crest_url: f.home_crest_url, away_crest_url: f.away_crest_url, league_logo_url: f.league_logo_url, pick: null };
    const h = c.form?.home_team[contextView === 'all' ? 'overall' : 'home'], a = c.form?.away_team[contextView === 'all' ? 'overall' : 'away'];
    const patterns = data.patterns?.[patternView];
    const positions = contextView === 'venue' ? c.position.venue : c.position;
    const compareRows: [
        string,
        number | null | undefined,
        number | null | undefined,
        boolean,
        boolean
    ][] = [['Goals scored / match', h?.attack_rate, a?.attack_rate, true, false], ['Goals conceded / match', h?.defense_rate, a?.defense_rate, false, false], ['Attack rank', h?.attack_rank, a?.attack_rank, false, true], ['Defence rank', h?.defense_rank, a?.defense_rank, false, true]];
    const positionText = (p: { rank: number | null; group_size: number } | null | undefined) => p?.rank == null ? '—' : `#${p.rank} of ${p.group_size}`;
    const renderPattern = (p: Pattern) => <PatternRow key={`${p.team}:${p.event}:${p.context}`} p={p} highlight={entry === 'patterns' && search.get('team') === p.team && search.get('event') === p.event}/>;
    const title = `${f.home} v ${f.away}`;
    return <><div className={s.sticky}><div className={s.row}><Button aria-label="Back to previous page" onClick={() => { if (window.history.length > 1)
        router.back();
    else
        router.push('/discover'); }}>←</Button><div className={s.stickyTitle}><h1 aria-label={title}><FixtureIdentity card={f}/></h1></div><Button asChild aria-label="How these numbers work"><a href="#methodology" onClick={() => track('methodology_opened', { from_section: 'match_header' })}>i</a></Button><Button className={s.active} aria-pressed={saved.isSaved(f.fixture_id)} onClick={() => saved.toggle(card, f.run_ds)}>{saved.isSaved(f.fixture_id) ? '✓ Saved' : '+ Save'}</Button></div><div className={s.matchMetadata}><p className={`${s.caption} ${s.competition}`}><Crest name={f.league} url={f.league_logo_url} league/><span>{f.league}{f.country && ` · ${f.country}`}</span></p><p className={s.caption}>{stamp(f.kickoff_utc)}</p></div><nav className={s.chips} aria-label="Match sections">{['context', ...(data.strength ? ['strength'] : []), ...(data.patterns ? ['patterns'] : []), ...(data.goals ? ['goals'] : [])].map(key => <a href={`#${key}`} className={active === key ? s.active : ''} onClick={() => setActive(key)} key={key}>{key[0].toUpperCase() + key.slice(1)}</a>)}</nav></div>
 <div className={s.mobileIdentity}><h2 className={s.team}><Crest name={f.home} url={f.home_crest_url}/>{f.home}</h2><h2 className={s.team}><Crest name={f.away} url={f.away_crest_url}/>{f.away}</h2><p className={`${s.caption} ${s.competition}`}><Crest name={f.league} url={f.league_logo_url} league/><span>{f.league}{f.country && ` · ${f.country}`}</span></p><p className={s.caption}>{stamp(f.kickoff_utc)}</p><p className={s.caption}>{f.run_finished_at ? `Engine updated ${stamp(f.run_finished_at)} · Run ${f.run_ds}` : 'No engine analysis in the latest complete run.'}</p>{entry && <p className={s.caption}>Opened from Discover · {entry === 'strength' ? 'Team strength' : entry === 'patterns' ? 'Team patterns' : 'Goals outlook'}</p>}</div>
 {f.fixture_status && !forecastStatuses.has(f.fixture_status) && <p className={s.notice} role="status">{statusNotices[f.fixture_status] || 'This match is not going ahead as scheduled.'} The pre-match forecast is not shown for this match.</p>}
 <section id="context" data-match-section className={`${s.panel} ${s.contextPanel}`}><h2>Match context</h2><div className={s.contextCore}><Segment value={contextView} onChange={setContextView}/><table className={s.comparison}><thead><tr><th scope="col"><span aria-label="Comparison metric"/></th><th scope="col">{f.home}<br />{contextView === 'all' ? 'All venues' : 'Home'}</th><th scope="col">{f.away}<br />{contextView === 'all' ? 'All venues' : 'Away'}</th></tr></thead><tbody><tr><td>{contextView === 'venue' ? 'Home / away rank' : 'League position'}</td>{[positions?.home, positions?.away].map((p, i) => <td key={i}>{positionText(p)}{p?.source === 'calculated' && <p className={s.caption}>Calculated from results</p>}{p?.points != null && <><br />{p.points} pts</>}{p?.show_group_name && <p className={s.caption}>{p.group_name}</p>}{!p && <p className={s.caption}>Not in table yet</p>}</td>)}</tr>{compareRows.map(([label, left, right, higher, rank]) => <tr key={label}><td>{label}</td>{[left, right].map((n, i) => <td key={i} className={left != null && right != null && left !== right && ((higher ? left > right : left < right) === (i === 0)) ? s.better : undefined}>{n == null ? '—' : rank ? `#${n}${(i === 0 ? h : a)?.league_size ? ` of ${(i === 0 ? h : a)?.league_size}` : ''}` : n.toFixed(2)}</td>)}</tr>)}</tbody></table><p className={s.caption}>Home / away rank uses the league’s venue records when available. Missing venue or overall ranks can be calculated from completed league results and are labelled. The full standings show the provider table. Other rows follow the venue switch. #1 is best. Shaded = better side. Standings updated {f.standings_updated_at ? stamp(f.standings_updated_at) : 'unavailable'}. Engine {f.run_finished_at ? stamp(f.run_finished_at) : 'unavailable'}.</p></div>
 <div className={s.supportingContext}>
 <details className={s.contextDisclosure}><summary>Head-to-head <span className={s.caption}>{c.h2h.meetings ? `Last ${c.h2h.meetings} meetings` : 'Previous meetings'}</span></summary>
 {c.h2h.meetings ? <><div className={s.h2hTally}>{[[f.home, c.h2h.tally?.home_wins], ['Draws', c.h2h.tally?.draws], [f.away, c.h2h.tally?.away_wins]].map(([name, wins]) => <div key={name}><strong>{wins ?? '—'}</strong><span>{name}</span></div>)}</div>
 <p className={s.caption}>Latest completed meetings in our records, across venues. Historical results, not a prediction.</p>
 {c.h2h.stats && <p className={s.caption}>{c.h2h.stats.goals_per_game.toFixed(1)} goals / game · Over 2.5: {c.h2h.stats.over_2_5}/{c.h2h.meetings} · Both scored: {c.h2h.stats.both_scored}/{c.h2h.meetings}</p>}
 <ol className={s.h2hGames}>{c.h2h.games.map(g => <li key={g.fixture_id}><time dateTime={g.kickoff_utc}>{new Date(g.kickoff_utc).toLocaleDateString('en-GB', {day: 'numeric', month: 'short', year: 'numeric', timeZone: zone || 'UTC'})}</time><div className={s.h2hScore}><span>{g.home}</span><strong>{g.home_goals}–{g.away_goals}</strong><span>{g.away}</span></div></li>)}</ol></> : <p className={s.caption}>No completed previous meetings available in our records.</p>}
 </details>
 <details className={s.contextDisclosure}><summary>League standings <span className={s.caption}>View full table</span></summary><h3>{f.league}{f.country && ` · ${f.country}`}</h3><p className={s.caption}>Current table · Updated {f.standings_updated_at ? stamp(f.standings_updated_at) : 'unavailable'}. Match teams are highlighted. — means unavailable.</p>
 {c.table.available ? c.table.groups.map(g => <div className={s.tableWrap} key={g.group_name} role="region" aria-label={`${g.group_name} standings`} tabIndex={0}><table className={s.leagueTable}><caption>{g.group_name}</caption><thead><tr><th scope="col">Pos</th><th scope="col">Team</th><th scope="col"><abbr title="Played">P</abbr></th><th scope="col"><abbr title="Goal difference">GD</abbr></th><th scope="col"><abbr title="Points">Pts</abbr></th></tr></thead><tbody>{g.rows.map(r => <tr key={r.team_id} className={[f.home_team_id, f.away_team_id].includes(r.team_id) ? s.better : ''}><td>{r.rank ?? '—'}</td><th scope="row">{r.team_name}</th><td>{r.games_played ?? '—'}</td><td>{r.goal_diff ?? '—'}</td><td><strong>{r.points ?? '—'}</strong></td></tr>)}</tbody></table></div>) : <p className={s.caption}>Standings are unavailable for this competition, or its groups cannot be reliably separated.</p>}</details></div></section>
 <div className={s.detailGrid}><div className={s.detailColumn}>{data.patterns && <section id="patterns" data-match-section className={s.panel}><h2>Team patterns</h2>{guide('patterns')}<Segment value={patternView} disabled={!data.patterns.venue} onChange={view => { setPatternView(view); setMore(false); }}/>{patterns ? <>{patterns.top_results.length > 0 && <><h3>Results</h3>{patterns.top_results.map(renderPattern)}</>}{patterns.top_attack_defence.length > 0 && <><h3>Attack / defence</h3>{patterns.top_attack_defence.map(renderPattern)}</>}{patterns.also.length > 0 && <><h3>Also</h3>{patterns.also.map(renderPattern)}</>}{!patterns.top_results.length && !patterns.top_attack_defence.length && !patterns.also.length && !patterns.see_all.length && <p>No qualifying patterns in this view.</p>}{patterns.see_all.length > 0 && <><Button aria-expanded={more} onClick={() => setMore(!more)}>{more ? 'Hide extra patterns' : `See all patterns · ${patterns.see_all.length} more`}</Button>{more && <>{patterns.see_all.map(renderPattern)}<Button onClick={() => { setMore(false); document.getElementById('patterns')?.scrollIntoView({ block: 'start' }); }}>Show fewer patterns ↑</Button></>}</>}</> : <p>No records for this venue view.</p>}<p className={s.caption}>Historical records, not predictions. Comparisons use the league average for the same event and scope. Small samples can change quickly.</p></section>}</div><div className={s.detailColumn}>{data.strength && <section id="strength" data-match-section className={`${s.panel} ${entry === 'strength' ? s.highlight : ''}`}><p className={s.caption}>Team strength</p>{guide('strength')}{data.strength.state === 'big_dog' ? <><h2>{data.strength.favourite} · Big dog</h2><h3>{data.strength.underdog} · Underdog</h3><hr /><p>Big dogs usually win in this league</p><p className={s.caption}>Based on results across several seasons.</p></> : data.strength.state === 'slight_favourite' ? <><h2>{data.strength.favourite} · {data.strength.level === 'favourite' ? 'Favourite' : data.strength.level === 'slight_edge' ? 'Slight edge' : 'Slight favourite'}</h2><h3>{data.strength.underdog} · {data.strength.level === 'favourite' ? 'Underdog' : 'Slight underdog'}</h3></> : <><h2>{data.strength.state === 'close' ? 'Closely matched' : 'No Big dog label'}</h2><p className={s.caption}>{data.strength.state === 'close' ? 'The sides are close on team strength.' : data.strength.reason === 'top_flight_only' ? 'Big dog labels are available for top-flight leagues.' : data.strength.reason === 'not_enough_matches' ? 'Not enough league matches for this label.' : 'League results do not support this label.'}</p></>}{data.strength.tiers && (data.strength.state === 'slight_favourite' || data.strength.state === 'close') && <div className={s.tierContext}><h3 className={s.tierTitle}>Within their league</h3><dl className={s.tierRows}><div><dt>{data.fixture.home}</dt><dd>{data.strength.tiers.home} tier</dd></div><div><dt>{data.fixture.away}</dt><dd>{data.strength.tiers.away} tier</dd></div></dl><p className={s.caption}>Based on team ratings, not table position.</p><details className={s.tierHelp}><summary>How are tiers grouped?</summary><p>Teams with similar Elo ratings are grouped into Top, Middle and Bottom tiers within the {data.fixture.league}. Groups can have different sizes. Tiers provide context; they do not determine the match verdict.</p></details></div>}</section>}{data.goals && <section id="goals" data-match-section className={s.panel}><h2>Goals outlook</h2>{guide('goals')}{data.goals.nothing_stands_out && <p className={s.caption}>Nothing stands out</p>}{data.goals.rows.map(g => <div className={`${s.evidence} ${entry === 'goals' && search.get('event') === g.market ? s.highlight : ''}`} key={g.market}><div><h3>{g.market}</h3></div><strong>{Math.round(g.probability * 100)}%</strong></div>)}<p className={s.caption}>Estimates, not guarantees. Related goal markets are not independent confirmations.</p></section>}</div></div>
 <details id="methodology" className={s.panel}><summary>How these numbers work</summary><p>Match context compares observed team records. Strength labels use team ratings, with a separate league-history rule for Big dogs. Patterns describe this season’s results and compare the same event and scope with the league where available.</p><p>Goals outlook uses a Dixon–Coles based model to estimate the chance of named goal outcomes from scoring and conceding evidence. Its percentages are estimates, not guarantees or exact score predictions; related goal markets can overlap.</p><p>Confirmed lineups and late injuries are not included. Nothing here says whether a bookmaker’s price offers value. Missing values are shown as —.</p><Link className={s.link} href="/forecast-record">Explore the forecast record →</Link></details><Toast message={saved.message}/></>;
}
export function MatchPage({ fixtureId }: {
    fixtureId: string;
}) { const resource = useResource(`/api/research/match/${encodeURIComponent(fixtureId)}`, matchSchema); return <Shell active="Matches"><ResourceState busy={resource.busy} error={resource.error} retry={resource.retry}/>{resource.data && <Details key={fixtureId} data={resource.data}/>}</Shell>; }
