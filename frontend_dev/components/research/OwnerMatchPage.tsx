'use client';
import { ArrowLeft, Bookmark } from 'lucide-react';
import { FINAL, IN_PLAY } from '@/lib/live';
import { MatchForecastRecord } from './MatchForecastRecord';
import { matchReturnTo } from '@/lib/research';
import Link from 'next/link';
import { usePreferences, useStamp } from './PreferencesProvider';
import { useSearchParams, useRouter } from 'next/navigation';
import { useEffect, useRef, useState } from 'react';
import { useUser } from '@clerk/nextjs';
import { matchSchema, type Match, type Pattern, type Card } from '@/lib/research';
import { Shell, Button, Crest, FixtureIdentity, ResourceState, useResource, useSaved, Toast } from './shared';
import s from './research.module.css';
import { matchGuideKey } from './FunctionalityTip';
import { EvidenceSort } from './EvidenceControls';
import { highestFirst } from '@/lib/evidence-controls';
import { track } from '@/lib/usage-events';
import { LiveForecast, LiveScore, useMatchLive } from './Live';
// Mirrors FORECAST_STATUSES in engine/api/match_detail.py: not started, in play and finished keep the forecast.
const forecastStatuses = new Set(['NS', '1H', 'HT', '2H', 'ET', 'BT', 'P', 'LIVE', 'FT', 'AET', 'PEN']);
// Statuses refresh nightly (00:00 UTC), so a match played today reads NS until then.
const UNCONFIRMED_AFTER_MS = 36 * 3600 * 1000;
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
    const live = useMatchLive(f.fixture_id, f.kickoff_utc);
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
                : section === 'goals' ? <><strong>What goal outcomes look likely?</strong><p>Each row shows a goal outcome and the estimated chance of it happening in this match. The estimate draws on scoring and conceding evidence, but it is not a promised score or an exact goal range.</p><p>Related goal outcomes can overlap, so several high percentages are not separate confirmations.</p>{data.goals?.nothing_stands_out && <p>The available estimates do not identify a standout goal outcome here.</p>}</>
                : <><strong>What has each team been doing this season?</strong><p>These are observed results and goal patterns from this season. A count such as 6/8 means that event happened in six of eight relevant matches. The league comparison uses the same event and home/away or all-venue setting.</p><p>Patterns describe what has happened, not the chance of it happening next time. Smaller samples can change quickly.</p></>}
            <div className={s.row}>{guideIndex > 0 && <Button onClick={() => setGuideIndex(guideIndex - 1)}>Back</Button>}<Button className={s.active} onClick={() => last ? closeGuide('done') : setGuideIndex(guideIndex + 1)}>{last ? 'Done' : `Look at ${guideSections[guideIndex + 1]}`}</Button><Button onClick={() => closeGuide('skip')}>Skip guide</Button></div>
            <a className={s.link} href="#methodology" onClick={() => { track('methodology_opened', { from_section: 'match_guide' }); const details = document.getElementById('methodology') as HTMLDetailsElement | null; if (details) details.open = true; }}>{section === 'goals' ? 'How these estimates work' : 'Read how the numbers work'} →</a>
        </aside>;
    };
    const search = useSearchParams(), router = useRouter(), saved = useSaved();
    const [contextView, setContextView] = useState<'venue' | 'all'>('venue');
    const [standingsView, setStandingsView] = useState<'all' | 'home' | 'away'>('all');
    const [resultsView, setResultsView] = useState<'recent' | 'meetings'>('recent');
    const [expandedRecent, setExpandedRecent] = useState({ home: false, away: false });
    const [patternView, setPatternView] = useState<'venue' | 'all'>(search.get('section') === 'patterns' && search.get('context') ? (search.get('context') === 'all' || !data.patterns?.venue ? 'all' : 'venue') : data.patterns?.default_view || 'all');
    const [more, setMore] = useState(search.get('section') === 'patterns');
    const entry = ['strength', 'patterns', 'goals'].includes(search.get('section') || '') ? search.get('section') : null;
    const [active, setActive] = useState(entry === 'strength' ? 'overview' : entry || 'overview');
    const [compactHeader, setCompactHeader] = useState(false);
    useEffect(() => {
        const update = () => setCompactHeader(current => window.scrollY > 180 ? true : window.scrollY < 60 ? false : current);
        update(); window.addEventListener('scroll', update, {passive:true});
        return () => window.removeEventListener('scroll', update);
    }, []);
    useEffect(() => {
        const nav = document.querySelector<HTMLElement>('[aria-label="Match sections"]');
        const chip = nav?.querySelector<HTMLElement>('[aria-current="location"]');
        if (nav && chip) {
            const bounds = nav.getBoundingClientRect(), item = chip.getBoundingClientRect();
            if (item.left < bounds.left) nav.scrollLeft -= bounds.left - item.left;
            else if (item.right > bounds.right) nav.scrollLeft += item.right - bounds.right;
        }
    }, [active]);
    const headerRef = useRef<HTMLDivElement>(null);
    useEffect(() => {
        const root = document.documentElement;
        const previousPadding = root.style.scrollPaddingTop;
        root.dataset.matchDetails = 'true';
        const header = headerRef.current;
        const measure = () => {
            if (header) root.style.scrollPaddingTop = `${header.getBoundingClientRect().height + 16}px`;
        };
        measure();
        const observer = new ResizeObserver(measure);
        if (header) observer.observe(header);
        return () => {
            observer.disconnect();
            delete root.dataset.matchDetails;
            root.style.scrollPaddingTop = previousPadding;
        };
    }, []);
    useEffect(() => {
        if (entry) document.getElementById(entry)?.scrollIntoView({ block: 'start' });
        const sections = [...document.querySelectorAll<HTMLElement>('[data-match-section]')];
        const update = () => {
            const headerBottom = document.querySelector('[aria-label="Match sections"]')?.getBoundingClientRect().bottom ?? 150;
            let current = sections[0];
            for (const section of sections) if (section.getBoundingClientRect().top <= headerBottom + 60) current = section;
            if (current) setActive(current.id);
        };
        update();
        window.addEventListener('scroll', update, { passive: true });
        window.addEventListener('resize', update);
        return () => { window.removeEventListener('scroll', update); window.removeEventListener('resize', update); };
    }, [entry]);
    const card: Card = { fixture_id: f.fixture_id, home: f.home, away: f.away, league_id: f.league_id, league: f.league, country: f.country, kickoff: f.kickoff_utc, home_crest_url: f.home_crest_url, away_crest_url: f.away_crest_url, league_logo_url: f.league_logo_url, pick: null };
    const h = c.form?.home_team[contextView === 'all' ? 'overall' : 'home'], a = c.form?.away_team[contextView === 'all' ? 'overall' : 'away'];
    const [goalsSort, setGoalsSort] = useState('');
    const [patternsSort, setPatternsSort] = useState('');
    const originalPatterns = data.patterns?.[patternView];
    const patterns = originalPatterns && (patternsSort === 'highest' ? {
        ...originalPatterns,
        top_results: highestFirst(originalPatterns.top_results, p => p.k / p.n),
        top_attack_defence: highestFirst(originalPatterns.top_attack_defence, p => p.k / p.n),
        also: highestFirst(originalPatterns.also, p => p.k / p.n),
        see_all: highestFirst(originalPatterns.see_all, p => p.k / p.n),
    } : originalPatterns);
    const goalsRows = goalsSort === 'highest' ? highestFirst(data.goals?.rows || [], g => g.probability) : data.goals?.rows || [];
    const positions = contextView === 'venue' ? c.position.venue : c.position;
    const tableTeams = c.table.groups.flatMap(group => group.rows.map(row => ({ ...row, groupName: group.group_name })));
    const homeTable = tableTeams.find(row => row.team_id === f.home_team_id);
    const awayTable = tableTeams.find(row => row.team_id === f.away_team_id);
    const standingSummary = c.table.available && homeTable?.rank != null && awayTable?.rank != null && homeTable.groupName === awayTable.groupName
        ? `Overall · ${f.home} #${homeTable.rank} · ${f.away} #${awayTable.rank}` : 'View full table';
    // After kickoff the page keeps the last pre-kickoff forecast (D152). The latest
    // run often still holds the fixture on match day, so check the run's finish time.
    const kickoff = Date.parse(f.kickoff_utc);
    // Still NS after at least one nightly update since kickoff: the provider has not
    // confirmed the match (it can keep a postponed match NS on its old date).
    const unconfirmed = f.fixture_status === 'NS' && Date.now() - kickoff > UNCONFIRMED_AFTER_MS;
    const preKickoff = !unconfirmed && f.run_finished_at != null && kickoff <= Date.now() && Date.parse(f.run_finished_at) <= kickoff;
    const dataIssues = data.data_quality?.issues ?? [];
    const compareRows: [
        string,
        number | null | undefined,
        number | null | undefined,
        boolean,
        boolean
    ][] = [['Goals scored / match', h?.attack_rate, a?.attack_rate, true, false], ['Goals conceded / match', h?.defense_rate, a?.defense_rate, false, false], ['Attack rank', h?.attack_rank, a?.attack_rank, false, true], ['Defence rank', h?.defense_rank, a?.defense_rank, false, true]];
    const positionText = (p: { rank: number | null; group_size: number } | null | undefined) => p?.rank == null ? '—' : `#${p.rank} of ${p.group_size}`;
    const renderPattern = (p: Pattern) => <PatternRow key={`${p.team}:${p.event}:${p.context}`} p={p} highlight={entry === 'patterns' && search.get('team') === p.team && search.get('event') === p.event}/>;
    const preview = forecastStatuses.has(f.fixture_status || '') || !!live.data?.state;
    const status = live.data?.state?.status ?? f.fixture_status ?? 'NS';
    const stage = FINAL.has(status) ? 'Full time' : IN_PLAY.has(status) ? 'Live' : 'Before kickoff';
    const [finishedSection, setFinishedSection] = useState('forecast-record');
    const finishedPreview = preview && stage === 'Full time';
    const [researchOpen, setResearchOpen] = useState(false);
    const showResearch = !preview || stage === 'Before kickoff' || (stage === 'Live' && researchOpen);
    const goal = data.goals?.rows[0];
    const pattern = originalPatterns?.top_results[0] ?? originalPatterns?.top_attack_defence[0] ?? originalPatterns?.also[0];
    const title = `${f.home} v ${f.away}`;
    function navigateSection(event: React.MouseEvent<HTMLDivElement>) {
        const link = (event.target as HTMLElement).closest('a[href^="#"]');
        if (!link || event.defaultPrevented || event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
        const hash = link.getAttribute('href')!;
        const target = document.getElementById(hash.slice(1));
        if (!target) return;
        event.preventDefault();
        window.history.replaceState(window.history.state, '', `${window.location.pathname}${window.location.search}${hash}`);
        target.scrollIntoView({ block: 'start' });
    }
    return <div className={`${s.matchBody} ${s.ownerDetails}`} onClick={navigateSection}><div ref={headerRef} className={`${s.sticky} ${s.matchSticky} ${compactHeader ? s.ownerHeaderCompact : ''}`}><div className={s.row}><Button aria-label="Back to previous page" onClick={() => { router.replace(matchReturnTo(search.get('returnTo')), { scroll: false }); }}><ArrowLeft size={18} aria-hidden="true"/></Button><div className={s.stickyTitle}><h1 aria-label={title}><FixtureIdentity card={f}/></h1></div><Button className={s.headerBookmark} aria-label={saved.isSaved(f.fixture_id) ? 'Remove match from Saved' : 'Save match'} title={saved.isSaved(f.fixture_id) ? 'Remove from Saved' : 'Save match'} aria-pressed={saved.isSaved(f.fixture_id)} onClick={() => saved.toggle(card, f.run_ds)}><Bookmark size={20} fill={saved.isSaved(f.fixture_id) ? 'currentColor' : 'none'} aria-hidden="true"/></Button></div><div className={s.matchMetadata}><p className={`${s.caption} ${s.competition}`}><Crest name={f.league} url={f.league_logo_url} league/><span>{f.league}{f.country && ` · ${f.country}`}</span></p><p className={s.caption}>{stamp(f.kickoff_utc)}</p></div>{stage !== 'Before kickoff' && <><LiveScore live={live.data}/>{!live.data?.state && <p className={s.caption}>{stage} · Score unavailable</p>}</>}<span className={s.srOnly}>{stage}</span>{showResearch && <nav className={s.chips} aria-label="Match sections">{['overview', ...(data.goals ? ['goals'] : []), ...(data.patterns ? ['patterns'] : []), 'past-results', 'standings'].map(key => <a href={`#${key}`} className={active === key ? s.active : ''} aria-current={active === key ? 'location' : undefined} onClick={() => setActive(key)} key={key} aria-label={key === 'goals' ? 'Goals outlook' : key === 'patterns' ? 'Team patterns' : key === 'standings' ? 'League standings' : undefined}>{key === 'past-results' ? 'Past results' : key === 'standings' ? 'Standings' : key[0].toUpperCase() + key.slice(1)}</a>)}</nav>} {finishedPreview && <nav className={s.chips} aria-label="Full time sections">
   {[['forecast-record', 'Forecast record'], ['past-results', 'Past results'], ['standings', 'Standings']].map(([key, label]) =>
     <Button key={key} aria-pressed={finishedSection === key} className={finishedSection === key ? s.active : ''} onClick={() => setFinishedSection(key)}>{label}</Button>)}
 </nav>}
</div>
 {preview && stage === 'Live' && <>
 <LiveForecast data={data} live={live.data} failed={live.failed}/>
 <section className={s.panel} aria-label="Pre-match research">
 <Button aria-expanded={researchOpen} onClick={() => setResearchOpen(!researchOpen)}>{researchOpen ? 'Hide pre-match research' : 'View pre-match research'}</Button>
 <p className={s.caption}>{f.run_finished_at && Date.parse(f.run_finished_at) <= kickoff ? 'These estimates were made before kickoff; they do not update with the live score.' : 'These estimates do not update with the live score. Pre-kickoff timing could not be confirmed.'}</p>
 {!live.data?.state && <p role="status">Live score unavailable. Retrying.</p>}
 </section></>}
 {finishedPreview && <div hidden={finishedSection !== 'forecast-record'}><MatchForecastRecord fixtureId={f.fixture_id}/></div>}
 {showResearch && <>
 {f.fixture_status && !forecastStatuses.has(f.fixture_status) && <p className={s.notice} role="status">{statusNotices[f.fixture_status] || 'This match is not going ahead as scheduled.'} The pre-match forecast is not shown for this match.</p>}
 {unconfirmed && <p className={s.notice} role="status"><strong>Kickoff not confirmed.</strong> Our data provider still lists this match as not started, more than a day after its scheduled time. It may have been postponed; the date will update when the provider confirms it.{f.run_finished_at && ' Any forecast below was made before the original kickoff.'}</p>}
 {dataIssues.length > 0 && <details className={s.dataQuality}><summary>Data check · {dataIssues.length} {dataIssues.length === 1 ? 'note' : 'notes'}</summary><ul>{dataIssues.map(i => <li key={`${i.section}:${i.code}`}>{i.message}</li>)}</ul></details>}
 <div id="overview" data-match-section className={s.overviewGroup}>
 <h2>Overview</h2>
 {preKickoff && <p className={s.notice} role="status"><strong>Made before kickoff.</strong> Strength, Patterns and Goals below are the forecast from engine run {f.run_ds}, finished {stamp(f.run_finished_at!)}. They are not updated during or after the match.</p>}
 {!preview && <LiveForecast data={data} live={live.data} failed={live.failed}/>}
 {data.strength && <section id="strength" className={`${s.panel} ${s.strengthLead} ${data.strength.state === 'big_dog' ? s.strengthBigDog : ''} ${entry === 'strength' ? s.strengthFocus : ''}`}><p className={s.caption}>Team strength</p>{data.strength.state === 'big_dog' ? <><h2>{data.strength.favourite} · Big dog</h2><h3>{data.strength.underdog} · Underdog</h3><hr /><p>Big dogs usually win in this league</p><p className={s.caption}>Based on results across several seasons.</p></> : data.strength.state === 'slight_favourite' ? <><h2>{data.strength.favourite} · {data.strength.level === 'favourite' ? 'Favourite' : data.strength.level === 'slight_edge' ? 'Slight edge' : 'Slight favourite'}</h2><h3>{data.strength.underdog} · {data.strength.level === 'favourite' ? 'Underdog' : 'Slight underdog'}</h3></> : <><h2>{data.strength.state === 'close' ? 'Closely matched' : 'No Big dog label'}</h2><p className={s.caption}>{data.strength.state === 'close' ? 'The sides are close on team strength.' : data.strength.reason === 'top_flight_only' ? 'Big dog labels are available for top-flight leagues.' : data.strength.reason === 'not_enough_matches' ? 'Not enough league matches for this label.' : 'League results do not support this label.'}</p></>}{data.strength.tiers && (data.strength.state === 'slight_favourite' || data.strength.state === 'close') && <div className={s.tierContext}><h3 className={s.tierTitle}>Within their league</h3><dl className={s.tierRows}><div><dt>{data.fixture.home}</dt><dd>{data.strength.tiers.home} tier</dd></div><div><dt>{data.fixture.away}</dt><dd>{data.strength.tiers.away} tier</dd></div></dl><p className={s.caption}>Based on team ratings, not table position.</p><details className={s.tierHelp}><summary>How are tiers grouped?</summary><p>Teams with similar strength ratings are grouped into Top, Middle and Bottom tiers within the {data.fixture.league}. Groups can have different sizes. Tiers provide context; they do not determine the match verdict.</p></details></div>}{guide('strength')}</section>}
 {(goal || pattern) && <section className={`${s.panel} ${s.overviewHighlights}`} aria-label="Match overview evidence">
 {goal && <a href="#goals" className={s.evidence}><span><span className={s.caption}>Goals estimate</span><strong>{goal.market}</strong></span><strong>{Math.round(goal.probability * 100)}%</strong></a>}
 {pattern && <a href="#patterns" className={s.evidence}><span><span className={s.caption}>Season pattern · {pattern.team}</span><strong>{pattern.label}</strong><small>{pattern.context === 'all' ? 'All venues' : pattern.context === 'home' ? 'At home' : 'Away games'}</small></span><strong>{pattern.k}/{pattern.n}</strong></a>}
 <p className={s.caption}>Open a finding to see its supporting evidence.</p>
 </section>}


 </div>
 {data.goals && <section id="goals" data-match-section className={s.panel}><h2>Goals outlook</h2>{guide('goals')}<EvidenceSort kind="goals" value={goalsSort} onChange={setGoalsSort}/>{data.goals.nothing_stands_out && <p className={s.caption}>Nothing stands out</p>}{goalsRows.map(g => <div className={`${s.evidence} ${entry === 'goals' && search.get('event') === g.market ? s.highlight : ''}`} key={g.market}><div><h3>{g.market}</h3></div><strong>{Math.round(g.probability * 100)}%</strong></div>)}<p className={s.caption}>Estimates, not guarantees. Related goal markets are not independent confirmations.</p></section>}
 {data.patterns && <section id="patterns" data-match-section className={s.panel}><h2>Team patterns</h2>{guide('patterns')}<EvidenceSort kind="patterns" value={patternsSort} onChange={setPatternsSort}/>{patternsSort === 'highest' && <p className={s.caption}>Highest hit rate first within each group.</p>}<Segment value={patternView} disabled={!data.patterns.venue} onChange={view => { setPatternView(view); setMore(false); }}/>{patterns ? <>{patterns.top_results.length > 0 && <><h3>Results</h3>{patterns.top_results.map(renderPattern)}</>}{patterns.top_attack_defence.length > 0 && <><h3>Attack / defence</h3>{patterns.top_attack_defence.map(renderPattern)}</>}{patterns.also.length > 0 && <><h3>Also</h3>{patterns.also.map(renderPattern)}</>}{!patterns.top_results.length && !patterns.top_attack_defence.length && !patterns.also.length && !patterns.see_all.length && <p>No qualifying patterns in this view.</p>}{patterns.see_all.length > 0 && <><Button aria-expanded={more} onClick={() => setMore(!more)}>{more ? 'Hide extra patterns' : `See all patterns · ${patterns.see_all.length} more`}</Button>{more && <>{patterns.see_all.map(renderPattern)}<Button onClick={() => { setMore(false); document.getElementById('patterns')?.scrollIntoView({ block: 'start' }); }}>Show fewer patterns ↑</Button></>}</>}</> : <p>No records for this venue view.</p>}<p className={s.caption}>Historical records, not predictions. Comparisons use the league average for the same event and scope. Small samples can change quickly.</p></section>}
 </>}
 {(showResearch || finishedPreview) && <div hidden={finishedPreview && finishedSection !== 'past-results'}>
 <section id="past-results" data-match-section className={s.panel}><h2>Past results</h2><p className={s.caption}>Each team’s recent games and previous meetings</p>
 <div className={`${s.segment} ${s.pastTabs}`} role="group" aria-label="Past results view"><Button className={resultsView === 'recent' ? s.active : ''} aria-pressed={resultsView === 'recent'} onClick={() => setResultsView('recent')}>Recent games</Button><Button className={resultsView === 'meetings' ? s.active : ''} aria-pressed={resultsView === 'meetings'} onClick={() => setResultsView('meetings')}>Previous meetings</Button></div>
 {resultsView === 'recent' ? <><p className={s.caption}>Latest completed fixtures before this match, across competitions. Scores and W/D/L are from each team’s perspective.</p><div className={s.recentColumns}>{(['home', 'away'] as const).map(side => { const teamName = side === 'home' ? f.home : f.away; const games = c.recent?.[side] || []; const expanded = expandedRecent[side]; return <div className={s.recentTeam} key={side}><h4>{teamName} · last {games.length} {games.length === 1 ? 'game' : 'games'}</h4>{games.length ? <><ol className={`${s.h2hGames} ${s.recentGames}`}>{games.slice(0, expanded ? 5 : 3).map(g => <li key={g.fixture_id}><time dateTime={g.kickoff_utc}>{new Date(g.kickoff_utc).toLocaleDateString('en-GB', {day:'numeric', month:'short', year:'numeric', timeZone: zone || 'UTC'})}</time><div className={s.h2hScore}><span>{g.venue === 'home' ? 'vs' : 'at'} {g.opponent}</span><strong>{g.scored}–{g.conceded}</strong><span className={`${s.resultBadge} ${g.result === 'W' ? s.formWin : g.result === 'D' ? s.formDraw : s.formLoss}`}>{g.result}</span></div><small>{g.league}</small></li>)}</ol>{games.length > 3 && <Button className={s.recentMore} aria-expanded={expanded} onClick={() => setExpandedRecent(value => ({ ...value, [side]: !value[side] }))}>{expanded ? 'Show fewer' : `Show all ${games.length} games`}</Button>}</> : <p className={s.caption}>No completed recent fixtures in our records.</p>}</div>; })}</div><p className={s.caption}>Past results provide context, not a prediction.</p></> : c.h2h.meetings ? <><div className={s.h2hTally}>{[[f.home, c.h2h.tally?.home_wins], ['Draws', c.h2h.tally?.draws], [f.away, c.h2h.tally?.away_wins]].map(([name, wins]) => <div key={name}><strong>{wins ?? '—'}</strong><span>{name}</span></div>)}</div><p className={s.caption}>Latest completed meetings in our records, across venues. Historical results, not a prediction.</p>{c.h2h.stats && <p className={s.caption}>{c.h2h.stats.goals_per_game.toFixed(1)} goals / game · Over 2.5: {c.h2h.stats.over_2_5}/{c.h2h.meetings} · Both scored: {c.h2h.stats.both_scored}/{c.h2h.meetings}</p>}<ol className={s.h2hGames}>{c.h2h.games.map(g => <li key={g.fixture_id}><time dateTime={g.kickoff_utc}>{new Date(g.kickoff_utc).toLocaleDateString('en-GB', {day: 'numeric', month: 'short', year: 'numeric', timeZone: zone || 'UTC'})}</time><div className={s.h2hScore}><span>{g.home}</span><strong>{g.home_goals}–{g.away_goals}</strong><span>{g.away}</span></div></li>)}</ol></> : <p className={s.caption}>No completed previous meetings available in our records.</p>}
 </section>
 </div>}
 {(showResearch || finishedPreview) && <div hidden={finishedPreview && finishedSection !== 'standings'}>
 <section id="standings" data-match-section className={s.panel}><h2>League standings</h2><p className={s.caption}>{standingSummary}</p><p className={s.caption}>{f.league}{f.country && ` · ${f.country}`} · Updated {f.standings_updated_at ? stamp(f.standings_updated_at) : 'unavailable'}. Match teams are highlighted.</p>
 <div className={s.segment} role="group" aria-label="Standings venue"><Button className={standingsView === 'all' ? s.active : ''} aria-pressed={standingsView === 'all'} onClick={() => setStandingsView('all')}>All venues</Button><Button className={standingsView === 'home' ? s.active : ''} aria-pressed={standingsView === 'home'} onClick={() => setStandingsView('home')}>Home</Button><Button className={standingsView === 'away' ? s.active : ''} aria-pressed={standingsView === 'away'} onClick={() => setStandingsView('away')}>Away</Button></div><p className={s.caption}>{standingsView === 'all' ? 'Official overall position.' : 'Venue rank calculated from the provider’s complete split records within each group.'}</p><p className={`${s.caption} ${s.mobileSwipeCue}`}>Swipe table sideways for more columns →</p>
 {c.table.available ? c.table.groups.map(g => standingsView !== 'all' && !g[`${standingsView}_available`] ? <p key={g.group_name} className={s.caption}>{g.group_name}: {standingsView} records are incomplete, so this table is unavailable.</p> : <div className={s.tableWrap} key={g.group_name} role="region" aria-label={`${g.group_name} standings; scroll horizontally for all columns`} tabIndex={0}><table className={s.leagueTable}><caption>{g.group_name}</caption><thead><tr><th scope="col">Pos</th><th scope="col">Team</th><th scope="col"><abbr title="Played">P</abbr></th><th scope="col"><abbr title="Won">W</abbr></th><th scope="col"><abbr title="Drawn">D</abbr></th><th scope="col"><abbr title="Lost">L</abbr></th><th scope="col"><abbr title="Goals for">GF</abbr></th><th scope="col"><abbr title="Goals against">GA</abbr></th><th scope="col"><abbr title="Goal difference">GD</abbr></th><th scope="col"><abbr title="Points">Pts</abbr></th><th scope="col">Last 5</th></tr></thead><tbody>{[...g.rows].sort((a, b) => standingsView === 'all' ? 0 : (a[standingsView]?.rank ?? 999) - (b[standingsView]?.rank ?? 999)).map(team => { const r = standingsView === 'all' ? team : team[standingsView]; return <tr key={team.team_id} className={[f.home_team_id, f.away_team_id].includes(team.team_id) ? s.better : ''}><td>{r.rank ?? '—'}</td><th scope="row">{team.team_name}</th><td>{r.games_played ?? '—'}</td><td>{r.wins ?? '—'}</td><td>{r.draws ?? '—'}</td><td>{r.losses ?? '—'}</td><td>{r.goals_for ?? '—'}</td><td>{r.goals_against ?? '—'}</td><td>{r.goal_diff ?? '—'}</td><td><strong>{r.points ?? '—'}</strong></td><td>{r.form && /^[WDL]+$/i.test(r.form) ? <span className={s.formSequence} aria-label={`Recent form ${r.form.toUpperCase().split('').join(', ')}`}>{r.form.toUpperCase().slice(-5).split('').map((result: string, i: number) => <span key={i} className={result === 'W' ? s.formWin : result === 'D' ? s.formDraw : s.formLoss} aria-hidden="true">{result}</span>)}</span> : '—'}</td></tr>})}</tbody></table></div>) : <p className={s.caption}>Standings are unavailable for this competition, or its groups cannot be reliably separated.</p>}<p className={s.caption}>P played · W won · D drawn · L lost · GF goals scored · GA goals conceded · GD goal difference. Last 5 shows recent results where available. Missing values appear as —.</p></section>
 </div>}
 {showResearch && <>
 <details id="methodology" className={s.panel}><summary>How these numbers work</summary><p>Strength labels compare the teams’ ratings. Big dog labels also take account of how strong favourites have performed in that league. Patterns describe this season’s results and compare the same event and scope with the league where available.</p><p>Goals outlook uses teams’ scoring and conceding records to estimate how likely different goal outcomes are. Its percentages are estimates, not guarantees or exact score predictions; related goal markets can overlap.</p><p>Confirmed lineups and late injuries are not included. Nothing here says whether a bookmaker’s price offers value. Missing values are shown as —.</p><Link className={s.link} href="/forecast-record">Explore the forecast record →</Link></details></>}<Toast message={saved.message}/></div>;
}
export function OwnerMatchPage({ fixtureId }: {
    fixtureId: string;
}) { const resource = useResource(`/api/research/match/${encodeURIComponent(fixtureId)}`, matchSchema); return <Shell active="Matches"><ResourceState busy={resource.busy} error={resource.error} retry={resource.retry}/>{resource.data && <Details key={fixtureId} data={resource.data}/>}</Shell>; }
