'use client';
import Link from 'next/link';
import { usePreferences, useStamp } from './PreferencesProvider';
import { DiscoverDates } from './DiscoverDates';
import { FunctionalityTip } from './FunctionalityTip';
import { useSearchParams, useRouter, usePathname } from 'next/navigation';
import { useDeferredValue, useEffect, useRef, useState } from 'react';
import { discoverSchema, labels, headline, reason, matchHref, type Card, type Kind } from '@/lib/research';
import { Button, Crest, FixtureIdentity, Shell, ResourceState, useResource, useSaved, Toast } from './shared';
import s from './research.module.css';
import { DiscoverLeagues } from './DiscoverLeagues';
import { EvidenceFilter, EvidenceSort } from './EvidenceControls';
import { categoryResults, evidenceOptions, filterMatches, matchEvidence } from '@/lib/evidence-controls';
import { track } from '@/lib/usage-events';
import { MatchesControls } from './MatchesControls';
import { useFollowedLeagues } from './useFollowedLeagues';
import { useListPosition } from './useListPosition';
import { TeamSearch } from './TeamSearch';
function FixtureCard({ card, featured, matchPreview, matchingFinding, rank, total, saved, onSave }: {
    card: Card;
    featured?: boolean;
    matchPreview?: boolean;
    matchingFinding?: boolean;
    rank?: number;
    total?: number;
    saved: boolean;
    onSave: () => void;
}) {
    const stamp = useStamp();
    return <article className={`${s.card} ${featured ? s.featured : ''} ${matchPreview ? s.matchCard : ''}`} aria-label={`${card.home} vs ${card.away}`}>
 {featured && <p className={s.rank}>STANDOUT {rank} OF {total}</p>}
 <h3><FixtureIdentity card={card}/></h3>
 {featured && <div className={s.reason}><small>WHY IT STANDS OUT</small><p>{reason(card)}</p></div>}
 {!featured && !matchPreview && card.pick && <div className={s.categoryInsight}><p>{headline(card)}</p></div>}
 {matchPreview && <div className={s.matchInsight}>{card.preview ? <><small>{matchingFinding ? 'MATCHING INSIGHT' : 'LEADING INSIGHT'} · {labels[card.preview.list].toUpperCase()}</small><p>{headline({ ...card, pick: card.preview })}</p></> : <p className={s.caption}>No leading insight in this snapshot.</p>}</div>}
 <div className={`${s.caption} ${featured ? s.metadata : s.small}`}><p className={s.competition}><Crest name={card.league} url={card.league_logo_url} league/><span>{card.league}{card.country && ` · ${card.country}`}</span></p><p>{stamp(card.kickoff)}</p></div>
 <div className={featured ? s.actions : s.compactActions}>{featured ? <Button asChild className={s.active}><Link href={matchHref(card)}>Explore match</Link></Button> : <Link className={s.link} href={matchingFinding && card.preview ? matchHref({ ...card, pick: card.preview }) : matchHref(card, !matchPreview)}>View match →</Link>}<Button aria-pressed={saved} onClick={onSave}>{saved ? '✓ Saved' : '+ Save fixture'}</Button></div>
 </article>;
}
function OnboardingSaveTip() {
    const [visible, setVisible] = useState(false);
    useEffect(() => { setVisible(sessionStorage.getItem('ballistics:onboarding:save-tip') === '1'); }, []);
    if (!visible) return null;
    return <div className={s.notice} role="status">Tap Save on a fixture to build your matchday shortlist. <Button onClick={() => { sessionStorage.removeItem('ballistics:onboarding:save-tip'); setVisible(false); }}>Got it</Button></div>;
}
export function DiscoverPage({ all = false, matches = false }: {
    matches?: boolean;
    all?: boolean;
}) {
    const search = useSearchParams(), router = useRouter(), path = usePathname();
    const query = search.toString();
    const { zone } = usePreferences();
    const stamp = useStamp();
    const scope = ['week', 'today', 'tomorrow', 'date', 'range'].includes(search.get('scope') || '') ? search.get('scope')! : 'week';
    const selected = search.get('leagues')?.split(',').filter(Boolean) || [];
    const category = (['strength', 'goals', 'patterns'].includes(search.get('category') || '') ? search.get('category') : 'strength') as Kind;
    const params = new URLSearchParams(query);
    params.delete('category');
    params.delete('q');
    for (const key of ['goals_filter', 'patterns_filter', 'goals_sort', 'patterns_sort', 'finding']) params.delete(key);
    if (zone)
        params.set('zone', zone);
    const { data, busy, error, retry } = useResource(zone ? `/api/research/discover?${params}` : null, discoverSchema);
    const positionQuery = new URLSearchParams(query);
    if (!all && !matches) positionQuery.set('category', category);
    // The preference zone arrives asynchronously; it must not change the
    // return-position key halfway through restoring this list.
    positionQuery.sort();
    // The team search narrows the same list, so it keeps the list's position.
    const positionKey = new URLSearchParams(positionQuery);
    positionKey.delete('q');
    const position = useListPosition(`${path}?${positionKey}`, !!data);
    const saved = useSaved();
    const follows = useFollowedLeagues();
    useEffect(() => { if (follows.ready) track('discover_viewed', { page: matches ? 'matches' : all ? 'all' : 'discover', followed_league_count: follows.ids.length }); }, [follows.ready, matches, all]);
    const carousel = useRef<HTMLDivElement>(null);
    const [edges, setEdges] = useState({ start: true, end: true });
    const updateEdges = () => { const el = carousel.current; if (el)
        setEdges({ start: el.scrollLeft < 2, end: el.scrollLeft + el.clientWidth >= el.scrollWidth - 2 }); };
    useEffect(() => { updateEdges(); const el = carousel.current; if (!el)
        return; const observer = new ResizeObserver(updateEdges); observer.observe(el); return () => observer.disconnect(); }, [data]);
    function move(direction: number) { const el = carousel.current; if (el)
        el.scrollBy({ left: direction * ((el.firstElementChild as HTMLElement)?.offsetWidth + 16 || el.clientWidth), behavior: matchMedia('(prefers-reduced-motion: reduce)').matches ? 'instant' : 'smooth' }); }
    function change(key: string, value: string) {
        const next = new URLSearchParams(query); value ? next.set(key, value) : next.delete(key);
        // Refilter at the current controls, rather than jumping back above Standouts.
        if (key.endsWith('_filter') || key.endsWith('_sort') || key === 'finding') {
            const positionParams = new URLSearchParams(next);
            if (!all && !matches) positionParams.set('category', category);
            positionParams.delete('q');
            positionParams.sort();
            try { sessionStorage.setItem(`ballistics:list:${path}?${positionParams}`, JSON.stringify({ count: 12, y: window.scrollY, height: window.scrollY + window.innerHeight })); } catch { /* Optional position memory. */ }
        }
        router.replace(`${path}?${next}`, { scroll: false });
    }
    const scopeText = scope === 'week' ? 'Next 7 days' : scope === 'today' ? 'Today' : scope === 'tomorrow' ? 'Tomorrow' : scope === 'range' ? `${search.get('day') || 'Start'} to ${search.get('end_day') || 'end'}` : search.get('day') || 'Choose a date';
    const card = (item: Card, i: number, featured = false) => <FixtureCard key={item.fixture_id} card={item} featured={featured} matchPreview={matches} matchingFinding={matches && Boolean(search.get('finding'))} rank={i + 1} total={all ? data?.lists.standouts.length : Math.min(5, data?.lists.standouts.length || 0)} saved={saved.isSaved(item.fixture_id)} onSave={() => saved.toggle(item, data?.run_ds || null)}/>;
    // Deferred so refiltering the cards never blocks the controls.
    const teamQuery = useDeferredValue((search.get('q') || '').trim().toLocaleLowerCase());
    const teamMatches = (data?.matches || []).filter(f => !teamQuery || `${f.home} ${f.away}`.toLocaleLowerCase().includes(teamQuery));
    const availableEvidence = matchEvidence(data, teamMatches);
    const findingKey = search.get('finding') || '';
    const matchResults = filterMatches(teamMatches, availableEvidence, findingKey);
    const filterKey = search.get(`${category}_filter`) || '';
    const sortKey = search.get(`${category}_sort`) || '';
    const categoryCards = categoryResults(data?.lists[category] || [], category === 'strength' ? '' : filterKey, category !== 'strength' && sortKey === 'highest');
    const standoutResults = (data?.lists.standouts || []).filter(f => !all || !teamQuery || `${f.home} ${f.away}`.toLocaleLowerCase().includes(teamQuery));
    const shownMatches = matchResults.slice(0, position.count);
    const matchDays = new Map<string, Card[]>();
    const dayFormat = new Intl.DateTimeFormat('en-GB', { timeZone: zone || 'UTC', year: 'numeric', month: '2-digit', day: '2-digit' });
    for (const fixture of shownMatches) {
        const parts = dayFormat.formatToParts(new Date(fixture.kickoff));
        const value = (type: string) => parts.find(part => part.type === type)?.value || '';
        const day = `${value('year')}-${value('month')}-${value('day')}`;
        matchDays.set(day, [...(matchDays.get(day) || []), fixture]);
    }
    const dateLabel = (day: string) => new Intl.DateTimeFormat('en-GB', { timeZone: 'UTC', weekday: 'long', day: 'numeric', month: 'long', year: 'numeric' }).format(new Date(`${day}T12:00:00Z`));
    const matchScope = scope === 'range' ? 'Custom date range' : scopeText;
    const leagueScope = selected.length ? `${selected.length} ${selected.length === 1 ? 'league' : 'leagues'} selected` : 'All leagues';
    return <Shell active={matches ? 'Matches' : 'Discover'} mainMinHeight={position.restoringHeight}>{!all && !matches && <><OnboardingSaveTip/><FunctionalityTip id="discover" title="Explore the evidence" suppressIfOnboardingSaveTip moreHref="#discover-methodology"><p>Strength, Goals and Patterns are separate ways to find matches worth researching. Open a card to see its supporting evidence.</p></FunctionalityTip></>}{matches && <FunctionalityTip id="filters" title="Narrow the fixtures"><p>Choose dates and leagues above. Your leagues uses the leagues you follow in Settings; All leagues broadens the view. Matches lists fixtures in the current research snapshot.</p></FunctionalityTip>}<div className={s.controls}>{matches ? <MatchesControls data={data} zone={zone} follows={follows}/> : <>{matches && <><h1>Matches</h1><p className={s.caption}>Upcoming fixtures in the current engine snapshot, ordered by kickoff.</p></>}{all && <><Link className={s.link} href={`/discover?${query}`}>← Discover</Link><h1>All standouts</h1></>}<p>{scopeText}</p><details><summary>Help with dates and leagues</summary><p className={s.caption}>Choose a date and leagues to narrow this snapshot. Your leagues uses the leagues saved in Settings; All leagues broadens the view.</p></details><DiscoverDates zone={zone}/>{all && <TeamSearch className={s.leagueSearch}/>}<div className={s.row}><Button className={!selected.length ? s.active : ''} onClick={() => change('leagues', '')}>All leagues</Button>{follows.signedIn && follows.ready && !follows.error && follows.ids.length > 0 && <Button className={selected.length === follows.ids.length && follows.ids.every(id => selected.includes(String(id))) ? s.active : ''} disabled={!follows.ready} onClick={() => change('leagues', follows.ids.join(','))}>Your leagues · {follows.ids.length}</Button>}<DiscoverLeagues leagues={data?.leagues} selected={selected} onApply={value => change('leagues', value)}/>{follows.signedIn && follows.ready && !follows.error && !follows.ids.length && <Link className={s.link} href="/settings">Set your leagues →</Link>}</div><p className={s.caption}>{data?.run_finished_at ? `Engine updated ${stamp(data.run_finished_at)}` : 'Engine snapshot'} · {zone || 'Detecting timezone…'}</p></>}</div>
 {follows.error && <p className={s.notice} role="alert">Could not load your leagues. <Button onClick={follows.reload}>Retry leagues</Button></p>}{follows.signedIn && follows.ids.length > 0 && <Link className={s.link} href="/settings">Manage followed leagues →</Link>}<ResourceState busy={busy} error={error} retry={retry}/>{data && <>{data.coverage === 'needs_refresh' && <p className={s.notice}>Discover needs a refreshed engine snapshot. Rankings from older snapshots are not substituted.</p>}{matches ? <section className={s.section}><div className={s.row}><EvidenceFilter label="Finding" allLabel="All findings" options={evidenceOptions(availableEvidence)} value={findingKey} onChange={value => change('finding', value)} total={teamMatches.length}/><span className={s.caption}>Ordered by kickoff</span></div><p className={s.caption}>Filters use qualifying findings available in this snapshot.</p><h2 aria-live="polite">{matchResults.length} {matchResults.length === 1 ? 'fixture' : 'fixtures'}</h2><p className={s.caption}>{matchScope} · {zone || 'UTC'} · {leagueScope}{teamQuery ? ` · Teams: ${search.get('q')}` : ''} · Engine updated {data.run_finished_at ? stamp(data.run_finished_at) : 'unavailable'}</p>{data.matches === undefined ? <p role="alert">Restart the research API to load the Matches list.</p> : matchResults.length ? <>{[...matchDays].map(([day, fixtures]) => <div className={s.matchDay} key={day}><h3>{dateLabel(day)}</h3><div className={s.grid}>{fixtures.map((f, i) => card(f, i))}</div></div>)}{position.count < matchResults.length && <Button onClick={position.more}>Show more matches</Button>}</> : <p className={s.empty}>{findingKey ? 'No fixtures have this qualifying finding in the selected dates, leagues and team search. Clear the finding filter to broaden the results.' : teamQuery ? 'No teams match this search in the selected dates and leagues.' : 'No fixtures for these dates and leagues.'}</p>}</section> : <><section className={s.section} aria-label="Standouts">{!all && <div className={s.between}><h1>Standouts</h1><div className={s.row}><Link className={s.link} href={`/standouts?${query}`}>All standouts →</Link><div className={`${s.row} ${s.arrows}`}><Button className={s.active} aria-label="Previous standouts" disabled={edges.start} onClick={() => move(-1)}>← Previous</Button><Button className={s.active} aria-label="Next standouts" disabled={edges.end} onClick={() => move(1)}>Next →</Button></div></div></div>}<p className={s.caption}>{scopeText}</p>{all && <p className={s.caption}>{standoutResults.length} {standoutResults.length === 1 ? 'standout' : 'standouts'}{teamQuery ? ` matching “${search.get('q')}”` : ''}</p>}{standoutResults.length ? <div className={all ? s.grid : s.carousel} ref={all ? undefined : carousel} onScroll={updateEdges}>{(all ? standoutResults.slice(0, position.count) : standoutResults.slice(0, 5)).map((f, i) => card(f, all ? data.lists.standouts.indexOf(f) : i, true))}</div> : <p className={s.empty}>{all && teamQuery ? 'No teams match this search in the selected dates and leagues.' : 'No standouts for these dates and leagues. Try a wider selection.'}</p>}{all && position.count < standoutResults.length && <Button onClick={position.more}>Show more standouts</Button>}</section>{!all && <section className={s.section}><h2 className={s.sectionTitle}>Explore by category</h2><div className={s.tabs} aria-label="Discover category">{(Object.keys(labels) as Kind[]).map(kind => <Button key={kind} className={category === kind ? s.active : ''} aria-pressed={category === kind} onClick={() => change('category', kind)}>{labels[kind]}</Button>)}</div>{category !== 'strength' && <div className={s.row}><EvidenceFilter label={category === 'goals' ? 'Market' : 'Pattern'} allLabel={category === 'goals' ? 'All markets' : 'All patterns'} options={evidenceOptions(data.lists[category])} value={filterKey} onChange={value => change(`${category}_filter`, value)} total={data.lists[category].length}/><EvidenceSort kind={category} value={sortKey} onChange={value => change(`${category}_sort`, value)}/></div>}<h3 aria-live="polite">{labels[category]} · {categoryCards.length} fixtures</h3>{categoryCards.length ? <div className={s.grid}>{categoryCards.slice(0, position.count).map((f, i) => card(f, i))}</div> : <p className={s.empty}>{filterKey ? 'No fixtures match this filter. Clear it or choose other dates and leagues.' : 'No fixtures in this category for the selected dates and leagues.'}</p>}{position.count < categoryCards.length && <Button onClick={position.more}>Show more {labels[category].toLowerCase()}</Button>}</section>}</>}</>}
 <details id="discover-methodology"><summary>How it works</summary><p className={s.caption}>Standouts show each fixture’s strongest qualifying reason. Categories are ranked separately, so a fixture can appear in more than one category. Team patterns are historical records; goals outlook values are estimates.</p></details><Toast message={saved.message}/></Shell>;
}
