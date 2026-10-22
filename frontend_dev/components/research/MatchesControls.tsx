'use client';
import Link from 'next/link';
import { useState } from 'react';
import { useRouter, useSearchParams } from 'next/navigation';
import { type Discover } from '@/lib/research';
import { Button, Crest } from './shared';
import s from './research.module.css';
import { useStamp } from './PreferencesProvider';
import { dayInZone } from '@/lib/preferences';
import { useFollowedLeagues } from './useFollowedLeagues';

export function MatchesControls({ data, zone, follows }: { data: Discover | null; zone: string | null; follows: ReturnType<typeof useFollowedLeagues> }) {
    const stamp = useStamp();
    const localDay = (offset = 0) => dayInZone(zone || 'UTC', offset);
    const search = useSearchParams(), router = useRouter();
    const scope = ['week', 'today', 'tomorrow', 'date', 'range'].includes(search.get('scope') || '') ? search.get('scope')! : 'week';
    const selected = search.get('leagues')?.split(',').filter(Boolean) || [];
    const day = scope === 'date' ? search.get('day') || localDay() : localDay(scope === 'tomorrow' ? 1 : 0);
    const [open, setOpen] = useState(false), [draft, setDraft] = useState<string[]>([]), [term, setTerm] = useState('');
    const [rangeOpen, setRangeOpen] = useState(false);
    const [rangeStart, setRangeStart] = useState(search.get('day') || localDay());
    const [rangeEnd, setRangeEnd] = useState(search.get('end_day') || dayInZone(zone || 'UTC', 6));
    const validRange = Boolean(rangeStart && rangeEnd && rangeStart <= rangeEnd);
    const dateInZone = (value: string | number | Date) => new Intl.DateTimeFormat('en-GB', { timeZone: zone || 'UTC', day: 'numeric', month: 'short', year: 'numeric' }).format(new Date(value));
    const windowText = data?.window ? `${dateInZone(data.window.from)} – ${dateInZone(new Date(data.window.to).getTime() - 1)}` : null;
    function update(values: Record<string, string>) {
        const next = new URLSearchParams(search.toString());
        for (const [key, value] of Object.entries(values)) value ? next.set(key, value) : next.delete(key);
        router.replace(`/matches?${next}`, { scroll: false });
    }
    function moveDay(offset: number) {
        const d = new Date(`${day}T12:00:00Z`);
        d.setUTCDate(d.getUTCDate() + offset);
        update({ scope: 'date', day: d.toISOString().slice(0, 10), end_day: '' });
    }
    const groups = new Map<string, Discover['leagues']>();
    for (const league of data?.leagues || []) {
        if (!`${league.league} ${league.country || ''}`.toLocaleLowerCase().includes(term.trim().toLocaleLowerCase())) continue;
        const country = league.country || 'Other competitions';
        groups.set(country, [...(groups.get(country) || []), league]);
    }
    return <>
        <h1>Matches</h1><p className={s.caption}>Browse upcoming matches by date and league.</p>
        <div className={s.dateTabs} aria-label="Match dates">{[['week', 'Next 7 days'], ['today', 'Today'], ['tomorrow', 'Tomorrow'], ['range', 'Date range']].map(([key, label]) => <Button key={key} className={scope === key || (key === 'range' && scope === 'date') ? s.active : ''} aria-pressed={scope === key || (key === 'range' && scope === 'date')} onClick={() => { if (key === 'range') { setRangeStart(search.get('day') || localDay()); setRangeEnd(search.get('end_day') || dayInZone(zone || 'UTC', 6)); setRangeOpen(true); } else { setRangeOpen(false); update({ scope: key, day: '', end_day: '' }); } }}>{label}</Button>)}</div>
        {rangeOpen && <div className={s.matchDateRow}><label>From<input type="date" value={rangeStart} onChange={e => setRangeStart(e.target.value)}/></label><label>To<input type="date" value={rangeEnd} min={rangeStart} onChange={e => setRangeEnd(e.target.value)}/></label><Button onClick={() => setRangeOpen(false)}>Cancel</Button><Button className={s.active} disabled={!validRange} onClick={() => { update({ scope: 'range', day: rangeStart, end_day: rangeEnd }); setRangeOpen(false); }}>Apply</Button>{!validRange && <span role="alert">End date must be on or after start date.</span>}</div>}
        {(scope === 'today' || scope === 'tomorrow' || scope === 'date') && <div className={s.matchDateRow}><Button aria-label="Previous day" onClick={() => moveDay(-1)}>←</Button><label>Match date<input aria-label="Match date" type="date" value={day} onChange={e => { if (e.target.value) update({ scope: 'date', day: e.target.value, end_day: '' }); }}/></label><Button aria-label="Next day" onClick={() => moveDay(1)}>→</Button></div>}
        <p className={s.caption}>{windowText || (scope === 'week' ? 'Next 7 days' : scope === 'range' ? `${search.get('day')} to ${search.get('end_day')}` : new Date(`${day}T12:00:00Z`).toLocaleDateString('en-GB', {weekday:'long',day:'numeric',month:'long',year:'numeric',timeZone:'UTC'}))} · {zone || 'Detecting timezone…'}</p>
        <label className={s.leagueSearch}>Search teams<input type="search" value={search.get('q') || ''} onChange={e => update({ q: e.target.value })} placeholder="Enter a team name"/></label>
        <div className={s.row}><Button aria-expanded={open} aria-controls="matches-leagues" onClick={() => { if (open) setOpen(false); else { setDraft(selected); setTerm(''); setOpen(true); } }}>{selected.length ? `${selected.length} ${selected.length === 1 ? 'league' : 'leagues'} selected` : 'All leagues'} · {open ? 'Close' : 'Choose leagues'}</Button>{follows.signedIn && follows.ready && !follows.error && follows.ids.length > 0 && <Button disabled={!follows.ready} onClick={() => update({ leagues: follows.ids.join(',') })}>Your leagues · {follows.ids.length}</Button>}{follows.signedIn && follows.ready && !follows.error && !follows.ids.length && <Link className={s.link} href="/settings">Set your leagues →</Link>}{selected.length > 0 && <Button onClick={() => { update({leagues:''}); setDraft([]); }}>Clear leagues</Button>}</div>
        {open && <section id="matches-leagues" className={s.leaguePicker} aria-label="Choose leagues"><label className={s.leagueSearch}>Search leagues or countries<input type="search" value={term} onChange={e => setTerm(e.target.value)} placeholder="League or country" /></label><div className={s.leagueOptions}>{[...groups].map(([country, leagues]) => <details key={`${country}:${Boolean(term)}`} open={term ? true : undefined}><summary>{country} <span className={s.caption}>{leagues.filter(l => draft.includes(String(l.league_id))).length || ''}</span></summary>{leagues.map(l => <label key={l.league_id}><input type="checkbox" checked={draft.includes(String(l.league_id))} onChange={e => setDraft(current => e.target.checked ? [...current, String(l.league_id)] : current.filter(id => id !== String(l.league_id)))}/><Crest name={l.league} url={l.league_logo_url} league/>{l.league}</label>)}</details>)}{!groups.size && <p>{data ? 'No leagues match your search.' : 'Loading leagues…'}</p>}</div><div className={s.leaguePickerActions}><span className={s.caption}>{draft.length ? `${draft.length} selected` : 'All leagues'}</span><Button onClick={() => setDraft([])}>Clear</Button><Button onClick={() => setOpen(false)}>Cancel</Button><Button className={s.active} onClick={() => { update({ leagues: draft.join(',') }); setOpen(false); }}>Apply</Button></div></section>}
        <p className={s.caption}>{data?.run_finished_at ? `Updated ${stamp(data.run_finished_at)}` : 'Loading fixture update…'}</p>
    </>;
}
