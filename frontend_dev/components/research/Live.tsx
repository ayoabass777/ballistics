'use client';
// Live scores (docs/live_scores.md, phase 1): the Matches "Live now" list and
// the Match details live header and forecast tracker.
import Link from 'next/link';
import { useEffect, useRef, useState } from 'react';
import { z } from 'zod';
import { type Match } from '@/lib/research';
import { liveListSchema, liveFixtureSchema, clockLabel, isLive, isStale, eventLabel, eventMinute, trackForecast, trackPick, trackChip, FINAL, LIVE_POLL_MS, type LiveCard, type LiveFixture, type LiveState, type TrackRow } from '@/lib/live';
import { Crest } from './shared';
import s from './research.module.css';
import l from './live.module.css';

/** 'poll': every LIVE_POLL_MS while the tab is visible; 'once': one read; 'off'.
 *  A failed poll keeps the last data on screen and reports `failed`. */
export function useLive<T>(url: string | null, schema: z.ZodType<T>, mode: 'poll' | 'once' | 'off') {
    const [data, setData] = useState<T | null>(null);
    const [failed, setFailed] = useState(false);
    useEffect(() => {
        if (!url || mode === 'off') return;
        let timer: ReturnType<typeof setTimeout> | undefined;
        let abort: AbortController | undefined;
        const load = async () => {
            clearTimeout(timer);
            if (document.visibilityState === 'hidden') return;
            abort?.abort();
            abort = new AbortController();
            try {
                const r = await fetch(url, { cache: 'no-store', signal: abort.signal });
                if (!r.ok) throw new Error(String(r.status));
                setData(schema.parse(await r.json()));
                setFailed(false);
            } catch {
                if (abort.signal.aborted) return;
                setFailed(true);
            }
            if (mode === 'poll') timer = setTimeout(load, LIVE_POLL_MS);
        };
        const visible = () => { if (mode === 'poll' && document.visibilityState === 'visible') load(); };
        load();
        document.addEventListener('visibilitychange', visible);
        return () => { clearTimeout(timer); abort?.abort(); document.removeEventListener('visibilitychange', visible); };
    }, [url, schema, mode]);
    return { data, failed };
}

function Clock({ state }: { state: Pick<LiveState, 'status' | 'minute' | 'extra' | 'finished' | 'polled_at'> }) {
    const live = isLive(state);
    return <span className={`${l.clock} ${live ? l.inPlay : ''}`}>{live && <span className={l.dot} aria-hidden="true"/>}{clockLabel(state)}{isStale(state) && <span className={l.stale}> · delayed</span>}</span>;
}
function Reds({ n }: { n: number }) { return n > 0 ? <span className={l.red} aria-label={`${n} red ${n === 1 ? 'card' : 'cards'}`}>{n > 1 ? n : ''}</span> : null; }
const chipClass: Record<TrackRow['status'], string> = { landed: l.landed, missed: l.missed, on_track: l.onTrack, off_track: l.offTrack };
function Chip({ row }: { row: TrackRow }) { return <span className={`${l.chip} ${chipClass[row.status]}`}>{trackChip(row)}</span>; }

function LiveRow({ m, returnTo, flash }: { m: LiveCard; returnTo: string; flash: boolean }) {
    const track = trackPick(m);
    return <li><Link className={`${l.liveRow} ${flash ? l.flash : ''}`} href={`/match/${m.fixture_id}?returnTo=${encodeURIComponent(returnTo)}`} aria-label={`${m.home} ${m.home_goals ?? ''} ${m.away} ${m.away_goals ?? ''}, ${clockLabel(m)}`}>
        <Clock state={m}/>
        <span className={l.teams}><span className={l.team}><Crest name={m.home} url={m.home_crest_url}/>{m.home}<Reds n={m.home_red_cards}/></span><span className={l.team}><Crest name={m.away} url={m.away_crest_url}/>{m.away}<Reds n={m.away_red_cards}/></span></span>
        <strong className={l.score}><span>{m.home_goals ?? '–'}</span><span>{m.away_goals ?? '–'}</span></strong>
        {track && <span className={l.track}><span className={s.caption}>{track.title}</span><Chip row={track}/></span>}
    </Link></li>;
}

/** Matches lists upcoming fixtures only, so live and just-finished ones get their own list above it. */
export function LiveNow({ returnTo, leagues }: { returnTo: string; leagues?: string[] }) {
    const { data, failed } = useLive('/api/research/live', liveListSchema, 'poll');
    const scores = useRef(new Map<number, string>());
    const [flashing, setFlashing] = useState<Set<number>>(new Set());
    useEffect(() => {
        if (!data) return;
        const scored = new Set<number>();
        for (const m of data.matches) {
            const now = `${m.home_goals}-${m.away_goals}`, before = scores.current.get(m.fixture_id);
            if (before !== undefined && before !== now) scored.add(m.fixture_id);
            scores.current.set(m.fixture_id, now);
        }
        if (!scored.size) return;
        setFlashing(scored);
        const t = setTimeout(() => setFlashing(new Set()), 6000);
        return () => clearTimeout(t);
    }, [data]);
    const shown = (data?.matches || []).filter(m => !leagues?.length || (m.league_id != null && leagues.includes(String(m.league_id))));
    if (!shown.length) return null;
    const groups = (items: LiveCard[]) => {
        const out = new Map<string, LiveCard[]>();
        for (const m of items) out.set(`${m.league_id}`, [...(out.get(`${m.league_id}`) || []), m]);
        return [...out.values()];
    };
    const leagues_ = (items: LiveCard[]) => groups(items).map(g => <div key={g[0].league_id ?? g[0].league} className={l.league}><h3 className={s.competition}><Crest name={g[0].league} url={g[0].league_logo_url} league/><span>{g[0].league}{g[0].country && ` · ${g[0].country}`}</span></h3><ul>{g.map(m => <LiveRow key={m.fixture_id} m={m} returnTo={returnTo} flash={flashing.has(m.fixture_id)}/>)}</ul></div>);
    const playing = shown.filter(m => !m.finished && !FINAL.has(m.status));
    const finished = shown.filter(m => m.finished || FINAL.has(m.status));
    return <section className={`${s.section} ${l.live}`} aria-label="Live scores">
        {playing.length > 0 && <div className={l.block}><h2 aria-live="polite">Live now · {playing.length}</h2>{leagues_(playing)}</div>}
        {/* Collapsed: on a busy day dozens of finished games would push the upcoming list far down. */}
        {finished.length > 0 && <details className={`${l.block} ${l.finished}`}><summary><h2>Finished today · {finished.length}</h2></summary>{leagues_(finished)}</details>}
        <p className={s.caption}>Scores update about every 15 seconds{failed ? ' · Live updates paused, retrying' : ''}. The chip checks each match’s leading pre-match insight against the score; the forecast itself does not change.</p>
    </section>;
}

/** Match details: poll while the match is on, one read otherwise.
 *  The kickoff here comes from the nightly snapshot and can be stale (a match
 *  moved after the run), so the poller's own state also decides: a match it
 *  has seen live is polled whatever the snapshot says. */
export function useMatchLive(fixtureId: number, kickoffUtc: string) {
    const kickoff = Date.parse(kickoffUtc);
    const [now, setNow] = useState(() => Date.now());
    useEffect(() => { const t = setInterval(() => setNow(Date.now()), 60000); return () => clearInterval(t); }, []);
    const inWindow = now >= kickoff - 10 * 60000 && now <= kickoff + 4 * 3600000;
    const [seen, setSeen] = useState<'none' | 'live' | 'done'>('none');
    const live = useLive(`/api/research/live/${fixtureId}`, liveFixtureSchema,
        seen === 'done' ? 'once' : inWindow || seen === 'live' ? 'poll' : 'once');
    const st = live.data?.state;
    const status = !st ? 'none' : st.finished ? 'done' : 'live';
    useEffect(() => { setSeen(status); }, [status]);
    return live;
}

export function LiveScore({ live }: { live: LiveFixture | null }) {
    const st = live?.state;
    if (!st || st.home_goals == null) return null;
    return <p className={l.headerScore} role="status" aria-live="polite"><strong>{st.home_goals} – {st.away_goals}</strong><Clock state={st}/></p>;
}

export function LiveForecast({ data, live, failed }: { data: Match; live: LiveFixture | null; failed: boolean }) {
    const st = live?.state;
    if (!st) return null;
    const rows = trackForecast(data, st);
    const final = FINAL.has(st.status);
    const events = live.events.filter(e => e.type === 'Goal' || e.type === 'Var' || (e.type === 'Card' && e.detail !== 'Yellow Card'));
    const team = (side: string | null) => side === 'home' ? data.fixture.home : side === 'away' ? data.fixture.away : '';
    return <section id="live" className={`${s.panel} ${l.forecast}`} aria-label="Your forecast, live">
        <div className={s.between}><h2>{final ? 'Your forecast, graded' : 'Your forecast, live'}</h2><Clock state={st}/></div>
        <p className={s.caption}>{final ? 'Provisional result from the live score. The Forecast record confirms it after the nightly update.' : 'The pre-match forecast below is checked against the live score. The forecast itself does not change.'}{failed ? ' Live updates paused, retrying.' : ''}</p>
        {rows.length ? <ul className={l.trackRows}>{rows.map(r => <li key={r.key}><div><h3>{r.title}</h3><p className={s.caption}>{r.detail}</p></div><Chip row={r}/></li>)}</ul>
            : <p className={s.caption}>{['ET', 'BT', 'P', 'AET', 'PEN'].includes(st.status) ? 'Goals and pattern checks use 90 minutes, so extra time is not graded here.' : 'No forecast for this match can be checked against the score.'}</p>}
        {events.length > 0 && <><h3>Match events</h3><ol className={l.events}>{events.map((e, i) => <li key={i}><span className={l.minute}>{eventMinute(e)}</span><span>{eventLabel(e)}{e.player ? ` · ${e.player}` : ''}{team(e.side) ? ` (${team(e.side)})` : ''}</span></li>)}</ol></>}
    </section>;
}
