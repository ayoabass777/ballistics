'use client';
import { recordSchema } from '@/lib/forecast-record';
import { FixtureRow, type ProvisionalGrade } from '../forecast/ForecastRecordPage';
import { trackMarket, type LiveState } from '@/lib/live';
import { usePreferences } from './PreferencesProvider';
import { ResourceState, useResource } from './shared';
import s from './research.module.css';
import recordStyles from '../forecast/forecast-record.module.css';

/** Grades pending goal forecasts from the live full-time score until the nightly
 *  update records the result. FT only: AET and PEN feeds carry the extra-time score. */
export function liveGrade(state: Pick<LiveState, 'status' | 'home_goals' | 'away_goals'> | null | undefined, home: string, away: string): ProvisionalGrade | undefined {
    if (!state || state.status !== 'FT' || state.home_goals == null || state.away_goals == null) return undefined;
    const sc = { home: state.home_goals, away: state.away_goals, final: true };
    return f => {
        const t = trackMarket(f.event_label || f.event_key, home, away, sc);
        return t && (t[0] === 'landed' || t[0] === 'missed') ? { status: t[0] === 'landed' ? 'hit' : 'miss', score: `${sc.home}–${sc.away}` } : null;
    };
}

export function MatchForecastRecord({ fixtureId, live }: { fixtureId: number; live?: Pick<LiveState, 'status' | 'home_goals' | 'away_goals'> | null }) {
    const { zone } = usePreferences();
    const record = useResource(`/api/forecast-record?fixture_id=${fixtureId}`, recordSchema);
    const fixture = record.data?.fixtures.find(item => item.fixture_id === fixtureId);
    // Use the Goals display floor on captured values, before percentage rounding.
    const visibleFixture = fixture ? { ...fixture, forecasts: fixture.forecasts.filter(item => item.probability >= 0.75) } : null;
    return <section className={s.panel} aria-label="Match forecast record">
        <h2>Forecast record</h2>
        <p className={s.caption}>Recorded goal forecasts of 75% or higher and their outcomes.</p>
        <ResourceState busy={record.busy} error={record.error} retry={record.retry}/>
        {record.data && (visibleFixture?.forecasts.length
            ? <div className={recordStyles.embedded}><FixtureRow fixture={visibleFixture} asOf={record.data.as_of} zone={zone || Intl.DateTimeFormat().resolvedOptions().timeZone} initiallyOpen provisional={liveGrade(live, visibleFixture.home || '', visibleFixture.away || '')}/></div>
            : <p>No recorded forecasts meet the 75% display cutoff for this match.</p>)}
    </section>;
}
