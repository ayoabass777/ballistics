'use client';
import { recordSchema } from '@/lib/forecast-record';
import { FixtureRow } from '../forecast/ForecastRecordPage';
import { usePreferences } from './PreferencesProvider';
import { ResourceState, useResource } from './shared';
import s from './research.module.css';
import recordStyles from '../forecast/forecast-record.module.css';

export function MatchForecastRecord({ fixtureId }: { fixtureId: number }) {
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
            ? <div className={recordStyles.embedded}><FixtureRow fixture={visibleFixture} asOf={record.data.as_of} zone={zone || Intl.DateTimeFormat().resolvedOptions().timeZone} initiallyOpen/></div>
            : <p>No recorded forecasts meet the 75% display cutoff for this match.</p>)}
    </section>;
}
