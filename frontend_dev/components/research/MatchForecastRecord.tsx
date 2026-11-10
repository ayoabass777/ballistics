'use client';
import { recordSchema } from '@/lib/forecast-record';
import { FixtureRow } from '../forecast/ForecastRecordPage';
import { usePreferences } from './PreferencesProvider';
import { ResourceState, useResource } from './shared';
import s from './research.module.css';

export function MatchForecastRecord({ fixtureId }: { fixtureId: number }) {
    const { zone } = usePreferences();
    const record = useResource(`/api/forecast-record?fixture_id=${fixtureId}`, recordSchema);
    const fixture = record.data?.fixtures.find(item => item.fixture_id === fixtureId);
    return <section className={s.panel} aria-label="Match forecast record">
        <h2>Forecast record</h2>
        <p className={s.caption}>Recorded goal forecasts and their outcomes.</p>
        <ResourceState busy={record.busy} error={record.error} retry={record.retry}/>
        {record.data && (fixture
            ? <FixtureRow fixture={fixture} asOf={record.data.as_of} zone={zone || Intl.DateTimeFormat().resolvedOptions().timeZone} initiallyOpen/>
            : <p>No recorded forecasts are available for this match.</p>)}
    </section>;
}
