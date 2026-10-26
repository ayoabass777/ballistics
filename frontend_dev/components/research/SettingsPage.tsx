'use client';
import Link from 'next/link';
import { useEffect, useState, type FormEvent } from 'react';
import { useUser } from '@clerk/nextjs';
import { useRouter } from 'next/navigation';
import { FunctionalityTip, replayFunctionalityTips } from './FunctionalityTip';
import { Shell, Button, Crest, useResource, ResourceState } from './shared';
import { usePreferences } from './PreferencesProvider';
import { useFollowedLeagues } from './useFollowedLeagues';
import { leagueDirectorySchema } from '@/lib/account';
import { track } from '@/lib/usage-events';
import s from './research.module.css';

export function SettingsPage() {
    const router = useRouter();
    const prefs = usePreferences();
    const follows = useFollowedLeagues();
    const { user } = useUser();
    const [draft, setDraft] = useState('');
    const [saving, setSaving] = useState(false);
    const [message, setMessage] = useState('');
    const [failure, setFailure] = useState('');
    const [zones, setZones] = useState<string[]>([]);
    const [leagueDraft, setLeagueDraft] = useState<number[]>([]);
    const [leagueSearch, setLeagueSearch] = useState('');
    const [leagueSaving, setLeagueSaving] = useState(false);
    const [leagueMessage, setLeagueMessage] = useState('');
    const [leagueFailure, setLeagueFailure] = useState('');
    const [usageSaving, setUsageSaving] = useState(false);
    const [usageMessage, setUsageMessage] = useState('');
    const [usageFailure, setUsageFailure] = useState('');
    const directory = useResource(
        prefs.signedIn ? '/api/account/leagues/directory' : null,
        leagueDirectorySchema,
    );
    useEffect(() => { setDraft(prefs.override || ''); }, [prefs.override]);
    useEffect(() => { setZones(Array.from(new Set(['UTC', prefs.browserZone, ...(Intl.supportedValuesOf?.('timeZone') || [])])).sort()); }, [prefs.browserZone]);
    useEffect(() => { if (follows.ready && !follows.error) setLeagueDraft(follows.ids); }, [follows.ready, follows.error, follows.ids]);
    async function submit(event: FormEvent) {
        event.preventDefault(); setSaving(true); setMessage(''); setFailure('');
        try { await prefs.save(draft || null); setMessage('Preferences saved.'); }
        catch (error) { setFailure(error instanceof Error ? error.message : 'Could not save preferences.'); }
        finally { setSaving(false); }
    }
    async function saveLeagues(event: FormEvent) {
        event.preventDefault(); setLeagueSaving(true); setLeagueMessage(''); setLeagueFailure('');
        try { await follows.save(leagueDraft); setLeagueMessage('Your leagues were saved to your account.'); }
        catch (error) { setLeagueFailure(error instanceof Error ? error.message : 'Could not save leagues.'); }
        finally { setLeagueSaving(false); }
    }
    async function toggleUsageEvents(enabled: boolean) {
        setUsageSaving(true); setUsageMessage(''); setUsageFailure('');
        try { await prefs.saveUsageEvents(enabled); setUsageMessage(enabled ? 'Usage events are on.' : 'Usage events are off. Nothing more will be recorded.'); }
        catch (error) { setUsageFailure(error instanceof Error ? error.message : 'Could not save this setting.'); }
        finally { setUsageSaving(false); }
    }
    const filtered = directory.data?.data.leagues.filter(league =>
        `${league.league} ${league.country || ''}`.toLocaleLowerCase().includes(leagueSearch.trim().toLocaleLowerCase())
    ).sort((a, b) => (a.country || '').localeCompare(b.country || '') || a.league.localeCompare(b.league)) || [];
    const countries = Array.from(new Set(filtered.map(league => league.country || 'Other')));
    const sameLeagues = leagueDraft.length === follows.ids.length &&
        leagueDraft.every(id => follows.ids.includes(id));
    return <Shell active="Settings">
        <h1>Account settings</h1>
        <FunctionalityTip id="settings" title="Make Ballistics yours"><p>Follow leagues to use Your leagues on Discover and Matches. Set a timezone for fixture dates, or keep Automatic to use your browser timezone.</p></FunctionalityTip>
        {!prefs.signedIn ? <div className={s.empty}><p>Sign in to manage your account.</p><Link href="/sign-in?redirect_url=%2Fsettings">Sign in →</Link></div> : <>
            <section className={s.panel}>
                <h2>Account</h2>
                <p>{user?.primaryEmailAddress?.emailAddress || 'Your signed-in email address'}</p>
                <p className={s.caption}>Use the account menu in the header to manage your password and sign out.</p>
                <p><Link href="/onboarding?replay=1" onClick={() => track('tutorial_replayed', { what: 'introduction' })}>Replay the introduction →</Link></p>
                <p><Button type="button" onClick={() => { if (user?.id) { replayFunctionalityTips(user.id); router.push('/discover'); } }}>Replay feature tips</Button></p>
            </section>
            <section className={s.panel}>
                <h2>Your leagues</h2>
                <p>Follow leagues here, then use Your leagues on Discover or Matches. Your choices sync with your account.</p>
                {!follows.ready ? <p role="status">Loading your leagues…</p> : follows.error ?
                    <div role="alert"><p>{follows.error}</p><Button onClick={follows.reload}>Try again</Button></div> : <>
                    <ResourceState busy={directory.busy} error={directory.error} retry={directory.retry}/>
                    {directory.data && <form onSubmit={saveLeagues} className={s.followForm}>
                        <div className={s.followSearch}><label htmlFor="league-search">Find a league</label>
                        <input id="league-search" className={s.searchInput} value={leagueSearch} onChange={event => setLeagueSearch(event.target.value)} placeholder="Search by league or country"/></div>
                        <fieldset className={s.leagueChoices} disabled={leagueSaving}>
                            <legend className={s.srOnly}>Leagues to follow</legend>
                            {countries.map(country => <details key={`${country}-${!!leagueSearch.trim()}`} className={s.followCountry} open={leagueSearch.trim() ? true : undefined}>
                                <summary><span>{country}</span><span className={s.followCountryCount}>{filtered.filter(league => (league.country || 'Other') === country).filter(league => leagueDraft.includes(league.league_id)).length} selected</span></summary>
                                <div className={s.followCountryLeagues}>{filtered.filter(league => (league.country || 'Other') === country).map(league => <label key={league.league_id} className={leagueDraft.includes(league.league_id) ? s.followSelected : undefined}>
                                    <input type="checkbox" checked={leagueDraft.includes(league.league_id)}
                                        onChange={event => { setLeagueMessage(''); setLeagueFailure(''); setLeagueDraft(current => event.target.checked ?
                                            [...current, league.league_id] : current.filter(id => id !== league.league_id)); }}/>
                                    <Crest name={league.league} url={league.league_logo_url} league/>
                                    <span>{league.league}</span>
                                </label>)}</div>
                            </details>)}
                            {!filtered.length && <p>No leagues match your search.</p>}
                        </fieldset>
                        <div className={s.followActions}><p className={s.caption}>{leagueDraft.length} selected{!sameLeagues ? ' · Unsaved changes' : ''}</p><div className={s.row}>
                            <Button type="button" onClick={() => { setLeagueDraft([]); setLeagueMessage(''); }} disabled={leagueSaving || !leagueDraft.length}>Clear selection</Button>
                            <Button type="submit" className={s.active} disabled={sameLeagues || leagueSaving}>
                                {leagueSaving ? 'Saving…' : 'Save leagues'}
                            </Button>
                        </div></div>
                    </form>}
                </>}
                {leagueFailure && <p role="alert">{leagueFailure}</p>}
                {leagueMessage && <p role="status">{leagueMessage}</p>}
            </section>
            <section className={s.panel}>
                <h2>Timezone</h2>
                <p>Use your browser timezone automatically, or choose a timezone for fixture times and date filters.</p>
                {prefs.loading ? <p role="status">Loading preferences…</p> : prefs.error ?
                    <div role="alert"><p>{prefs.error}</p><Button onClick={prefs.reload}>Try again</Button></div> :
                    <form onSubmit={submit}><fieldset disabled={saving} style={{ border: 0, padding: 0 }}>
                        <label htmlFor="timezone-choice">Timezone</label>
                        <select id="timezone-choice" value={draft} onChange={event => { setDraft(event.target.value); setMessage(''); }} style={{ display: 'block', maxWidth: '100%', minHeight: 44, margin: '12px 0' }}>
                            <option value="">Automatic · {prefs.browserZone}</option>
                            {Array.from(new Set([...zones, ...(prefs.override ? [prefs.override] : [])])).map(zone =>
                                <option key={zone} value={zone}>{zone.replaceAll('_', ' ')}</option>)}
                        </select>
                        <p className={s.caption}>Currently using {prefs.zone}. Automatic follows the device you’re using.</p>
                        <Button type="submit" disabled={draft === (prefs.override || '')}>{saving ? 'Saving…' : 'Save preferences'}</Button>
                    </fieldset></form>}
                {failure && <p role="alert">{failure}</p>}
                <p role="status">{message}</p>
            </section>
            <section className={s.panel}>
                <h2>Privacy</h2>
                <p>During the beta, Ballistics records which pages and matches you open and when you skip a tip, so we can find what’s confusing. This is stored with your account, never sold or shared, and deleted within 90 days of the beta ending.</p>
                {prefs.loading ? <p role="status">Loading preferences…</p> : prefs.usageEvents == null ?
                    <p className={s.caption}>This setting is temporarily unavailable.</p> :
                    <label style={{ display: 'flex', alignItems: 'center', gap: 12, minHeight: 44 }}>
                        <input type="checkbox" checked={prefs.usageEvents} disabled={usageSaving} onChange={event => toggleUsageEvents(event.target.checked)}/>
                        <span>Record usage events</span>
                    </label>}
                {usageFailure && <p role="alert">{usageFailure}</p>}
                <p role="status">{usageMessage}</p>
            </section>
        </>}
    </Shell>;
}
