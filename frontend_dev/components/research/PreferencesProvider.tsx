'use client';
import { createContext, useContext, useEffect, useRef, useState, useCallback, type ReactNode } from 'react';
import { useUser } from '@clerk/nextjs';
import { preferencesSchema } from '@/lib/preferences';
import { stamp as formatStamp } from '@/lib/research';
import { setUsageEventsEnabled, usageEventsSettingResponseSchema } from '@/lib/usage-events';
const Context = createContext({ zone: null as string | null, browserZone: 'UTC', override: null as string | null, loading: true, error: '', signedIn: false, reload: () => {}, save: async (_zone: string | null): Promise<void> => {}, usageEvents: null as boolean | null, saveUsageEvents: async (_enabled: boolean): Promise<void> => {} });
export function PreferencesProvider({ children }: { children: ReactNode }) {
    const { user, isLoaded } = useUser();
    // The app below remounts when the account changes, so no state carries
    // from one person to the next. Not when Clerk first finishes loading,
    // though: the identity it resolves to is the one the page opened with, and
    // remounting then cancelled and resent every request on the page (Discover
    // and Saved were fetched twice per load).
    const identity = user?.id || 'anonymous';
    const first = useRef<string | null>(null);
    if (isLoaded && first.current === null) first.current = identity;
    const key = !isLoaded || identity === first.current ? 'initial' : identity;
    // Outside the remount, so a later account change keeps the zone steady.
    const [browserZone, setBrowserZone] = useState<string | null>(null);
    useEffect(() => setBrowserZone(Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC'), []);
    return <AccountPreferences key={key} signedIn={!!user} authLoaded={isLoaded} browserZone={browserZone}>{children}</AccountPreferences>;
}
function AccountPreferences({ children, signedIn, authLoaded, browserZone }: { children: ReactNode; signedIn: boolean; authLoaded: boolean; browserZone: string | null }) {
    const [override, setOverride] = useState<string | null>(null);
    const [usageEvents, setUsageEvents] = useState<boolean | null>(null);
    const [loading, setLoading] = useState(true), [error, setError] = useState(''), [revision, setRevision] = useState(0);
    useEffect(() => {
        if (!authLoaded) return;
        if (!signedIn) { setUsageEventsEnabled(null); setLoading(false); return; }
        const abort = new AbortController();
        setLoading(true); setError('');
        fetch('/api/preferences', { cache: 'no-store', signal: abort.signal }).then(async response => {
            const body = await response.json();
            if (!response.ok) throw new Error(body.error || 'Could not load preferences.');
            return preferencesSchema.parse(body).data;
        }).then(data => { if (!abort.signal.aborted) { setOverride(data.timezone_override); setUsageEvents(data.usage_events_enabled ?? null); setUsageEventsEnabled(data.usage_events_enabled ?? null); setLoading(false); } })
          .catch(() => { if (!abort.signal.aborted) { setError('Could not load account preferences. Times use your browser timezone.'); setOverride(null); setLoading(false); } });
        return () => abort.abort();
    }, [authLoaded, signedIn, revision]);
    const save = useCallback(async (zone: string | null) => {
        const response = await fetch('/api/preferences', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ timezone_override: zone }) });
        const body = await response.json();
        if (!response.ok) throw new Error(body.error || 'Could not save preferences.');
        const data = preferencesSchema.parse(body).data;
        setOverride(data.timezone_override); setError('');
    }, []);
    const saveUsageEvents = useCallback(async (enabled: boolean) => {
        const response = await fetch('/api/preferences/usage-events', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ enabled }) });
        const body = await response.json();
        if (!response.ok) throw new Error(body.error || 'Could not save this setting.');
        const value = usageEventsSettingResponseSchema.parse(body).data.usage_events_enabled;
        setUsageEvents(value); setUsageEventsEnabled(value);
    }, []);
    return <Context.Provider value={{ zone: browserZone ? override || browserZone : null, browserZone: browserZone || 'UTC', override, loading, error, signedIn, reload: () => setRevision(n => n + 1), save, usageEvents, saveUsageEvents }}>{children}</Context.Provider>;
}
export const usePreferences = () => useContext(Context);
export function useStamp() { const { zone } = usePreferences(); return (value: string) => formatStamp(value, zone || 'UTC'); }
