'use client';
import Link from 'next/link';
import { ProductHeader } from '@/components/account/ProductHeader';
import { useUser } from '@clerk/nextjs';
import { useEffect, useState, useCallback, useRef, type ComponentProps, type ReactNode } from 'react';
import { GlassButton } from '@/components/GlassButton';
import { savedSchema, type Saved, type Card } from '@/lib/research';
import { savedResponseSchema, savedIdsSchema } from '@/lib/account';
import { z } from 'zod';
import s from './research.module.css';
import { usePreferences } from './PreferencesProvider';
import { Bookmark, CalendarDays, ChartNoAxesCombined, Compass, PanelLeftClose, PanelLeftOpen } from 'lucide-react';
export function Button(props: ComponentProps<typeof GlassButton>) { return <GlassButton variant="outline" {...props} className={`${s.button} ${props.className || ''}`}/>; }
const nav = [
    { name: 'Discover', href: '/discover', mobileIcon: '◎', Icon: Compass },
    { name: 'Matches', href: '/matches', mobileIcon: '☰', Icon: CalendarDays },
    { name: 'Forecast record', href: '/forecast-record', mobileIcon: '◷', Icon: ChartNoAxesCombined },
    { name: 'Saved', href: '/saved', mobileIcon: '☆', Icon: Bookmark },
];
export function Shell({ children, active = 'Discover', mainMinHeight }: {
    children: ReactNode;
    active?: string;
    mainMinHeight?: number;
}) {
    const preferences = usePreferences();
    const [collapsed, setCollapsed] = useState(false);
    useEffect(() => {
        try { setCollapsed(localStorage.getItem('ballistics:sidebar:collapsed') === 'true'); } catch { /* Use expanded default. */ }
    }, []);
    const toggleSidebar = () => setCollapsed(current => {
        const next = !current;
        try { localStorage.setItem('ballistics:sidebar:collapsed', String(next)); } catch { /* Navigation still works. */ }
        return next;
    });
    return <div className={s.page}><a href="#research-main" className={s.skip}>Skip to content</a><ProductHeader/><div className={`${s.workspace} ${collapsed ? s.workspaceCollapsed : ''}`}><nav className={s.sidebar} aria-label="Primary"><button type="button" className={s.sidebarToggle} onClick={toggleSidebar} aria-label={collapsed ? 'Expand navigation' : 'Collapse navigation'} aria-expanded={!collapsed} title={collapsed ? 'Expand navigation' : 'Collapse navigation'}>{collapsed ? <PanelLeftOpen size={20} aria-hidden="true"/> : <PanelLeftClose size={20} aria-hidden="true"/>}<span className={s.srOnly}>{collapsed ? 'Expand menu' : 'Collapse menu'}</span></button>{nav.map(({ name, href, Icon }) => <Button asChild key={name} className={`${s.sidebarLink} ${active === name ? s.active : ''}`}><Link href={href} aria-label={collapsed ? name : undefined} aria-current={active === name ? 'page' : undefined} title={collapsed ? name : undefined}><Icon size={20} aria-hidden="true"/><span className={collapsed ? s.srOnly : ''}>{name}</span></Link></Button>)}</nav><main id="research-main" className={s.main} style={mainMinHeight ? { minHeight: mainMinHeight } : undefined}>{preferences.error && active !== 'Settings' && <p className={s.notice} role="status">{preferences.error} <Link href="/settings">Retry in Settings</Link></p>}{children}</main></div><nav className={s.bottomNav} aria-label="Mobile primary">{nav.map(({ name, href, mobileIcon }) => <Link key={name} href={href} aria-current={active === name ? 'page' : undefined}><span aria-hidden>{mobileIcon}</span>{name === 'Forecast record' ? 'Record' : name}</Link>)}</nav></div>;
}
export function useResource<T>(url: string | null, schema: z.ZodType<T>) {
    const [state, setState] = useState<{
        url: string | null;
        data: T | null;
        error: string;
        busy: boolean;
    }>({ url: null, data: null, error: '', busy: true });
    const [revision, setRevision] = useState(0);
    useEffect(() => {
        if (!url)
            return;
        const abort = new AbortController();
        setState({ url, data: null, error: '', busy: true });
        fetch(url, { cache: 'no-store', signal: abort.signal }).then(async (r) => { const body = await r.json(); if (!r.ok)
            throw new Error(body.error || 'Unable to load match research.'); return schema.parse(body); }).then(data => { if (!abort.signal.aborted)
            setState({ url, data, error: '', busy: false }); }).catch(e => { if (!abort.signal.aborted)
            setState({ url, data: null, error: e instanceof z.ZodError ? 'The response could not be verified.' : e.message, busy: false }); });
        return () => abort.abort();
    }, [url, schema, revision]);
    return { ...(state.url === url ? state : { data: null, error: '', busy: true }), retry: () => setRevision(x => x + 1) };
}
export function ResourceState({ busy, error, retry }: {
    busy: boolean;
    error: string;
    retry: () => void;
}) { return busy ? <p className={s.empty} role="status">Loading match research…</p> : error ? <div className={s.notice} role="alert"><p>{error}</p><Button onClick={retry}>Try again</Button></div> : null; }
export function useSaved() {
    const { user, isLoaded } = useUser();
    const key = user ? `ballistics:saved:v1:${user.id}` : null;
    const [state, setState] = useState<{
        key: string | null;
        items: Saved;
        ids?: number[];
        ready: boolean;
        error: string;
    }>({ key: null, items: [], ready: false, error: '' });
    const [message, setMessage] = useState('');
    const [revision, setRevision] = useState(0);
    const [busy, setBusy] = useState(false);
    const busyRef = useRef(false);
    const timer = useRef<ReturnType<typeof setTimeout> | null>(null);
    const announce = useCallback((text: string) => { setMessage(text); if (timer.current)
        clearTimeout(timer.current); timer.current = setTimeout(() => setMessage(''), 5000); }, []);
    useEffect(() => () => { if (timer.current)
        clearTimeout(timer.current); }, []);
    useEffect(() => {
        if (!isLoaded) return;
        if (!key) { setState({ key: null, items: [], ready: true, error: '' }); return; }
        const accountKey = key;
        const abort = new AbortController();
        async function load() {
            setState({ key, items: [], ready: false, error: '' });
            try {
                const response = await fetch('/api/account/saved', { cache: 'no-store', signal: abort.signal });
                const body = await response.json();
                if (!response.ok) throw new Error(body.error || 'Saved fixtures could not be loaded.');
                let items = savedResponseSchema.parse(body).data;
                // Move older browser-only saves once, without replacing an existing server snapshot.
                let legacy: Saved = [];
                try {
                    const raw = localStorage.getItem(accountKey);
                    if (raw) legacy = savedSchema.parse(JSON.parse(raw));
                } catch { announce('Earlier browser saves could not be read.'); }
                for (let index = 0; index < legacy.length; index += 50) {
                    const imported = await fetch('/api/account/saved/import', {
                        method: 'POST', headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ items: legacy.slice(index, index + 50) }), signal: abort.signal,
                    });
                    const result = await imported.json();
                    if (!imported.ok) throw new Error(result.error || 'Earlier saves could not be moved to your account.');
                    items = savedResponseSchema.parse(result).data;
                }
                if (legacy.length) {
                    try { localStorage.removeItem(accountKey); } catch { /* Server import is idempotent. */ }
                }
                const membership = await fetch('/api/account/saved/ids', { cache: 'no-store', signal: abort.signal });
                if (!membership.ok) throw new Error('Saved fixtures could not be loaded.');
                const ids = savedIdsSchema.parse(await membership.json()).data.fixture_ids;
                if (!abort.signal.aborted) setState({ key, items, ids, ready: true, error: '' });
            } catch (error) {
                if (!abort.signal.aborted) setState({ key, items: [], ready: true, error: error instanceof Error ? error.message : 'Saved fixtures could not be loaded.' });
            }
        }
        load();
        const sync = (event: Event) => {
            const detail = (event as CustomEvent<{ key: string; items: Saved; ids: number[] }>).detail;
            if (detail?.key === key) setState({ key, items: detail.items, ids: detail.ids, ready: true, error: '' });
        };
        const refresh = () => { if (document.visibilityState === 'visible') load(); };
        window.addEventListener('ballistics-saved', sync);
        document.addEventListener('visibilitychange', refresh);
        return () => { abort.abort(); window.removeEventListener('ballistics-saved', sync); document.removeEventListener('visibilitychange', refresh); };
    }, [key, isLoaded, revision, announce]);
    const items = state.key === key ? state.items : [];
    async function toggle(card: Card, run: string | null) { if (!key) {
        announce('Sign in to save fixtures.');
        return;
    }
    if (busyRef.current || !state.ready) return;
    busyRef.current = true; setBusy(true);
    const removing = (state.ids || []).includes(card.fixture_id);
    try {
        const response = await fetch(removing ? `/api/account/saved/${card.fixture_id}` : '/api/account/saved', {
            method: removing ? 'DELETE' : 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: removing ? undefined : JSON.stringify({ card, run_ds: run }),
        });
        const body = await response.json();
        if (!response.ok) throw new Error(body.error || 'Could not update Saved.');
        const next = savedResponseSchema.parse(body).data;
        const ids = removing ? (state.ids || []).filter(id => id !== card.fixture_id) : [...new Set([...(state.ids || []), card.fixture_id])];
        setState({ key, items: next, ids, ready: true, error: '' });
        window.dispatchEvent(new CustomEvent('ballistics-saved', { detail: { key, items: next, ids } }));
        announce(removing ? 'Fixture removed from Saved.' : 'Fixture saved to your account.');
    }
    catch (error) { announce(error instanceof Error ? error.message : 'Could not update Saved.'); }
    finally { busyRef.current = false; setBusy(false); } }
    return { accountKey: key, items, isSaved: (id: number) => state.key === key && (state.ids || []).includes(id), toggle, ready: isLoaded && state.key === key && state.ready, signedIn: !!user, message, busy, error: state.key === key ? state.error : '', reload: () => setRevision(n => n + 1) };
}
export function Toast({ message }: {
    message: string;
}) { return <div role="status" aria-live="polite" className={message ? s.toast : undefined}>{message}</div>; }
export function Crest({ name, url, league = false }: {
    name: string;
    url?: string | null;
    league?: boolean;
}) {
    const [failedUrl, setFailedUrl] = useState<string | null>(null);
    const available = !!url && failedUrl !== url;
    if (league && !available) return null;
    return <span className={`${s.crest} ${league ? s.leagueLogo : ''} ${available ? s.hasImage : ''}`} aria-hidden="true">{available ? <img src={url!} alt="" loading="lazy" decoding="async" onError={() => setFailedUrl(url!)} /> : name.split(/\s+/).map(w => w[0]).slice(0, 2).join('')}</span>;
}
export function FixtureIdentity({ card }: { card: Pick<Card, 'home' | 'away' | 'home_crest_url' | 'away_crest_url'> }) {
    return <span className={s.fixtureIdentity}><span><Crest name={card.home} url={card.home_crest_url}/><span>{card.home}</span></span><span className={s.srOnly}> versus </span><span><Crest name={card.away} url={card.away_crest_url}/><span>{card.away}</span></span></span>;
}
