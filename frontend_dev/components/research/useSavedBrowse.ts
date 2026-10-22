'use client';
import { useCallback, useEffect, useRef, useState } from 'react';
import { savedPageSchema } from '@/lib/account';
import type { Saved } from '@/lib/research';

const scopes = ['all', 'upcoming', 'live', 'finished', 'other'];
type Position = { pages: number; y: number };
type Memory = { scope: string; views: Record<string, Position> };
function read(key: string): Memory {
    try {
        const value = JSON.parse(sessionStorage.getItem(key) || '{}');
        const views: Record<string, Position> = {};
        for (const scope of scopes) {
            const v = value.views?.[scope];
            if (Number.isInteger(v?.pages) && v.pages > 0 && v.pages <= 1000 && Number.isFinite(v.y) && v.y >= 0)
                views[scope] = v;
        }
        return { scope: scopes.includes(value.scope) ? value.scope : 'all', views };
    } catch { return { scope: 'all', views: {} }; }
}

export function useSavedBrowse(accountKey: string | null, ready: boolean) {
    const [scope, setScope] = useState('all');
    const [items, setItems] = useState<Saved>([]);
    const [cursor, setCursor] = useState<string | null>(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState('');
    const [revision, setRevision] = useState(0);
    const [initialized, setInitialized] = useState<string | null>(null);
    const memory = useRef<Memory>({ scope: 'all', views: {} });
    const pages = useRef(1);
    const request = useRef<AbortController | null>(null);
    const storageKey = accountKey ? `${accountKey}:browse:v1` : null;
    const persist = useCallback(() => {
        if (!storageKey || loading || initialized !== storageKey) return;
        memory.current.scope = scope;
        memory.current.views[scope] = { pages: pages.current, y: window.scrollY };
        try { sessionStorage.setItem(storageKey, JSON.stringify(memory.current)); } catch { /* Optional session memory. */ }
    }, [storageKey, loading, initialized, scope]);

    useEffect(() => {
        request.current?.abort();
        setItems([]); setCursor(null); setError(''); setLoading(true);
        if (!storageKey || !ready) { setInitialized(null); return; }
        memory.current = read(storageKey);
        setScope(memory.current.scope); setInitialized(storageKey);
    }, [storageKey, ready]);

    async function load(after: string | null = null) {
        if (!storageKey || initialized !== storageKey) return;
        request.current?.abort();
        const controller = new AbortController(); request.current = controller;
        setLoading(true); setError('');
        const position = memory.current.views[scope] || { pages: 1, y: 0 };
        if (!after) { setItems([]); setCursor(null); }
        let next = after;
        let fetched: Saved = [];
        let loaded = 0;
        try {
            do {
                const params = new URLSearchParams({ scope, limit: '12' });
                if (next) params.set('cursor', next);
                const response = await fetch(`/api/account/saved/page?${params}`, { cache: 'no-store', signal: controller.signal });
                const body = await response.json();
                if (!response.ok) throw new Error(body.error || 'Saved fixtures could not be loaded.');
                const page = savedPageSchema.parse(body).data;
                fetched = [...fetched, ...page.items]; next = page.next_cursor; loaded++;
            } while (!after && next && loaded < position.pages);
            if (controller.signal.aborted) return;
            pages.current = after ? pages.current + loaded : loaded;
            setItems(previous => {
                const combined = after ? [...previous, ...fetched] : fetched;
                return [...new Map(combined.map(item => [item.card.fixture_id, item])).values()];
            });
            setCursor(next);
            if (!after) requestAnimationFrame(() => requestAnimationFrame(() => {
                if (!controller.signal.aborted) window.scrollTo({ top: position.y, behavior: 'instant' });
            }));
        } catch (err) {
            if (!controller.signal.aborted) setError(err instanceof Error ? err.message : 'Could not load Saved.');
        } finally { if (!controller.signal.aborted) setLoading(false); }
    }
    useEffect(() => {
        if (ready && initialized === storageKey && storageKey) void load();
        return () => request.current?.abort();
    }, [scope, revision, ready, initialized, storageKey]);
    useEffect(() => {
        const refresh = (event: Event) => {
            if ((event as CustomEvent).detail?.key === accountKey) { persist(); setRevision(value => value + 1); }
        };
        window.addEventListener('ballistics-saved', refresh);
        window.addEventListener('pagehide', persist);
        return () => { window.removeEventListener('ballistics-saved', refresh); window.removeEventListener('pagehide', persist); };
    }, [persist, accountKey]);
    function changeScope(next: string) {
        if (next === scope) return;
        persist(); memory.current.scope = next;
        try { if (storageKey) sessionStorage.setItem(storageKey, JSON.stringify(memory.current)); } catch { /* Optional. */ }
        setScope(next);
    }
    return { scope, changeScope, items, cursor, loading, error, load, persist };
}
