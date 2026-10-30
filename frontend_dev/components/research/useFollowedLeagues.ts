'use client';
import { useCallback, useEffect, useState } from 'react';
import { useUser } from '@clerk/nextjs';
import { followedResponseSchema } from '@/lib/account';
import { track } from '@/lib/usage-events';

export function useFollowedLeagues() {
    const { user, isLoaded } = useUser();
    const userId = user?.id || null;
    const [state, setState] = useState<{ userId: string | null; ids: number[]; ready: boolean; error: string }>({
        userId: null, ids: [], ready: false, error: '',
    });
    const [busy, setBusy] = useState(false);
    const [revision, setRevision] = useState(0);
    useEffect(() => {
        if (!isLoaded) return;
        if (!userId) { setState({ userId: null, ids: [], ready: true, error: '' }); return; }
        const abort = new AbortController();
        setState({ userId, ids: [], ready: false, error: '' });
        fetch('/api/account/leagues', { cache: 'no-store', signal: abort.signal }).then(async response => {
            const body = await response.json();
            if (!response.ok) throw new Error(body.error || 'Could not load followed leagues.');
            return followedResponseSchema.parse(body).data.league_ids;
        }).then(ids => { if (!abort.signal.aborted) setState({ userId, ids, ready: true, error: '' }); })
          .catch(error => { if (!abort.signal.aborted) setState({ userId, ids: [], ready: true, error: error instanceof Error ? error.message : 'Could not load followed leagues.' }); });
        return () => abort.abort();
    }, [isLoaded, userId, revision]);
    const save = useCallback(async (ids: number[]) => {
        if (!userId) throw new Error('Sign in to follow leagues.');
        setBusy(true);
        try {
            const response = await fetch('/api/account/leagues', {
                method: 'PUT', headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ league_ids: ids }),
            });
            const body = await response.json();
            if (!response.ok) throw new Error(body.error || 'Could not save followed leagues.');
            const next = followedResponseSchema.parse(body).data.league_ids;
            setState({ userId, ids: next, ready: true, error: '' });
            window.dispatchEvent(new CustomEvent('ballistics-leagues', { detail: { userId, ids: next } }));
        } finally { setBusy(false); }
    }, [userId]);
    useEffect(() => {
        if (!userId) return;
        const sync = (event: Event) => {
            const detail = (event as CustomEvent<{ userId: string; ids: number[] }>).detail;
            if (detail?.userId === userId) setState({ userId, ids: detail.ids, ready: true, error: '' });
        };
        window.addEventListener('ballistics-leagues', sync);
        return () => window.removeEventListener('ballistics-leagues', sync);
    }, [userId]);
    return {
        ids: state.userId === userId ? state.ids : [],
        ready: isLoaded && state.userId === userId && state.ready,
        error: state.userId === userId ? state.error : '',
        signedIn: !!userId, busy, save, reload: () => { track('retry_clicked', { api: '/api/account/leagues' }); setRevision(n => n + 1); },
    };
}
