'use client';
import { useEffect, useRef, useState } from 'react';

const BATCH = 12;
type Position = { count: number; y: number; height: number };
function read(key: string): Position {
    try {
        const value = JSON.parse(sessionStorage.getItem(key) || 'null');
        if (Number.isInteger(value?.count) && value.count >= BATCH && Number.isFinite(value.y) && value.y >= 0)
            return { count: value.count, y: value.y, height: Number.isFinite(value.height) && value.height >= 0 ? value.height : value.y + window.innerHeight };
    } catch { /* Storage can be unavailable. Browsing still works. */ }
    return { count: BATCH, y: 0, height: 0 };
}
/** Restore only after async cards and their expanded height are rendered. */
export function useListPosition(url: string, ready: boolean) {
    const key = `ballistics:list:${url}`;
    const [state, setState] = useState({ key: '', count: BATCH, y: 0, height: 0 });
    const restored = useRef(false);
    const [restorationDone, setRestorationDone] = useState(false);
    const count = state.key === key ? state.count : BATCH;
    useEffect(() => {
        // Next's history restoration can run after the cards render and undo
        // our saved position on browser Back. Keep list restoration authoritative.
        window.history.scrollRestoration = 'manual';
        restored.current = false;
        setRestorationDone(false);
        const stored = read(key);
        setState({ key, ...stored });
    }, [key]);
    useEffect(() => {
        if (ready || state.key !== key || !state.y) return;
        window.scrollTo({ top: state.y, behavior: 'instant' });
    }, [key, ready, state.key, state.y, state.height]);
    useEffect(() => {
        if (!ready || state.key !== key) return;
        let frame = 0;
        let quietTimer: ReturnType<typeof setTimeout> | undefined;
        let deadline: ReturnType<typeof setTimeout> | undefined;
        let observer: ResizeObserver | undefined;
        let cancelled = false;
        let departing = false;
        const y = state.y;
        const finish = () => {
            if (cancelled) return;
            clearTimeout(quietTimer); clearTimeout(deadline);
            observer?.disconnect();
            restored.current = true;
        };
        const restore = () => {
            if (cancelled || restored.current) return;
            window.scrollTo({ top: y, behavior: 'instant' });
            clearTimeout(quietTimer);
            quietTimer = setTimeout(finish, 250);
        };
        const interrupt = () => { finish(); };
        const keyInterrupt = (event: KeyboardEvent) => {
            if (['ArrowDown', 'ArrowUp', 'PageDown', 'PageUp', 'Home', 'End', ' '].includes(event.key)) finish();
        };
        if (!restored.current) {
            // Remove the loading placeholder before measuring the real cards.
            // Observe layout shifts instead of assuming eight frames are enough.
            setRestorationDone(true);
            frame = requestAnimationFrame(() => {
                if (cancelled || restored.current) return;
                observer = new ResizeObserver(restore);
                observer.observe(document.body);
                restore();
                deadline = setTimeout(finish, 3000);
                document.fonts?.ready.then(() => { if (!cancelled) restore(); });
            });
        }
        const save = () => {
            if (!restored.current || departing) return;
            try { sessionStorage.setItem(key, JSON.stringify({ count, y: window.scrollY, height: window.scrollY + window.innerHeight })); } catch { /* Optional persistence. */ }
        };
        const beforeNavigate = (event: MouseEvent) => {
            const link = event.target instanceof Element ? event.target.closest('a[href]') : null;
            if (!link || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
            const destination = new URL(link.getAttribute('href')!, window.location.href);
            if (destination.pathname !== window.location.pathname) {
                save();
                // Route-transition scrolling must never overwrite the list position.
                departing = true;
            }
        };
        window.addEventListener('wheel', interrupt, { passive: true });
        window.addEventListener('touchstart', interrupt, { passive: true });
        window.addEventListener('keydown', keyInterrupt);
        window.addEventListener('scroll', save, { passive: true });
        window.addEventListener('pagehide', save);
        document.addEventListener('click', beforeNavigate, true);
        return () => {
            cancelled = true;
            cancelAnimationFrame(frame);
            clearTimeout(quietTimer); clearTimeout(deadline); observer?.disconnect();
            window.removeEventListener('wheel', interrupt);
            window.removeEventListener('touchstart', interrupt);
            window.removeEventListener('keydown', keyInterrupt);
            window.removeEventListener('scroll', save);
            window.removeEventListener('pagehide', save);
            document.removeEventListener('click', beforeNavigate, true);
        };
    }, [key, ready, state.key, count]);
    return { count, restoringHeight: state.key === key && !restorationDone ? state.height : 0, more: () => {
        const next = count + BATCH;
        const position = { key, count: next, y: window.scrollY, height: window.scrollY + window.innerHeight };
        try { sessionStorage.setItem(key, JSON.stringify(position)); } catch { /* Optional persistence. */ }
        setState(position);
    } };
}
