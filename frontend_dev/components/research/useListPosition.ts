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
        if (!restored.current) {
            const y = read(key).y;
            frame = requestAnimationFrame(() => {
                window.scrollTo({ top: y, behavior: 'instant' });
                restored.current = true;
                setRestorationDone(true);
            });
        }
        const save = () => {
            if (!restored.current) return;
            try { sessionStorage.setItem(key, JSON.stringify({ count, y: window.scrollY, height: window.scrollY + window.innerHeight })); } catch { /* Optional persistence. */ }
        };
        window.addEventListener('scroll', save, { passive: true });
        window.addEventListener('pagehide', save);
        document.addEventListener('click', save, true);
        return () => {
            cancelAnimationFrame(frame);
            window.removeEventListener('scroll', save);
            window.removeEventListener('pagehide', save);
            document.removeEventListener('click', save, true);
        };
    }, [key, ready, state.key, count]);
    return { count, restoringHeight: state.key === key && !restorationDone ? state.height : 0, more: () => {
        const next = count + BATCH;
        const position = { key, count: next, y: window.scrollY, height: window.scrollY + window.innerHeight };
        try { sessionStorage.setItem(key, JSON.stringify(position)); } catch { /* Optional persistence. */ }
        setState(position);
    } };
}
