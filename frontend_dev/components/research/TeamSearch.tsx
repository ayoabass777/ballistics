'use client';
import { useEffect, useRef, useState } from 'react';
import { Search } from 'lucide-react';
import s from './research.module.css';
import { useSearchParams } from 'next/navigation';

// Typing stays in local state so each key shows at once. The URL, and the list
// filtered from it, follow once typing pauses. history.replaceState keeps
// useSearchParams in sync without router.replace's server round trip.
const PAUSE_MS = 200;

export function TeamSearch({ className, label = 'Search teams', placeholder = 'Enter a team name', clearable = false, icon = false }: { className?: string; label?: string; placeholder?: string; clearable?: boolean; icon?: boolean }) {
    const urlValue = useSearchParams().get('q') || '';
    const [text, setText] = useState(urlValue);
    const pending = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
    // Back/forward or another control can change q; don't overwrite pending typing.
    useEffect(() => { if (!pending.current) setText(urlValue); }, [urlValue]);
    useEffect(() => () => clearTimeout(pending.current), []);
    function change(value: string) {
        setText(value);
        clearTimeout(pending.current);
        pending.current = setTimeout(() => {
            pending.current = undefined;
            const next = new URLSearchParams(window.location.search);
            value.trim() ? next.set('q', value) : next.delete('q');
            window.history.replaceState(null, '', `${window.location.pathname}${next.size ? `?${next}` : ''}`);
        }, PAUSE_MS);
    }
    return <div className={className}><label>{label}<span className={icon ? s.iconSearch : undefined}>{icon && <Search size={18} aria-hidden="true"/>}<input type="search" value={text} onChange={e => change(e.target.value)} placeholder={placeholder}/></span></label>{clearable && text && <button type="button" onClick={() => change('')}>Clear search</button>}</div>;
}
