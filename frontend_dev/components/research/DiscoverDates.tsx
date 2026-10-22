'use client';
import { useState } from 'react';
import { usePathname, useRouter, useSearchParams } from 'next/navigation';
import { dayInZone } from '@/lib/preferences';
import { Button } from './shared';
import s from './research.module.css';

export function DiscoverDates({ zone }: { zone: string | null }) {
    const search = useSearchParams(), router = useRouter(), path = usePathname();
    const scope = search.get('scope') || 'week';
    const [open, setOpen] = useState(false);
    const [start, setStart] = useState(''), [end, setEnd] = useState('');
    const valid = !!start && !!end && start <= end;
    function apply(nextScope: string, day = '', endDay = '') {
        const next = new URLSearchParams(search.toString());
        next.set('scope', nextScope);
        day ? next.set('day', day) : next.delete('day');
        endDay ? next.set('end_day', endDay) : next.delete('end_day');
        router.replace(`${path}?${next}`, { scroll: false });
        setOpen(false);
    }
    return <>
        <div className={s.dateTabs} aria-label="Discover dates">
            {['week', 'today', 'tomorrow'].map((key, i) => <Button key={key}
                className={scope === key ? s.active : ''} aria-pressed={scope === key}
                onClick={() => apply(key)}>{['Next 7 days', 'Today', 'Tomorrow'][i]}</Button>)}
            <Button className={scope === 'date' || scope === 'range' ? s.active : ''}
                aria-expanded={open} aria-controls="discover-date-picker"
                onClick={() => {
                    const day = search.get('day') || dayInZone(zone || 'UTC');
                    setStart(day); setEnd(search.get('end_day') || day); setOpen(!open);
                }}>Choose dates</Button>
        </div>
        {open && <div id="discover-date-picker" className={s.matchDateRow}>
            <label>From<input type="date" value={start} onChange={e => setStart(e.target.value)}/></label>
            <label>To<input type="date" min={start} value={end} onChange={e => setEnd(e.target.value)}/></label>
            <Button onClick={() => setOpen(false)}>Cancel</Button>
            <Button className={s.active} disabled={!valid} onClick={() => apply(start === end ? 'date' : 'range', start, start === end ? '' : end)}>Apply dates</Button>
            {!valid && <span role="alert">Choose an end date on or after the start date.</span>}
        </div>}
    </>;
}
