'use client';
import { CalendarDays, ChevronLeft, ChevronRight } from 'lucide-react';
import { useEffect, useState } from 'react';
import { usePathname, useRouter, useSearchParams } from 'next/navigation';
import { dayInZone } from '@/lib/preferences';
import { Button } from './shared';
import s from './research.module.css';

export function DiscoverDates({ zone, dayWindow = false }: { zone: string | null; dayWindow?: boolean }) {
    const search = useSearchParams(), router = useRouter(), path = usePathname();
    const scope = search.get('scope') || 'week';
    const [open, setOpen] = useState(false);
    const [start, setStart] = useState(''), [end, setEnd] = useState('');
    const today = dayInZone(zone || 'UTC');
    const chosenDay = search.get('day');
    const [windowStart, setWindowStart] = useState(today);
    const shiftDay = (day: string, offset: number) => {
        const date = new Date(`${day}T12:00:00Z`);
        date.setUTCDate(date.getUTCDate() + offset);
        return date.toISOString().slice(0, 10);
    };
    useEffect(() => {
        const anchor = chosenDay && /^\d{4}-\d{2}-\d{2}$/.test(chosenDay) ? chosenDay : scope === 'tomorrow' ? shiftDay(today, 1) : today;
        setWindowStart(current => anchor >= current && anchor <= shiftDay(current, 4) ? current : anchor);
    }, [chosenDay, scope, today]);
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
            {!dayWindow && ['week', 'today', 'tomorrow'].map((key, i) => <Button key={key}
                className={scope === key ? s.active : ''} aria-pressed={scope === key}
                onClick={() => apply(key)}>{['Next 7 days', 'Today', 'Tomorrow'][i]}</Button>)}
            {dayWindow && <>
                <Button className={scope === 'week' ? s.active : ''} aria-pressed={scope === 'week'} onClick={() => { setWindowStart(today); apply('week'); }}>Next 7 days</Button>
                <div className={s.discoverDateWindow} role="group" aria-label="Browse dates">
                <Button aria-label="Earlier dates" onClick={() => setWindowStart(shiftDay(windowStart, -5))}><ChevronLeft size={18} aria-hidden="true"/></Button>
                {[0, 1, 2, 3, 4].map(offset => {
                    const day = shiftDay(windowStart, offset);
                    const isToday = day === today, isTomorrow = day === shiftDay(today, 1);
                    const selected = (scope === 'today' && isToday) || (scope === 'tomorrow' && isTomorrow) || (scope === 'date' && search.get('day') === day);
                    return <Button key={day} className={selected ? s.active : ''} aria-pressed={selected} onClick={() => apply('date', day)}>
                        <span>{isToday ? 'Today' : new Date(day + 'T12:00:00Z').toLocaleDateString('en-GB', {weekday:'short',timeZone:'UTC'})}<small className={s.dateDayNumber}>{new Date(day + 'T12:00:00Z').toLocaleDateString('en-GB', {day:'numeric',month:'short',timeZone:'UTC'})}</small></span>
                    </Button>;
                })}
                <Button aria-label="Later dates" onClick={() => setWindowStart(shiftDay(windowStart, 5))}><ChevronRight size={18} aria-hidden="true"/></Button>
                </div>
            </>}
            <Button className={scope === 'date' || scope === 'range' ? s.active : ''}
                aria-expanded={open} aria-controls="discover-date-picker"
                onClick={() => {
                    const day = search.get('day') || dayInZone(zone || 'UTC');
                    setStart(day); setEnd(search.get('end_day') || day); setOpen(!open);
                }}><CalendarDays size={17} aria-hidden="true"/>Calendar</Button>
        </div>
        <p className={`${s.caption} ${s.dateSelectionSummary}`} aria-live="polite">{scope === 'date' || scope === 'range' ? <>{search.get('day')}{scope === 'range' && ` to ${search.get('end_day')}`}</> : '\u00a0'}</p>
        {open && <div id="discover-date-picker" className={s.matchDateRow}>
            <label>From<input type="date" value={start} onChange={e => setStart(e.target.value)}/></label>
            <label>To<input type="date" min={start} value={end} onChange={e => setEnd(e.target.value)}/></label>
            <Button onClick={() => setOpen(false)}>Cancel</Button>
            <Button className={s.active} disabled={!valid} onClick={() => apply(start === end ? 'date' : 'range', start, start === end ? '' : end)}>Apply dates</Button>
            {!valid && <span role="alert">Choose an end date on or after the start date.</span>}
        </div>}
    </>;
}
