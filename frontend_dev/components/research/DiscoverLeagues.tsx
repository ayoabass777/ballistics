'use client';
import { useState } from 'react';
import { type Discover } from '@/lib/research';
import { Button, Crest } from './shared';
import styles from './DiscoverLeagues.module.css';

export function DiscoverLeagues({ leagues, selected, onApply }: {
    leagues?: Discover['leagues']; selected: string[]; onApply: (value: string) => void;
}) {
    const [open, setOpen] = useState(false);
    const [draft, setDraft] = useState<string[]>([]);
    const [term, setTerm] = useState('');
    const groups = new Map<string, Discover['leagues']>();
    for (const league of [...(leagues || [])].sort((a, b) =>
        (a.country || '').localeCompare(b.country || '') || a.league.localeCompare(b.league))) {
        if (!`${league.country} ${league.league}`.toLowerCase().includes(term.trim().toLowerCase())) continue;
        const country = league.country || 'International / other';
        groups.set(country, [...(groups.get(country) || []), league]);
    }
    return <div className={styles.root}>
        <Button aria-expanded={open} aria-controls="discover-leagues" onClick={() => {
            setDraft(selected); setTerm(''); setOpen(!open);
        }}>{selected.length ? `${selected.length} selected leagues` : 'Choose leagues'}</Button>
        {open && <section id="discover-leagues" className={styles.panel} aria-label="Choose leagues">
            <label className={styles.search}>Search leagues or countries
                <input autoFocus type="search" value={term} onChange={e => setTerm(e.target.value)} placeholder="League or country"/>
            </label>
            <div className={styles.options}>
                {[...groups].map(([country, items]) => <details key={`${country}:${Boolean(term.trim())}`} open={term.trim() ? true : undefined}>
                    <summary>{country}<span>{items.filter(l => draft.includes(String(l.league_id))).length} / {items.length}</span></summary>
                    {items.map(l => <label className={styles.option} key={l.league_id}>
                        <input type="checkbox" checked={draft.includes(String(l.league_id))} onChange={e => setDraft(current => e.target.checked ? [...current, String(l.league_id)] : current.filter(id => id !== String(l.league_id)))}/>
                        <Crest name={l.league} url={l.league_logo_url} league/><span>{l.league}</span>
                    </label>)}
                </details>)}
                {!groups.size && <p>{leagues ? 'No leagues match your search.' : 'Loading leagues…'}</p>}
            </div>
            <div className={styles.actions}><span>{draft.length ? `${draft.length} selected` : 'All leagues'}</span>
                <Button onClick={() => setDraft([])}>Clear</Button><Button onClick={() => setOpen(false)}>Cancel</Button>
                <Button onClick={() => { onApply(draft.join(',')); setOpen(false); }}>Apply leagues</Button>
            </div>
        </section>}
    </div>;
}
