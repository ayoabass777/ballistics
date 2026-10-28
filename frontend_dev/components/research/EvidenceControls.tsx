'use client';
import { useId, useRef, useState } from 'react';
import type { EvidenceOption } from '@/lib/evidence-controls';
import { Button } from './shared';
import s from './EvidenceControls.module.css';

export function EvidenceFilter({ label, allLabel, options, value, onChange, total }: {
    label: string; allLabel: string; options: EvidenceOption[]; value: string; onChange: (key: string) => void; total: number;
}) {
    const dialog = useRef<HTMLDialogElement>(null), trigger = useRef<HTMLButtonElement>(null);
    const id = useId();
    const [openGroup, setOpenGroup] = useState<string | null>(null);
    const selected = options.find(option => option.key === value);
    const groups = [...new Set(options.map(option => option.group))];
    const close = () => { dialog.current?.close(); trigger.current?.focus(); };
    const select = (key: string) => { onChange(key); close(); };
    return <div className={s.filter}>
        <button className={s.control} ref={trigger} aria-haspopup="dialog" onClick={() => { setOpenGroup(null); dialog.current?.showModal(); }}>{label}: {selected?.label || (value ? 'Selected finding (0)' : allLabel)} ▾</button>
        {value && <Button onClick={() => onChange('')}>Clear filter ×</Button>}
        <dialog ref={dialog} className={s.dialog} aria-labelledby={`${id}-title`} onClick={event => { const bounds = event.currentTarget.getBoundingClientRect(); if (event.target === event.currentTarget && (event.clientX < bounds.left || event.clientX > bounds.right || event.clientY < bounds.top || event.clientY > bounds.bottom)) close(); }} onCancel={event => { event.preventDefault(); close(); }}>
            <h2 id={`${id}-title`}>Choose {label === 'Market' ? 'a goal market' : label === 'Pattern' ? 'a team pattern' : 'a finding'}</h2>
            <p>Choose one option. Results update immediately.</p>
            <button autoFocus className={s.option} aria-pressed={!value} onClick={() => select('')}>{!value && '✓ '}{allLabel}<span>{total}</span></button>
            {value && !selected && <p role="status">Your selected finding has no results in this selection. Clear it to see all results.</p>}
            {groups.map((group, index) => <div key={group}>
                <button className={s.group} aria-expanded={openGroup === group} aria-controls={`${id}-${index}`} onClick={() => setOpenGroup(openGroup === group ? null : group)}>{label === 'Finding' ? group : group.replace(/^(Goals outlook|Team patterns) · /, '')}{selected?.group === group && ' ✓'}<span aria-hidden="true">{openGroup === group ? '▾' : '▸'}</span></button>
                <div id={`${id}-${index}`} hidden={openGroup !== group}>{options.filter(option => option.group === group).map(option => <button key={option.key} className={s.option} aria-pressed={value === option.key} onClick={() => select(option.key)}>{value === option.key && '✓ '}{option.label}<span>{option.count}</span></button>)}</div>
            </div>)}
            {!options.length && <p>No qualifying findings for these dates and leagues.</p>}
            <Button onClick={close}>Cancel</Button>
        </dialog>
    </div>;
}
export function EvidenceSort({ kind, value, onChange }: { kind: 'goals' | 'patterns'; value: string; onChange: (value: string) => void }) {
    return <label className={s.sort}>Sort <select aria-label={`Sort ${kind === 'goals' ? 'Goals outlook' : 'Team patterns'}`} value={value === 'highest' ? 'highest' : ''} onChange={e => onChange(e.target.value)}><option value="">Recommended</option><option value="highest">{kind === 'goals' ? 'Highest % first' : 'Highest hit rate first'}</option></select></label>;
}
