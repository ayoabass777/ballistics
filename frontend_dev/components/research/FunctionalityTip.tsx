'use client';
import Link from 'next/link';
import { useUser } from '@clerk/nextjs';
import { useEffect, useState } from 'react';
import { track } from '@/lib/usage-events';
import s from './research.module.css';

export const tutorialIds = ['discover', 'filters', 'match', 'saved', 'settings', 'record'] as const;
type TutorialId = typeof tutorialIds[number];
const key = (userId: string, id: TutorialId) => `ballistics:tutorial:${userId}:${id}`;
export const matchGuideKey = (userId: string) => key(userId, 'match');

export function replayFunctionalityTips(userId: string) {
    for (const id of tutorialIds) localStorage.removeItem(key(userId, id));
    track('tutorial_replayed', { what: 'tips' });
}

export function FunctionalityTip({ id, title, children, moreHref, moreLabel = 'How it works', suppressIfOnboardingSaveTip = false }: {
    id: TutorialId;
    title: string;
    children: React.ReactNode;
    moreHref?: string;
    moreLabel?: string;
    suppressIfOnboardingSaveTip?: boolean;
}) {
    const { user, isLoaded } = useUser();
    const [visible, setVisible] = useState(false);
    useEffect(() => {
        if (!isLoaded || !user?.id) { setVisible(false); return; }
        try { setVisible(localStorage.getItem(key(user.id, id)) !== '1' && (!suppressIfOnboardingSaveTip || sessionStorage.getItem('ballistics:onboarding:save-tip') !== '1')); }
        catch { setVisible(true); }
    }, [isLoaded, user?.id, id, suppressIfOnboardingSaveTip]);
    if (!visible || !user?.id) return null;
    return <aside className={s.tutorialTip} aria-label={`${title} help`}>
        <strong>{title}</strong><div>{children}</div>
        <div className={s.row}>{moreHref && <Link className={s.link} href={moreHref} onClick={() => { if (id === 'discover' || id === 'record') track('methodology_opened', { from_section: id }); }}>{moreLabel} →</Link>}
            <button type="button" onClick={() => { track('tutorial_dismissed', { tutorial_id: id, action: 'got_it' }); try { localStorage.setItem(key(user.id, id), '1'); } catch { /* Dismiss for this visit. */ } setVisible(false); }}>Got it</button>
        </div>
    </aside>;
}
