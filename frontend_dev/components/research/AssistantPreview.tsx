'use client';

import { useState } from 'react';
import type { Match } from '@/lib/research';
import s from './AssistantPreview.module.css';

type Topic = 'overview' | 'goals' | 'patterns';

export function AssistantPreview({ match }: { match: Match }) {
    const [topic, setTopic] = useState<Topic>('overview');
    const [open, setOpen] = useState(false);
    const { fixture, strength, goals, patterns } = match;
    const questions: Record<Topic, string> = {
        overview: 'What stands out in this match?',
        goals: 'What do the goal estimates say?',
        patterns: 'Which team patterns matter?',
    };
    const unavailable = fixture.fixture_status && !new Set(['NS', '1H', 'HT', '2H', 'ET', 'BT', 'P', 'LIVE', 'FT', 'AET', 'PEN']).has(fixture.fixture_status);
    const bestGoal = goals?.rows.reduce((best, row) => !best || row.probability > best.probability ? row : best, goals.rows[0]);
    const pattern = patterns?.venue?.top_results[0] || patterns?.all?.top_results[0] || patterns?.venue?.top_attack_defence[0] || patterns?.all?.top_attack_defence[0];
    const strengthText = strength?.state === 'big_dog' && strength.favourite
        ? `${strength.favourite} has the strongest team-strength label in this analysis.`
        : strength?.state === 'slight_favourite' && strength.favourite
        ? `${strength.favourite} has a strength edge, but it does not meet the Big dog rule.`
        : strength?.state === 'close' ? 'The teams are close on the strength measure.'
        : 'There is no qualifying strength label in this snapshot.';
    const answer = unavailable
        ? 'This match is not going ahead as scheduled. Its pre-match forecast is hidden, so I would not explain it as a current prediction.'
        : topic === 'goals'
        ? bestGoal ? `${bestGoal.market} is estimated at ${Math.round(bestGoal.probability * 100)}% in this snapshot. That is an estimated chance, not a guarantee. It does not tell you whether the bookmaker’s odds offer value.` : 'There is no goals estimate available for this match.'
        : topic === 'patterns'
        ? pattern ? `${pattern.team} recorded ${pattern.label.toLowerCase()} in ${pattern.k} of ${pattern.n} ${pattern.context === 'all' ? 'matches across all venues' : `${pattern.context} matches`} this season. This is an observed record, not the chance it happens next time.` : 'There is no qualifying team pattern available for this match.'
        : `${strengthText}${bestGoal ? ` The highest displayed goal estimate is ${bestGoal.market} at ${Math.round(bestGoal.probability * 100)}%.` : ''} Open the evidence below to check each figure.`;
    const evidence = unavailable ? '#context' : topic === 'goals' && goals ? '#goals' : topic === 'patterns' && patterns ? '#patterns' : strength ? '#strength' : '#context';

    return <div className={s.dock}>
        {open && <section id="assistant-preview" className={s.preview} aria-label="Match assistant preview">
            <div className={s.heading}><div><span className={s.eyebrow}>BALLISTICS · PREVIEW</span><h2>Ask about this match</h2><p>{fixture.home} v {fixture.away}</p></div><button className={s.close} type="button" onClick={() => setOpen(false)} aria-label="Close match assistant">×</button></div>
            <div className={s.body}><div className={s.questions} aria-label="Example questions">{(Object.keys(questions) as Topic[]).map(key => <button key={key} type="button" className={topic === key ? s.selected : ''} aria-pressed={topic === key} onClick={() => setTopic(key)}>{questions[key]}</button>)}</div>
            <div className={s.conversation}><p className={s.userBubble}>{questions[topic]}</p><div className={s.answer}><span className={s.answerLabel}>EXAMPLE ANSWER</span><p>{answer}</p><a href={evidence} onClick={() => setOpen(false)}>View supporting evidence ↓</a></div></div></div>
            <div className={s.composer}><input aria-label="Ask a question" placeholder="Ask about this match…" disabled/><button type="button" disabled aria-label="Send question">↑</button></div>
            <p className={s.footnote}>Preview only · Free-text AI chat is not connected.</p>
        </section>}
        <button className={s.launcher} type="button" onClick={() => setOpen(!open)} aria-expanded={open} aria-controls="assistant-preview" aria-label={open ? 'Close match assistant' : 'Open match assistant preview'}><svg viewBox="0 0 24 24" width="21" height="21" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"><path d="M20 11.5a8 8 0 0 1-8 8 8.5 8.5 0 0 1-3.1-.6L4 20l1.2-4.4A8 8 0 1 1 20 11.5Z"/><path d="M8.5 11.5h7"/></svg><span>Ask Ballistics</span></button>
    </div>;
}
