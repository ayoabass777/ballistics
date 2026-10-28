'use client';

import { useEffect, useState } from 'react';
import { useRouter } from 'next/navigation';
import Link from 'next/link';
import { leagueDirectorySchema } from '@/lib/account';
import { track } from '@/lib/usage-events';
import { ProductHeader } from '@/components/account/ProductHeader';
import s from './onboarding.module.css';

type League = { league_id: number; league: string; country: string | null; league_logo_url: string | null };

export function Onboarding() {
  const router = useRouter();
  const [step, setStep] = useState<'explain' | 'leagues'>('explain');
  const [leagues, setLeagues] = useState<League[]>([]);
  const [selected, setSelected] = useState<number[]>([]);
  const [search, setSearch] = useState('');
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState('');
  const [replay, setReplay] = useState(false);

  useEffect(() => {
    let active = true;
    const reviewing = new URLSearchParams(window.location.search).has('replay');
    setReplay(reviewing);
    fetch('/api/account/profile', { cache: 'no-store' })
      .then(async response => {
        if (!response.ok) throw new Error('Could not load your account. Try again.');
        return response.json();
      }).then(profile => {
        if (active && profile?.data?.onboardingCompleted === true && !reviewing) router.replace('/discover');
      }).catch(() => { if (active) setError('Could not load your account. Try again.'); });
    fetch('/api/account/leagues/directory', { cache: 'no-store' })
      .then(async response => {
        if (!response.ok) throw new Error('League choices are temporarily unavailable.');
        return leagueDirectorySchema.parse(await response.json());
      }).then(result => { if (active) setLeagues(result.data.leagues); })
      .catch(() => { if (active) setError('League choices are temporarily unavailable. You can skip and choose them later in Settings.'); })
      .finally(() => { if (active) setLoading(false); });
    return () => { active = false; };
  }, [router]);

  async function finish(leagueIds: number[], outcome: 'completed' | 'skipped') {
    setSaving(true); setError('');
    try {
      const response = await fetch('/api/account/onboarding', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ league_ids: leagueIds }),
      });
      if (!response.ok) throw new Error('Could not save your setup. Try again.');
      track('onboarding_finished', { outcome, league_count: leagueIds.length });
      sessionStorage.setItem('ballistics:onboarding:save-tip', '1');
      router.replace(leagueIds.length ? `/discover?leagues=${leagueIds.join(',')}` : '/discover');
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : 'Could not save your setup. Try again.');
      setSaving(false);
    }
  }

  const filtered = leagues.filter(item => `${item.country || ''} ${item.league}`.toLowerCase().includes(search.toLowerCase()));
  const countries = [...new Set(filtered.map(item => item.country || 'Other'))];
  return <div className={s.page}>
    <ProductHeader publicPage />
    <main className={s.main}>
      <p className={s.eyebrow}>YOUR FIRST RUN · {step === 'explain' ? '1 OF 2' : '2 OF 2'}</p>
      {step === 'explain' ? <>
        <h1>Football context for every matchup.</h1>
        <p className={s.lead}>Ballistics helps you inspect the evidence behind a match. It does not tell you what to bet.</p>
        <div className={s.cards}>
          <section><span>01 · RECORD</span><h2>What happened</h2><p>Recent and season results describe past matches.</p></section>
          <section><span>02 · ESTIMATE</span><h2>What might happen</h2><p>Goals outlook shows estimated chances based on scoring and conceding records, never guarantees.</p></section>
          <section><span>03 · COMPARISON</span><h2>What stands out</h2><p>Strength and patterns help compare teams and leagues.</p></section>
        </div>
        <p className={s.caution}>If you bet, only risk what you can afford to lose.</p>
        <div className={s.actions}>{replay ? <Link className={s.primary} href="/discover">Back to Discover →</Link> : <><button className={s.primary} onClick={() => setStep('leagues')}>Choose my leagues →</button><button className={s.textButton} disabled={saving} onClick={() => finish([], 'skipped')}>Skip setup</button></>}</div>
      </> : <>
        <h1>Pick the leagues you follow.</h1>
        <p className={s.lead}>Your choices sync with your account. You can change them in Settings or switch to All leagues anytime.</p>
        <label className={s.searchLabel} htmlFor="onboarding-league-search">Find a league</label>
        <input id="onboarding-league-search" className={s.search} value={search} onChange={event => setSearch(event.target.value)} placeholder="Search by league or country" />
        {loading ? <p role="status">Loading leagues…</p> : <div className={s.directory}>{countries.map(country => <details key={country} open={!!search.trim()}><summary>{country} · {filtered.filter(item => (item.country || 'Other') === country && selected.includes(item.league_id)).length} selected</summary><div className={s.options}>{filtered.filter(item => (item.country || 'Other') === country).map(item => <label key={item.league_id}><input type="checkbox" checked={selected.includes(item.league_id)} onChange={event => setSelected(current => event.target.checked ? [...current, item.league_id] : current.filter(id => id !== item.league_id))}/><span>{item.league}</span></label>)}</div></details>)}</div>}
        <p className={s.count}>{selected.length} selected</p>
        <div className={s.actions}><button className={s.primary} disabled={saving || !selected.length} onClick={() => finish(selected, 'completed')}>Continue to Discover →</button><button className={s.textButton} disabled={saving} onClick={() => finish([], 'completed')}>Use all leagues</button></div>
      </>}
      {error && <p className={s.error} role="alert">{error}</p>}
      <p className={s.footer}>You can revisit this explanation in <Link href="/settings">Settings</Link>.</p>
    </main>
  </div>;
}
