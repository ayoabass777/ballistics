'use client';

import Link from 'next/link';
import { useEffect, useState } from 'react';
import type { FormEvent } from 'react';
import { SignedIn, SignedOut, useUser } from '@clerk/nextjs';
import { ProductHeader } from '@/components/account/ProductHeader';
import s from './welcome.module.css';

export default function WelcomePage() {
  const { isLoaded, isSignedIn } = useUser();
  const [signedOut, setSignedOut] = useState(false);
  const [code, setCode] = useState('');
  const [email, setEmail] = useState('');
  const [inviteState, setInviteState] = useState<'idle' | 'sending' | 'sent'>('idle');
  const [inviteError, setInviteError] = useState('');
  async function requestInvite(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setInviteError('');
    setInviteState('sending');
    try {
      const response = await fetch('/api/beta/invite', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ code, email }),
      });
      if (!response.ok) {
        const result = await response.json();
        throw new Error(result.error || 'Could not send invitation. Please try later.');
      }
      setInviteState('sent');
      setCode('');
    } catch (error) {
      setInviteState('idle');
      setInviteError(error instanceof Error ? error.message : 'Could not send invitation. Please try later.');
    }
  }
  useEffect(() => {
    if (!isLoaded) return;
    const url = new URL(window.location.href);
    if (url.searchParams.get('signed_out') === '1') {
      setSignedOut(!isSignedIn);
      url.searchParams.delete('signed_out');
      window.history.replaceState(window.history.state, '', `${url.pathname}${url.search}${url.hash}`);
    }
  }, [isLoaded, isSignedIn]);
  return <div className={s.page}>
    <ProductHeader publicPage/>
    <main className={s.main}>
      {signedOut && !isSignedIn && <div className={s.notice} role="status"><span>You’ve signed out.</span><button onClick={() => setSignedOut(false)} aria-label="Dismiss sign-out message">×</button></div>}
      <p className={s.eyebrow}>WELCOME TO BALLISTICS</p>
      <h1>See the match.<br/>Understand the context.</h1>
      <p className={s.intro}>Explore team strength, goal outlooks and patterns, with the evidence behind each insight.</p>
      <div className={s.actions}>
        <SignedIn><Link className={s.primary} href="/discover">Go to Discover →</Link></SignedIn>
      </div>
      <SignedOut>
        <section className={s.invite} aria-labelledby="invite-title">
          <h2 id="invite-title">Join the private beta</h2>
          <p className={s.returning}>Already accepted your invitation? <Link href="/sign-in">Sign in with the same account →</Link> You do not need another code.</p>
          {inviteState === 'sent' ? <p role="status">Check {email} for your account creation link. The code has been used.</p> :
            <form onSubmit={requestInvite}>
              <p>Enter your invitation code and the email address it was issued for.</p>
              <label htmlFor="invite-code">Invitation code</label>
              <input id="invite-code" autoComplete="off" value={code} onChange={e => setCode(e.target.value)} required maxLength={40} />
              <label htmlFor="invite-email">Invited email</label>
              <input id="invite-email" type="email" autoComplete="email" value={email} onChange={e => setEmail(e.target.value)} required maxLength={254} />
              {inviteError && <p className={s.error} role="alert">{inviteError}</p>}
              <button className={s.primary} disabled={inviteState === 'sending'} type="submit">{inviteState === 'sending' ? 'Sending…' : 'Send account creation link →'}</button>
            </form>}
        </section>
      </SignedOut>
      <div className={s.features}>
        <section><h2>Team strength</h2><p>See how the teams compare before kickoff.</p></section>
        <section><h2>Goals outlook</h2><p>Explore estimated outcomes alongside match context.</p></section>
        <section><h2>Team patterns</h2><p>Understand recent and season records across venues.</p></section>
      </div>
      <p className={s.caption}>Estimates are not guarantees. Historical patterns describe what happened.</p>
    </main>
  </div>;
}
