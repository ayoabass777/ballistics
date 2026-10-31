'use client';

import { useAuth } from '@clerk/nextjs';
import { usePathname } from 'next/navigation';
import { useRouter } from 'next/navigation';
import { useEffect, useState } from 'react';
import { discardQueued } from '@/lib/usage-events';
import { usePreferences } from '@/components/research/PreferencesProvider';
import s from './AccountProfileSync.module.css';

const memberPath = /^\/(dashboard|account|billing|discover|matches|standouts|match|saved|settings)(\/|$)/;

export function AccountProfileSync() {
  const { isLoaded, userId } = useAuth();
  const pathname = usePathname();
  const router = useRouter();
  const { error: preferencesError, reload: reloadPreferences } = usePreferences();
  const [failed, setFailed] = useState(false);
  const [dismissed, setDismissed] = useState(false);
  const [revision, setRevision] = useState(0);

  useEffect(() => {
    if (!isLoaded || !userId || !memberPath.test(pathname)) return;
    let active = true;
    void fetch('/api/account/profile', { cache: 'no-store' })
      .then(response => {
        // A server error here once hid a broken account API: members skipped
        // onboarding with no sign of a problem. Say so instead (401 is just
        // a session still settling, so it stays quiet).
        if (active) setFailed(response.status >= 500);
        return response.ok ? response.json() : null;
      })
      .then(profile => {
        if (active && profile?.data?.onboardingCompleted === false) {
          // The member page rendered briefly before this redirect; it was not really viewed.
          discardQueued('discover_viewed');
          router.replace('/onboarding');
        }
      }).catch(() => {
        // Member pages remain usable if account data is temporarily unavailable.
        if (active) setFailed(true);
      });
    return () => { active = false; };
  }, [isLoaded, userId, pathname, router, revision]);

  if (!memberPath.test(pathname) || (!failed && !preferencesError) || dismissed) return null;
  const retry = () => {
    setFailed(false);
    setRevision(value => value + 1);
    reloadPreferences();
  };
  return (
    <div className={s.notice} role="alert">
      <p>{failed ? 'Account unavailable. Setup and settings may be affected.' : 'Account preferences unavailable.'}{preferencesError && ' Times use your browser timezone.'}</p>
      <button type="button" onClick={retry}>Try again</button>
      <button className={s.dismiss} type="button" aria-label="Dismiss account notice" onClick={() => setDismissed(true)}>×</button>
    </div>
  );
}
