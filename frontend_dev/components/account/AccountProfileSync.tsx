'use client';

import { useAuth } from '@clerk/nextjs';
import { usePathname } from 'next/navigation';
import { useRouter } from 'next/navigation';
import { useEffect, useState } from 'react';
import { discardQueued } from '@/lib/usage-events';
import s from './AccountProfileSync.module.css';

const memberPath = /^\/(dashboard|account|billing|discover|matches|standouts|match|saved|settings)(\/|$)/;

export function AccountProfileSync() {
  const { isLoaded, userId } = useAuth();
  const pathname = usePathname();
  const router = useRouter();
  const [failed, setFailed] = useState(false);

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
  }, [isLoaded, userId, pathname, router]);

  if (!failed) return null;
  return (
    <div className={s.notice} role="alert">
      <p>We couldn&apos;t load your account, so onboarding and some settings may not work right now.</p>
      <button type="button" onClick={() => window.location.reload()}>Try again</button>
    </div>
  );
}
