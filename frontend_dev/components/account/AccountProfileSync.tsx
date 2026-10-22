'use client';

import { useAuth } from '@clerk/nextjs';
import { usePathname } from 'next/navigation';
import { useRouter } from 'next/navigation';
import { useEffect } from 'react';

const memberPath = /^\/(dashboard|account|billing|discover|matches|standouts|match|saved|settings)(\/|$)/;

export function AccountProfileSync() {
  const { isLoaded, userId } = useAuth();
  const pathname = usePathname();
  const router = useRouter();

  useEffect(() => {
    if (!isLoaded || !userId || !memberPath.test(pathname)) return;
    let active = true;
    void fetch('/api/account/profile', { cache: 'no-store' })
      .then(response => response.ok ? response.json() : null)
      .then(profile => {
        if (active && profile?.data?.onboardingCompleted === false) router.replace('/onboarding');
      }).catch(() => {
        // Member pages remain usable if account data is temporarily unavailable.
      });
    return () => { active = false; };
  }, [isLoaded, userId, pathname, router]);

  return null;
}
