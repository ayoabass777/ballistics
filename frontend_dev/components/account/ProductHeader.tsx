'use client';

import Link from 'next/link';
import { SignedIn, SignedOut, UserButton } from '@clerk/nextjs';
import { Settings } from 'lucide-react';
import s from './ProductHeader.module.css';

export function ProductHeader({ publicPage = false }: { publicPage?: boolean }) {
  return <header className={s.header}><div className={s.inner}>
    <Link href={publicPage ? '/welcome' : '/discover'} className={s.brand} aria-label="Ballistics home"><span aria-hidden="true">b.</span>ballistics</Link>
    <p className={s.tagline}>FOOTBALL, WITH CONTEXT</p>
    <nav className={s.actions} aria-label="Account">
      <SignedIn>
        <Link href="/settings" className={s.settings} aria-label="Settings" title="Settings"><Settings size={18} aria-hidden="true"/><span>Settings</span></Link>
        <div className={s.avatar}><UserButton afterSignOutUrl="/welcome?signed_out=1" appearance={{ elements: { userButtonTrigger: { width: 44, height: 44, display: 'flex', alignItems: 'center', justifyContent: 'center' }, avatarBox: { width: 32, height: 32 } } }}/></div>
      </SignedIn>
      <SignedOut><Link className={s.signIn} href="/sign-in">Sign in</Link></SignedOut>
    </nav>
  </div></header>;
}
