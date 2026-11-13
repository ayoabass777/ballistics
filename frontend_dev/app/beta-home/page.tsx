import { currentUser } from '@clerk/nextjs/server';
import { redirect } from 'next/navigation';
import { betaHome } from '@/lib/beta-navigation';

export default async function BetaHome() {
    const user = await currentUser();
    if (!user) redirect('/sign-in');
    redirect(betaHome(user.publicMetadata));
}
