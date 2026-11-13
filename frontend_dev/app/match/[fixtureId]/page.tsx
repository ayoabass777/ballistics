import { Suspense } from 'react';
import { MatchPage } from '@/components/research/MatchPage';
import { hasLiveMatchesPreview } from '@/lib/server/owner-live';
import { OwnerMatchPage } from '@/components/research/OwnerMatchPage';
export default async function Page({ params }: {
    params: Promise<{
        fixtureId: string;
    }>;
}) { const { fixtureId } = await params; const Component = await hasLiveMatchesPreview() ? OwnerMatchPage : MatchPage; return <Suspense><Component fixtureId={fixtureId}/></Suspense>; }
