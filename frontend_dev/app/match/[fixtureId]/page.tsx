import { Suspense } from 'react';
import { MatchPage } from '@/components/research/MatchPage';
export default async function Page({ params }: {
    params: Promise<{
        fixtureId: string;
    }>;
}) { const { fixtureId } = await params; return <Suspense><MatchPage fixtureId={fixtureId}/></Suspense>; }
