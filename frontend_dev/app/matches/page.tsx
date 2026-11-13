import { Suspense } from 'react';
import { DiscoverPage } from '@/components/research/DiscoverPage';
import { hasLiveMatchesPreview } from '@/lib/server/owner-live';
export default async function Page() { return <Suspense><DiscoverPage matches liveMatches={await hasLiveMatchesPreview()} /></Suspense>; }
