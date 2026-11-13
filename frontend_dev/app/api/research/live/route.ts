import { hasLiveMatchesPreview } from '@/lib/server/owner-live';
import { NextRequest } from 'next/server';
import { researchProxy } from '@/lib/server/research-proxy';
import { liveListSchema } from '@/lib/live';
export const dynamic = 'force-dynamic';
export async function GET(request: NextRequest) {
    if (!(await hasLiveMatchesPreview())) return new Response(null, { status: 403 });
    return researchProxy(request, 'live', liveListSchema);
}
