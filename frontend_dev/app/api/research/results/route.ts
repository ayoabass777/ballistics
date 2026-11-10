import { hasLiveMatchesPreview } from '@/lib/server/owner-live';
import { NextRequest } from 'next/server';
import { researchProxy } from '@/lib/server/research-proxy';
import { resultsSchema } from '@/lib/results';
export const dynamic = 'force-dynamic';
export async function GET(request: NextRequest) { if (!(await hasLiveMatchesPreview())) return new Response(null, { status: 403 });
    return researchProxy(request, 'results', resultsSchema, ['zone', 'day']); }
