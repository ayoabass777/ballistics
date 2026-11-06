import { NextRequest } from 'next/server';
import { researchProxy } from '@/lib/server/research-proxy';
import { liveListSchema } from '@/lib/live';
export const dynamic = 'force-dynamic';
export async function GET(request: NextRequest) {
    return researchProxy(request, 'live', liveListSchema);
}
