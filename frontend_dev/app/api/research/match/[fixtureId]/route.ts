import { NextRequest, NextResponse } from 'next/server';
import { researchProxy } from '@/lib/server/research-proxy';
import { matchSchema } from '@/lib/research';
export const dynamic = 'force-dynamic';
export async function GET(request: NextRequest, { params }: {
    params: Promise<{
        fixtureId: string;
    }>;
}) {
    const { fixtureId } = await params;
    if (!/^[1-9]\d{0,14}$/.test(fixtureId))
        return NextResponse.json({ error: 'Invalid fixture.' }, { status: 422 });
    return researchProxy(request, `match/${fixtureId}`, matchSchema);
}
