import { NextRequest, NextResponse } from 'next/server';
import { accountProxy } from '@/lib/server/account-proxy';
import { savedResponseSchema } from '@/lib/account';
export function DELETE(request: NextRequest, { params }: { params: { fixtureId: string } }) {
    if (!/^[1-9]\d*$/.test(params.fixtureId))
        return NextResponse.json({ error: 'Invalid fixture ID.' }, { status: 422, headers: { 'Cache-Control': 'no-store' } });
    return accountProxy(request, `account/saved/${params.fixtureId}`, savedResponseSchema);
}
