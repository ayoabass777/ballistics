import { auth } from '@clerk/nextjs/server';
import { NextRequest, NextResponse } from 'next/server';
import { preferencesSchema } from '@/lib/preferences';
const headers = { 'Cache-Control': 'no-store' };
async function proxy(request: NextRequest) {
    const fail = (status: number, error: string) => NextResponse.json({ error }, { status, headers });
    try {
        const session = await auth();
        if (!session.userId) return fail(401, 'Sign in to manage preferences.');
        if (request.method === 'POST' && request.headers.get('origin') !== request.nextUrl.origin)
            return fail(403, 'Please save preferences from this site.');
        const origin = process.env.ACCOUNT_API_BASE_URL || process.env.BACKEND_API_BASE_URL;
        if (!origin) return fail(503, 'Account settings are temporarily unavailable.');
        const url = new URL(origin);
        if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password || url.search || url.hash)
            return fail(503, 'Account settings are temporarily unavailable.');
        url.pathname = `${url.pathname.replace(/\/$/, '')}/preferences`;
        const token = await session.getToken();
        if (!token) return fail(401, 'Please sign in again.');
        let body: string | undefined;
        if (request.method === 'POST') {
            let value;
            try { value = await request.json(); } catch { return fail(422, 'Choose a valid timezone.'); }
            if (!value || typeof value !== 'object' || Array.isArray(value) || Object.keys(value).length !== 1 || !('timezone_override' in value) || (value.timezone_override !== null && typeof value.timezone_override !== 'string'))
                return fail(422, 'Choose a valid timezone.');
            body = JSON.stringify(value);
        }
        const response = await fetch(url, { method: request.method, body, redirect: 'error', cache: 'no-store', signal: AbortSignal.timeout(8000), headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' } });
        if (!response.ok) return fail(response.status === 401 ? 401 : response.status === 422 ? 422 : 503, response.status === 422 ? 'Choose a valid IANA timezone.' : 'Account settings are temporarily unavailable.');
        const parsed = preferencesSchema.safeParse(await response.json());
        if (!parsed.success) return fail(502, 'The settings response could not be verified.');
        return NextResponse.json(parsed.data, { headers });
    } catch { return fail(503, 'Account settings are temporarily unavailable.'); }
}
export const GET = proxy;
export const POST = proxy;
