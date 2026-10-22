import { auth } from '@clerk/nextjs/server';
import { NextRequest, NextResponse } from 'next/server';
import { z } from 'zod';

const headers = { 'Cache-Control': 'no-store' };
const fail = (status: number, error: string) => NextResponse.json({ error }, { status, headers });

export async function accountProxy(
    request: NextRequest,
    path: string,
    output: z.ZodTypeAny,
    input?: z.ZodTypeAny,
) {
    try {
        const session = await auth();
        if (!session.userId) return fail(401, 'Sign in to manage your account.');
        if (request.method !== 'GET' && request.headers.get('origin') !== request.nextUrl.origin)
            return fail(403, 'Please make account changes from this site.');
        const origin = process.env.ACCOUNT_API_BASE_URL || process.env.BACKEND_API_BASE_URL;
        if (!origin) return fail(503, 'Account data is temporarily unavailable.');
        const url = new URL(origin);
        if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password || url.search || url.hash)
            return fail(503, 'Account data is temporarily unavailable.');
        url.pathname = `${url.pathname.replace(/\/$/, '')}/${path}`;
        if (path === 'account/saved/page') {
            for (const name of ['scope', 'cursor', 'limit']) {
                const value = request.nextUrl.searchParams.get(name);
                if (value !== null) url.searchParams.set(name, value);
            }
        }
        const token = await session.getToken();
        if (!token) return fail(401, 'Please sign in again.');
        let body: string | undefined;
        if (input) {
            let raw: unknown;
            try { raw = await request.json(); } catch { return fail(422, 'Check the account data and try again.'); }
            const parsed = input.safeParse(raw);
            if (!parsed.success) return fail(422, 'Check the account data and try again.');
            body = JSON.stringify(parsed.data);
            if (body.length > 200_000) return fail(413, 'Too many saved fixtures to import at once.');
        }
        const response = await fetch(url, {
            method: request.method, body, redirect: 'error', cache: 'no-store',
            signal: AbortSignal.timeout(8000),
            headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json', Accept: 'application/json' },
        });
        if (!response.ok)
            return fail(response.status === 401 ? 401 : response.status === 422 ? 422 : 503,
                response.status === 422 ? 'Check the account data and try again.' : 'Account data is temporarily unavailable.');
        const parsed = output.safeParse(await response.json());
        if (!parsed.success) return fail(502, 'The account response could not be verified.');
        return NextResponse.json(parsed.data, { headers });
    } catch {
        return fail(503, 'Account data is temporarily unavailable.');
    }
}
