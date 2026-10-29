import { NextRequest } from 'next/server';

// CSRF guard for mutating routes: the browser's Origin must be this site.
// Behind Caddy, Next.js receives plain HTTP on :3000, so request.nextUrl.origin
// is not the public origin. Rebuild it from the Host header (Caddy passes it
// through and only serves the staging/beta hostnames) and X-Forwarded-Proto
// (https behind Cloudflare and Caddy); locally neither proxy header is set.
export function isSameOrigin(request: NextRequest): boolean {
    const origin = request.headers.get('origin');
    const host = request.headers.get('host');
    if (!origin || !host) return false;
    const proto = request.headers.get('x-forwarded-proto')?.split(',')[0].trim()
        || request.nextUrl.protocol.replace(/:$/, '');
    return origin === `${proto}://${host}`;
}
