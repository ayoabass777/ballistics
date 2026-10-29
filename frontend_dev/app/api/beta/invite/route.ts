import { NextRequest, NextResponse } from 'next/server';
import { isSameOrigin } from '@/lib/server/same-origin';

export const dynamic = 'force-dynamic';

export async function POST(request: NextRequest) {
  if (!isSameOrigin(request)) {
    return NextResponse.json({ error: 'Invalid request' }, { status: 403 });
  }
  const key = process.env.BETA_INVITE_API_KEY;
  const base = process.env.ACCOUNT_API_BASE_URL || process.env.BACKEND_API_BASE_URL;
  if (!key || !base) return NextResponse.json({ error: 'Invitations are unavailable' }, { status: 503 });
  let body: { code?: unknown; email?: unknown };
  try { body = await request.json(); } catch { return NextResponse.json({ error: 'Invalid request' }, { status: 400 }); }
  if (typeof body.code !== 'string' || typeof body.email !== 'string' ||
      body.code.length > 40 || body.email.length > 254) {
    return NextResponse.json({ error: 'Invalid request' }, { status: 400 });
  }
  try {
    const upstream = await fetch(`${base.replace(/\/$/, '')}/beta/invite/claim`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Beta-Invite-Api-Key': key },
      body: JSON.stringify({ code: body.code, email: body.email }),
      cache: 'no-store',
      redirect: 'error',
      signal: AbortSignal.timeout(15000),
    });
    if (upstream.ok) return NextResponse.json({ sent: true }, { headers: { 'Cache-Control': 'no-store' } });
    return NextResponse.json({ error: upstream.status === 400 ? 'Invalid or expired code for this email' : 'Could not send invitation. Please try later.' }, { status: upstream.status === 400 ? 400 : 503 });
  } catch {
    return NextResponse.json({ error: 'Could not send invitation. Please try later.' }, { status: 503 });
  }
}
