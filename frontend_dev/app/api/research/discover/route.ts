import { NextRequest } from 'next/server';
import { researchProxy } from '@/lib/server/research-proxy';
import { discoverSchema } from '@/lib/research';
export const dynamic = 'force-dynamic';
export function GET(request: NextRequest) { return researchProxy(request, 'discover', discoverSchema, ['zone', 'scope', 'day', 'end_day', 'leagues']); }
