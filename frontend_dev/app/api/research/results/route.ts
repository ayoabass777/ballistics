import { NextRequest } from 'next/server';
import { researchProxy } from '@/lib/server/research-proxy';
import { resultsSchema } from '@/lib/results';
export const dynamic = 'force-dynamic';
export function GET(request: NextRequest) { return researchProxy(request, 'results', resultsSchema, ['zone', 'day']); }
