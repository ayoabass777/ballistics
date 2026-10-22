import { NextRequest } from 'next/server';
import { accountProxy } from '@/lib/server/account-proxy';
import { saveCardSchema, savedResponseSchema } from '@/lib/account';
export const dynamic = 'force-dynamic';
export function GET(request: NextRequest) { return accountProxy(request, 'account/saved', savedResponseSchema); }
export function POST(request: NextRequest) { return accountProxy(request, 'account/saved', savedResponseSchema, saveCardSchema); }
