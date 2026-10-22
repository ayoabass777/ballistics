import { NextRequest } from 'next/server';
import { accountProxy } from '@/lib/server/account-proxy';
import { followedResponseSchema, followedUpdateSchema } from '@/lib/account';
export const dynamic = 'force-dynamic';
export function GET(request: NextRequest) { return accountProxy(request, 'account/leagues', followedResponseSchema); }
export function PUT(request: NextRequest) { return accountProxy(request, 'account/leagues', followedResponseSchema, followedUpdateSchema); }
