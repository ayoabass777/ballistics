import { NextRequest } from 'next/server';
import { accountProxy } from '@/lib/server/account-proxy';
import { leagueDirectorySchema } from '@/lib/account';
export const dynamic = 'force-dynamic';
export function GET(request: NextRequest) { return accountProxy(request, 'account/leagues/directory', leagueDirectorySchema); }
