import { NextRequest } from 'next/server';
import { accountProxy } from '@/lib/server/account-proxy';
import { savedIdsSchema } from '@/lib/account';
export function GET(request: NextRequest) { return accountProxy(request, 'account/saved/ids', savedIdsSchema); }
