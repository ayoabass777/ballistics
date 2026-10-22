import { NextRequest } from 'next/server';
import { accountProxy } from '@/lib/server/account-proxy';
import { savedPageSchema } from '@/lib/account';
export function GET(request: NextRequest) { return accountProxy(request, 'account/saved/page', savedPageSchema); }
