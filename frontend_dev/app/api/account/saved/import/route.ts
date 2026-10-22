import { NextRequest } from 'next/server';
import { accountProxy } from '@/lib/server/account-proxy';
import { savedImportSchema, savedResponseSchema } from '@/lib/account';
export function POST(request: NextRequest) { return accountProxy(request, 'account/saved/import', savedResponseSchema, savedImportSchema); }
