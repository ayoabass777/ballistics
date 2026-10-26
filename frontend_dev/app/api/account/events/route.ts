import { NextRequest } from 'next/server';
import { accountProxy } from '@/lib/server/account-proxy';
import { usageEventBatchSchema, usageEventsResponseSchema } from '@/lib/usage-events';

export const dynamic = 'force-dynamic';

export function POST(request: NextRequest) {
  return accountProxy(request, 'account/events', usageEventsResponseSchema, usageEventBatchSchema);
}
