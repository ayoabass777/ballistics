import { NextRequest } from 'next/server';
import { accountProxy } from '@/lib/server/account-proxy';
import { usageEventsSettingResponseSchema, usageEventsSettingSchema } from '@/lib/usage-events';

export const dynamic = 'force-dynamic';

export function POST(request: NextRequest) {
  return accountProxy(request, 'preferences/usage-events', usageEventsSettingResponseSchema, usageEventsSettingSchema);
}
