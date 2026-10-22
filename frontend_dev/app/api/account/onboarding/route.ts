import { NextRequest } from 'next/server';
import { accountProxy } from '@/lib/server/account-proxy';
import { followedUpdateSchema, onboardingResponseSchema } from '@/lib/account';

export const dynamic = 'force-dynamic';

export function POST(request: NextRequest) {
  return accountProxy(request, 'account/onboarding/complete', onboardingResponseSchema, followedUpdateSchema);
}
