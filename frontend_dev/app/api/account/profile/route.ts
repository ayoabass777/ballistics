import { NextRequest } from 'next/server';
import { z } from 'zod';
import { accountProxy } from '@/lib/server/account-proxy';

export const dynamic = 'force-dynamic';

const profileSchema = z.object({
  data: z.object({
    userId: z.string(),
    email: z.string().nullable(),
    firstName: z.string().nullable(),
    lastName: z.string().nullable(),
    imageUrl: z.string().nullable(),
    plan: z.string(),
    onboardingCompleted: z.boolean(),
  }),
  meta: z.object({
    generatedAtIso: z.string(),
    authProvider: z.string(),
    source: z.string(),
  }),
});

export function GET(request: NextRequest) {
  return accountProxy(request, 'me', profileSchema);
}
