import { auth } from '@clerk/nextjs/server';

// Reads the signed-in user from the session token. currentUser() gave the same
// answer but called Clerk's Backend API on every request (~350 ms from staging),
// slowing /match, /matches and the live/results routes.
export async function hasLiveMatchesPreview() {
    const { userId } = await auth();
    return Boolean(userId);
}
