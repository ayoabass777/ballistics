import { currentUser } from '@clerk/nextjs/server';

export async function hasLiveMatchesPreview() {
    const user = await currentUser();
    return Boolean(user);
}
