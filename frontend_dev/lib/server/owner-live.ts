import { currentUser } from '@clerk/nextjs/server';

export async function hasLiveMatchesPreview() {
    const user = await currentUser();
    return user?.publicMetadata.live_matches_preview === true;
}
