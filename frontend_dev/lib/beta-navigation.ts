export function betaHome(metadata?: Record<string, unknown> | null) {
    return metadata?.matches_first_beta === true ? '/matches' : '/discover';
}
