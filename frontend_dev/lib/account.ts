import { z } from 'zod';
import { cardSchema, savedSchema } from './research';

const id = z.number().int().positive().max(Number.MAX_SAFE_INTEGER);
export const savedResponseSchema = z.object({ data: savedSchema });
export const saveCardSchema = z.object({ card: cardSchema.omit({ preview: true }), run_ds: z.string().max(100).nullable() });
export const savedImportSchema = z.object({
    items: z.array(saveCardSchema.extend({ saved_at: z.string().datetime({ offset: true }) })).max(200),
});
export const followedResponseSchema = z.object({ data: z.object({ league_ids: z.array(id).max(200) }) });
export const followedUpdateSchema = z.object({ league_ids: z.array(id).max(200).refine(ids => new Set(ids).size === ids.length) }).strict();
export const onboardingResponseSchema = z.object({ data: z.object({ onboarding_completed: z.literal(true) }) });

export const leagueDirectorySchema = z.object({ data: z.object({ leagues: z.array(z.object({ league_id: id, league: z.string(), country: z.string().nullable(), league_logo_url: z.string().nullable() })) }) });

export const savedPageSchema = z.object({ data: z.object({ items: savedSchema, next_cursor: z.string().nullable() }) });
export const savedIdsSchema = z.object({ data: z.object({ fixture_ids: z.array(id) }) });
