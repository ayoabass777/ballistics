/**
 * Mock fixture data - backwards compatibility re-export
 *
 * This file re-exports from the new data layer location.
 * New code should import directly from @/lib/data/mock/fixture-mock-data
 */

import { DEFAULT_FIXTURE, MOCK_FIXTURES } from "@/lib/data/mock/fixture-mock-data"
import type { FormMatch } from "@/types/fixture-page"

// Re-export the default fixture for backwards compatibility
export const mockFixture = DEFAULT_FIXTURE

// Re-export form data for backwards compatibility
export const mockHomeTeamForm: FormMatch[] = DEFAULT_FIXTURE.homeTeamForm

export const mockAwayTeamForm: FormMatch[] = DEFAULT_FIXTURE.awayTeamForm

// Re-export fixtures map for direct access
export { MOCK_FIXTURES }
