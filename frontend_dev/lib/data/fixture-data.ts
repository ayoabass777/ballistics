import type { FixturePageViewModel } from "@/types/fixture-page"

/**
 * Data service interface for fixture page data
 * This abstraction allows easy swapping between mock and API implementations
 */
export interface FixtureDataService {
  getFixtureById(fixtureId: string): Promise<FixturePageViewModel | null>
}

/**
 * Fetches fixture data by ID
 * Currently uses mock data, will be replaced with API call when backend is ready
 */
export async function getFixtureById(fixtureId: string): Promise<FixturePageViewModel | null> {
  // Dynamic import to avoid bundling mock data in production when not needed
  const { MOCK_FIXTURES } = await import("./mock/fixture-mock-data")
  return MOCK_FIXTURES[fixtureId] || null
}

/**
 * Gets all available fixture IDs (for static generation)
 */
export async function getAllFixtureIds(): Promise<string[]> {
  const { MOCK_FIXTURES } = await import("./mock/fixture-mock-data")
  return Object.keys(MOCK_FIXTURES)
}
