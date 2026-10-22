export function FixturePageSkeleton() {
  return (
    <div className="min-h-screen bg-gradient-to-b from-background via-background to-muted/20 animate-pulse">
      {/* Matchup Hero Skeleton */}
      <div className="max-w-7xl mx-auto px-4 sm:px-6 py-6 sm:py-8">
        <div className="relative glass-panel-elevated overflow-hidden p-4 sm:p-6 lg:p-8">
          {/* League badge skeleton */}
          <div className="flex items-center justify-center gap-2 mb-8">
            <div className="h-10 w-64 bg-muted/40 rounded-full" />
          </div>

          {/* Teams skeleton */}
          <div className="grid grid-cols-[1fr_auto_1fr] gap-6 sm:gap-10 lg:gap-16 mb-6 sm:mb-8">
            {/* Home team */}
            <div className="flex flex-col items-center gap-4">
              <div className="w-8 h-8 rounded-full bg-muted/40" />
              <div className="w-16 h-16 sm:w-20 sm:h-20 lg:w-24 lg:h-24 rounded-3xl bg-muted/40" />
              <div className="h-6 w-32 bg-muted/40 rounded-lg" />
            </div>

            {/* VS Badge */}
            <div className="flex flex-col items-center justify-center gap-2 sm:gap-3">
              <div className="h-8" />
              <div className="w-14 h-14 sm:w-20 sm:h-20 lg:w-24 lg:h-24 rounded-2xl bg-gradient-to-br from-blue-500/20 to-cyan-500/20" />
              <div className="h-10 w-24 bg-muted/40 rounded-lg" />
            </div>

            {/* Away team */}
            <div className="flex flex-col items-center gap-4">
              <div className="w-8 h-8 rounded-full bg-muted/40" />
              <div className="w-16 h-16 sm:w-20 sm:h-20 lg:w-24 lg:h-24 rounded-3xl bg-muted/40" />
              <div className="h-6 w-32 bg-muted/40 rounded-lg" />
            </div>
          </div>

          {/* Venue skeleton */}
          <div className="flex flex-wrap items-center justify-center gap-3 pt-6 border-t border-border/40">
            <div className="h-8 w-48 bg-muted/40 rounded-full" />
          </div>
        </div>
      </div>

      {/* Team Streaks Skeleton */}
      <div className="max-w-7xl mx-auto px-4 sm:px-6 py-6 sm:py-8">
        <div className="grid md:grid-cols-2 gap-6 lg:gap-8">
          {/* Home Team Streaks */}
          <div className="glass-panel-elevated p-5 sm:p-6">
            <div className="flex flex-col items-center text-center gap-3 mb-5">
              <div className="w-10 h-10 rounded-xl bg-muted/40" />
              <div className="h-7 w-48 bg-muted/40 rounded-lg" />
              <div className="h-4 w-64 bg-muted/40 rounded" />
            </div>
            <div className="space-y-3">
              {[1, 2, 3, 4].map((i) => (
                <div key={i} className="h-20 bg-muted/20 rounded-xl" />
              ))}
            </div>
          </div>

          {/* Away Team Streaks */}
          <div className="glass-panel-elevated p-5 sm:p-6">
            <div className="flex flex-col items-center text-center gap-3 mb-5">
              <div className="w-10 h-10 rounded-xl bg-muted/40" />
              <div className="h-7 w-48 bg-muted/40 rounded-lg" />
              <div className="h-4 w-64 bg-muted/40 rounded" />
            </div>
            <div className="space-y-3">
              {[1, 2, 3, 4].map((i) => (
                <div key={i} className="h-20 bg-muted/20 rounded-xl" />
              ))}
            </div>
          </div>
        </div>
      </div>

      {/* H2H Section Skeleton */}
      <div className="max-w-7xl mx-auto px-4 sm:px-6 py-6 sm:py-8">
        {/* H2H Overview Skeleton */}
        <div className="mb-8">
          <div className="flex flex-col items-center text-center gap-3 mb-5">
            <div className="w-10 h-10 rounded-xl bg-muted/40" />
            <div className="h-8 w-56 bg-muted/40 rounded-lg" />
            <div className="h-4 w-72 bg-muted/40 rounded" />
          </div>

          <div className="glass-panel-elevated p-5 sm:p-6">
            <div className="flex items-stretch gap-2 sm:gap-3">
              {[1, 2, 3].map((i) => (
                <div key={i} className="flex-1 text-center p-4 sm:p-6 rounded-2xl bg-muted/10 border border-border/30">
                  <div className="flex justify-center mb-4">
                    <div className="w-12 h-12 sm:w-14 sm:h-14 lg:w-16 lg:h-16 rounded-2xl bg-muted/40" />
                  </div>
                  <div className="h-12 w-16 mx-auto mb-2 bg-muted/40 rounded-lg" />
                  <div className="h-4 w-12 mx-auto bg-muted/40 rounded" />
                </div>
              ))}
            </div>
            <div className="text-center mt-5 pt-5 border-t border-border/30">
              <div className="h-4 w-48 mx-auto bg-muted/40 rounded" />
            </div>
          </div>
        </div>

        {/* H2H Streaks Skeleton */}
        <div className="mb-8">
          <div className="flex flex-col items-center text-center gap-3 mb-5">
            <div className="w-10 h-10 rounded-xl bg-muted/40" />
            <div className="h-7 w-64 bg-muted/40 rounded-lg" />
          </div>

          <div className="glass-panel-elevated p-5 sm:p-6">
            <div className="space-y-3">
              {[1, 2, 3, 4].map((i) => (
                <div key={i} className="h-20 bg-muted/20 rounded-xl" />
              ))}
            </div>
          </div>
        </div>

        {/* Recent Meetings Skeleton */}
        <div>
          <div className="flex flex-col items-center text-center gap-3 mb-5">
            <div className="w-10 h-10 rounded-xl bg-muted/40" />
            <div className="h-7 w-56 bg-muted/40 rounded-lg" />
          </div>

          <div className="glass-panel-elevated p-5 sm:p-6">
            <div className="space-y-3">
              {[1, 2, 3, 4, 5].map((i) => (
                <div key={i} className="h-24 bg-muted/20 rounded-xl" />
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
