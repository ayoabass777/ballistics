import React from "react";

import {
  ChartRenderer, compact, DataComponent, DataTable, MetricCard, ReportSection,
  RichNarrative, SortableItem, SortableRegion, useDataApp,
} from "../../data-app-public.jsx";

const specialistScatter = {
  type: "scatter", x: "elo_percentile", y: "specialist_percentile", series: "specialty",
  xLabel: "Elo percentile within league", yLabel: "Strongest attack or defence percentile",
  colors: { attack: "var(--chart-1)", defence: "var(--chart-2)" }, valueDecimals: 1,
};

const persistenceChart = {
  type: "horizontalBar", x: "team_label", y: "total_leading_seasons",
  xLabel: "League-seasons led in attack or defence", sortOrder: "descending",
  showValues: true, colors: { total_leading_seasons: "var(--chart-1)" },
};

const sectionOrder = [
  "current-leaders", "elo-specialists", "historical-patterns",
  "validation-and-use", "data-quality-and-methods",
];

const oneDecimal = (value) => value == null ? "—" : Number(value).toFixed(1);
const percentile = (value) => value == null ? "—" : `${Number(value).toFixed(1)} pctile`;
const rank = (value) => value == null ? "—" : `#${Number(value).toFixed(0)}`;
const signedPoints = (value) => value == null ? "—" : `${Number(value) >= 0 ? "+" : ""}${Number(value).toFixed(1)} pts`;
const percent = (value) => value == null ? "—" : `${(Number(value) * 100).toFixed(1)}%`;
const naturalList = (items) => items.length < 2
  ? (items[0] ?? "")
  : `${items.slice(0, -1).join(", ")}${items.length > 2 ? "," : ""} and ${items.at(-1)}`;

export function ReportContent() {
  const { snapshot, chartOverrides, chartProps, visible,
    canEdit, mode, appTitle, setAppTitle } = useDataApp();
  const rows = (queryId) => snapshot.queries?.[queryId]?.rows ?? [];
  const overviewRows = rows("overview");
  const overview = overviewRows[0] ?? {};
  const leaders = rows("league_leaders");
  const current = rows("current_ratings");
  const watchlist = rows("specialist_watchlist");
  const historical = rows("historical_leaders");
  const persistent = rows("persistent_leaders");
  const validation = rows("model_validation");
  const quality = rows("data_quality");
  const reliableCurrentSource = current
    .filter((row) => row.confidence !== "low" && row.elo_percentile != null);
  const reliableCurrent = reliableCurrentSource
    .map((row) => ({ ...row,
      specialist_percentile: Math.max(row.attack_percentile, row.defence_percentile) }));
  const topPersistentSource = persistent.slice(0, 15);
  const topPersistent = topPersistentSource.map((row) => ({ ...row,
      team_label: `${row.team_name} · ${row.league_label}` }));
  const dixonValidation = validation.find((row) => row.model === "dixon_coles") ?? {};
  const specialistShare = overview.current_rated_teams
    ? overview.watchlist_teams / overview.current_rated_teams : 0;
  const specialistExamples = naturalList(watchlist
    .filter((row) => row.confidence === "high")
    .slice(0, 4)
    .map((row) => {
      const unitPercentile = row.specialty === "attack"
        ? row.attack_percentile : row.defence_percentile;
      return `**${row.team_name} — ${row.specialty} at ${percentile(unitPercentile)} versus Elo at ${percentile(row.elo_percentile)}**`;
    }));
  const compressedLeader = leaders
    .flatMap((row) => [
      { league: row.league_label, unit: "attack", team: row.attack_leader,
        index: row.attack_index, eloRank: row.attack_elo_rank },
      { league: row.league_label, unit: "defence", team: row.defence_leader,
        index: row.defence_index, eloRank: row.defence_elo_rank },
    ])
    .filter((row) => row.eloRank != null)
    .reduce((lowestElo, row) => !lowestElo || row.eloRank > lowestElo.eloRank ? row : lowestElo, null);
  const persistentExamples = naturalList(persistent.slice(0, 5)
    .map((row) => `**${row.team_name} (${row.total_leading_seasons})**`));
  const factFreshnessLag = quality.find((row) =>
    row.check === "Transformed fact freshness lag (days)")?.value;
  const factFreshnessText = Number(factFreshnessLag) === 0
    ? "and is current through the same result day"
    : `and is ${factFreshnessLag} result-day${Number(factFreshnessLag) === 1 ? "" : "s"} behind`;
  const scatter = chartOverrides["current-strength-scatter"] ?? specialistScatter;
  const persistence = chartOverrides["historical-persistence-chart"] ?? persistenceChart;

  return <article className="report-content" aria-label="Team attack and defence analytical report">
    <header className="report-hero">
      <h1 data-data-app-title contentEditable={canEdit && mode === "edit"} suppressContentEditableWarning
        aria-label={canEdit && mode === "edit" ? "Edit report heading" : undefined}
        onBlur={canEdit && mode === "edit" ? (event) => setAppTitle(event.currentTarget.textContent.trim() || appTitle) : undefined}
        onKeyDown={canEdit && mode === "edit" ? (event) => {
          if (event.key === "Enter") { event.preventDefault(); event.currentTarget.blur(); }
        } : undefined}>{appTitle}</h1>
      <RichNarrative id="report:description" className="report-deck" label="Edit report introduction"
        value={`A league-relative view of ${compact(overview.current_rated_teams)} current teams and ${compact(overview.finished_fixtures)} completed matches. The aim is to expose attacking and defensive specialists that one overall Elo number can hide—and to show how those signals should enter a SportyBet prediction workflow.`} />
    </header>

    {visible("report-summary") && <ReportSection id="report-summary" title="The strongest unit and overall Elo often tell different stories"
      queryId="overview" queryIds={["overview", "specialist_watchlist"]}
      sourceRowsByQuery={{ overview: overviewRows, specialist_watchlist: watchlist }} showHeading={false} className="report-summary">
      <RichNarrative id="report-summary:body" className="report-summary-lead" label="Edit executive summary"
        value={`## The strongest unit and overall Elo often tell different stories

**${overview.watchlist_teams} teams (${percent(specialistShare)})** have an attack or defence rating in their league’s top quartile and at least a 15-percentile-point advantage over their Elo standing, after removing low-confidence histories. Current high-confidence examples include ${specialistExamples}.

That is useful feature information, not automatic betting value. Elo measures overall result strength; a specialist gap says which side of a matchup deserves separate attention. The price comparison still has to come from calibrated fixture probabilities versus margin-adjusted SportyBet odds.`} />
    </ReportSection>}

    <div className="report-facts" aria-label="Coverage and signal summary">
      {visible("coverage-kpis") && <>
        <MetricCard id="coverage-kpis:fixtures" title="Completed fixtures" queryId="overview"
          sourceRows={overviewRows} value={compact(overview.finished_fixtures)}
          description={`${overview.first_match} through ${overview.data_as_of}; final-time results only.`} />
        <MetricCard id="coverage-kpis:leagues" title="Active rated leagues" queryId="overview"
          sourceRows={overviewRows} value={compact(overview.current_rated_leagues)}
          description={`${overview.leagues} leagues in history; stale latest seasons stay historical only.`} />
        <MetricCard id="coverage-kpis:teams" title="Current rated teams" queryId="overview"
          sourceRows={overviewRows} value={compact(overview.current_rated_teams)}
          description="Opponent-adjusted attack and defence ratings, with small-sample shrinkage." />
        <MetricCard id="coverage-kpis:specialists" title="Specialist candidates" queryId="overview"
          sourceRows={overviewRows} value={compact(overview.watchlist_teams)}
          description={`Top-quartile unit and ≥15-point gap versus league-relative Elo (${percent(specialistShare)} of rated teams).`} />
      </>}
    </div>

    <SortableRegion id="report:sections" label="Report sections" variant="stack" authoredOrder={sectionOrder}
      className="report-sortable-sections">
      {visible("current-leaders") && <SortableItem id="current-leaders" label="Current league leaders" kind="table">
        <section className="report-section">
          <ReportSection id="current-leaders:narrative" title="Current attack and defence leaders by league"
            queryId="league_leaders" sourceRows={leaders} showHeading={false}>
            <RichNarrative id="current-leaders:body" className="report-analysis" label="Edit league leader interpretation"
              value={`## Current attack and defence leaders by league

The index is centred at **100 inside each league**. A value of 120 means the fitted unit is roughly 20% stronger than that league’s current reference after opponent, venue, recency, xG availability, and the six-effective-match prior are accounted for. The Elo-rank columns make the compression visible: ${compressedLeader ? `**${compressedLeader.team} leads ${compressedLeader.league} ${compressedLeader.unit} at ${oneDecimal(compressedLeader.index)} despite ranking ${rank(compressedLeader.eloRank)} by Elo**.` : "the widest current unit-versus-Elo mismatch is unavailable."}` } />
          </ReportSection>
          <DataComponent id="league-leaders-table" title="All current league leaders" queryId="league_leaders"
            kind="table" displayRows={leaders} sourceRows={leaders}
            description="Latest active season per league. Index above 100 is stronger than the current league reference.">
            <DataTable rows={leaders} caption="Current opponent-adjusted attack and defence leaders by league"
              compactNumbers={false} columns={[
                { field: "league_label", label: "League", presentation: "identity" },
                { field: "attack_leader", label: "Best attack" },
                { field: "attack_index", label: "Attack index", renderCell: oneDecimal },
                { field: "attack_elo_rank", label: "Attack leader Elo", renderCell: rank },
                { field: "defence_leader", label: "Best defence" },
                { field: "defence_index", label: "Defence index", renderCell: oneDecimal },
                { field: "defence_elo_rank", label: "Defence leader Elo", renderCell: rank },
                { field: "as_of", label: "As of" },
              ]} />
          </DataComponent>
        </section>
      </SortableItem>}

      {visible("elo-specialists") && <SortableItem id="elo-specialists" label="Specialists hidden by Elo" kind="chart">
        <section className="report-section">
          <ReportSection id="elo-specialists:narrative" title="Use Elo and unit strength together"
            queryId="specialist_watchlist" queryIds={["specialist_watchlist", "current_ratings"]}
            sourceRowsByQuery={{ specialist_watchlist: watchlist, current_ratings: reliableCurrentSource }} showHeading={false}>
            <RichNarrative id="elo-specialists:body" className="report-analysis" label="Edit specialist interpretation"
              value={`## Use Elo and unit strength together

Points far above their Elo percentile have one unit performing much better than the overall team rating suggests. This pattern is especially useful for **team totals, BTTS, match totals, and handicap reasoning**: a strong attack paired with a weak defence is a different betting object from a balanced high-Elo team.`} />
          </ReportSection>
          <DataComponent id="current-strength-scatter" title="Strongest unit percentile versus Elo percentile"
            queryId="current_ratings" kind="chart" chart={scatter}
            displayRows={reliableCurrent} sourceRows={reliableCurrentSource}
            description="Medium/high-confidence current teams. Each point uses the stronger of its attack and defence percentiles; colour identifies the unit.">
            <ChartRenderer spec={scatter} rows={reliableCurrent} height={430}
              {...chartProps("current-strength-scatter")} />
          </DataComponent>
          <DataComponent id="specialist-watchlist" title="Largest specialist gaps versus Elo" queryId="specialist_watchlist"
            kind="table" displayRows={watchlist} sourceRows={watchlist}
            description="Top 60 medium/high-confidence gaps. This screen is a scouting list, not a bet recommendation.">
            <DataTable rows={watchlist} caption="Attack or defence specialists whose unit percentile most exceeds Elo percentile"
              compactNumbers={false} columns={[
                { field: "team_name", label: "Team", presentation: "identity", secondaryField: "league_label" },
                { field: "specialty", label: "Unit", presentation: "status" },
                { field: "attack_percentile", label: "Attack", renderCell: percentile },
                { field: "defence_percentile", label: "Defence", renderCell: percentile },
                { field: "elo_percentile", label: "Elo", renderCell: percentile },
                { field: "specialist_gap", label: "Gap", renderCell: signedPoints },
                { field: "effective_matches", label: "Effective matches", renderCell: oneDecimal },
                { field: "confidence", label: "Confidence", presentation: "status" },
              ]} />
          </DataComponent>
          <DataComponent id="current-ratings-table" title="Search all current team ratings" queryId="current_ratings"
            kind="table" displayRows={current} sourceRows={current}
            description="All active-league teams, including low-confidence rows so their uncertainty remains visible.">
            <DataTable rows={current} caption="All current attack, defence, and Elo rankings"
              compactNumbers={false} columns={[
                { field: "team_name", label: "Team", presentation: "identity", secondaryField: "league_label" },
                { field: "attack_index", label: "Attack index", renderCell: oneDecimal },
                { field: "attack_rank", label: "Attack rank", renderCell: rank },
                { field: "defence_index", label: "Defence index", renderCell: oneDecimal },
                { field: "defence_rank", label: "Defence rank", renderCell: rank },
                { field: "elo_rank", label: "Elo rank", renderCell: rank },
                { field: "current_games", label: "Season matches" },
                { field: "confidence", label: "Confidence", presentation: "status" },
              ]} />
          </DataComponent>
        </section>
      </SortableItem>}

      {visible("historical-patterns") && <SortableItem id="historical-patterns" label="Patterns across seasons" kind="chart">
        <section className="report-section">
          <ReportSection id="historical-patterns:narrative" title="Persistent leaders are the stable end of the signal"
            queryId="persistent_leaders" queryIds={["persistent_leaders", "historical_leaders"]}
            sourceRowsByQuery={{ persistent_leaders: persistent, historical_leaders: historical }} showHeading={false}>
            <RichNarrative id="historical-patterns:body" className="report-analysis" label="Edit historical interpretation"
              value={`## Persistent leaders are the stable end of the signal

Across ${overview.league_seasons} league-seasons, the most persistent attack/defence leaders are ${persistentExamples}. This historical layer is descriptive and shrunk within season; it is valuable as a prior, while the recency-weighted current rating should drive fixture-level forecasts.`} />
          </ReportSection>
          <DataComponent id="historical-persistence-chart" title="Most persistent attack and defence leaders, 2020–2026"
            queryId="persistent_leaders" kind="chart" chart={persistence}
            displayRows={topPersistent} sourceRows={topPersistentSource}
            description="Count of league-seasons in which each team finished first in shrunk scoring rate or first in lowest shrunk concession rate.">
            <ChartRenderer spec={persistence} rows={topPersistent} height={520}
              {...chartProps("historical-persistence-chart")} />
          </DataComponent>
          <DataComponent id="historical-leaders-table" title="Every historical league-season leader" queryId="historical_leaders"
            kind="table" displayRows={historical} sourceRows={historical}
            description="Empirical-Bayes goals rates, ranked only within the same league-season.">
            <DataTable rows={historical} caption="Historical attack and defence leaders by league-season"
              compactNumbers={false} columns={[
                { field: "league_label", label: "League", presentation: "identity" },
                { field: "season", label: "Season" },
                { field: "attack_leader", label: "Attack leader" },
                { field: "attack_goals_per_game_eb", label: "Attack goals/match", renderCell: (v) => Number(v).toFixed(3) },
                { field: "defence_leader", label: "Defence leader" },
                { field: "defence_goals_allowed_per_game_eb", label: "Goals allowed/match", renderCell: (v) => Number(v).toFixed(3) },
              ]} />
          </DataComponent>
        </section>
      </SortableItem>}

      {visible("validation-and-use") && <SortableItem id="validation-and-use" label="How to use this for SportyBet" kind="narrative">
        <section className="report-section">
          <ReportSection id="validation-and-use:narrative" title="Turn ratings into prices before turning them into bets"
            queryId="model_validation" sourceRows={validation} showHeading={false}>
            <RichNarrative id="validation-and-use:body" className="report-analysis" label="Edit betting workflow guidance"
              value={`## Turn ratings into prices before turning them into bets

The existing leak-free walk-forward evaluation gives Dixon–Coles **${compact(dixonValidation.predictions)} predictions**, a **${Number(dixonValidation.brier_score ?? 0).toFixed(4)} Brier score**, and only **${percent(dixonValidation.calibration_gap)} overconfidence** after calibration. That supports using the model family for probabilities, but it does **not** demonstrate profit against SportyBet prices.

For each fixture: combine the home attack with the away defence, the away attack with the home defence, and home advantage; generate the scoreline distribution; map it to the exact SportyBet market; calibrate on earlier matches only; remove the bookmaker margin; then bet only when model probability exceeds the no-vig implied probability by a pre-tested edge. Keep stake sizing separate and conservative.`} />
          </ReportSection>
          <DataComponent id="model-validation" title="Existing walk-forward probability validation" queryId="model_validation"
            kind="table" displayRows={validation} sourceRows={validation}
            description="Calibration report already stored in the repository. Metrics pool the markets evaluated by that workflow.">
            <DataTable rows={validation} caption="Walk-forward model calibration metrics" searchable={false}
              compactNumbers={false} columns={[
                { field: "model", label: "Model", presentation: "identity" },
                { field: "predictions", label: "Predictions" },
                { field: "brier_score", label: "Brier", renderCell: (v) => Number(v).toFixed(4) },
                { field: "average_predicted", label: "Average predicted", renderCell: percent },
                { field: "actual_hit_rate", label: "Actual hit rate", renderCell: percent },
                { field: "calibration_gap", label: "Overconfidence", renderCell: percent },
              ]} />
          </DataComponent>
        </section>
      </SortableItem>}

      {visible("data-quality-and-methods") && <SortableItem id="data-quality-and-methods" label="Data quality and method" kind="table">
        <section className="report-section">
          <ReportSection id="data-quality-note" title="Use the raw fixture source until the dbt fact is rebuilt"
            queryId="data_quality" queryIds={["data_quality", "overview"]}
            sourceRowsByQuery={{ data_quality: quality, overview: overviewRows }} showHeading={false}>
            <RichNarrative id="data-quality-and-methods:body" className="report-caveat" label="Edit data-quality caveat"
              value={`## Use the raw fixture source until the dbt fact is rebuilt

The raw source has complete scores, no duplicate API fixture IDs, and no future-dated finished fixtures. The transformed team-match fact contains only **${overview.fact_fixtures.toLocaleString()} of ${overview.raw_ft_fixtures.toLocaleString()} raw FT fixtures (${percent(overview.fact_coverage)})** ${factFreshnessText}. Any production strength feature sourced from that fact would be materially biased toward the subset it contains.

**Method boundary:** historical rankings describe realised goals with empirical-Bayes shrinkage. Current indices are model estimates: Dixon–Coles opponent/home adjustment, ~107-day half-life, a 50% xG blend on ${compact(overview.xg_fixtures)} available fixtures, and a six-effective-match neutral prior. **Low confidence** means fewer than six effective matches after time decay.`} />
          </ReportSection>
          <DataComponent id="data-quality-table" title="Source checks" queryId="data_quality"
            kind="table" displayRows={quality} sourceRows={quality}
            description="Checks run against raw completed fixtures and the transformed played-fixture fact.">
            <DataTable rows={quality} caption="Data-quality checks and analytical impact" searchable={false}
              compactNumbers={false} columns={[
                { field: "check", label: "Check", presentation: "identity" },
                { field: "value", label: "Value" },
                { field: "rate", label: "Rate", renderCell: percent },
                { field: "severity", label: "Status", presentation: "status" },
                { field: "interpretation", label: "Interpretation" },
              ]} />
          </DataComponent>
        </section>
      </SortableItem>}
    </SortableRegion>
  </article>;
}
