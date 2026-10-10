"""Generate the reader-facing companion notebook from reviewed outputs."""

from pathlib import Path

import nbformat as nbf


HERE = Path(__file__).resolve().parent
OUT = HERE / "output"
overview = __import__("json").loads((OUT / "overview.json").read_text())

nb = nbf.v4.new_notebook()
nb["metadata"]["kernelspec"] = {
    "display_name": "Python 3",
    "language": "python",
    "name": "python3",
}
nb["metadata"]["language_info"] = {"name": "python", "version": "3"}

nb["cells"] = [
    nbf.v4.new_markdown_cell(
        f"""# Team attack and defence across Ballistics leagues

## tl;dr

- The reviewed source contains **{overview['finished_fixtures']:,} completed fixtures**, **{overview['leagues']} leagues**, and **{overview['league_seasons']} league-seasons** from **{overview['first_match']} through {overview['data_as_of']}**.
- Current strength is measured with the repository's time-decayed Dixon–Coles model, not Elo. It adjusts for opponent and home advantage, blends xG where available, and shrinks thin histories toward neutral.
- The specialist watchlist highlights teams whose attack or defence percentile materially exceeds their Elo percentile. This is a feature-discovery list, not proof of positive expected value on SportyBet.
- The transformed `raw.fact_head_to_head` model covers only **{overview['fact_coverage']:.1%}** of raw completed fixtures, so the analysis uses the fresher `raw.raw_fixtures` source.
"""
    ),
    nbf.v4.new_markdown_cell(
        """## Context & Methods

The decision is whether separate attacking and defensive strengths reveal useful predictive information that a single Elo rating compresses. All rankings are league-relative because goal environments differ materially by competition.

### Key Assumptions

- Only `FT` fixtures are outcomes; postponed, abandoned, awarded, extra-time, and penalty statuses are excluded.
- Historical leaderboards use empirical-Bayes goals per match within each league-season.
- Current ratings use time decay with the production Dixon–Coles half-life (about 107 days), opponent and home adjustment, a 50% xG blend when xG is present, and a six-effective-match neutral prior.
- A high specialist gap is a reason to inspect a fixture and market price. It is not by itself a bet.
"""
    ),
    nbf.v4.new_code_cell(
        """from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

ROOT = Path.cwd()
if not (ROOT / 'analysis/team_attack_defence/output').exists():
    ROOT = next(p for p in ROOT.parents if (p / 'analysis/team_attack_defence/output').exists())
OUT = ROOT / 'analysis/team_attack_defence/output'

overview = pd.read_csv(OUT / 'overview.csv')
quality = pd.read_csv(OUT / 'data_quality.csv')
leaders = pd.read_csv(OUT / 'league_leaders.csv')
current = pd.read_csv(OUT / 'current_ratings.csv')
watchlist = pd.read_csv(OUT / 'specialist_watchlist.csv')
historical = pd.read_csv(OUT / 'historical_leaders.csv')
persistent = pd.read_csv(OUT / 'persistent_leaders.csv')
validation = pd.read_csv(OUT / 'model_validation.csv')

overview.T"""
    ),
    nbf.v4.new_markdown_cell("## Data"),
    nbf.v4.new_code_cell(
        """quality.assign(rate=lambda d: d['rate'].map(lambda x: f'{x:.1%}' if pd.notna(x) else '—'))"""
    ),
    nbf.v4.new_markdown_cell(
        """The raw fixture source passes the result-completeness, uniqueness, and future-result checks. The material analytical caveat is warehouse transformation coverage, not raw match quality."""
    ),
    nbf.v4.new_markdown_cell("## Results"),
    nbf.v4.new_code_cell(
        """major_leagues = {
    'England — Premier League', 'Spain — La Liga', 'Germany — Bundesliga',
    'Italy — Serie A', 'France — Ligue 1', 'Netherlands — Eredivisie',
    'Portugal — Primeira Liga', 'Belgium — Jupiler Pro League',
    'Brazil — Serie A', 'USA — Major League Soccer',
}
leaders[leaders['league_label'].isin(major_leagues)][[
    'league_label', 'attack_leader', 'attack_index', 'attack_elo_rank',
    'defence_leader', 'defence_index', 'defence_elo_rank', 'as_of'
]].sort_values('league_label')"""
    ),
    nbf.v4.new_code_cell(
        """watchlist[[
    'league_label', 'team_name', 'specialty', 'attack_percentile',
    'defence_percentile', 'elo_percentile', 'specialist_gap',
    'effective_matches', 'confidence'
]].head(25)"""
    ),
    nbf.v4.new_code_cell(
        """fig, axes = plt.subplots(1, 2, figsize=(13, 5), sharex=True, sharey=True)
eligible = current[current['confidence'] != 'low']
axes[0].scatter(eligible['elo_percentile'], eligible['attack_percentile'], s=14, alpha=.45, color='#2f6fed')
axes[1].scatter(eligible['elo_percentile'], eligible['defence_percentile'], s=14, alpha=.45, color='#c07a17')
for ax, title, ylabel in [
    (axes[0], 'Attack strength versus Elo', 'Attack percentile'),
    (axes[1], 'Defence strength versus Elo', 'Defence percentile'),
]:
    ax.plot([0, 100], [0, 100], color='#444', linewidth=1, linestyle='--')
    ax.set(title=title, xlabel='Elo percentile', ylabel=ylabel, xlim=(0, 100), ylim=(0, 100))
    ax.grid(alpha=.18)
fig.suptitle('League-relative specialist strengths can diverge from overall Elo')
fig.tight_layout()
plt.show()"""
    ),
    nbf.v4.new_code_cell(
        """persistent[[
    'league_label', 'team_name', 'attack_leading_seasons',
    'defence_leading_seasons', 'total_leading_seasons'
]].head(20)"""
    ),
    nbf.v4.new_markdown_cell("### Existing walk-forward calibration evidence"),
    nbf.v4.new_code_cell(
        """validation.assign(
    average_predicted=lambda d: d['average_predicted'].map('{:.1%}'.format),
    actual_hit_rate=lambda d: d['actual_hit_rate'].map('{:.1%}'.format),
    calibration_gap=lambda d: d['calibration_gap'].map('{:+.1%}'.format),
)"""
    ),
    nbf.v4.new_markdown_cell(
        f"""## Takeaways

1. Use attack and defence as separate match-up features. Elo is still useful for overall team quality, but it can hide teams that are strong on one side of the ball and weak on the other.
2. Read the index multiplicatively: 120 means roughly 20% stronger than the current league reference on that unit after model adjustment; percentiles are better for quick within-league comparison.
3. Filter or down-weight `low` confidence teams. Their effective history is below six matches after time decay.
4. For SportyBet, convert the two teams' ratings into fixture probabilities, calibrate those probabilities out of sample, and compare them with the bookmaker's implied probability after removing margin. A ranking alone is not a wager.
5. Repair/rebuild the transformed fact before making it the model source. It currently represents only {overview['fact_coverage']:.1%} of raw completed fixtures.
"""
    ),
]

nbf.write(nb, HERE / "team_attack_defence_analysis.ipynb")
print(HERE / "team_attack_defence_analysis.ipynb")
