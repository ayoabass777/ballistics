# Strength mock data — for Figma

> **2026-10-02 update (D44 prototype):** threshold > 250 with the league filter kept. See [strength_250_prototype_2026-10-02.json](strength_250_prototype_2026-10-02.json): 29 fixtures (13 confirmed, 16 needing the DB upset check, e.g. Crvena Zvezda, Dinamo Minsk, Bayern München, PSG). Discover shows the top 10 of 29.

Updated 2026-10-02. Rule agreed in **D43**: Discover Strength = engine `elo_gap_shortlist`.

## Source and rule

- Source: `engine/eval/consensus_slips_2026-10-02.json` (generated 2026-10-03 00:07 UTC). Full data with both-team context, DC goal forecasts and pattern counts: [strength_shortlist_2026-10-02.json](strength_shortlist_2026-10-02.json).
- Why not Sep 28 (the other mocks' source): its shortlist is empty. The largest gap that day was 287, under the 300 minimum. This is the first mock from a different export; label its date separately.
- Engine rule (`consensus_slips.py::_build_elo_gap_shortlist`): raw Elo gap ≥ 300 (no home advantage), league upset rate ≤ 27% over ≥ 50 matches, ≤ 7 per league, ≤ 40 total, sorted by gap, then lower upset rate.
- All entries are Big dog / Underdog. The numeric gap and upset rate stay hidden in the main UI (D27), and Methodology explains them.

## Shortlist (13 fixtures, kickoff UTC)

| # | Fixture (home v away) | League | Kickoff | Big dog |
| --- | --- | --- | --- | --- |
| 1 | Zlin v Slavia Praha | Czech Liga | 9 Oct 16:00 | Slavia Praha |
| 2 | Tukums v Riga | Virsliga (Latvia) | 10 Oct 11:00 | Riga |
| 3 | Rīgas FS v FS Jelgava | Virsliga (Latvia) | 11 Oct 13:00 | Rīgas FS |
| 4 | RAAL La Louvière v Club Brugge KV | Jupiler Pro League | 10 Oct 16:15 | Club Brugge KV |
| 5 | Union St. Gilloise v OH Leuven | Jupiler Pro League | 11 Oct 14:00 | Union St. Gilloise |
| 6 | Kairat Almaty v Zhetysu | Premier League (Kazakhstan) | 10 Oct 12:00 | Kairat Almaty |
| 7 | Benfica v Guimaraes | Primeira Liga | 11 Oct 17:00 | Benfica |
| 8 | Barcelona v Getafe | La Liga | 10 Oct 16:30 | Barcelona |
| 9 | Rangers v Kilmarnock | Premiership (Scotland) | 10 Oct 14:00 | Rangers |
| 10 | Inter v Parma | Serie A (Italy) | 10 Oct 16:00 | Inter |
| 11 | Veres Rivne v Shakhtar Donetsk | Premier League (Ukraine) | 10 Oct 15:00 | Shakhtar Donetsk |
| 12 | Maritimo v FC Porto | Primeira Liga | 10 Oct 17:00 | FC Porto |
| 13 | Galatasaray v Kasımpaşa | Süper Lig | 9 Oct 17:00 | Galatasaray |

Discover shows the top entries; the card order follows the shortlist rank.

## Known gaps and consequences

- **Dates and leagues are outside the Matches demo** (28 Sep–8 Oct; Serie B and the two USL leagues). Discover Strength cards have no existing detail screens. They link to new detail screens only where those are built; otherwise their card links are disabled and labelled preview-only.
- **Away favourites** (Slavia Praha, Riga, Club Brugge, Shakhtar, Porto): the gap ignores home advantage, so an away "Big dog" is facing a real home-pitch effect the label doesn't include. Methodology should say so.
- **Match-detail labels still use D27.** Real values from the Sep 28 export: Naples v Richmond gap 115 → Naples · Slight favourite (the old illustrative "Big dog" was wrong). Brooklyn v Detroit gap 159 → Detroit City · Slight favourite. A fixture with no gap record shows "unavailable", never Closely matched.
- The Sep 28 classification dataset is kept as [strength_mock_2026-09-28.json](strength_mock_2026-09-28.json) for detail-screen labels.
