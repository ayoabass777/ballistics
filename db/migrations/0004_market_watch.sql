-- Market watch (D120): calibration of watched markets after each weekly
-- refit. Written by engine/eval/diagnostics/market_watch.py (the
-- engine_calibration DAG's market_watch task), read by the daily email's Model
-- watch (etl/src/data_detector/model_watch.py). Replaces
-- engine/eval/watch/<market>.csv, which lived in the checkout: deploys reset
-- it and the nightly backup didn't cover it.
--
-- id keeps the logging order: the first row per (market, source) with at
-- least 100 picks is the drift baseline, and the reader compares the latest
-- refit with the one before.
CREATE TABLE IF NOT EXISTS raw.market_watch (
  id                BIGSERIAL   PRIMARY KEY,
  logged_at         TIMESTAMPTZ NOT NULL,
  report_stamp      TEXT        NOT NULL,   -- which calibration_report.json
  market            TEXT        NOT NULL,
  source            TEXT        NOT NULL,   -- all, dixon_coles_only
  n                 INT         NOT NULL,
  avg_predicted     NUMERIC     NOT NULL,
  hit_rate          NUMERIC     NOT NULL,
  overconfidence    NUMERIC     NOT NULL,
  brier             NUMERIC     NOT NULL,
  flag              TEXT        NOT NULL DEFAULT '',
  UNIQUE (market, source, report_stamp)
);

-- History from the committed CSV (Oct 12-26), so the D120 baseline is the
-- same everywhere. The staging server imports its own later rows once
-- (market_watch --import-csv); duplicates are skipped.
INSERT INTO raw.market_watch
  (logged_at, report_stamp, market, source, n, avg_predicted, hit_rate,
   overconfidence, brier, flag)
VALUES
  ('2026-10-12T18:24:12Z', '2026-10-08-before-refit', 'Match Over 1.5', 'all', 3769, 0.763, 0.7854, -0.0223, 0.1668, ''),
  ('2026-10-12T18:24:12Z', '2026-10-08-before-refit', 'Match Over 1.5', 'dixon_coles_only', 95, 0.7126, 0.7789, -0.0663, 0.1731, ''),
  ('2026-10-12T18:24:12Z', '2026-10-12T17:36Z-refit', 'Match Over 1.5', 'all', 3556, 0.7752, 0.7868, -0.0116, 0.1663, ''),
  ('2026-10-12T18:24:12Z', '2026-10-12T17:36Z-refit', 'Match Over 1.5', 'dixon_coles_only', 851, 0.7424, 0.7615, -0.0191, 0.1796, ''),
  ('2026-10-12T20:29:25Z', '2026-10-12T20:29:25Z', 'Match Over 1.5', 'all', 3538, 0.7736, 0.7869, -0.0133, 0.1662, ''),
  ('2026-10-12T20:29:25Z', '2026-10-12T20:29:25Z', 'Match Over 1.5', 'dixon_coles_only', 813, 0.738, 0.7601, -0.0222, 0.1801, ''),
  ('2026-10-18T00:15:57Z', '2026-10-18T00:08:42Z', 'Match Over 1.5', 'all', 3470, 0.7777, 0.7922, -0.0145, 0.1639, ''),
  ('2026-10-18T00:15:57Z', '2026-10-18T00:08:42Z', 'Match Over 1.5', 'dixon_coles_only', 776, 0.7453, 0.7925, -0.0473, 0.166, 'overconfidence -4.7 pts'),
  ('2026-10-26T18:54:34Z', '2026-10-26T18:52:44Z', 'Match Over 1.5', 'all', 4605, 0.7768, 0.7904, -0.0136, 0.1645, ''),
  ('2026-10-26T18:54:34Z', '2026-10-26T18:52:44Z', 'Match Over 1.5', 'dixon_coles_only', 857, 0.7469, 0.776, -0.0291, 0.1721, '')
ON CONFLICT (market, source, report_stamp) DO NOTHING;
