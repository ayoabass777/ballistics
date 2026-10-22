# Codex Backend Todo — `API`

**Owner:** Codex (API)
**API spec:** [docs/fixture-analysis-api-spec.md](../docs/fixture-analysis-api-spec.md)
**Roadmap:** [docs/roadmap.md](../docs/roadmap.md)
**Last updated:** 2026-03-02

---

## Status Key

- ✅ Done
- 🔧 To do
- ⚠️ Partial

---

## Task 1 — Add DB models for `team_form` and `elo.elo_rating`

**Status:** 🔧 To do

**File:** `API/models/db_models.py`

Neither table has a SQLAlchemy model yet. Add both.

### `DBTeamForm`

Maps to `public.team_form` (written by `team_form/src/load_team_form_table.py`).

```python
class DBTeamForm(Base):
    __tablename__ = "team_form"
    __table_args__ = {"schema": "public"}

    league_season_id = Column(Integer, primary_key=True)
    event_name       = Column(String,  primary_key=True)
    context          = Column(String,  primary_key=True)   # "all" | "home" | "away"
    team_id          = Column(Integer, primary_key=True)
    team_name        = Column(String)
    league_name      = Column(String)
    next_opponent_id = Column(Integer)
    next_opponent_name = Column(String)
    next_kickoff_utc = Column(DateTime)
    score            = Column(Float)    # Bayesian final_score [0, 1]
    n                = Column(Integer)  # games in window
    k                = Column(Integer)  # hits
    misses           = Column(Integer)
    p_bayes          = Column(Float)    # raw Bayesian rate (pre-uncertainty-discount)
    streak_length    = Column(Integer)
```

### `DBEloRating`

Maps to `elo.elo_rating` (written by `elo_rating/src/extract_team_elo.py`).

```python
class DBEloRating(Base):
    __tablename__ = "elo_rating"
    __table_args__ = {"schema": "elo"}

    fixture_id      = Column(Integer,  primary_key=True)
    team_id         = Column(Integer,  primary_key=True)
    kickoff_utc     = Column(DateTime)
    pre_elo_rating  = Column(Float)
    post_elo_rating = Column(Float)
```

**Current Elo for a team** = `post_elo_rating` from the row with the latest `kickoff_utc` for that `team_id`.

---

## Task 2 — Add Pydantic response models

**Status:** 🔧 To do

**File:** `API/models/api_models.py`

Add these models. Append after the existing models — do not modify existing ones.

```python
class TeamFormScore(BaseModel):
    event_name:    str
    metric_key:    str           # derived from event_name via existing _normalize_metric_key()
    score:         float         # Bayesian final_score [0, 1]
    p_bayes:       float
    n:             int
    k:             int
    streak_length: int

class TeamFormResponse(BaseModel):
    team_id:     int
    team_name:   str
    context:     str
    form_scores: List[TeamFormScore]

class BriefingFixtureInfo(BaseModel):
    fixture_id:   Optional[int]
    kickoff_utc:  Optional[str]
    league_name:  Optional[str]
    country_name: Optional[str]

class BriefingTeam(BaseModel):
    team_id:            int
    team_name:          str
    elo_rating:         Optional[float]
    elo_tier:           Optional[int]     # 1=Strong, 2=Mid, 3=Weak
    recent_form:        List[str]
    win_rate:           Optional[float]
    avg_goals_for:      Optional[float]
    avg_goals_against:  Optional[float]
    form_scores:        List[TeamFormScore]
    streaks:            List[TeamStreakRecord]

class BriefingH2H(BaseModel):
    matches_played:  int
    home_wins:       int
    away_wins:       int
    draws:           int
    avg_total_goals: Optional[float]
    btts_rate:       Optional[float]
    over_2_5_rate:   Optional[float]
    recent_matches:  List[Dict[str, Any]]

class FixtureBriefingResponse(BaseModel):
    fixture:   BriefingFixtureInfo
    home_team: BriefingTeam
    away_team: BriefingTeam
    h2h:       BriefingH2H
    meta:      Dict[str, Any]
```

---

## Task 3 — Implement `GET /teams/form`

**Status:** 🔧 To do

**File:** `API/app.py`

**Route:** `GET /teams/form`

**Query params:**
- `team_id: int` — required
- `context: str` — optional, default `"all"`, accepts `"all"`, `"home"`, `"away"`

**Logic:**
1. Query `DBTeamForm` filtering by `team_id` and `context`.
2. Join to current league seasons only — filter by `league_season_id` values returned by the existing `get_current_league_season_ids()` call (already used elsewhere in `app.py`).
3. For each row, derive `metric_key` using the existing `_normalize_metric_key(event_name)` helper.
4. Return `TeamFormResponse`.

**Example:**
```
GET /teams/form?team_id=42&context=all
GET /teams/form?team_id=42&context=home
```

**Error handling:**
- 404 if no rows found for `team_id`.
- 503 on `SQLAlchemyError`.

**No auth required** — same as `/home/streaks`.

---

## Task 4 — Implement `GET /fixtures/briefing`

**Status:** 🔧 To do

**File:** `API/app.py`

**Route:** `GET /fixtures/briefing`

**Query params:**
- `fixture_id: int` — preferred lookup
- `home_team_id: int` — fallback (required with `away_team_id`)
- `away_team_id: int` — fallback (required with `home_team_id`)

Either `fixture_id` OR both `home_team_id` + `away_team_id` must be provided. Return 400 if neither.

**Logic:**

### Step 1 — Resolve team IDs

If `fixture_id` provided:
- Query `DBFixturePageContext` by `fixture_id` → get `home_team_id`, `away_team_id`.

If team IDs provided directly:
- Use as-is. `fixture_id` in response will be `None`.

### Step 2 — Fetch team stats

For each team, query `DBTeamStat` (most recent row by `kickoff_utc` desc) → win rate, avg goals, recent form.

Use existing `_serialize_team_summary()` helper.

### Step 3 — Fetch form scores

For each team, query `DBTeamForm` where `team_id = X` and `context = "all"` and `league_season_id` in current seasons.

### Step 4 — Fetch current Elo

For each team, query `DBEloRating` where `team_id = X`, order by `kickoff_utc DESC`, take first row → `post_elo_rating`.

Apply tier bucketing:
```python
def _elo_tier(rating: float) -> int:
    if rating >= 1700: return 1   # Strong
    if rating >= 1550: return 2   # Mid
    return 3                       # Weak
```

### Step 5 — Fetch streaks

For each team, query `DBTeamStreakRecord` — use existing `_serialize_team_streaks()` helper.

### Step 6 — Fetch H2H

If `fixture_id` available:
- Load `DBFixturePageContext.h2h_info_json` and `h2h_matches_json`.
- Derive `btts_rate` and `over_2_5_rate` from `h2h_matches_json` match list:
  ```python
  # btts: both teams scored (home_score > 0 AND away_score > 0)
  # over_2_5: total goals > 2
  ```
- `recent_matches`: last 5 matches from `h2h_matches_json`, formatted as:
  ```python
  {"kickoff_utc": ..., "home_team": ..., "away_team": ..., "score": "2-1", "result": "H"}
  ```

If no `fixture_id`:
- Return `BriefingH2H` with all counts = 0 and rates = None.

### Step 7 — Return `FixtureBriefingResponse`

**Error handling:**
- 400 if neither `fixture_id` nor both team IDs provided.
- 404 if `fixture_id` not found in `DBFixturePageContext`.
- 503 on `SQLAlchemyError`.

**No auth required.**

---

## Task 5 — Thread `confidence_score` and `opponent_tier` into streak responses

**Status:** 🔧 To do

**Files:** `API/app.py`, `API/models/api_models.py`, `API/models/db_models.py`

The `Streak` Pydantic model and `DBHomepageStreak` DB model do not yet have these fields. The frontend adapter (`frontend_dev/app/api/streaks/route.ts`) is already reading them — backend just needs to supply them.

### 5a — Add fields to `Streak` Pydantic model

`API/models/api_models.py` — add to `Streak`:
```python
confidence_score: Optional[float] = None   # Bayesian final_score for team's primary streak event
opponent_tier:    Optional[int]   = None   # 1=Strong, 2=Mid, 3=Weak
```

### 5b — Add `confidence_score` to `DBHomepageStreak`

`API/models/db_models.py` — add column to `DBHomepageStreak` if the mart table has it, otherwise compute it live (see 5d).

### 5c — Resolve `opponent_tier` from Elo

In `/home/streaks` and `/explore/streaks` route handlers, for each streak row:
1. Get `next_opponent_id` from the streak row.
2. Query `DBEloRating` for the opponent's latest `post_elo_rating`.
3. Apply `_elo_tier(rating)` → `opponent_tier`.

Cache the Elo lookup by `team_id` within the request (batch the opponent IDs upfront, single query).

### 5d — Resolve `confidence_score` from `team_form`

For each streak row, look up `DBTeamForm` where:
- `team_id = streak.team_id`
- `event_name = streak.event_name`
- `context = "all"`
- `league_season_id` in current seasons

Take `score` field → `confidence_score`.

Batch this lookup upfront (collect all `(team_id, event_name)` pairs, single query, build dict).

### Batch query pattern (avoid N+1):

```python
# Collect all team and opponent IDs upfront
team_ids    = {row.team_id for row in rows}
opponent_ids = {row.next_opponent_id for row in rows if row.next_opponent_id}

# Single Elo query for all opponents
elo_rows = db.query(DBEloRating) \
    .filter(DBEloRating.team_id.in_(opponent_ids)) \
    ... # latest per team via subquery or Python groupby

# Single form query for all (team_id, event_name) pairs
form_rows = db.query(DBTeamForm) \
    .filter(DBTeamForm.team_id.in_(team_ids)) \
    .filter(DBTeamForm.context == "all") \
    ...
```

---

## Non-Goals (Do Not Do)

- Do not implement backend `/explore/streaks` with real data — stub stays as-is.
- Do not modify `app/api/streaks/route.ts` in `frontend_dev` — adapter contract is frozen.
- Do not add xG fields — not available in DB.
- Do not add auth to the new endpoints.

---

## Acceptance Checklist

- [ ] Task 1: `DBTeamForm` and `DBEloRating` models added, table mappings correct
- [ ] Task 2: All new Pydantic models added, no existing models modified
- [ ] Task 3: `GET /teams/form?team_id=42` returns form scores for all events in current season
- [ ] Task 4: `GET /fixtures/briefing?fixture_id=9801` returns full dossier with both teams, H2H, Elo, form
- [ ] Task 4: `GET /fixtures/briefing?home_team_id=42&away_team_id=55` fallback works when no fixture_id
- [ ] Task 5: `/home/streaks` response includes `confidence_score` and `opponent_tier` per streak
- [ ] Task 5: Elo and form lookups are batched (no N+1 queries per streak row)
