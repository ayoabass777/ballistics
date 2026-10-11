"""
Pure Elo math. No IO, no DB — just the formulas.

Two formulas, one football tweak:
    E_A = 1 / (1 + 10^((R_B - R_A) / SCALING))         # expected score
    R_A_new = R_A + K * (S_A - E_A)                    # update
    Home team gets HOME_ADVANTAGE added for expectation calc only.

League-adaptive K: volatile leagues (high upset rate) get a lower K so
ratings stay compressed — reflecting that anyone can beat anyone.
"""

INITIAL_RATING: float = 1500.0
K_FACTOR: int = 30
K_MIN: int = 15
K_MAX: int = 40
HOME_ADVANTAGE: float = 70.0
SCALING: float = 400.0


def expected_score(rating_a: float, rating_b: float) -> float:
    """
    Probability of team A beating team B given their ratings.
    Returns a value in [0, 1].

    A 400-point gap means the favourite is expected to win ~91% of the time.
    """
    return 1.0 / (1.0 + 10 ** ((rating_b - rating_a) / SCALING))


def actual_score(goals_for: int, goals_against: int) -> float:
    """
    Translate a match result into an Elo score from the perspective of the
    team that scored `goals_for`.

    Win = 1.0, Draw = 0.5, Loss = 0.0
    """
    if goals_for > goals_against:
        return 1.0
    if goals_for < goals_against:
        return 0.0
    return 0.5


def update_rating(
    rating: float,
    expected: float,
    actual: float,
    k: int = K_FACTOR,
) -> float:
    """
    Apply the Elo update. Returns the new rating (no in-place mutation).

    Positive surprise (actual > expected) raises the rating.
    Negative surprise lowers it. Exactly-as-expected leaves it unchanged.
    """
    return rating + k * (actual - expected)


def adaptive_k(upset_rate: float, k_min: int = K_MIN, k_max: int = K_MAX) -> int:
    """
    Map league upset rate to a K-factor. Higher upset rate → lower K.

    Upset rate range in our data: ~0.22 (predictable, e.g. Primeira Liga)
    to ~0.33 (volatile, e.g. 2. Bundesliga). We linearly interpolate:
        0.20 upset rate → k_max (predictable league, big swings are meaningful)
        0.35 upset rate → k_min (volatile league, dampen noise)
    """
    low, high = 0.20, 0.35
    t = max(0.0, min(1.0, (upset_rate - low) / (high - low)))
    return round(k_max - t * (k_max - k_min))


def predict_match(
    rating_home: float,
    rating_away: float,
    home_advantage: float = HOME_ADVANTAGE,
) -> tuple[float, float]:
    """
    Convenience wrapper for the matchup case.
    Returns (P(home wins or draws as 1), P(away wins or draws as 1)) — i.e.
    Elo expected scores, not pure win probabilities. They sum to 1.0.

    Home advantage is applied to the home rating for the calculation only;
    stored ratings are never modified by this function.
    """
    home_effective = rating_home + home_advantage
    e_home = expected_score(home_effective, rating_away)
    return e_home, 1.0 - e_home
