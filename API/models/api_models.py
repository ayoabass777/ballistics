from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel


class Streak(BaseModel):
    streak_id: str
    team_id: int
    event_name: str
    metric_key: str
    headline: str
    streak_type: Literal["goal", "result"]
    current_streak_length: int
    team_name: str
    league_name: str
    next_kickoff_utc: Optional[str] = None
    next_opponent_name: Optional[str] = None
    next_opponent_id: Optional[int] = None
    next_fixture_id: Optional[int] = None
    next_is_home: Optional[bool] = None
    context_scope: Optional[str] = None
    team_logo_url: Optional[str] = None
    league_logo_url: Optional[str] = None
    country_logo_url: Optional[str] = None
    team_logo_file_id: Optional[str] = None
    league_logo_file_id: Optional[str] = None
    country_name: Optional[str] = None
    country_logo_file_id: Optional[str] = None
    recent_form: Optional[List[str]] = None
    confidence_score: Optional[float] = None
    team_tier: Optional[int] = None
    opponent_tier: Optional[int] = None


class NavigationLeagueItem(BaseModel):
    league_season_id: int
    league_name: str
    league_slug: str
    country_name: str
    league_logo_url: Optional[str] = None
    country_logo_url: Optional[str] = None
    href: str


class NavigationLeaguesResponse(BaseModel):
    data: Dict[str, List[NavigationLeagueItem]]
    meta: Dict[str, Any]


class HomeStatsData(BaseModel):
    active_streaks: int
    leagues: int
    teams_tracked: int


class HomeStatsResponse(BaseModel):
    data: HomeStatsData
    meta: Dict[str, Any]


class TeamSearchSuggestion(BaseModel):
    team_id: int
    name: str
    slug: str
    href: str
    logo_url: Optional[str] = None
    league_name: Optional[str] = None
    country_logo_url: Optional[str] = None


class TeamSearchResponse(BaseModel):
    data: Dict[str, List[TeamSearchSuggestion]]
    meta: Dict[str, Any]


class TeamSummary(BaseModel):
    team_id: int
    team_name: str
    league_name: str
    league_season_id: int
    season: Optional[str]
    win_rate: Optional[float]
    draw_rate: Optional[float]
    loss_rate: Optional[float]
    avg_goals_for: Optional[float]
    avg_goals_against: Optional[float]
    next_opponent_id: Optional[int]
    next_opponent_name: Optional[str]
    next_fixture_id: Optional[int]
    next_kickoff_utc: Optional[str]
    recent_form: List[str]


class TeamStreakRecord(BaseModel):
    team_id: int
    team_name: str
    event_name: str
    streak_type: Literal["goal", "result"]
    current_streak_length: int


class FixtureAnalysisResponse(BaseModel):
    team: TeamSummary
    opponent: Optional[TeamSummary] = None
    team_streaks: List[TeamStreakRecord]
    opponent_streaks: List[TeamStreakRecord]


class TeamPagePeer(BaseModel):
    team_id: int
    name: str
    team_href: Optional[str] = None
    logo_file_id: Optional[str] = None


class TeamPageContextStreak(BaseModel):
    team_id: int
    streak_id: Optional[str] = None
    event_name: str
    streak_type: Literal["goal", "result"]
    streak_length: int
    polarity: Optional[str] = None
    scope: Optional[str] = None


class TeamPageContextData(BaseModel):
    team_id: int
    team_name: str
    team_slug: Optional[str] = None
    rank: Optional[int] = None
    form: List[str]
    league_name: Optional[str] = None
    season_label: Optional[str] = None
    country_name: Optional[str] = None
    league_teams: List[TeamPagePeer]
    streaks: List[TeamPageContextStreak]
    next_fixture_id: Optional[int] = None
    next_opponent_id: Optional[int] = None
    next_opponent_name: Optional[str] = None
    next_is_home: Optional[bool] = None
    next_kickoff_utc: Optional[str] = None


class TeamPageContextResponse(BaseModel):
    data: TeamPageContextData
    meta: Dict[str, Any]


class FixturePageContextData(BaseModel):
    fixture_id: int
    fixture_slug: Optional[str] = None
    team_pair_key: Optional[str] = None
    home_team_id: Optional[int] = None
    away_team_id: Optional[int] = None
    fixture_info: Dict[str, Any]
    fixture_streaks: Dict[str, Any]
    h2h_info: Dict[str, Any]
    h2h_streaks: List[Dict[str, Any]]
    h2h_matches: List[Dict[str, Any]]
    h2h_context_scopes: List[Any]
    team_fixtures: Dict[str, Any]


class FixturePageContextResponse(BaseModel):
    data: FixturePageContextData
    meta: Dict[str, Any]
