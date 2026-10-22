from sqlalchemy import JSON, Boolean, Column, DateTime, Float, Integer, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class DBStreak(Base):
    __tablename__ = "mart_top_team_streaks"
    __table_args__ = {"schema": "raw_mart"}
    streak_id = Column(Integer, primary_key=True, index=True)
    team_id = Column(Integer, nullable=False)
    league_season_id = Column(Integer)
    league_name = Column(String, nullable=False)
    team_name = Column(String, nullable=False)
    next_opponent_name = Column(String)
    next_opponent_id = Column(Integer)
    next_fixture_id = Column(Integer)
    next_kickoff_utc = Column(DateTime)
    event_name = Column(String, nullable=False)
    streak_type = Column(String, nullable=False)
    context_scope = Column(String)
    current_streak_length = Column(Integer, nullable=False)


class DBHomepageStreak(Base):
    __tablename__ = "mart_homepage_top_streaks"
    __table_args__ = {"schema": "raw_mart"}
    streak_id = Column(String, primary_key=True)
    team_id = Column(Integer, nullable=False)
    league_season_id = Column(Integer)
    event_name = Column(String, nullable=False)
    streak_type = Column(String, nullable=False)
    streak_length = Column(Integer, nullable=False)
    team_name = Column(String, nullable=False)
    team_logo_file_id = Column(String)
    league_name = Column(String, nullable=False)
    league_logo_file_id = Column(String)
    country_name = Column(String)
    country_logo_file_id = Column(String)
    next_opponent_name = Column(String)
    next_opponent_id = Column(Integer)
    next_fixture_id = Column(Integer)
    next_kickoff_utc = Column(DateTime)
    context_scope = Column(String)
    relevance_score = Column(Float)


class DBTeamForm(Base):
    __tablename__ = "team_form"
    __table_args__ = {"schema": "public"}
    league_season_id = Column(Integer, primary_key=True)
    event_name = Column(String, primary_key=True)
    context = Column(String, primary_key=True)
    team_id = Column(Integer, primary_key=True)
    team_name = Column(String)
    score = Column(Float)


class DBEloRating(Base):
    __tablename__ = "elo_rating"
    __table_args__ = {"schema": "elo"}
    team_id = Column(Integer, primary_key=True)
    league_season_id = Column(Integer, primary_key=True)
    fixture_id = Column(Integer, primary_key=True)
    post_elo_rating = Column(Float)
    snapshot_type = Column(String)


class DBNavLeague(Base):
    __tablename__ = "mart_nav_leagues"
    __table_args__ = {"schema": "raw_mart"}
    league_season_id = Column(Integer, primary_key=True)
    league_name = Column(String, nullable=False)
    country_name = Column(String, nullable=False)
    league_logo_file_id = Column(String)
    country_logo_file_id = Column(String)


class DBLeagueInfo(Base):
    __tablename__ = "mart_league_info"
    __table_args__ = {"schema": "raw_mart"}
    league_season_id = Column(Integer, primary_key=True)
    league_name = Column(String, nullable=False)
    league_slug = Column(String)
    season_label = Column(String)
    country_name = Column(String)


class DBTeamInfo(Base):
    __tablename__ = "mart_team_info"
    __table_args__ = {"schema": "raw_mart"}
    team_id = Column(Integer, primary_key=True)
    team_name = Column(String, nullable=False)
    team_slug = Column(String)
    league_season_id = Column(Integer)
    logo_file_id = Column(String)
    next_is_home = Column(Boolean)


class DBTeamStat(Base):
    __tablename__ = "mart_team_stats"
    __table_args__ = {"schema": "raw_mart"}
    kickoff_utc = Column(DateTime, primary_key=True)
    team_id = Column(Integer, primary_key=True)
    team_name = Column(String, nullable=False)
    is_home = Column(Boolean)
    league_name = Column(String, nullable=False)
    league_id = Column(Integer)
    league_season_id = Column(Integer, nullable=False)
    country = Column(String)
    season = Column(String)
    win_rate = Column(Float)
    draw_rate = Column(Float)
    loss_rate = Column(Float)
    avg_goals_for = Column(Float)
    avg_goals_against = Column(Float)
    recent_form = Column(String)
    next_opponent_id = Column(Integer)
    next_opponent_name = Column(String)
    next_fixture_id = Column(Integer)
    next_kickoff_utc = Column(DateTime)


class DBTeamStreakRecord(Base):
    __tablename__ = "mart_team_streaks"
    __table_args__ = {"schema": "raw_mart"}
    streak_id = Column(String)
    team_id = Column(Integer, primary_key=True)
    event_name = Column(String, primary_key=True)
    streak_type = Column(String, nullable=False)
    streak_length = Column(Integer, nullable=False)
    team_name = Column(String, nullable=False)
    league_season_id = Column(Integer)
    league_name = Column(String)
    opponent_name = Column(String)
    opponent_id = Column(Integer)
    fixture_id = Column(Integer)
    kickoff_utc = Column(DateTime)


class DBTeamPageContext(Base):
    __tablename__ = "mart_team_page_context"
    __table_args__ = {"schema": "raw_mart"}
    team_id = Column(Integer, primary_key=True)
    team_name = Column(String, nullable=False)
    team_slug = Column(String)
    rank = Column(Integer)
    form = Column(JSON)
    league_name = Column(String)
    season_label = Column(String)
    country_name = Column(String)
    league_teams_json = Column(JSON)
    streaks_json = Column(JSON)
    next_fixture_id = Column(Integer)
    next_opponent_id = Column(Integer)
    next_opponent_name = Column(String)
    next_is_home = Column(Boolean)
    next_kickoff_utc = Column(DateTime)


class DBFixturePageContext(Base):
    __tablename__ = "mart_fixture_page_context"
    __table_args__ = {"schema": "raw_mart"}
    fixture_id = Column(Integer, primary_key=True)
    fixture_slug = Column(String)
    team_pair_key = Column(String)
    home_team_id = Column(Integer)
    away_team_id = Column(Integer)
    fixture_info_json = Column(JSON)
    fixture_streaks_json = Column(JSON)
    h2h_info_json = Column(JSON)
    h2h_streaks_json = Column(JSON)
    h2h_matches_json = Column(JSON)
    h2h_context_scopes_json = Column(JSON)
    team_fixtures_json = Column(JSON)
