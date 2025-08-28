"""
Team Game Stats table model for D3 Football Predictions.
This table stores detailed statistics for each team in each game.
"""
from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, Float, DateTime, Boolean,
    ForeignKey, UniqueConstraint, Index, CheckConstraint
)
from sqlalchemy.orm import relationship
from src.database.connection import Base

class TeamGameStats(Base):
    """
    Represents one team's statistics for a single game.
    
    Design Philosophy:
    - One row per team per game (2 rows total per game)
    - Denormalized for analytics performance
    - Stats stored from team's perspective (opponent stats prefixed with 'opp_')
    - All stats stored as proper numeric types for calculations
    """
    __tablename__ = 'team_game_stats'

    #primary key (auto incrementing unique identifier)
    id = Column(Integer, primary_key = True)

    #foreing keys to identify which game and team
    game_id = Column(Integer, ForeignKey('games.id'), nullable=False)
    team_id = Column(Integer, ForeignKey('teams.id'), nullable=False)
    opponent_id = Column(Integer, ForeignKey('teams.id'), nullable=False)

    # Game context
    is_home = Column(Boolean, nullable=False)  # True if team was home

    # ================== SCORING ==================
    points_scored = Column(Integer, nullable=False)
    points_allowed = Column(Integer, nullable=False)

    # ================== FIRST DOWNS ==================
    first_downs = Column(Integer)
    first_downs_rushing = Column(Integer)
    first_downs_passing = Column(Integer)
    first_downs_penalty = Column(Integer)

    # Opponent first downs
    opp_first_downs = Column(Integer)
    opp_first_downs_rushing = Column(Integer)
    opp_first_downs_passing = Column(Integer)
    opp_first_downs_penalty = Column(Integer)

    # ================== RUSHING ==================
    rushing_attempts = Column(Integer)
    rushing_yards = Column(Integer)  # Net yards
    rushing_avg = Column(Float)
    rushing_tds = Column(Integer)

    # Opponent rushing
    opp_rushing_attempts = Column(Integer)
    opp_rushing_yards = Column(Integer)
    opp_rushing_avg = Column(Float)

    # ================== PASSING ==================
    passing_completions = Column(Integer)
    passing_attempts = Column(Integer)
    passing_yards = Column(Integer)  # Net yards
    passing_avg = Column(Float)
    passing_tds = Column(Integer)
    passing_interceptions = Column(Integer)
    
    # Opponent passing
    opp_passing_completions = Column(Integer)
    opp_passing_attempts = Column(Integer)
    opp_passing_yards = Column(Integer)
    opp_passing_avg = Column(Float)
    opp_passing_tds = Column(Integer)
    opp_passing_interceptions = Column(Integer)

    # ================== TOTAL OFFENSE ==================
    total_offense_plays = Column(Integer)
    total_offense_yards = Column(Integer)
    total_offense_avg = Column(Float)
    
    # Opponent total offense
    opp_total_offense_plays = Column(Integer)
    opp_total_offense_yards = Column(Integer)
    opp_total_offense_avg = Column(Float)

    # ================== DEFENSE ==================
    sacks = Column(Integer)
    tackles_for_loss = Column(Integer)
    
    # ================== TURNOVERS ==================
    fumbles = Column(Integer)
    fumbles_lost = Column(Integer)
    
    # Opponent turnovers
    opp_fumbles = Column(Integer)
    opp_fumbles_lost = Column(Integer)

    #add turnover margin here?

    # ================== PENALTIES ==================
    penalties_number = Column(Integer)
    penalties_yards = Column(Integer)
    
    # Opponent penalties
    opp_penalties_number = Column(Integer)
    opp_penalties_yards = Column(Integer)

    # ================== KICKING ==================
    punts_number = Column(Integer)
    punts_yards = Column(Integer)
    punts_avg = Column(Float)
    
    # Opponent punting
    opp_punts_number = Column(Integer)
    opp_punts_yards = Column(Integer)
    opp_punts_avg = Column(Float)

    # ================== RETURNS ==================
    punt_returns_number = Column(Integer)
    punt_returns_yards = Column(Integer)
    kickoff_returns_number = Column(Integer)
    kickoff_returns_yards = Column(Integer)
    interception_returns_number = Column(Integer)
    interception_returns_yards = Column(Integer)
    
    # Opponent returns
    opp_punt_returns_number = Column(Integer)
    opp_punt_returns_yards = Column(Integer)
    opp_kickoff_returns_number = Column(Integer)
    opp_kickoff_returns_yards = Column(Integer)
    opp_interception_returns_number = Column(Integer)
    opp_interception_returns_yards = Column(Integer)
    
    #calculate average starting field position here?

    # ================== EFFICIENCY ==================
    third_down_conversions = Column(Integer)
    third_down_attempts = Column(Integer)
    third_down_pct = Column(Float)
    
    fourth_down_conversions = Column(Integer)
    fourth_down_attempts = Column(Integer)
    
    # Opponent efficiency
    opp_third_down_conversions = Column(Integer)
    opp_third_down_attempts = Column(Integer)
    opp_third_down_pct = Column(Float)
    
    opp_fourth_down_conversions = Column(Integer)
    opp_fourth_down_attempts = Column(Integer)

    # ================== CALCULATED FIELDS ==================
    # These come from your JSON and save computation time
    margin = Column(Integer)  # points_scored - points_allowed
    total_points = Column(Integer)  # points_scored + points_allowed
    win = Column(Boolean)
    
    turnover_diff = Column(Float)
    yards_per_play = Column(Float)
    third_down_rate = Column(Float)  # As decimal (0.3 not 30%)
    pass_rush_ratio = Column(Float)  # Pass yards / total yards

    
    # ================== ADVANCED CONTEXT ==================
    # Strength of schedule and records
    sos_before = Column(Float)  # Opponent win % before this game
    sos_current = Column(Float)  # Opponent win % including this game
    team_record_before = Column(String(10))  # e.g., "5-2"
    
    # Audit fields
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    game = relationship("Game", backref="team_stats")
    team = relationship("Team", foreign_keys=[team_id], backref="game_stats")
    opponent = relationship("Team", foreign_keys=[opponent_id])


    # Table constraints
    __table_args__ = (
        # Each team can only have one entry per game
        UniqueConstraint('game_id', 'team_id', name='uq_game_team'),
        
        # Performance indexes
        Index('idx_team_stats_team', 'team_id'),
        Index('idx_team_stats_game', 'game_id'),
        Index('idx_team_stats_team_week', 'team_id', 'game_id'),
        
        # Data integrity checks
        CheckConstraint('team_id != opponent_id', name='check_different_teams'),
        CheckConstraint('points_scored >= 0', name='check_positive_score'),
        CheckConstraint('points_allowed >= 0', name='check_positive_allowed'),
    )

    def __repr__(self):
        return f"<TeamGameStats(team_id={self.team_id}, game_id={self.game_id}, points={self.points_scored})>"

    @classmethod
    def create_from_json(cls, session, game_id, team_id, opponent_id, json_data):
        """
        Factory method to create a TeamGameStats record from JSON data.
        Handles type conversions and missing fields gracefully.
        """
        # We'll implement this after discussing the approach
        pass
    