from datetime import datetime
from sqlalchemy import (
    Column, Integer, Float, DateTime, Boolean,
    ForeignKey, UniqueConstraint, Index
)
from sqlalchemy.orm import relationship
from src.database.connection import Base

class TeamRollingStats(Base):
    """
    Pre-calculated rolling statistics for each team before each game.
    
    Design decisions:
    1. One row per team per game (mirrors team_game_stats structure)
    2. Stats calculated BEFORE the game to prevent data leakage
    3. Uses 3 and 5 week windows (7 week removed as it won't be used until week 8)
    4. Previous season games used for early weeks with decay weight
    5. NULL values when insufficient data rather than misleading zeros
    
    Why these specific windows:
    - 3 week: Captures current form and momentum
    - 5 week: Smooths out outlier performances  
    - Season: Overall baseline performance
    """
    
    __tablename__ = 'team_rolling_stats'
    
    # Primary key
    id = Column(Integer, primary_key=True)
    
    # Foreign keys - linking to the game these stats are FOR
    team_id = Column(Integer, ForeignKey('teams.id'), nullable=False)
    game_id = Column(Integer, ForeignKey('games.id'), nullable=False)
    opponent_id = Column(Integer, ForeignKey('teams.id'), nullable=False)
    
    # Context fields (denormalized for query performance)
    year = Column(Integer, nullable=False)
    week = Column(Integer, nullable=False)
    
    # ================== DATA QUALITY INDICATORS ==================
    # How many games were used in each calculation?
    games_in_3wk = Column(Integer)
    games_in_5wk = Column(Integer)
    games_in_season = Column(Integer)
    
    # How many games were from previous season (with decay weight)?
    prev_season_games_in_3wk = Column(Integer, default=0)
    prev_season_games_in_5wk = Column(Integer, default=0)
    
    # ================== OFFENSIVE METRICS ==================
    # Points per game
    ppg_3wk = Column(Float)
    ppg_5wk = Column(Float)
    ppg_season = Column(Float)
    
    # Yards per play (offensive efficiency)
    ypp_3wk = Column(Float)
    ypp_5wk = Column(Float)
    ypp_season = Column(Float)
    
    # Total yards per game
    total_yards_3wk = Column(Float)
    total_yards_5wk = Column(Float)
    total_yards_season = Column(Float)
    
    # Third down conversion rate (sustaining drives)
    third_down_pct_3wk = Column(Float)
    third_down_pct_5wk = Column(Float)
    third_down_pct_season = Column(Float)
    
    # Turnover differential per game (huge impact on outcomes)
    turnover_diff_3wk = Column(Float)
    turnover_diff_5wk = Column(Float)
    turnover_diff_season = Column(Float)
    
    # ================== DEFENSIVE METRICS ==================
    # Points allowed per game
    papg_3wk = Column(Float)
    papg_5wk = Column(Float)
    papg_season = Column(Float)
    
    # Opponent yards per play (defensive efficiency)
    opp_ypp_3wk = Column(Float)
    opp_ypp_5wk = Column(Float)
    opp_ypp_season = Column(Float)
    
    # Opponent total yards per game
    opp_total_yards_3wk = Column(Float)
    opp_total_yards_5wk = Column(Float)
    opp_total_yards_season = Column(Float)
    
    # Opponent third down conversion rate
    opp_third_down_pct_3wk = Column(Float)
    opp_third_down_pct_5wk = Column(Float)
    opp_third_down_pct_season = Column(Float)
    
    # ================== COMPOSITE METRICS ==================
    # Scoring margin (point differential)
    margin_3wk = Column(Float)
    margin_5wk = Column(Float)
    margin_season = Column(Float)
    
    # Pass/rush balance (0 = all rush, 1 = all pass)
    pass_ratio_3wk = Column(Float)
    pass_ratio_5wk = Column(Float)
    pass_ratio_season = Column(Float)
    
    # Pass defense ratio (how much opponents pass against this team)
    opp_pass_ratio_3wk = Column(Float)
    opp_pass_ratio_5wk = Column(Float)
    opp_pass_ratio_season = Column(Float)
    
    # ================== STRENGTH OF SCHEDULE ==================
    # Average opponent win percentage in window
    sos_3wk = Column(Float)
    sos_5wk = Column(Float)
    sos_season = Column(Float)
    
    # ================== MOMENTUM INDICATORS ==================
    # Trend: (most recent 3 weeks) - (previous 3 weeks)
    ppg_trend = Column(Float)  # Positive = scoring more recently
    ypp_trend = Column(Float)  # Positive = more efficient recently  
    margin_trend = Column(Float)  # Positive = winning by more recently
    defensive_trend = Column(Float)  # Negative = allowing fewer points recently
    
    # Win/loss streaks
    win_streak = Column(Integer)  # Positive for wins, negative for losses
    last_3_wins = Column(Integer)  # Wins in last 3 games
    last_5_wins = Column(Integer)  # Wins in last 5 games
    
    # ================== SPECIAL SITUATIONS ==================
    # Performance in similar situations
    home_ppg_3wk = Column(Float)  # Points per game at home
    away_ppg_3wk = Column(Float)  # Points per game on road
    
    # Consistency metrics (standard deviation)
    ppg_std_3wk = Column(Float)  # Volatility in scoring
    margin_std_3wk = Column(Float)  # Volatility in outcomes
    
    # ================== METADATA ==================
    # Audit fields
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    team = relationship("Team", foreign_keys=[team_id], backref="rolling_stats")
    opponent = relationship("Team", foreign_keys=[opponent_id])
    game = relationship("Game", backref="team_rolling_stats")
    
    # Table constraints
    __table_args__ = (
        # Each team can only have one rolling stats entry per game
        UniqueConstraint('team_id', 'game_id', name='uq_team_game_rolling'),
        
        # Performance indexes for common queries
        Index('idx_rolling_team_year_week', 'team_id', 'year', 'week'),
        Index('idx_rolling_game', 'game_id'),
        Index('idx_rolling_opponent', 'opponent_id'),
    )
    
    def __repr__(self):
        return f"<TeamRollingStats(team_id={self.team_id}, game_id={self.game_id}, week={self.week})>"
    
    @property
    def has_sufficient_data(self):
        """Check if we have enough data for reliable predictions."""
        return self.games_in_season >= 2
    
    @property
    def is_early_season(self):
        """Check if this is early in the season (weeks 1-3)."""
        return self.week <= 3
    
    @property
    def used_previous_season(self):
        """Check if previous season data was used in calculations."""
        return (self.prev_season_games_in_3wk > 0 or 
                self.prev_season_games_in_5wk > 0)