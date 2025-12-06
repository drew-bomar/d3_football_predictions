"""
Games table model for D3 Football Predictions.
This table stores the basic information about each game.
"""
from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, DateTime, Date,
    ForeignKey, UniqueConstraint, Index
)
from sqlalchemy.orm import relationship
from src.database.connection import Base

class Game(Base):
    """
    Represents a single D3 football game.
    
    Design decisions:
    1. We store NCAA's contest_id to match with external data
    2. Home/away teams are foreign keys to ensure referential integrity
    3. Week and year help with seasonal queries
    4. We keep score here (not in stats) as it's fundamental game data
    """
    __tablename__= 'games'

    # Primary key (auto-incrementing unique identifier for each game seperate from ncaa id) 
    id = Column(Integer, primary_key = True)

    # NCAA's unique identifier for this game
    contest_id = Column(String(50), unique = True, nullable = False, index = True)

    game_date = Column(Date, nullable=False)
    year = Column(Integer, nullable = False)
    week = Column(Integer, nullable = False)

    # who played? with foreign keys to teams table
    home_team_id = Column(Integer, ForeignKey('teams.id'), nullable = False)
    away_team_id = Column(Integer, ForeignKey('teams.id'), nullable = False)

    #final scores
    home_score = Column(Integer)
    away_score = Column(Integer)

    # Game status (useful for tracking incomplete data)
    status = Column(String(20), default='final')  # final, cancelled, postponed

    # Audit fields
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    #creates python object reference to team objects from teams table, reverses the relationship as
    #well with back ref (game.home_team.team give the team object & team.home_games gives list   of home games  
    home_team = relationship("Team", foreign_keys = [home_team_id],backref = "home_games")
    away_team = relationship("Team", foreign_keys = [away_team_id],backref = "away_games")

    #table constraints and indices
    __table_args__= (
       # Can't have the same team as both home and away
        # This is a CHECK constraint at the database level
        # CheckConstraint('home_team_id != away_team_id', name='check_different_teams'),
        
        # Indexes for common queries
        Index('idx_game_date', 'game_date'),
        Index('idx_game_year_week', 'year', 'week'),
        Index('idx_game_teams', 'home_team_id', 'away_team_id'),
    )
    

    def __repr__(self):
        return f"<Game(id={self.id}, contest_id='{self.contest_id}', week={self.week})>"

    
    #property decorator means this method acts as an attribute (call game.margin not game.margin())
    @property
    def margin(self):
        """Calculate point margin (positive = home team won)."""
        if self.home_score is not None and self.away_score is not None:
            return self.home_score - self.away_score
        return None

    @property
    def total_points(self):
        """Calculate total points scored."""
        if self.home_score is not None and self.away_score is not None:
            return self.home_score + self.away_score
        return None
    
    @property
    def winner(self):
        """Determine winner ('home', 'away', or 'tie')."""
        margin = self.margin
        if margin is None:
            return None
        elif margin > 0:
            return 'home'
        elif margin < 0:
            return 'away'
        else:
            return 'tie'

    

    