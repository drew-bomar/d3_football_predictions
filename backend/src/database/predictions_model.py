# src/database/predictions_model.py
"""
Predictions table - stores model predictions for comparison with actual results
"""
from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, Float, DateTime, Boolean,
    ForeignKey, UniqueConstraint, Index
)
from sqlalchemy.orm import relationship
from src.database.connection import Base

class Prediction(Base):
    """
    Stores predictions from various models for later evaluation.
    
    Design: One row per game per model. Multiple models can predict
    the same game by using different model_name values.
    """
    __tablename__ = 'predictions'
    
    id = Column(Integer, primary_key=True)
    
    # Model identifier (e.g., 'logistic_baseline_v1', 'xgboost_v2')
    model_name = Column(String(100), nullable=False)
    model_version = Column(String(50))  # Optional version tracking
    
    # Game identifiers
    year = Column(Integer, nullable=False)
    week = Column(Integer, nullable=False)
    home_team_id = Column(Integer, ForeignKey('teams.id'), nullable=False)
    away_team_id = Column(Integer, ForeignKey('teams.id'), nullable=False)
    
    # Prediction outputs
    predicted_winner_id = Column(Integer, ForeignKey('teams.id'), nullable=False)
    home_win_prob = Column(Float, nullable=False)  # 0.0 to 1.0
    away_win_prob = Column(Float, nullable=False)  # Should sum to 1.0
    confidence = Column(Float, nullable=False)     # Max of home/away prob
    
    # Optional: store key factors that drove prediction
    key_factors = Column(String(500))  # JSON or comma-separated
    
    # Evaluation (filled after game is played)
    actual_winner_id = Column(Integer, ForeignKey('teams.id'))
    actual_home_score = Column(Integer)
    actual_away_score = Column(Integer)
    was_correct = Column(Boolean)
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    evaluated_at = Column(DateTime)  # When actual results were recorded
    
    # Relationships
    home_team = relationship("Team", foreign_keys=[home_team_id])
    away_team = relationship("Team", foreign_keys=[away_team_id])
    predicted_winner = relationship("Team", foreign_keys=[predicted_winner_id])
    actual_winner = relationship("Team", foreign_keys=[actual_winner_id])
    
    # Constraints
    __table_args__ = (
        # One prediction per model per game
        UniqueConstraint('model_name', 'year', 'week', 'home_team_id', 'away_team_id',
                        name='uq_prediction_model_game'),
        
        # Indexes for common queries
        Index('idx_predictions_week', 'year', 'week'),
        Index('idx_predictions_model', 'model_name'),
        Index('idx_predictions_team', 'home_team_id', 'away_team_id'),
        Index('idx_predictions_evaluated', 'was_correct'),
    )
    
    def __repr__(self):
        return f"<Prediction(model={self.model_name}, {self.year} W{self.week})>"
    
    @classmethod
    def save_prediction(cls, session, model_name: str, year: int, week: int,
                       home_team_id: int, away_team_id: int,
                       predicted_winner_id: int, home_win_prob: float,
                       key_factors: str = None, model_version: str = None):
        """Save a single prediction."""
        pred = cls(
            model_name=model_name,
            model_version=model_version,
            year=year,
            week=week,
            home_team_id=home_team_id,
            away_team_id=away_team_id,
            predicted_winner_id=predicted_winner_id,
            home_win_prob=home_win_prob,
            away_win_prob=1.0 - home_win_prob,
            confidence=max(home_win_prob, 1.0 - home_win_prob),
            key_factors=key_factors
        )
        session.add(pred)
        return pred
    
    @classmethod
    def update_with_results(cls, session, prediction_id: int,
                           actual_winner_id: int, home_score: int, away_score: int):
        """Update prediction with actual game results."""
        pred = session.query(cls).get(prediction_id)
        if pred:
            pred.actual_winner_id = actual_winner_id
            pred.actual_home_score = home_score
            pred.actual_away_score = away_score
            pred.was_correct = (pred.predicted_winner_id == actual_winner_id)
            pred.evaluated_at = datetime.utcnow()
        return pred