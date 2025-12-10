"""
src/models/matchup_predictor.py - Prediction for hypothetical game between just two teams

This service:
1. Loads model + scaler ONCE at startup (not per request)
2. Fetches the most recent rolling stats for any team
3. Builds features and returns predictions

Unlike WeeklyPredictor, this doesn't re-run the training pipeline.
"""

import joblib
import logging
import numpy as np
import pandas as pd
from pathlib import Path
from typing import Dict, Optional, Tuple
from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


class MatchupPredictor:
    """
    Stateless prediction service for the API layer.
    
    Key design decisions:
    - Model and scaler loaded once at instantiation
    - No dependency on training pipeline at runtime
    - Works with any two teams, not just scheduled games
    """
    
    def __init__(
        self, 
        model_path: str = 'models/logistic_regression.pkl',
        scaler_path: str = 'models/scaler.pkl'
    ):
        """
        Load model and scaler from disk.
        
        This runs once when the API starts, not per request.
        """
        model_path = Path(model_path)
        scaler_path = Path(scaler_path)
        
        # Validate files exist
        if not model_path.exists():
            raise FileNotFoundError(f"Model not found at {model_path}")
        if not scaler_path.exists():
            raise FileNotFoundError(f"Scaler not found at {scaler_path}")
        
        # Load model (the sklearn CalibratedClassifierCV object)
        self.model = joblib.load(model_path)
        logger.info(f"Loaded model from {model_path}")
        
        # Load scaler and feature names
        scaler_data = joblib.load(scaler_path)
        self.scaler = scaler_data['scaler']
        self.feature_names = scaler_data['feature_names']
        logger.info(f"Loaded scaler with {len(self.feature_names)} features")
    
    def get_latest_rolling_stats(self, db: Session, team_id: int) -> Optional[Dict]:
        """
        Get the most recent rolling stats for a team.
        
        Unlike WeeklyPredictor._get_latest_rolling_stats(), this doesn't 
        require year/week — it just gets the newest stats available.
        
        Returns None if team has no stats (new team, no games played yet).
        """
        query = text("""
            SELECT trs.*
            FROM team_rolling_stats trs
            JOIN games g ON trs.game_id = g.id
            WHERE trs.team_id = :team_id
            ORDER BY g.year DESC, g.week DESC
            LIMIT 1
        """)
        
        result = db.execute(query, {'team_id': team_id})
        row = result.fetchone()
        
        if row:
            return dict(row._mapping)
        
        logger.warning(f"No rolling stats found for team {team_id}")
        return None
    
    def get_team_info(self, db: Session, team_id: int) -> Optional[Dict]:
        """Fetch basic team info for response."""
        query = text("""
            SELECT id, name, slug, conference
            FROM teams
            WHERE id = :team_id
        """)
        result = db.execute(query, {'team_id': team_id})
        row = result.fetchone()
        
        if row:
            return dict(row._mapping)
        return None
    
    def _safe_get(self, stats: Dict, key: str, default: float = 0.0) -> float:
        """Safely extract numeric value from stats dict."""
        value = stats.get(key, default)
        return default if value is None else float(value)
    
    def _build_features(self, home_stats: Dict, away_stats: Dict) -> np.ndarray:
        """
        Build the feature vector for a matchup.
        
        This must match EXACTLY what data_prep.py produces during training.
        The feature order is determined by self.feature_names (from scaler.pkl).
        """
        features = {}
        
        # ---- Base feature names (what's stored in team_rolling_stats) ----
        # These come from data_prep.py's feature_groups
        base_features = [
            # offensive_efficiency
            'ppg_3wk', 'ppg_5wk', 'ypp_3wk', 'ypp_5wk', 'third_down_pct_3wk',
            'total_yards_3wk', 'total_yards_5wk',
            # defensive_efficiency
            'papg_3wk', 'papg_5wk', 'opp_ypp_3wk', 'opp_ypp_5wk', 
            'opp_third_down_pct_3wk', 'opp_total_yards_3wk', 'opp_total_yards_5wk',
            # overall_performance
            'margin_3wk', 'margin_5wk', 'win_streak', 'last_3_wins', 'last_5_wins',
            # consistency
            'ppg_std_3wk', 'margin_std_3wk',
            # trends
            'ppg_trend', 'margin_trend', 'defensive_trend', 'ypp_trend',
            # turnovers
            'turnover_diff_3wk', 'turnover_diff_5wk',
            # style
            'pass_ratio_3wk', 'pass_ratio_5wk', 'opp_pass_ratio_3wk', 'opp_pass_ratio_5wk',
            # context
            'sos_3wk', 'sos_5wk', 'games_in_season', 'prev_season_games_in_3wk',
            # elo
            'current_elo', 'elo_change_3wk', 'elo_change_5wk', 
            'avg_opp_elo_3wk', 'avg_opp_elo_5wk'
        ]
        
        # ---- Home team stats (40 features) ----
        for col in base_features:
            features[f'home_{col}'] = self._safe_get(home_stats, col)
        
        # ---- Away team stats (40 features) ----
        for col in base_features:
            features[f'away_{col}'] = self._safe_get(away_stats, col)
        
        # ---- Matchup features (12 features) ----
        # These must match exactly what data_prep._add_matchup_features() creates
        
        # Offensive vs Defensive matchup
        features['ppg_vs_papg'] = (
            self._safe_get(home_stats, 'ppg_3wk') - 
            self._safe_get(away_stats, 'papg_3wk')
        )
        features['papg_vs_ppg'] = (
            self._safe_get(home_stats, 'papg_3wk') - 
            self._safe_get(away_stats, 'ppg_3wk')
        )
        
        # Overall strength differential
        features['margin_diff'] = (
            self._safe_get(home_stats, 'margin_3wk') - 
            self._safe_get(away_stats, 'margin_3wk')
        )
        features['margin_diff_5wk'] = (
            self._safe_get(home_stats, 'margin_5wk') - 
            self._safe_get(away_stats, 'margin_5wk')
        )
        
        # Efficiency differential
        features['ypp_diff'] = (
            self._safe_get(home_stats, 'ypp_3wk') - 
            self._safe_get(away_stats, 'ypp_3wk')
        )
        
        # Defensive efficiency (note: away - home, not home - away)
        features['def_ypp_diff'] = (
            self._safe_get(away_stats, 'opp_ypp_3wk') - 
            self._safe_get(home_stats, 'opp_ypp_3wk')
        )
        
        # Momentum differential
        features['streak_diff'] = (
            self._safe_get(home_stats, 'win_streak') - 
            self._safe_get(away_stats, 'win_streak')
        )
        features['trend_diff'] = (
            self._safe_get(home_stats, 'margin_trend') - 
            self._safe_get(away_stats, 'margin_trend')
        )
        
        # Turnover battle
        features['turnover_diff'] = (
            self._safe_get(home_stats, 'turnover_diff_3wk') - 
            self._safe_get(away_stats, 'turnover_diff_3wk')
        )
        
        # Experience differential
        features['experience_diff'] = (
            self._safe_get(home_stats, 'games_in_season') - 
            self._safe_get(away_stats, 'games_in_season')
        )
        
        # Strength of schedule differential
        features['sos_diff'] = (
            self._safe_get(home_stats, 'sos_3wk', 0.5) - 
            self._safe_get(away_stats, 'sos_3wk', 0.5)
        )
        
        # Style matchup
        features['style_contrast'] = abs(
            self._safe_get(home_stats, 'pass_ratio_3wk', 0.5) - 
            self._safe_get(away_stats, 'pass_ratio_3wk', 0.5)
        )
        
        # ---- Build DataFrame in correct feature order ----
        df = pd.DataFrame([features])
        
        # Ensure all expected features exist (fill missing with 0)
        for feature in self.feature_names:
            if feature not in df.columns:
                logger.warning(f"Missing feature: {feature}, filling with 0")
                df[feature] = 0
        
        # Select in exact order model expects
        df = df[self.feature_names]
        
        # Apply normalization
        scaled = self.scaler.transform(df.values)
        
        return scaled[0]
    
    def predict(
        self, 
        db: Session, 
        home_team_id: int, 
        away_team_id: int
    ) -> Dict:
        """
        Predict the outcome of a matchup between any two teams.
        
        Returns a dict with:
        - home_team: team info
        - away_team: team info  
        - home_win_prob: float 0-1
        - away_win_prob: float 0-1
        - predicted_winner: "home" or "away"
        - confidence: float 0.5-1.0
        - error: str if something went wrong
        """
        # Get team info
        home_team = self.get_team_info(db, home_team_id)
        away_team = self.get_team_info(db, away_team_id)
        
        if not home_team:
            return {'error': f'Home team {home_team_id} not found'}
        if not away_team:
            return {'error': f'Away team {away_team_id} not found'}
        
        # Get rolling stats
        home_stats = self.get_latest_rolling_stats(db, home_team_id)
        away_stats = self.get_latest_rolling_stats(db, away_team_id)
        
        if not home_stats:
            return {
                'error': f"No stats available for {home_team['name']}",
                'home_team': home_team,
                'away_team': away_team
            }
        if not away_stats:
            return {
                'error': f"No stats available for {away_team['name']}",
                'home_team': home_team,
                'away_team': away_team
            }
        
        # Build features and predict
        features = self._build_features(home_stats, away_stats)
        home_win_prob = float(self.model.predict_proba([features])[0][1])
        away_win_prob = 1 - home_win_prob
        
        predicted_winner = "home" if home_win_prob > 0.5 else "away"
        confidence = max(home_win_prob, away_win_prob)
        
        return {
            'home_team': home_team,
            'away_team': away_team,
            'home_win_prob': round(home_win_prob, 4),
            'away_win_prob': round(away_win_prob, 4),
            'predicted_winner': predicted_winner,
            'confidence': round(confidence, 4)
        }