"""
weekly_predictor.py - Generate predictions for upcoming week

CORRECTED VERSION - All bugs fixed, reuses existing components
"""

import joblib
import logging
from pathlib import Path
from typing import Dict, List, Optional
import numpy as np
import pandas as pd
from sqlalchemy import text

from src.database.teams_model import Team
from src.database.predictions_model import Prediction
from src.database.connection import DatabaseConnection
from src.models.data_prep import GameDataPrep
from src.pipeline.ncaa_api_client import NCAAAPIClient
from src.pipeline.team_manager import TeamManager
from src.database.predictions_model import Prediction

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WeeklyPredictor:
    """
    Generates predictions for upcoming games.
    Reuses existing pipeline components for team resolution and feature creation.
    """
    
    def __init__(self, model_path: str = 'models/logistic_regression.pkl'):
        """Initialize predictor with trained model and existing components."""
        self.model_path = Path(model_path)
        
        if not self.model_path.exists():
            raise FileNotFoundError(
                f"Model not found at {model_path}. "
                "Train a model first using: python -m src.models.train_logistic_baseline"
            )
        
        # Load trained model (should be just the sklearn model)
        try:
            self.model = joblib.load(self.model_path)
            logger.info(f"Loaded model from {model_path}")
        except Exception as e:
            raise RuntimeError(f"Failed to load model: {e}")
        
        # Initialize components
        self.db = DatabaseConnection()
        self.prep = GameDataPrep(self.db)
        self.api_client = NCAAAPIClient()
        self.team_manager = TeamManager(self.db)
        
        # Load and store the scaler from training (do this once, not per prediction)
        logger.info("Loading training scaler for feature normalization...")
        train_data = self.prep.prepare_full_pipeline(
            start_year=2022, end_year=2023,
            min_week=4, target_type='home_win',
            test_size=0.2, normalize=True
        )
        self.scaler = train_data['scaler']
        self.feature_names = self.prep.get_feature_names()
        
        logger.info(f"Predictor ready. Expecting {len(self.feature_names)} features")
    
    def get_upcoming_games(self, year: int, week: int, include_played: bool = False) -> List[Dict]:
        """
        Get games for prediction using existing API client.

        Args:
            year: Season year
            week: Week number
            include_played: If True, include games that have already been played
                          (useful for backtesting/evaluation)

        Returns:
            List of games ready for prediction
        """
        logger.info(f"Fetching games for {year} Week {week} (include_played={include_played})")

        # Use existing API client
        week_data = self.api_client.get_week_games(year, week)

        if not week_data['success']:
            logger.warning(f"Failed to fetch games for {year} Week {week}")
            return []

        upcoming_games = []

        with self.db.get_session() as session:
            # Check which games are already in database with scores
            played_query = text("""
                SELECT contest_id FROM games
                WHERE year = :year AND week = :week
                    AND home_score IS NOT NULL
            """)
            played = session.execute(played_query, {'year': year, 'week': week})
            played_ids = {str(row[0]) for row in played}

            # Process games
            for game in week_data['games']:
                contest_id = str(game.get('contestId'))

                # Skip played games only if include_played is False
                if not include_played and contest_id in played_ids:
                    continue
                
                # Extract teams
                teams = game.get('teams', [])
                home_team = next((t for t in teams if t.get('isHome')), None)
                away_team = next((t for t in teams if not t.get('isHome')), None)
                
                if not home_team or not away_team:
                    logger.warning(f"Skipping game {contest_id} - missing team data")
                    continue
                
                # Use TeamManager to resolve IDs (handles team creation if needed)
                home_id, _ = self.team_manager.find_or_create_team(session, home_team)
                away_id, _ = self.team_manager.find_or_create_team(session, away_team)
                
                upcoming_games.append({
                    'contest_id': contest_id,
                    'game_date': game.get('startDate'),
                    'home_team_id': home_id,
                    'away_team_id': away_id,
                    'home_team_name': home_team.get('nameShort'),
                    'away_team_name': away_team.get('nameShort')
                })
        
        logger.info(f"Found {len(upcoming_games)} upcoming games")
        return upcoming_games
    
    def _get_latest_rolling_stats(self, team_id: int, year: int, week: int) -> Optional[Dict]:
        """
        Get the most recent rolling stats for a team.
        Looks for stats from the game just before the prediction week.
        """
        with self.db.get_session() as session:
            query = text("""
                SELECT trs.*
                FROM team_rolling_stats trs
                JOIN games g ON trs.game_id = g.id
                WHERE trs.team_id = :team_id
                    AND g.year = :year
                    AND g.week < :week
                ORDER BY g.week DESC
                LIMIT 1
            """)
            
            result = session.execute(query, {
                'team_id': team_id, 
                'year': year, 
                'week': week
            })
            
            row = result.fetchone()
            
            if row:
                return dict(row._mapping)
            
            # Early season fallback - try previous year
            if week <= 4:
                logger.info(f"Early season - checking {year-1} for team {team_id}")
                query_prev = text("""
                    SELECT trs.*
                    FROM team_rolling_stats trs
                    JOIN games g ON trs.game_id = g.id
                    WHERE trs.team_id = :team_id
                        AND g.year = :year
                    ORDER BY g.week DESC
                    LIMIT 1
                """)
                
                result_prev = session.execute(query_prev, {
                    'team_id': team_id, 
                    'year': year - 1
                })
                
                row_prev = result_prev.fetchone()
                if row_prev:
                    return dict(row_prev._mapping)
            
            logger.warning(f"No rolling stats found for team {team_id} before {year} Week {week}")
            return None
    
    def _build_prediction_features(self, home_stats: Dict, away_stats: Dict) -> np.ndarray:
        """
        Build feature vector matching training format.
        Calculates all matchup features and applies normalization.
        Handles None values in rolling stats.
        """
        
        # Helper function to safely get numeric values
        def safe_get(stats_dict: Dict, key: str, default: float = 0.0) -> float:
            """Get value from dict, converting None to default."""
            value = stats_dict.get(key, default)
            return default if value is None else float(value)
        
        features_dict = {}
        
        # Add all home team rolling stats
        for feature in self.prep.all_features:
            features_dict[f'home_{feature}'] = safe_get(home_stats, feature, 0)
        
        # Add all away team rolling stats  
        for feature in self.prep.all_features:
            features_dict[f'away_{feature}'] = safe_get(away_stats, feature, 0)
        
        # Calculate matchup features (same as data_prep._add_matchup_features)
        # Using safe_get to handle None values
        features_dict['ppg_vs_papg'] = (
            safe_get(home_stats, 'ppg_3wk') - safe_get(away_stats, 'papg_3wk')
        )
        features_dict['papg_vs_ppg'] = (
            safe_get(home_stats, 'papg_3wk') - safe_get(away_stats, 'ppg_3wk')
        )
        features_dict['margin_diff'] = (
            safe_get(home_stats, 'margin_3wk') - safe_get(away_stats, 'margin_3wk')
        )
        features_dict['margin_diff_5wk'] = (
            safe_get(home_stats, 'margin_5wk') - safe_get(away_stats, 'margin_5wk')
        )
        features_dict['ypp_diff'] = (
            safe_get(home_stats, 'ypp_3wk') - safe_get(away_stats, 'ypp_3wk')
        )
        features_dict['def_ypp_diff'] = (
            safe_get(away_stats, 'opp_ypp_3wk') - safe_get(home_stats, 'opp_ypp_3wk')
        )
        features_dict['streak_diff'] = (
            safe_get(home_stats, 'win_streak') - safe_get(away_stats, 'win_streak')
        )
        features_dict['trend_diff'] = (
            safe_get(home_stats, 'margin_trend') - safe_get(away_stats, 'margin_trend')
        )
        features_dict['turnover_diff'] = (
            safe_get(home_stats, 'turnover_diff_3wk') - 
            safe_get(away_stats, 'turnover_diff_3wk')
        )
        features_dict['experience_diff'] = (
            safe_get(home_stats, 'games_in_season') - 
            safe_get(away_stats, 'games_in_season')
        )
        features_dict['sos_diff'] = (
            safe_get(home_stats, 'sos_3wk', 0.5) - safe_get(away_stats, 'sos_3wk', 0.5)
        )
        features_dict['style_contrast'] = abs(
            safe_get(home_stats, 'pass_ratio_3wk', 0.5) - 
            safe_get(away_stats, 'pass_ratio_3wk', 0.5)
        )
        
        # Convert to DataFrame in correct order
        df = pd.DataFrame([features_dict])
        
        # Ensure all expected features exist
        for feature in self.feature_names:
            if feature not in df.columns:
                df[feature] = 0
        
        # Select features in correct order
        df = df[self.feature_names]
        
        # Apply normalization
        features_scaled = self.scaler.transform(df.values)
        
        return features_scaled[0]
    
    def predict_game(self, home_team_id: int, away_team_id: int, 
                year: int, week: int) -> Dict:
        """
        Generate prediction for a single game.
        
        Returns dict with prediction details or error message.
        """
        # Helper function to safely get numeric values
        def safe_get(stats_dict: Dict, key: str, default: float = 0.0) -> float:
            """Get value from dict, converting None to default."""
            value = stats_dict.get(key, default)
            return default if value is None else float(value)
        
        with self.db.get_session() as session:
            # Get team names first
            home_team = self.team_manager.get_team_by_id(session, home_team_id)
            away_team = self.team_manager.get_team_by_id(session, away_team_id)
            
            home_name = home_team.name if home_team else 'Unknown'
            away_name = away_team.name if away_team else 'Unknown'
            
            # Get rolling stats for both teams
            home_stats = self._get_latest_rolling_stats(home_team_id, year, week)
            away_stats = self._get_latest_rolling_stats(away_team_id, year, week)
            
            if not home_stats or not away_stats:
                return {
                    'home_team': home_name,
                    'away_team': away_name,
                    'predicted_winner': 'UNAVAILABLE',
                    'confidence': 0.0,
                    'error': 'Missing rolling stats for one or both teams'
                }
            
            # Build features
            features = self._build_prediction_features(home_stats, away_stats)
            
            # Make prediction
            home_win_prob = self.model.predict_proba([features])[0][1]
            
            # Determine winner
            if home_win_prob > 0.5:
                predicted_winner = home_name
                confidence = home_win_prob
            else:
                predicted_winner = away_name
                confidence = 1 - home_win_prob
            
            # Extract key statistical advantages (using safe_get)
            advantages = []
            
            margin_diff = safe_get(home_stats, 'margin_3wk') - safe_get(away_stats, 'margin_3wk')
            if abs(margin_diff) > 3:
                advantages.append(f"margin: {margin_diff:+.1f}")
            
            ypp_diff = safe_get(home_stats, 'ypp_3wk') - safe_get(away_stats, 'ypp_3wk')
            if abs(ypp_diff) > 0.5:
                advantages.append(f"YPP: {ypp_diff:+.1f}")
            
            streak_diff = safe_get(home_stats, 'win_streak') - safe_get(away_stats, 'win_streak')
            if abs(streak_diff) >= 2:
                advantages.append(f"streak: {int(streak_diff):+d}")
            
            return {
                'home_team': home_name,
                'away_team': away_name,
                'predicted_winner': predicted_winner,
                'confidence': confidence,
                'home_win_prob': home_win_prob,
                'away_win_prob': 1 - home_win_prob,
                'key_advantages': advantages
            }
    
    def predict_week(self, year: int, week: int, include_played: bool = False) -> List[Dict]:
        """
        Generate predictions for all games in a week.

        Args:
            year: Season year
            week: Week number
            include_played: If True, include already-played games (for backtesting)

        Returns:
            List of predictions
        """
        print(f"\nGenerating predictions for {year} Week {week}")
        if include_played:
            print("(Backtesting mode: including already-played games)")
        print("="*60)

        games = self.get_upcoming_games(year, week, include_played=include_played)
        
        if not games:
            print(f"No upcoming games found for Week {week}")
            return []
        
        predictions = []
        for game in games:
            prediction = self.predict_game(
                game['home_team_id'],
                game['away_team_id'],
                year, week
            )
            prediction['game_date'] = game['game_date']
            prediction['year'] = year  # ADD THIS
            prediction['week'] = week  # ADD THIS
            predictions.append(prediction)
    
        predictions.sort(key=lambda x: x['confidence'], reverse=True)
        return predictions
    
    def format_predictions(self, predictions: List[Dict]) -> None:
        """Display predictions in user-friendly format."""
        if not predictions:
            print("No predictions to display")
            return
        
        print(f"\nPREDICTIONS ({len(predictions)} games)")
        print("="*60)
        
        for i, pred in enumerate(predictions, 1):
            if pred.get('error'):
                print(f"\n{i}. {pred['away_team']} @ {pred['home_team']}")
                print(f"   ERROR: {pred['error']}")
                continue
            
            print(f"\n{i}. {pred['away_team']} @ {pred['home_team']}")
            print(f"   Winner: {pred['predicted_winner']} ({pred['confidence']*100:.1f}% confidence)")
            print(f"   Home win prob: {pred['home_win_prob']*100:.1f}%")
            print(f"   Away win prob: {pred['away_win_prob']*100:.1f}%")
            
            if pred['key_advantages']:
                print(f"   Key factors: {', '.join(pred['key_advantages'])}")

    def save_predictions_to_db(self, predictions: List[Dict], 
                           model_name: str = 'logistic_baseline',
                           model_version: str = 'v1') -> int:
        """Save predictions to database."""
        saved = 0
        
        with self.db.get_session() as session:
            for pred in predictions:
                if pred.get('error'):
                    continue
                
                home_team = session.query(Team).filter(Team.name == pred['home_team']).first()
                away_team = session.query(Team).filter(Team.name == pred['away_team']).first()
                
                if not home_team or not away_team:
                    continue
                
                predicted_winner_id = (home_team.id if pred['home_win_prob'] > 0.5 
                                      else away_team.id)
                
                # Convert numpy types to Python types
                home_win_prob = float(pred['home_win_prob'])
                
                Prediction.save_prediction(
                    session=session,
                    model_name=model_name,
                    model_version=model_version,
                    year=pred.get('year', 2025),
                    week=pred.get('week', 6),
                    home_team_id=home_team.id,
                    away_team_id=away_team.id,
                    predicted_winner_id=predicted_winner_id,
                    home_win_prob=home_win_prob,  # Now a Python float
                    key_factors=', '.join(pred.get('key_advantages', []))
                )
                saved += 1
            
            session.commit()
        
        logger.info(f"Saved {saved} predictions to database")
        return saved