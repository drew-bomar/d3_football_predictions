"""
data_prep.py - Prepare D3 football data for machine learning

This module extracts data from PostgreSQL and transforms it into 
the format needed by scikit-learn (feature matrices and target vectors).
"""

import numpy as np
import pandas as pd
from typing import Tuple, Optional, List, Dict
import logging
from datetime import datetime
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

from src.database.connection import DatabaseConnection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GameDataPrep:
    """
    Transforms database records into ML-ready feature matrices.
    
    Key responsibilities:
    1. Extract rolling stats for both teams in each game
    2. Combine into feature vectors
    3. Create target variables (win/loss, spread, total)
    4. Handle missing data appropriately
    5. Create train/test splits properly (no data leakage)
    """
    
    def __init__(self, db_connection: Optional[DatabaseConnection] = None):
        """Initialize with database connection."""
        self.db = db_connection or DatabaseConnection()
        
        # Define feature groups for organization and understanding
        self.feature_groups = {
            'offensive_efficiency': [
                'ppg_3wk', 'ppg_5wk',              # Points per game
                'ypp_3wk', 'ypp_5wk',              # Yards per play
                'third_down_pct_3wk',              # 3rd down conversion %
                'total_yards_3wk', 'total_yards_5wk'  # Total yards per game
            ],
            'defensive_efficiency': [
                'papg_3wk', 'papg_5wk',            # Points allowed per game
                'opp_ypp_3wk', 'opp_ypp_5wk',      # Opponent yards per play
                'opp_third_down_pct_3wk',          # Opponent 3rd down %
                'opp_total_yards_3wk', 'opp_total_yards_5wk'
            ],
            'overall_performance': [
                'margin_3wk', 'margin_5wk',        # Point differential
                'win_streak',                      # Current streak
                'last_3_wins', 'last_5_wins'       # Recent wins
            ],
            'consistency': [
                'ppg_std_3wk',                     # Scoring consistency
                'margin_std_3wk'                   # Outcome consistency
            ],
            'trends': [
                'ppg_trend',                       # Scoring trend
                'margin_trend',                    # Margin trend
                'defensive_trend',                 # Defensive trend (negative is better)
                'ypp_trend'                        # Efficiency trend
            ],
            'turnovers': [
                'turnover_diff_3wk', 'turnover_diff_5wk'  # Turnover margin
            ],
            'style': [
                'pass_ratio_3wk', 'pass_ratio_5wk',       # Offensive style
                'opp_pass_ratio_3wk', 'opp_pass_ratio_5wk' # Defensive style faced
            ],
            'context': [
                'sos_3wk', 'sos_5wk',              # Strength of schedule
                'games_in_season',                 # Experience in current season
                'prev_season_games_in_3wk'         # Using previous season data?
            ],
            'elo': [
                'current_elo',                     # Team's current ELO rating
                'elo_change_3wk', 'elo_change_5wk', # ELO momentum
                'avg_opp_elo_3wk', 'avg_opp_elo_5wk' # True strength of schedule
            ]
        }
        
        # Flatten all features into single list
        self.all_features = []
        for group_features in self.feature_groups.values():
            self.all_features.extend(group_features)
        
        # Remove duplicates while preserving order
        self.all_features = list(dict.fromkeys(self.all_features))
        
        logger.info(f"Initialized with {len(self.all_features)} features per team")
        logger.info(f"Feature groups: {list(self.feature_groups.keys())}")
        
        # Store scaler for feature normalization
        self.scaler = None
        
    def fetch_game_data(self, 
                       start_year: int = 2022, 
                       end_year: int = 2023,
                       min_week: int = 4) -> pd.DataFrame:
        """
        Fetch games with rolling stats for both teams.
        
        Args:
            start_year: First year to include
            end_year: Last year to include  
            min_week: Minimum week (to ensure enough rolling data)
                     Week 4+ means all teams have 3 games of history
        
        Returns:
            DataFrame with game info and rolling stats for both teams
        """
        
        # Build dynamic column selection for all features
        home_columns = [f"home_stats.{feat} as home_{feat}" for feat in self.all_features]
        away_columns = [f"away_stats.{feat} as away_{feat}" for feat in self.all_features]
        
        # Use %(param)s syntax for pandas read_sql
        query = f"""
        SELECT 
            g.id as game_id,
            g.year,
            g.week,
            g.home_team_id,
            g.away_team_id,
            g.home_score,
            g.away_score,
            
            -- Home team rolling stats
            {', '.join(home_columns)},
            
            -- Away team rolling stats
            {', '.join(away_columns)}
            
        FROM games g
        INNER JOIN team_rolling_stats home_stats 
            ON g.id = home_stats.game_id 
            AND g.home_team_id = home_stats.team_id
        INNER JOIN team_rolling_stats away_stats 
            ON g.id = away_stats.game_id 
            AND g.away_team_id = away_stats.team_id
        WHERE 
            g.year BETWEEN %(start_year)s AND %(end_year)s
            AND g.week >= %(min_week)s
            AND g.home_score IS NOT NULL
            AND home_stats.games_in_season >= 2
            AND away_stats.games_in_season >= 2
        ORDER BY g.year, g.week, g.id
        """
        
        with self.db.get_session() as session:
            df = pd.read_sql(query, session.bind, params={
                'start_year': start_year,
                'end_year': end_year,
                'min_week': min_week
            })
        
        logger.info(f"Fetched {len(df)} games from {start_year}-{end_year}, weeks {min_week}+")
        
        # Add derived features (differences between teams)
        df = self._add_matchup_features(df)
        
        return df
    
    def _add_matchup_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add features that compare the two teams directly.
        These "matchup" features often have strong predictive power.
        """
        # Offensive vs Defensive matchup (handle NaN values)
        df['ppg_vs_papg'] = df['home_ppg_3wk'].fillna(0) - df['away_papg_3wk'].fillna(0)
        df['papg_vs_ppg'] = df['home_papg_3wk'].fillna(0) - df['away_ppg_3wk'].fillna(0)
        
        # Overall strength differential
        df['margin_diff'] = df['home_margin_3wk'].fillna(0) - df['away_margin_3wk'].fillna(0)
        df['margin_diff_5wk'] = df['home_margin_5wk'].fillna(0) - df['away_margin_5wk'].fillna(0)
        
        # Efficiency differential
        df['ypp_diff'] = df['home_ypp_3wk'].fillna(0) - df['away_ypp_3wk'].fillna(0)
        
        # Defensive efficiency (check if columns exist first)
        if 'home_opp_ypp_3wk' in df.columns and 'away_opp_ypp_3wk' in df.columns:
            df['def_ypp_diff'] = df['away_opp_ypp_3wk'].fillna(0) - df['home_opp_ypp_3wk'].fillna(0)
        else:
            df['def_ypp_diff'] = 0
        
        # Momentum differential
        df['streak_diff'] = df['home_win_streak'].fillna(0) - df['away_win_streak'].fillna(0)
        df['trend_diff'] = df['home_margin_trend'].fillna(0) - df['away_margin_trend'].fillna(0)
        
        # Turnover battle
        df['turnover_diff'] = df['home_turnover_diff_3wk'].fillna(0) - df['away_turnover_diff_3wk'].fillna(0)
        
        # Experience differential (games played this season)
        df['experience_diff'] = df['home_games_in_season'].fillna(0) - df['away_games_in_season'].fillna(0)
        
        # Strength of schedule differential
        df['sos_diff'] = df['home_sos_3wk'].fillna(0.5) - df['away_sos_3wk'].fillna(0.5)
        
        # Style matchup (do opposites clash?)
        df['style_contrast'] = abs(df['home_pass_ratio_3wk'].fillna(0.5) - df['away_pass_ratio_3wk'].fillna(0.5))
        
        # Count actual matchup features created
        matchup_features = [col for col in df.columns if 'diff' in col or 'vs' in col or 'contrast' in col]
        logger.info(f"Added {len(matchup_features)} matchup features")
        
        return df
    
    def create_feature_matrix(self, df: pd.DataFrame) -> np.ndarray:
        """
        Convert DataFrame to feature matrix (X) for ML.
        
        Args:
            df: DataFrame with game data
            
        Returns:
            2D NumPy array with shape (n_games, n_features)
        """
        # Select feature columns (all home_, away_, and matchup features)
        feature_cols = []
        
        # Team-specific features
        for feat in self.all_features:
            feature_cols.append(f'home_{feat}')
            feature_cols.append(f'away_{feat}')
        
        # Matchup features
        matchup_cols = [
            'ppg_vs_papg', 'papg_vs_ppg', 'margin_diff', 'margin_diff_5wk',
            'ypp_diff', 'def_ypp_diff', 'streak_diff', 'trend_diff',
            'turnover_diff', 'experience_diff', 'sos_diff', 'style_contrast'
        ]
        feature_cols.extend(matchup_cols)
        
        # Extract features, handling any missing columns gracefully
        available_cols = [col for col in feature_cols if col in df.columns]
        missing_cols = [col for col in feature_cols if col not in df.columns]
        
        if missing_cols:
            logger.warning(f"Missing {len(missing_cols)} features: {missing_cols[:5]}...")
        
        # Get the data and convert to float explicitly
        X = df[available_cols].values.astype(float)
        
        # Handle missing values (NaN -> 0 for now, could be more sophisticated)
        X = np.nan_to_num(X, nan=0.0)
        
        logger.info(f"Created feature matrix: {X.shape} ({X.shape[0]} games, {X.shape[1]} features)")
        
        return X, available_cols
    
    def create_targets(self, df: pd.DataFrame) -> Dict[str, np.ndarray]:
        """
        Create various target variables for different prediction tasks.
        
        Args:
            df: DataFrame with game data
            
        Returns:
            Dictionary with different target arrays:
            - 'home_win': Binary (1 if home team won)
            - 'point_spread': Home score - Away score
            - 'total_points': Combined score
            - 'over_45': Binary (1 if total > 45)
        """
        targets = {
            'home_win': (df['home_score'] > df['away_score']).astype(int).values,
            'point_spread': (df['home_score'] - df['away_score']).values,
            'total_points': (df['home_score'] + df['away_score']).values,
            'over_45': ((df['home_score'] + df['away_score']) > 45).astype(int).values
        }
        
        logger.info(f"Created {len(targets)} target variables")
        logger.info(f"Home win rate: {targets['home_win'].mean():.1%}")
        logger.info(f"Avg spread: {targets['point_spread'].mean():.1f}")
        logger.info(f"Avg total: {targets['total_points'].mean():.1f}")
        
        return targets
    
    def prepare_train_test_split(self,
                                X: np.ndarray,
                                y: np.ndarray,
                                df: pd.DataFrame,
                                test_size: float = 0.2,
                                chronological: bool = True,
                                test_year: Optional[int] = None) -> Tuple:
        """
        Split data into training and test sets.

        Args:
            X: Feature matrix
            y: Target array
            df: Original DataFrame (for chronological splitting)
            test_size: Fraction of data for testing (ignored if test_year provided)
            chronological: If True, use time-based split (recommended!)
                          If False, use random split
            test_year: Specific year to use for testing (e.g., 2024).
                      If provided, trains on all years before this.

        Returns:
            X_train, X_test, y_train, y_test, train_indices, test_indices
        """
        if chronological:
            if test_year is not None:
                # Year-based split: train on all years before test_year
                train_mask = df['year'] < test_year
                test_mask = df['year'] >= test_year

                train_indices = np.where(train_mask)[0]
                test_indices = np.where(test_mask)[0]

                X_train = X[train_indices]
                X_test = X[test_indices]
                y_train = y[train_indices]
                y_test = y[test_indices]

                logger.info(f"Year-based split: {len(X_train)} train, {len(X_test)} test")
                logger.info(f"Train years: {df.iloc[train_indices]['year'].unique()}")
                logger.info(f"Test year: {test_year}")
            else:
                # Time-based split by percentage: train on earlier games, test on later
                # This prevents "data leakage" from future games
                split_point = int(len(X) * (1 - test_size))

                X_train = X[:split_point]
                X_test = X[split_point:]
                y_train = y[:split_point]
                y_test = y[split_point:]

                train_indices = np.arange(split_point)
                test_indices = np.arange(split_point, len(X))

                logger.info(f"Chronological split: {len(X_train)} train, {len(X_test)} test")
                logger.info(f"Train period: {df.iloc[:split_point]['year'].min()}-{df.iloc[:split_point]['year'].max()}")
                logger.info(f"Test period: {df.iloc[split_point:]['year'].min()}-{df.iloc[split_point:]['year'].max()}")

        else:
            # Random split (not recommended for time series!)
            X_train, X_test, y_train, y_test, train_indices, test_indices = \
                train_test_split(X, y, np.arange(len(X)),
                               test_size=test_size,
                               random_state=42)

            logger.info(f"Random split: {len(X_train)} train, {len(X_test)} test")

        return X_train, X_test, y_train, y_test, train_indices, test_indices
    
    def normalize_features(self, X_train: np.ndarray, X_test: np.ndarray) -> Tuple:
        """
        Normalize features to have zero mean and unit variance.
        This helps many ML algorithms perform better.
        
        Args:
            X_train: Training features
            X_test: Test features
            
        Returns:
            X_train_scaled, X_test_scaled, scaler
        """
        self.scaler = StandardScaler()
        
        # Fit scaler on training data ONLY (prevents data leakage)
        X_train_scaled = self.scaler.fit_transform(X_train)
        
        # Apply same transformation to test data
        X_test_scaled = self.scaler.transform(X_test)
        
        logger.info("Features normalized (zero mean, unit variance)")
        
        return X_train_scaled, X_test_scaled, self.scaler
    
    def get_feature_names(self) -> List[str]:
        """Get list of all feature names in order."""
        feature_names = []
        
        # Team-specific features
        for feat in self.all_features:
            feature_names.append(f'home_{feat}')
            feature_names.append(f'away_{feat}')
        
        # Matchup features
        feature_names.extend([
            'ppg_vs_papg', 'papg_vs_ppg', 'margin_diff', 'margin_diff_5wk',
            'ypp_diff', 'def_ypp_diff', 'streak_diff', 'trend_diff',
            'turnover_diff', 'experience_diff', 'sos_diff', 'style_contrast'
        ])
        
        return feature_names
    
    def prepare_full_pipeline(self,
                            start_year: int = 2022,
                            end_year: int = 2024,
                            min_week: int = 4,
                            target_type: str = 'home_win',
                            test_size: float = 0.2,
                            normalize: bool = True,
                            test_year: Optional[int] = None) -> Dict:
        """
        Complete pipeline from database to ML-ready data.
        This is the main method you'll use.

        Args:
            start_year: First year of data
            end_year: Last year of data (inclusive)
            min_week: Minimum week number
            target_type: Which target to predict ('home_win', 'point_spread', etc.)
            test_size: Fraction for test set (ignored if test_year provided)
            normalize: Whether to normalize features
            test_year: Specific year to holdout for testing (e.g., 2024).
                      Recommended for proper temporal validation.

        Returns:
            Dictionary with all prepared data and metadata
        """
        logger.info(f"Starting full pipeline for {target_type} prediction")

        # 1. Fetch data
        df = self.fetch_game_data(start_year, end_year, min_week)

        # 2. Create features
        X, feature_cols = self.create_feature_matrix(df)

        # 3. Create targets
        targets = self.create_targets(df)
        y = targets[target_type]

        # 4. Train/test split (year-based if test_year provided)
        X_train, X_test, y_train, y_test, train_idx, test_idx = \
            self.prepare_train_test_split(X, y, df, test_size=test_size,
                                         chronological=True, test_year=test_year)

        # 5. Normalize if requested
        if normalize:
            X_train, X_test, scaler = self.normalize_features(X_train, X_test)
        else:
            scaler = None

        # 6. Package everything
        result = {
            'X_train': X_train,
            'X_test': X_test,
            'y_train': y_train,
            'y_test': y_test,
            'feature_names': feature_cols,
            'scaler': scaler,
            'train_indices': train_idx,
            'test_indices': test_idx,
            'df': df,
            'target_type': target_type,
            'metadata': {
                'n_features': X.shape[1],
                'n_train': len(X_train),
                'n_test': len(X_test),
                'train_years': f"{df.iloc[train_idx]['year'].min()}-{df.iloc[train_idx]['year'].max()}",
                'test_years': f"{df.iloc[test_idx]['year'].min()}-{df.iloc[test_idx]['year'].max()}",
                'home_win_rate_train': y_train.mean() if target_type == 'home_win' else None
            }
        }

        logger.info(f"Pipeline complete: {result['metadata']}")

        return result