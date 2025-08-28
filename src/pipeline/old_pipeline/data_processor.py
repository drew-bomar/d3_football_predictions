"""
D3 Football Data Processing
Handles rolling statistics, feature engineering, and data validation for simplified scraper output

Key Functions:
- process_box_scores_to_team_games(): Convert box score data to team-game format
- calculate_rolling_stats(): Generate 3/5/7 week rolling averages with missing game handling
- engineer_features(): Create advanced metrics and efficiency calculations
- validate_data(): Quality checks and anomaly detection
- prepare_model_data(): Format data for ML models
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from typing import List, Dict, Tuple, Optional
import warnings

warnings.filterwarnings('ignore')
logger = logging.getLogger(__name__)


class D3DataProcessor:
    """
    Processes raw D3 football box score data into model-ready features with rolling statistics.
    Designed to work with SimplifiedD3Scraper output format.
    
    Attributes:
        core_stats (List[str]): Statistical fields to calculate rolling averages for
        rolling_windows (List[int]): Window sizes for rolling calculations [3, 5, 7]
        season_boundary_week (int): Maximum weeks per season to prevent cross-season rolling
    """
    
    def __init__(self):
        """
        Initialize the data processor with core statistics and rolling window configurations.
        
        Args:
            None
        
        Output:
            None (constructor)
        
        Helper Methods Called:
            None
        """
        # Core statistical fields that we calculate rolling averages for
        self.core_stats = [
            'final_score', 'first_downs', 'total_offense', 'net_yards_passing', 
            'net_yards_rushing', 'third_down_pct', 'third_down_conversions', 
            'third_down_att', 'fumbles', 'fumbles_lost', 'interceptions',
            'interception_return_yards', 'penalties_number', 'punts_number',
            'sacks_number', 'total_return_yards'
        ]
        
        # Rolling window sizes
        self.rolling_windows = [3, 5, 7]
        
        # Season boundaries (don't roll across seasons)
        self.season_boundary_week = 15  # Assume max 15 weeks per season
    
    def process_box_scores_to_team_games(self, box_scores: List[Dict]) -> pd.DataFrame:
        """
        Convert raw box score data from SimplifiedD3Scraper into team-game format where
        each game creates two rows (one per team) with opponent statistics included.
        
        Args:
            box_scores (List[Dict]): List of box score dictionaries from scraper output
        
        Output:
            pd.DataFrame: Team-game records with columns:
                - year, week, game_id, team, opponent: Game identifiers
                - stat_name: Team's statistical values (final_score, first_downs, etc.)
                - opp_stat_name: Opponent's statistical values
                - derived metrics: points_scored, points_allowed, margin, win
        
        Helper Methods Called:
            - _extract_team_game_stats(): Extracts stats for individual teams from box scores
            - _calculate_game_efficiency_metrics(): Adds derived efficiency metrics
        """
        logger.info(f"Processing {len(box_scores)} box scores to team-game format")
        
        team_games = []
        
        for box_score in box_scores:
            # Skip if parsing errors
            if box_score.get('parsing_errors'):
                logger.warning(f"Skipping box score due to errors: {box_score.get('parsing_errors')}")
                continue
            
            # Extract basic info
            year = box_score.get('year')
            week = box_score.get('week')
            game_id = box_score.get('game_id')
            
            team1 = box_score.get('team1')
            team2 = box_score.get('team2')
            
            if not all([year, week, team1, team2]):
                logger.warning(f"Missing required fields in box score: {game_id}")
                continue
            
            # Create team1's game record
            team1_game = self._extract_team_game_stats(
                box_score, 'team1', 'team2', year, week, game_id
            )
            if team1_game:
                team_games.append(team1_game)
            
            # Create team2's game record  
            team2_game = self._extract_team_game_stats(
                box_score, 'team2', 'team1', year, week, game_id
            )
            if team2_game:
                team_games.append(team2_game)
        
        df = pd.DataFrame(team_games)
        
        if len(df) > 0:
            # Sort by year, week, team for proper ordering
            df = df.sort_values(['year', 'week', 'team']).reset_index(drop=True)
            logger.info(f"Created {len(df)} team-game records")
        else:
            logger.warning("No valid team-game records created")
        
        return df
    
    def _extract_team_game_stats(self, box_score: Dict, team_prefix: str, 
                                opponent_prefix: str, year: int, week: int, 
                                game_id: str) -> Optional[Dict]:
        """
        Extract statistics for one team from a box score dictionary, including
        opponent statistics and basic derived metrics.
        
        Args:
            box_score (Dict): Complete box score data from scraper
            team_prefix (str): Prefix for team stats ('team1' or 'team2')
            opponent_prefix (str): Prefix for opponent stats ('team2' or 'team1')
            year (int): Season year
            week (int): Week number
            game_id (str): Unique game identifier
        
        Output:
            Optional[Dict]: Team game record with all stats and metadata, or None if invalid
        
        Helper Methods Called:
            - _clean_numeric_value(): Converts string values to numeric format
            - _calculate_game_efficiency_metrics(): Adds derived efficiency metrics
        """
        team_name = box_score.get(team_prefix)
        opponent_name = box_score.get(opponent_prefix)
        
        if not team_name or not opponent_name:
            return None
        
        game_stats = {
            'year': year,
            'week': week,
            'game_id': game_id,
            'team': team_name,
            'opponent': opponent_name,
            'scraped_at': box_score.get('scraped_at')
        }
        
        # Extract core stats for this team
        for stat in self.core_stats:
            team_key = f'{team_prefix}_{stat}'
            opponent_key = f'{opponent_prefix}_{stat}'
            
            # Team's stats
            if team_key in box_score:
                game_stats[stat] = self._clean_numeric_value(box_score[team_key])
            
            # Opponent's stats (prefixed with opp_)
            if opponent_key in box_score:
                game_stats[f'opp_{stat}'] = self._clean_numeric_value(box_score[opponent_key])
        
        # Add derived metrics
        game_stats['points_scored'] = game_stats.get('final_score', 0)
        game_stats['points_allowed'] = game_stats.get('opp_final_score', 0)
        game_stats['margin'] = game_stats['points_scored'] - game_stats['points_allowed']
        game_stats['win'] = 1 if game_stats['margin'] > 0 else 0
        
        # Calculate efficiency metrics
        game_stats.update(self._calculate_game_efficiency_metrics(game_stats))
        
        return game_stats
    
    def _clean_numeric_value(self, value) -> float:
        """
        Convert various string formats to numeric values, handling percentages,
        time formats, and empty values.
        
        Args:
            value: Raw value from box score (str, int, float, or None)
        
        Output:
            float: Cleaned numeric value, 0.0 if conversion fails
        
        Helper Methods Called:
            None
        """
        if pd.isna(value) or value == '':
            return 0.0
        
        if isinstance(value, (int, float)):
            return float(value)
        
        # Handle string values
        str_val = str(value).strip()
        
        # Remove percentage signs
        if str_val.endswith('%'):
            str_val = str_val[:-1]
        
        # Handle time format (MM:SS) - convert to total seconds
        if ':' in str_val:
            try:
                parts = str_val.split(':')
                if len(parts) == 2:
                    minutes, seconds = map(int, parts)
                    return minutes * 60 + seconds
            except ValueError:
                return 0.0
        
        # Try direct conversion
        try:
            return float(str_val)
        except ValueError:
            logger.warning(f"Could not convert value to numeric: {value}")
            return 0.0
    
    def _calculate_game_efficiency_metrics(self, game_stats: Dict) -> Dict:
        """
        Calculate advanced efficiency metrics for a single game including
        yards per play, third down rate, turnover differential, and offensive balance.
        
        Args:
            game_stats (Dict): Basic game statistics for one team
        
        Output:
            Dict: Additional efficiency metrics:
                - yards_per_play: Offensive efficiency
                - third_down_rate: Third down conversion percentage as decimal
                - turnover_diff: Turnovers forced minus turnovers committed
                - pass_rush_ratio: Passing yards / total yards (0.5 = balanced)
        
        Helper Methods Called:
            None
        """
        metrics = {}
        
        # Yards per play (if we have total plays)
        total_offense = game_stats.get('total_offense', 0)
        total_plays = game_stats.get('third_down_att', 0) * 3  # Rough estimate
        if total_plays > 0:
            metrics['yards_per_play'] = total_offense / total_plays
        else:
            metrics['yards_per_play'] = 0.0
        
        # Third down efficiency rate
        td_att = game_stats.get('third_down_att', 0)
        td_conv = game_stats.get('third_down_conversions', 0)
        if td_att > 0:
            metrics['third_down_rate'] = td_conv / td_att
        else:
            metrics['third_down_rate'] = 0.0
        
        # Turnover differential
        turnovers_forced = (game_stats.get('opp_fumbles_lost', 0) + 
                           game_stats.get('opp_interceptions', 0))
        turnovers_committed = (game_stats.get('fumbles_lost', 0) + 
                             game_stats.get('interceptions', 0))
        metrics['turnover_diff'] = turnovers_forced - turnovers_committed
        
        # Offensive balance (pass vs rush yards)
        pass_yards = game_stats.get('net_yards_passing', 0)
        rush_yards = game_stats.get('net_yards_rushing', 0)
        total_yards = pass_yards + rush_yards
        if total_yards > 0:
            metrics['pass_rush_ratio'] = pass_yards / total_yards
        else:
            metrics['pass_rush_ratio'] = 0.5  # Neutral
        
        return metrics
    
    def calculate_rolling_stats(self, team_games_df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate rolling averages (3, 5, 7 week) for each team with intelligent
        missing game handling that looks further back in season but never crosses seasons.
        
        Args:
            team_games_df (pd.DataFrame): Team-game records from process_box_scores_to_team_games()
        
        Output:
            pd.DataFrame: Rolling statistics with columns:
                - Base identifiers: team, year, week, game_id, opponent
                - stat_name_Nwk: Rolling averages for each window size
                - stat_name_season: Season-long averages up to current week
                - games_in_window: Number of games used for each calculation
        
        Helper Methods Called:
            - _calculate_team_rolling_stats(): Processes rolling stats for individual teams
            - _get_rolling_window_stats(): Calculates stats for specific window sizes
            - _get_season_stats(): Calculates season-long statistics
        """
        logger.info("Calculating rolling statistics")
        
        if team_games_df.empty:
            logger.warning("Empty DataFrame provided for rolling stats")
            return pd.DataFrame()
        
        # Ensure proper sorting
        df = team_games_df.sort_values(['team', 'year', 'week']).copy()
        
        rolling_data = []
        
        # Process each team separately
        for team in df['team'].unique():
            team_data = df[df['team'] == team].copy()
            team_rolling = self._calculate_team_rolling_stats(team_data)
            rolling_data.extend(team_rolling)
        
        result_df = pd.DataFrame(rolling_data)
        
        if len(result_df) > 0:
            result_df = result_df.sort_values(['year', 'week', 'team']).reset_index(drop=True)
            logger.info(f"Generated rolling stats for {len(result_df)} team-weeks")
        
        return result_df
    
    def _calculate_team_rolling_stats(self, team_data: pd.DataFrame) -> List[Dict]:
        """
        Calculate rolling statistics for a single team across all their games,
        handling missing games by looking further back in the same season.
        
        Args:
            team_data (pd.DataFrame): All games for one team, sorted by year and week
        
        Output:
            List[Dict]: Rolling statistics records for each game week
        
        Helper Methods Called:
            - _get_rolling_window_stats(): Gets stats for each rolling window
            - _get_season_stats(): Gets season-long statistics
        """
        team_rolling = []
        team_name = team_data['team'].iloc[0]
        
        for idx, row in team_data.iterrows():
            current_year = row['year']
            current_week = row['week']
            
            # Get base stats for this week
            week_stats = {
                'team': team_name,
                'year': current_year,
                'week': current_week,
                'game_id': row['game_id'],
                'opponent': row['opponent']
            }
            
            # Calculate rolling averages for each window
            for window in self.rolling_windows:
                window_stats = self._get_rolling_window_stats(
                    team_data, idx, window, current_year
                )
                
                # Add window-specific stats
                for stat_name, value in window_stats.items():
                    week_stats[f'{stat_name}_{window}wk'] = value
            
            # Calculate season-long averages (up to current week)
            season_stats = self._get_season_stats(team_data, idx, current_year)
            for stat_name, value in season_stats.items():
                week_stats[f'{stat_name}_season'] = value
            
            team_rolling.append(week_stats)
        
        return team_rolling
    
    def _get_rolling_window_stats(self, team_data: pd.DataFrame, current_idx: int, 
                                window_size: int, current_year: int) -> Dict:
        """
        Calculate rolling statistics for a specific window size, implementing
        the missing game strategy of looking further back in the same season.
        
        Args:
            team_data (pd.DataFrame): All games for one team
            current_idx (int): Index of current game
            window_size (int): Number of games to include (3, 5, or 7)
            current_year (int): Current season year to prevent cross-season rolling
        
        Output:
            Dict: Rolling averages for all core stats plus derived metrics
        
        Helper Methods Called:
            None
        """
        # Get games before current game in same season
        mask = ((team_data.index < current_idx) & 
                (team_data['year'] == current_year))
        previous_games = team_data[mask].tail(window_size)
        
        # If we don't have enough games, try to get more from earlier in season
        if len(previous_games) < window_size:
            # Get all previous games in season
            all_previous = team_data[mask]
            needed = window_size
            previous_games = all_previous.tail(needed)
        
        # Calculate averages
        if len(previous_games) == 0:
            # No previous games - return zeros
            return {stat: 0.0 for stat in self.core_stats + 
                   ['margin', 'turnover_diff', 'yards_per_play', 'third_down_rate']}
        
        # Calculate means for numeric columns
        rolling_stats = {}
        for stat in self.core_stats + ['margin', 'turnover_diff', 'yards_per_play', 'third_down_rate']:
            if stat in previous_games.columns:
                rolling_stats[stat] = previous_games[stat].mean()
            else:
                rolling_stats[stat] = 0.0
        
        # Calculate win percentage
        if 'win' in previous_games.columns:
            rolling_stats['win_pct'] = previous_games['win'].mean()
        else:
            rolling_stats['win_pct'] = 0.0
        
        # Add sample size info
        rolling_stats['games_in_window'] = len(previous_games)
        
        return rolling_stats
    
    def _get_season_stats(self, team_data: pd.DataFrame, current_idx: int, 
                         current_year: int) -> Dict:
        """
        Calculate season-long statistics up to (but not including) the current week,
        providing broader context for the rolling averages.
        
        Args:
            team_data (pd.DataFrame): All games for one team
            current_idx (int): Index of current game
            current_year (int): Current season year
        
        Output:
            Dict: Season averages for all core stats plus games_played count
        
        Helper Methods Called:
            None
        """
        # Get all games before current game in same season
        mask = ((team_data.index < current_idx) & 
                (team_data['year'] == current_year))
        season_games = team_data[mask]
        
        if len(season_games) == 0:
            return {stat: 0.0 for stat in self.core_stats + 
                   ['margin', 'turnover_diff', 'yards_per_play', 'third_down_rate', 'win_pct']}
        
        # Calculate season averages
        season_stats = {}
        for stat in self.core_stats + ['margin', 'turnover_diff', 'yards_per_play', 'third_down_rate']:
            if stat in season_games.columns:
                season_stats[stat] = season_games[stat].mean()
            else:
                season_stats[stat] = 0.0
        
        # Season win percentage
        if 'win' in season_games.columns:
            season_stats['win_pct'] = season_games['win'].mean()
        else:
            season_stats['win_pct'] = 0.0
        
        season_stats['games_played'] = len(season_games)
        
        return season_stats
    
    def validate_data(self, df: pd.DataFrame) -> Dict:
        """
        Perform comprehensive data quality validation including completeness checks,
        impossible value detection, and consistency verification.
        
        Args:
            df (pd.DataFrame): Processed data to validate (team games or rolling stats)
        
        Output:
            Dict: Validation report containing:
                - total_records: Number of records processed
                - issues: List of critical problems
                - warnings: List of potential concerns
                - summary: Key statistics about the dataset
        
        Helper Methods Called:
            None
        """
        logger.info("Validating processed data")
        
        validation_report = {
            'total_records': len(df),
            'issues': [],
            'warnings': [],
            'summary': {}
        }
        
        if df.empty:
            validation_report['issues'].append("DataFrame is empty")
            return validation_report
        
        # Check for required columns
        required_cols = ['team', 'year', 'week', 'opponent']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            validation_report['issues'].append(f"Missing required columns: {missing_cols}")
        
        # Check for impossible values
        if 'final_score' in df.columns:
            invalid_scores = df[(df['final_score'] < 0) | (df['final_score'] > 200)]
            if len(invalid_scores) > 0:
                validation_report['warnings'].append(f"Found {len(invalid_scores)} impossible scores")
        
        # Check for missing games (teams should have similar game counts)
        if 'team' in df.columns and 'year' in df.columns:
            team_game_counts = df.groupby(['team', 'year']).size()
            avg_games = team_game_counts.mean()
            teams_few_games = team_game_counts[team_game_counts < avg_games * 0.5]
            if len(teams_few_games) > 0:
                validation_report['warnings'].append(
                    f"Found {len(teams_few_games)} team-seasons with unusually few games"
                )
        
        # Summary statistics
        validation_report['summary'] = {
            'teams': df['team'].nunique() if 'team' in df.columns else 0,
            'years': df['year'].nunique() if 'year' in df.columns else 0,
            'weeks': df['week'].nunique() if 'week' in df.columns else 0,
            'avg_score': df['final_score'].mean() if 'final_score' in df.columns else 0
        }
        
        logger.info(f"Validation complete: {len(validation_report['issues'])} issues, "
                   f"{len(validation_report['warnings'])} warnings")
        
        return validation_report
    
    def prepare_model_data(self, rolling_stats_df: pd.DataFrame, 
                          prediction_week: Optional[int] = None) -> pd.DataFrame:
        """
        Transform rolling statistics into machine learning ready features including
        team strength rankings and matchup comparisons.
        
        Args:
            rolling_stats_df (pd.DataFrame): Rolling statistics from calculate_rolling_stats()
            prediction_week (Optional[int]): If specified, filter to specific week. Defaults to None.
        
        Output:
            pd.DataFrame: Model-ready data with normalized features and comparative metrics
        
        Helper Methods Called:
            - _add_team_strength_features(): Adds normalized team rankings
            - _add_matchup_features(): Adds head-to-head comparison features
        """
        logger.info("Preparing model-ready data")
        
        if rolling_stats_df.empty:
            return pd.DataFrame()
        
        # If prediction_week specified, filter to only that week
        if prediction_week:
            model_data = rolling_stats_df[rolling_stats_df['week'] == prediction_week].copy()
        else:
            model_data = rolling_stats_df.copy()
        
        # Create team strength features (normalized rankings)
        model_data = self._add_team_strength_features(model_data)
        
        # Create matchup features (team vs opponent comparisons)
        model_data = self._add_matchup_features(model_data)
        
        logger.info(f"Model data prepared: {len(model_data)} records with "
                   f"{len(model_data.columns)} features")
        
        return model_data
    
    def _add_team_strength_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add normalized team strength rankings based on key performance metrics,
        providing 0-1 scale features where 1.0 represents the best team in that week.
        
        Args:
            df (pd.DataFrame): Rolling statistics data
        
        Output:
            pd.DataFrame: Data with additional rank_norm columns for key metrics
        
        Helper Methods Called:
            None
        """
        df = df.copy()
        
        # For each week, calculate team rankings
        for week in df['week'].unique():
            week_mask = df['week'] == week
            week_data = df[week_mask]
            
            # Rank teams by key metrics (higher is better, so rank descending)
            ranking_metrics = ['final_score_5wk', 'total_offense_5wk', 'win_pct_5wk']
            
            for metric in ranking_metrics:
                if metric in week_data.columns:
                    # Rank teams (1 = best)
                    ranks = week_data[metric].rank(method='dense', ascending=False)
                    # Normalize to 0-1 scale (1 = best team)
                    max_rank = ranks.max()
                    if max_rank > 0:
                        normalized_ranks = 1 - (ranks - 1) / (max_rank - 1)
                    else:
                        normalized_ranks = 0.5
                    
                    df.loc[week_mask, f'{metric}_rank_norm'] = normalized_ranks
        
        return df
    
    def _add_matchup_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add head-to-head comparison features that measure relative team strength
        advantages in key statistical categories.
        
        Args:
            df (pd.DataFrame): Rolling statistics with team strength features
        
        Output:
            pd.DataFrame: Data with additional advantage columns for key comparisons
        
        Helper Methods Called:
            None
        """
        df = df.copy()
        
        # Create differential features (team - opponent)
        # Note: Full opponent stat merging would be implemented in pipeline_manager
        comparison_stats = ['final_score_5wk', 'total_offense_5wk', 'turnover_diff_5wk']
        
        for stat in comparison_stats:
            if stat in df.columns:
                # For now, create placeholder differential features
                # Will be enhanced when opponent data is merged
                df[f'{stat}_advantage'] = df[stat]
        
        return df


# Convenience functions for direct use
def process_week_data(box_scores: List[Dict]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Convenience function to process a week's box score data through the complete
    data processing pipeline from raw scraper output to rolling statistics.
    
    Args:
        box_scores (List[Dict]): Box score data from SimplifiedD3Scraper
    
    Output:
        Tuple[pd.DataFrame, pd.DataFrame]: (team_games, rolling_stats) DataFrames
    
    Helper Methods Called:
        - D3DataProcessor().process_box_scores_to_team_games(): Converts to team-game format
        - D3DataProcessor().calculate_rolling_stats(): Calculates rolling averages
    """
    processor = D3DataProcessor()
    
    # Convert to team-game format
    team_games = processor.process_box_scores_to_team_games(box_scores)
    
    # Calculate rolling stats
    rolling_stats = processor.calculate_rolling_stats(team_games)
    
    return team_games, rolling_stats