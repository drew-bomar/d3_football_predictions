"""
Translation layer to convert NCAA pipeline data to data_processor.py format
"""

import logging
from typing import List, Dict, Optional
import pandas as pd

logger = logging.getLogger(__name__)


class NCAAToProcessorTranslator:
    """
    Translates NCAA pipeline data format to the format expected by data_processor.py
    """
    
    def __init__(self):
        # Map NCAA stat names to data_processor expected names
        self.stat_mappings = {
            # These should already be mapped by NCAA scraper, but double-check
            'first_downs': 'first_downs',
            'total_offense': 'total_offense',
            'net_yards_passing': 'net_yards_passing',
            'net_yards_rushing': 'net_yards_rushing',
            'third_down_pct': 'third_down_pct',
            'third_down_conversions': 'third_down_conversions',
            'third_down_att': 'third_down_att',
            'fumbles': 'fumbles',
            'fumbles_lost': 'fumbles_lost',
            'interceptions': 'interceptions',
            'interception_return_yards': 'interception_return_yards',
            'penalties_number': 'penalties_number',
            'punts_number': 'punts_number',
            'sacks_number': 'sacks_number',
            'total_return_yards': 'total_return_yards',
            'final_score': 'final_score',
            
            # Add these missing fields that are needed for calculations
            'total_offense_plays': 'total_offense_plays',
            'rushing_attempts': 'rushing_attempts',
            'passing_attempts': 'passing_attempts',
            'passing_completions': 'passing_completions',
            'total_offense_avg_play': 'total_offense_avg_play',
        }
    
    def translate_team_games(self, ncaa_team_games: List[Dict]) -> pd.DataFrame:
        """
        Convert NCAA team-game records to data_processor format.
        
        Args:
            ncaa_team_games: List of team-game dictionaries from NCAA pipeline
            
        Returns:
            pd.DataFrame in the format expected by data_processor
        """
        if not ncaa_team_games:
            logger.warning("No team games to translate")
            return pd.DataFrame()
        
        translated_games = []
        
        for game in ncaa_team_games:
            try:
                translated = self._translate_single_game(game)
                if translated:
                    translated_games.append(translated)
            except Exception as e:
                logger.error(f"Error translating game {game.get('game_id')}: {e}")
                continue
        
        # Convert to DataFrame
        df = pd.DataFrame(translated_games)
        
        if len(df) > 0:
            # Ensure numeric columns are proper type
            numeric_cols = [col for col in df.columns if any(
                stat in col for stat in self.stat_mappings.keys()
            )]
            for col in numeric_cols:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            logger.info(f"Translated {len(df)} team-game records")
        
        return df
    
    def _translate_single_game(self, game: Dict) -> Optional[Dict]:
        """
        Translate a single NCAA team-game record.
        """
        # Start with required fields
        translated = {
            'year': game.get('year'),
            'week': game.get('week'),
            'game_id': game.get('game_id'),
            'team': game.get('team'),
            'opponent': game.get('opponent'),
            'scraped_at': game.get('date', game.get('scraped_at'))
        }
        
        # Check required fields
        if not all([translated['year'], translated['week'], translated['team'], translated['opponent']]):
            logger.warning(f"Missing required fields in game: {game.get('game_id')}")
            return None
        
        # Map team stats
        for processor_stat, ncaa_stat in self.stat_mappings.items():
            # Team's stat
            if ncaa_stat in game:
                translated[processor_stat] = game[ncaa_stat]
            else:
                # Try without mapping (might already be correct name)
                translated[processor_stat] = game.get(processor_stat, 0)
            
            # Opponent's stat
            opp_key = f'opp_{ncaa_stat}'
            if opp_key in game:
                translated[f'opp_{processor_stat}'] = game[opp_key]
            else:
                # Try without mapping
                translated[f'opp_{processor_stat}'] = game.get(f'opp_{processor_stat}', 0)
        
        # Add derived fields that data_processor expects
        translated['points_scored'] = translated.get('final_score', 0)
        translated['points_allowed'] = translated.get('opp_final_score', 0)
        
        # Calculate margin and win
        try:
            scored = float(translated['points_scored'])
            allowed = float(translated['points_allowed'])
            translated['margin'] = scored - allowed
            translated['win'] = 1 if scored > allowed else 0
        except (ValueError, TypeError):
            translated['margin'] = 0
            translated['win'] = 0
        
        # Calculate efficiency metrics
        translated.update(self._calculate_efficiency_metrics(translated))
        
        # Add any NCAA-specific fields that might be useful
        translated['location'] = game.get('location', 'neutral')  # home/away/neutral
        translated['sos_before'] = game.get('sos_before', 0)
        translated['sos_current'] = game.get('sos_current', 0)
        translated['record_before'] = game.get('record_before', '0-0')
        
        return translated
    
    def _calculate_efficiency_metrics(self, game_stats: Dict) -> Dict:
        """
        Calculate advanced efficiency metrics for the game.
        """
        metrics = {}
        
        # Turnover differential (turnovers forced - turnovers lost)
        try:
            # Turnovers forced (opponent's turnovers)
            opp_fumbles_lost = float(game_stats.get('opp_fumbles_lost', 0))
            opp_interceptions = float(game_stats.get('opp_interceptions', 0))
            turnovers_forced = opp_fumbles_lost + opp_interceptions
            
            # Turnovers committed (our turnovers)
            fumbles_lost = float(game_stats.get('fumbles_lost', 0))
            interceptions = float(game_stats.get('interceptions', 0))
            turnovers_committed = fumbles_lost + interceptions
            
            metrics['turnover_diff'] = turnovers_forced - turnovers_committed
        except (ValueError, TypeError):
            metrics['turnover_diff'] = 0
        
        # Yards per play
        try:
            # Get total offense, handling various input types
            offense_val = game_stats.get('total_offense', 0)
            if offense_val is None or offense_val == '':
                total_offense = 0
            else:
                total_offense = float(offense_val)
            
            # First try to get total plays from the data directly
            total_plays = 0
            plays_val = game_stats.get('total_offense_plays')
            
            if plays_val is not None and str(plays_val).strip() != '':
                try:
                    total_plays = float(plays_val)
                except (ValueError, TypeError):
                    total_plays = 0
            
            # If we don't have total plays or it's 0, calculate from components
            if total_plays == 0:
                rushing_attempts = 0
                passing_attempts = 0
                
                rush_val = game_stats.get('rushing_attempts')
                if rush_val is not None and str(rush_val).strip() != '':
                    try:
                        rushing_attempts = float(rush_val)
                    except (ValueError, TypeError):
                        rushing_attempts = 0
                
                pass_val = game_stats.get('passing_attempts')
                if pass_val is not None and str(pass_val).strip() != '':
                    try:
                        passing_attempts = float(pass_val)
                    except (ValueError, TypeError):
                        passing_attempts = 0
                
                total_plays = rushing_attempts + passing_attempts
            
            if total_plays > 0:
                metrics['yards_per_play'] = total_offense / total_plays
            else:
                metrics['yards_per_play'] = 0
                
        except Exception as e:
            logger.debug(f"Error calculating yards per play: {e}")
            metrics['yards_per_play'] = 0
        
        # Third down conversion rate (as decimal, not percentage)
        try:
            td_conversions = float(game_stats.get('third_down_conversions', 0))
            td_attempts = float(game_stats.get('third_down_att', 0))
            
            if td_attempts > 0:
                metrics['third_down_rate'] = td_conversions / td_attempts
            else:
                metrics['third_down_rate'] = 0
        except (ValueError, TypeError):
            metrics['third_down_rate'] = 0
        
        # Pass/rush balance
        try:
            pass_yards = float(game_stats.get('net_yards_passing', 0))
            rush_yards = float(game_stats.get('net_yards_rushing', 0))
            total_yards = pass_yards + rush_yards
            
            if total_yards > 0:
                metrics['pass_rush_ratio'] = pass_yards / total_yards
            else:
                metrics['pass_rush_ratio'] = 0.5  # Neutral if no yards
        except (ValueError, TypeError):
            metrics['pass_rush_ratio'] = 0.5
        
        # Time of possession percentage (if available)
        if 'time_of_possession' in game_stats:
            try:
                # Convert time to seconds if it's in MM:SS format
                top = game_stats['time_of_possession']
                if isinstance(top, str) and ':' in top:
                    parts = top.split(':')
                    minutes = int(parts[0])
                    seconds = int(parts[1])
                    total_seconds = minutes * 60 + seconds
                    # Game is 60 minutes = 3600 seconds
                    metrics['time_of_possession_pct'] = total_seconds / 3600
                else:
                    metrics['time_of_possession_pct'] = float(top) / 3600
            except:
                metrics['time_of_possession_pct'] = 0.5
        
        # Red zone efficiency (if we have the data)
        # This would need red zone attempts and conversions which NCAA might not provide
        
        # Scoring efficiency (points per yard)
        try:
            points = float(game_stats.get('points_scored', 0))
            total_offense = float(game_stats.get('total_offense', 0))
            
            if total_offense > 0:
                metrics['points_per_yard'] = points / total_offense
            else:
                metrics['points_per_yard'] = 0
        except (ValueError, TypeError):
            metrics['points_per_yard'] = 0
        
        return metrics
    
    def add_missing_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add placeholder values for any stats that NCAA doesn't provide.
        
        Args:
            df: DataFrame of translated team games
            
        Returns:
            DataFrame with all required stats (using 0 for missing)
        """
        # List of all stats data_processor expects
        required_stats = [
            'final_score', 'first_downs', 'total_offense', 'net_yards_passing',
            'net_yards_rushing', 'third_down_pct', 'third_down_conversions',
            'third_down_att', 'fumbles', 'fumbles_lost', 'interceptions',
            'interception_return_yards', 'penalties_number', 'punts_number',
            'sacks_number', 'total_return_yards'
        ]
        
        # Add both team and opponent versions
        all_required_cols = []
        for stat in required_stats:
            all_required_cols.append(stat)
            all_required_cols.append(f'opp_{stat}')
        
        # Add any missing columns with 0 values
        for col in all_required_cols:
            if col not in df.columns:
                logger.debug(f"Adding missing column with zeros: {col}")
                df[col] = 0
        
        return df
    
    def prepare_for_rolling_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Final preparation before sending to data_processor.
        
        Ensures data is in the exact format expected by calculate_rolling_stats().
        """
        # Ensure proper column order and types
        base_cols = ['year', 'week', 'game_id', 'team', 'opponent']
        
        # Get all other columns
        other_cols = [col for col in df.columns if col not in base_cols]
        
        # Reorder with base columns first
        df = df[base_cols + other_cols]
        
        # Ensure sorting
        df = df.sort_values(['team', 'year', 'week']).reset_index(drop=True)
        
        # Log summary
        logger.info(f"Prepared {len(df)} records for rolling stats calculation")
        logger.info(f"Teams: {df['team'].nunique()}, Weeks: {df['week'].nunique()}")
        
        return df


def integrate_with_data_processor(ncaa_team_games: List[Dict], 
                                 data_processor_instance=None) -> tuple:
    """
    Convenience function to translate NCAA data and calculate rolling stats.
    
    Args:
        ncaa_team_games: List of team-game records from NCAA pipeline
        data_processor_instance: Existing D3DataProcessor instance (optional)
        
    Returns:
        Tuple of (team_games_df, rolling_stats_df)
    """
    # Import here to avoid circular dependency
    from data_processor import D3DataProcessor
    
    # Create translator
    translator = NCAAToProcessorTranslator()
    
    # Translate to expected format
    team_games_df = translator.translate_team_games(ncaa_team_games)
    
    if team_games_df.empty:
        logger.warning("No games translated")
        return pd.DataFrame(), pd.DataFrame()
    
    # Add any missing stats
    team_games_df = translator.add_missing_stats(team_games_df)
    
    # Prepare for rolling stats
    team_games_df = translator.prepare_for_rolling_stats(team_games_df)
    
    # Use provided processor or create new one
    if data_processor_instance is None:
        processor = D3DataProcessor()
    else:
        processor = data_processor_instance
    
    # Calculate rolling stats
    rolling_stats_df = processor.calculate_rolling_stats(team_games_df)
    
    return team_games_df, rolling_stats_df


# Test function
def test_translation():
    """Test the translation with sample NCAA data."""
    
    # Sample NCAA team-game record
    sample_ncaa_game = {
        'year': 2024,
        'week': 5,
        'game_id': 'test123',
        'team': 'Centre',
        'opponent': 'Southwestern',
        'date': '2024-09-28',
        'location': 'away',
        'final_score': 38,
        'opp_final_score': 28,
        'first_downs': 18,
        'opp_first_downs': 15,
        'net_yards_rushing': 55,
        'opp_net_yards_rushing': 108,
        'net_yards_passing': 307,
        'opp_net_yards_passing': 226,
        'total_offense': 362,
        'opp_total_offense': 334,
        'third_down_conversions': 6,
        'third_down_att': 15,
        'third_down_pct': '40.0',
        'sos_before': 0.425,
        'sos_current': 0.438,
        'record_before': '3-1'
    }
    
    translator = NCAAToProcessorTranslator()
    
    # Test single game translation
    translated = translator._translate_single_game(sample_ncaa_game)
    
    print("Original NCAA game sample fields:")
    for key in ['team', 'final_score', 'first_downs', 'net_yards_rushing']:
        print(f"  {key}: {sample_ncaa_game.get(key)}")
    
    print("\nTranslated game sample fields:")
    for key in ['team', 'final_score', 'points_scored', 'margin', 'win']:
        print(f"  {key}: {translated.get(key)}")
    
    # Test DataFrame translation
    df = translator.translate_team_games([sample_ncaa_game])
    print(f"\nDataFrame shape: {df.shape}")
    print(f"Columns: {list(df.columns[:10])}...")


if __name__ == "__main__":
    test_translation()