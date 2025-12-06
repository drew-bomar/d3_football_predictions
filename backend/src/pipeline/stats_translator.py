"""
Stats Translator - CORRECTED VERSION
This version matches your exact database column names from team_game_stats_model.py
Replace the content of your src/pipeline/stats_translator.py with this
"""

import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)

class StatsTranslator:
    """
    Translates NCAA API response data into database-ready dictionaries.
    
    This class handles:
    1. Field name mapping (NCAA names -> our database columns)
    2. Data type conversion (strings -> integers/floats)
    3. Calculated field generation (turnover_diff, yards_per_play, etc.)
    4. Data validation and cleaning
    """
    
    def __init__(self):
        """
        Initialize the translator with field mappings.
        THESE NOW MATCH YOUR team_game_stats_model.py EXACTLY
        """
        # Map NCAA stat names to your database column names
        self.stat_field_mappings = {
            # Basic offensive stats
            'first_downs': 'first_downs',
            'first_downs_passing': 'first_downs_passing',
            'first_downs_rushing': 'first_downs_rushing',
            'first_downs_penalty': 'first_downs_penalty',
            
            # Total offense - CORRECTED
            'total_yards': 'total_offense_yards',
            'total_plays': 'total_offense_plays',
            'yards_per_play': 'total_offense_avg',  # Will be recalculated
            
            # Passing stats - CORRECTED (using 'passing_' prefix)
            'passing_completions': 'passing_completions',
            'passing_attempts': 'passing_attempts',
            'passing_yards': 'passing_yards',
            'passing_tds': 'passing_tds',
            'passing_interceptions': 'passing_interceptions',
            
            # Rushing stats - CORRECTED (using 'rushing_' prefix)
            'rushing_attempts': 'rushing_attempts',
            'rushing_yards': 'rushing_yards',
            'rushing_tds': 'rushing_tds',
        
            
            # Third/Fourth down conversions
            'third_down_conversions': 'third_down_conversions',
            'third_down_attempts': 'third_down_attempts',
            'fourth_down_conversions': 'fourth_down_conversions',
            'fourth_down_attempts': 'fourth_down_attempts',
            
            # Turnovers
            'fumbles': 'fumbles',
            'fumbles_lost': 'fumbles_lost',
            
            # Penalties - CORRECTED (using plural form)
            'penalties': 'penalties_number',
            'penalty_yards': 'penalties_yards',
            
            # Defense/Special teams
            'sacks': 'sacks',
            'tackles_for_loss': 'tackles_for_loss',
            
            # Punting - CORRECTED (using plural form)
            'punts': 'punts_number',
            'punt_yards': 'punts_yards',
            'punt_average': 'punts_avg',
            
            # Returns - CORRECTED (using full names)
            'kick_returns': 'kickoff_returns_number',
            'kick_return_yards': 'kickoff_returns_yards',
            'punt_returns': 'punt_returns_number',
            'punt_return_yards': 'punt_returns_yards',
            
            # Interception returns
            'defense_interceptions': 'interception_returns_number',
            # Note: interception_returns_yards would need to be added
        }
    
    def translate_game_for_db(self, week_game: Dict, game_stats: Dict, week_number: int = None) -> Dict:
        """
        Translate a complete game (schedule info + stats) into database format.
        NOW INCLUDES points_scored and points_allowed!
        
        Args:
            week_game: Game info from get_week_games() with teams and scores
            game_stats: Detailed stats from get_game_stats()
            week_number: Week number (passed from pipeline since NCAA doesn't include it)
            
        Returns:
            Dict with 'game' and 'team_stats' keys ready for database insertion
        """
        # Extract team information from the week schedule data
        home_team = next((t for t in week_game.get('teams', []) if t.get('isHome')), {})
        away_team = next((t for t in week_game.get('teams', []) if not t.get('isHome')), {})
        
        # Get the scores - CRITICAL FOR points_scored/points_allowed
        home_score = home_team.get('score')
        away_score = away_team.get('score')
        
        # Parse the date
        game_date = self._parse_date(week_game.get('startDate'))
        
        # Build the game record (matching games_model.py columns)
        game_record = {
            'contest_id': str(week_game.get('contestId')),
            'game_date': game_date,
            'year': game_date.year if game_date else None,
            'week': week_number,
            'home_team_name': home_team.get('nameShort'),
            'away_team_name': away_team.get('nameShort'),
            'home_team_seoname': home_team.get('seoname'),
            'away_team_seoname': away_team.get('seoname'),
            'home_score': home_score,
            'away_score': away_score,
            'status': week_game.get('gameState', 'F'),
        }
        
        # Process team stats if available
        team_stats_records = []
        if game_stats and game_stats.get('success'):
            for team_stats in game_stats.get('team_stats', []):
                translated_stats = self.translate_team_stats(team_stats)
                
                # ADD POINTS_SCORED AND POINTS_ALLOWED
                if team_stats.get('is_home'):
                    translated_stats['points_scored'] = home_score
                    translated_stats['points_allowed'] = away_score
                    translated_stats['margin'] = (home_score - away_score) if home_score is not None and away_score is not None else None
                    translated_stats['win'] = home_score > away_score if home_score is not None and away_score is not None else None
                else:
                    translated_stats['points_scored'] = away_score
                    translated_stats['points_allowed'] = home_score
                    translated_stats['margin'] = (away_score - home_score) if home_score is not None and away_score is not None else None
                    translated_stats['win'] = away_score > home_score if home_score is not None and away_score is not None else None
                
                # Add total points
                if home_score is not None and away_score is not None:
                    translated_stats['total_points'] = home_score + away_score
                else:
                    translated_stats['total_points'] = None
                
                team_stats_records.append(translated_stats)
        
        return {
            'game': game_record,
            'team_stats': team_stats_records
        }

    def translate_upcoming_game(self, game_info: Dict, week_number: int) -> Dict:
        """
        Translate an upcoming game (no stats yet) into database format.
        Only imports the matchup info, scores will be NULL.
        
        Args:
            game_info: Game data from NCAA schedule API
            week_number: Week number
            
        Returns:
            Dict with game info (no stats)
        """
        # Extract teams
        teams = game_info.get('teams', [])
        home_team = next((t for t in teams if t.get('isHome')), {})
        away_team = next((t for t in teams if not t.get('isHome')), {})
        
        # Parse date
        game_date = self._parse_date(game_info.get('startDate'))
        
        return {
            'game': {
                'contest_id': str(game_info.get('contestId')),
                'game_date': game_date,
                'year': game_date.year if game_date else None,
                'week': week_number,
                'home_team_name': home_team.get('nameShort'),
                'away_team_name': away_team.get('nameShort'),
                'home_team_seoname': home_team.get('seoname'),
                'away_team_seoname': away_team.get('seoname'),
                'home_score': None,  # NULL - game not played yet
                'away_score': None,  # NULL - game not played yet
                'status': 'scheduled'  # Mark as scheduled, not final
            },
            'team_stats': []  # No stats for upcoming games
        }


    
    def translate_team_stats(self, team_stats: Dict) -> Dict:
        """
        Translate team statistics from NCAA format to database format.
        
        Args:
            team_stats: Single team's stats from get_game_stats()
            
        Returns:
            Dict with database column names and converted values
        """
        translated = {
            'team_name': team_stats.get('team_name'),
            'is_home': team_stats.get('is_home'),
        }
        
        # Map all basic stats using our field mappings
        for ncaa_field, db_field in self.stat_field_mappings.items():
            value = team_stats.get(ncaa_field)
            translated[db_field] = self._convert_to_number(value)
        
        # Add calculated fields
        calculated = self._calculate_derived_fields(translated)
        translated.update(calculated)
        
        return translated
    
    def _convert_to_number(self, value) -> Optional[float]:
        """
        Convert various input types to numbers.
        
        Args:
            value: String, int, float, or None
            
        Returns:
            Float value or None if conversion fails
        """
        if value is None or value == '':
            return None
        
        if isinstance(value, (int, float)):
            return float(value)
        
        try:
            # Remove any commas (e.g., "1,234" -> "1234")
            if isinstance(value, str):
                value = value.replace(',', '')
                return float(value)
        except (ValueError, AttributeError):
            logger.debug(f"Could not convert '{value}' to number")
            return None
    
    def _parse_date(self, date_string: str) -> Optional[datetime]:
        """
        Parse NCAA date format (MM/DD/YYYY) to datetime object.
        
        Args:
            date_string: Date in format "10/19/2024"
            
        Returns:
            datetime object or None if parsing fails
        """
        if not date_string:
            return None
        
        try:
            return datetime.strptime(date_string, "%m/%d/%Y")
        except ValueError:
            logger.warning(f"Could not parse date: {date_string}")
            return None
    
    def _calculate_derived_fields(self, stats: Dict) -> Dict:
        """
        Calculate additional fields that NCAA doesn't provide directly.
        
        Args:
            stats: Translated statistics dictionary
            
        Returns:
            Dict with calculated fields
        """
        calculated = {}
        
        # Calculate averages
        rushing_attempts = stats.get('rushing_attempts', 0) or 0
        rushing_yards = stats.get('rushing_yards', 0) or 0
        if rushing_attempts > 0:
            calculated['rushing_avg'] = rushing_yards / rushing_attempts
        else:
            calculated['rushing_avg'] = None
        
        passing_attempts = stats.get('passing_attempts', 0) or 0
        passing_yards = stats.get('passing_yards', 0) or 0
        if passing_attempts > 0:
            calculated['passing_avg'] = passing_yards / passing_attempts
        else:
            calculated['passing_avg'] = None
        
        # Total offense average
        total_plays = stats.get('total_offense_plays', 0) or 0
        total_yards = stats.get('total_offense_yards', 0) or 0
        if total_plays > 0:
            calculated['total_offense_avg'] = total_yards / total_plays
            calculated['yards_per_play'] = total_yards / total_plays  # Same thing
        else:
            calculated['total_offense_avg'] = None
            calculated['yards_per_play'] = None
        
        # Third down percentage
        third_conversions = stats.get('third_down_conversions', 0) or 0
        third_attempts = stats.get('third_down_attempts', 0) or 0
        
        if third_attempts > 0:
            calculated['third_down_pct'] = (third_conversions / third_attempts) * 100
            calculated['third_down_rate'] = third_conversions / third_attempts  # As decimal
        else:
            calculated['third_down_pct'] = None
            calculated['third_down_rate'] = None
        
        # Pass/rush ratio
        if total_yards and total_yards > 0:
            calculated['pass_rush_ratio'] = (passing_yards or 0) / total_yards
        else:
            calculated['pass_rush_ratio'] = None
        
        # Turnover differential (we'll need opponent data for this, so leave null for now)
        calculated['turnover_diff'] = None
        
        # # Time of possession - not available from NCAA API
        # calculated['time_of_possession'] = None
        
        return calculated
    
    def validate_translated_data(self, translated_data: Dict) -> Tuple[bool, List[str]]:
        """
        Validate that translated data is ready for database insertion.
        Handles both completed games (with stats) and scheduled games (without stats).
        
        Args:
            translated_data: Complete translated game data
            
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        
        # Check game record
        game = translated_data.get('game', {})
        
        # Required game fields (for ALL games)
        if not game.get('contest_id'):
            errors.append("Missing contest_id")
        
        if not game.get('home_team_name'):
            errors.append("Missing home team name")
        
        if not game.get('away_team_name'):
            errors.append("Missing away team name")
        
        # Check if this is an upcoming/scheduled game
        is_upcoming = game.get('status') == 'scheduled'
        
        # Check team stats
        team_stats = translated_data.get('team_stats', [])
        
        if is_upcoming:
            # Upcoming games should have NO stats
            if len(team_stats) != 0:
                errors.append(f"Upcoming game should have 0 team stats, got {len(team_stats)}")
            
            # Scores should be NULL for upcoming games
            if game.get('home_score') is not None:
                errors.append(f"Upcoming game should have NULL home_score")
            if game.get('away_score') is not None:
                errors.append(f"Upcoming game should have NULL away_score")
        else:
            # Completed games should have exactly 2 team stat records
            if len(team_stats) != 2:
                errors.append(f"Expected 2 team stat records, got {len(team_stats)}")
            
            # Check for required fields in stats
            for i, stats in enumerate(team_stats):
                # Check required fields
                if stats.get('points_scored') is None:
                    errors.append(f"Team {i+1} missing required field: points_scored")
                
                if stats.get('points_allowed') is None:
                    errors.append(f"Team {i+1} missing required field: points_allowed")
                
                # Check for extreme values that might indicate bad data
                total_yards = stats.get('total_offense_yards', 0) or 0
                if total_yards > 1500:
                    errors.append(f"Team {i+1} has unrealistic total yards: {total_yards}")
                
                # Check for negative values where they shouldn't be
                for field in ['first_downs', 'passing_attempts', 'rushing_attempts']:
                    value = stats.get(field, 0) or 0
                    if value < 0:
                        errors.append(f"Team {i+1} has negative {field}: {value}")
        
        is_valid = len(errors) == 0
        return is_valid, errors