"""
import_checker.py - Verify game imports are up to date

Checks if all necessary weeks are imported before making predictions.
Handles previous season lookback for early-season games.
"""

import logging
from typing import List, Tuple, Optional
from sqlalchemy import text

from src.database.connection import DatabaseConnection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ImportChecker:
    """
    Checks import status and identifies missing weeks of data.
    
    Rolling stats need complete recent history, so we verify
    all necessary weeks are imported before predictions.
    """
    
    def __init__(self):
        self.db = DatabaseConnection()
    
    def get_imported_weeks(self, year: int) -> List[int]:
        """
        Get list of weeks that have been imported for a given year.
        
        Returns:
            Sorted list of week numbers with completed games
        """
        with self.db.get_session() as session:
            query = text("""
                SELECT DISTINCT week 
                FROM games 
                WHERE year = :year 
                    AND home_score IS NOT NULL
                ORDER BY week
            """)
            
            result = session.execute(query, {'year': year})
            weeks = [row[0] for row in result]
            
        return weeks
    
    def get_latest_completed_week(self, year: int) -> Optional[int]:
        """
        Find the most recent week with completed games.
        
        This helps auto-detect which week to predict.
        
        Returns:
            Latest week number with scores, or None if no games
        """
        with self.db.get_session() as session:
            query = text("""
                SELECT MAX(week)
                FROM games
                WHERE year = :year
                    AND home_score IS NOT NULL
            """)
            
            result = session.execute(query, {'year': year})
            latest_week = result.scalar()
            
        return latest_week
    
    def check_missing_weeks(self, year: int, up_to_week: int) -> List[int]:
        """
        Identify which weeks are missing from the database.
        
        Args:
            year: Season year to check
            up_to_week: Check weeks 1 through this week
            
        Returns:
            List of missing week numbers
        """
        imported = set(self.get_imported_weeks(year))
        expected = set(range(1, up_to_week + 1))
        missing = sorted(expected - imported)
        
        if missing:
            logger.warning(f"Missing weeks for {year}: {missing}")
        else:
            logger.info(f"All weeks 1-{up_to_week} are imported for {year}")
        
        return missing
    
    def check_rolling_stats_exist(self, year: int, week: int) -> bool:
        """
        Check if rolling stats have been calculated for the target week.
        
        Returns:
            True if rolling stats exist for teams before this week
        """
        with self.db.get_session() as session:
            query = text("""
                SELECT COUNT(DISTINCT trs.team_id)
                FROM team_rolling_stats trs
                JOIN games g ON trs.game_id = g.id
                WHERE g.year = :year
                    AND g.week < :week
            """)
            
            result = session.execute(query, {'year': year, 'week': week})
            team_count = result.scalar()
            
        return team_count > 0
    
    def generate_import_report(self, year: int, target_week: int) -> dict:
        """
        Create comprehensive report of what needs importing.
        
        Args:
            year: Current season
            target_week: Week we want to predict
            
        Returns:
            Dictionary with import status and recommendations
        """
        report = {
            'year': year,
            'target_week': target_week,
            'current_season_missing': [],
            'previous_season_missing': [],
            'needs_import': False,
            'needs_rolling_stats': False,
            'ready_for_prediction': False
        }
        
        # Check current season
        latest = self.get_latest_completed_week(year)
        if latest:
            report['latest_imported_week'] = latest
            
            # We need weeks up to (but not including) the target week
            if target_week > latest + 1:
                report['current_season_missing'] = list(range(latest + 1, target_week))
        else:
            report['latest_imported_week'] = 0
            report['current_season_missing'] = list(range(1, target_week))
        
        # Check if rolling stats exist
        has_rolling_stats = self.check_rolling_stats_exist(year, target_week)
        if not has_rolling_stats:
            report['needs_rolling_stats'] = True
        
        # Determine overall status
        report['needs_import'] = bool(report['current_season_missing'])
        report['ready_for_prediction'] = (
            not report['needs_import'] and 
            not report['needs_rolling_stats']
        )
        
        return report