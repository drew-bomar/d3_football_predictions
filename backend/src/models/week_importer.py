"""
week_importer.py - Import missing weeks with user confirmation

Handles the interactive import process when missing weeks are detected.
"""

import logging
from typing import List
from datetime import datetime

from src.pipeline.simple_pipeline import SimplePipeline
from src.features.rolling_stats_calculator import RollingStatsCalculator
from src.database.connection import DatabaseConnection

logger = logging.getLogger(__name__)

class WeekImporter:
    """
    Manages importing missing weeks and recalculating stats.
    
    Key feature: Always asks for confirmation before importing
    to avoid accidental data duplication or long-running imports.
    """
    
    def __init__(self):
        from src.models.import_checker import ImportChecker
        self.checker = ImportChecker()
        self.pipeline = SimplePipeline()
        
        # RollingStatsCalculator needs a database connection
        self.db = DatabaseConnection()
        self.stats_calc = RollingStatsCalculator(self.db)
    
    def display_import_plan(self, report: dict) -> None:
        """
        Show user what needs to be imported.
        """
        print("\n" + "="*60)
        print("IMPORT STATUS REPORT")
        print("="*60)
        
        print(f"Target: Predict Week {report['target_week']} of {report['year']}")
        print(f"Latest imported: Week {report.get('latest_imported_week', 0)}")
        
        if report['ready_for_prediction']:
            print("\n✅ All necessary data is imported. Ready for predictions!")
            return
        
        print("\n⚠️  Missing data detected:")
        
        if report['current_season_missing']:
            print(f"\n{report['year']} Season Missing Weeks:")
            for week in report['current_season_missing']:
                print(f"  - Week {week}")
        
        if report['previous_season_missing']:
            prev_year = report['year'] - 1
            print(f"\n{prev_year} Season Missing Weeks (for rolling stats):")
            for week in report['previous_season_missing']:
                print(f"  - Week {week}")
        
        print("\nWithout this data, predictions may be inaccurate.")
    
    def confirm_import(self) -> bool:
        """
        Get user confirmation to proceed with import.
        """
        print("\n" + "-"*40)
        response = input("Import missing weeks? (y/n): ").strip().lower()
        return response == 'y'
    
    def import_weeks(self, year: int, weeks: List[int]) -> bool:
        """
        Import specified weeks for a given year.
        
        Returns:
            True if all imports successful
        """
        success = True
        
        for week in weeks:
            try:
                print(f"\nImporting {year} Week {week}...")
                self.pipeline.import_week(year, week)
                print(f"✅ Successfully imported {year} Week {week}")
            except Exception as e:
                logger.error(f"Failed to import {year} Week {week}: {e}")
                print(f"❌ Failed to import {year} Week {week}: {e}")
                success = False
        
        return success
    
    def recalculate_stats(self, year: int) -> bool:
        """
        Recalculate rolling stats after imports.
        
        We recalculate the full year to ensure consistency.
        """
        try:
            print(f"\nRecalculating rolling stats for {year}...")
            self.stats_calc.calculate_for_all_games(year, year)
            print(f"✅ Rolling stats updated for {year}")
            return True
        except Exception as e:
            logger.error(f"Failed to calculate stats: {e}")
            print(f"❌ Failed to calculate stats: {e}")
            return False
    
    def ensure_data_ready(self, year: int, target_week: int) -> bool:
        """
        Main entry point: Check and import if necessary.
        
        Returns:
            True if data is ready for predictions
        """
        # Generate import report
        report = self.checker.generate_import_report(year, target_week)
        
        # Display status
        self.display_import_plan(report)
        
        if report['ready_for_prediction']:
            return True
        
        # Ask for confirmation
        if not self.confirm_import():
            print("Import cancelled. Predictions may be inaccurate.")
            return False
        
        # Import missing weeks
        import_success = True
        
        # Import previous season if needed
        if report['previous_season_missing']:
            prev_year = year - 1
            print(f"\nImporting {prev_year} season weeks...")
            import_success = self.import_weeks(
                prev_year, 
                report['previous_season_missing']
            )
            
            if import_success:
                import_success = self.recalculate_stats(prev_year)
        
        # Import current season
        if report['current_season_missing'] and import_success:
            print(f"\nImporting {year} season weeks...")
            import_success = self.import_weeks(
                year,
                report['current_season_missing']
            )
            
            if import_success:
                import_success = self.recalculate_stats(year)
        
        if import_success:
            print("\n✅ All imports complete! Ready for predictions.")
        else:
            print("\n⚠️  Some imports failed. Predictions may be incomplete.")
        
        return import_success