.3
import logging
import time
from typing import Dict, List, Optional
from datetime import datetime

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.database.connection import DatabaseConnection
from src.pipeline.ncaa_api_client import NCAAAPIClient
from src.pipeline.stats_translator import StatsTranslator
from src.pipeline.team_manager import TeamManager
from src.pipeline.game_importer import GameImporter, DuplicateGameError
from src.pipeline.progress_tracker import ProgressTracker
from src.database.games_model import Game

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SimplePipeline:
    """
    Orchestrates the full import pipeline with monitoring.
    
    This ties together all your components:
    - NCAAAPIClient: Fetches data
    - StatsTranslator: Converts to DB format
    - TeamManager: Handles team resolution
    - GameImporter: Saves to database
    - ProgressTracker: Monitors everything
    """

    def __init__(self, delay: float = 1.0):
        """Initialize all pipeline components."""
        logger.info("Initializing pipeline components...")
        
        self.db = DatabaseConnection()
        self.api_client = NCAAAPIClient(delay=delay)
        self.translator = StatsTranslator()
        self.team_manager = TeamManager(self.db)
        self.game_importer = GameImporter(self.db, self.team_manager)
        self.progress = ProgressTracker()
        
        # Performance tracking
        self.api_calls = 0
        self.start_time = None
    def import_week(self, year: int, week: int, stop_on_duplicate: bool = False) -> Dict:
        """
        Import all games from a specific week.
        
        This is the workhorse method that:
        1. Fetches the week schedule
        2. Gets stats for each game
        3. Translates and imports to DB
        4. Tracks progress throughout
        """
        logger.info(f"\n{'='*50}")
        logger.info(f"Starting import for {year} Week {week}")
        logger.info(f"{'='*50}")
        
        self.start_time = time.time()
        
        try:
            # 1. Fetch week schedule
            logger.info("Fetching week schedule from NCAA...")
            week_data = self.api_client.get_week_games(year, week)
            self.api_calls += 1
            
            if not week_data['success']:
                error = f"Failed to fetch week: {week_data.get('error')}"
                logger.error(error)
                self.progress.progress['failed_weeks'][f"{year}-{week}"] = error
                return {'success': False, 'error': error}
            
            games = week_data['games']
            logger.info(f"Found {len(games)} games for Week {week}")
            
            # Track progress
            self.progress.start_week(year, week, len(games))
            
            # 2. Pre-load all teams to optimize database queries
            logger.info("Pre-loading teams...")
            with self.db.get_session() as session:
                self.team_manager.bulk_ensure_teams(session, games)
            
            # 3. Process each game
            imported = 0
            skipped = 0
            failed = []
            
            for i, game in enumerate(games, 1):
                contest_id = str(game.get('contestId'))
                
                # Show progress every 10 games
                if i % 10 == 0:
                    logger.info(f"Progress: {i}/{len(games)} games processed")
                
                try:
                    # Check if game exists (avoid unnecessary API call)
                    with self.db.get_session() as session:
                        existing_game = session.query(Game).filter(Game.contest_id == contest_id).first()

                        if existing_game:
                            # Check if this is a scheduled game that now has results
                            if existing_game.home_score is None and existing_game.away_score is None:
                                logger.info(f"Game {contest_id} was scheduled, checking for results...")
                                # Don't skip - let it proceed to fetch and update
                            else:
                                # Game already has scores - true duplicate
                                logger.debug(f"Game {contest_id} already exists with scores, skipping")
                                skipped += 1
                                
                                if stop_on_duplicate:
                                    logger.info("Hit duplicate game, stopping week import")
                                    break
                                continue
                        
                         
                    #     if self.game_importer._game_exists(session, contest_id):
                    #         logger.debug(f"Game {contest_id} already exists, skipping")
                    #         skipped += 1
                            
                    #         if stop_on_duplicate:
                    #             logger.info("Hit duplicate game, stopping week import")
                    #             break
                    #         continue
                    
                    # Fetch detailed game stats
                        game_stats = self.api_client.get_game_stats(contest_id)
                        self.api_calls += 1

                    #handle unplayed games
                    if game_stats.get('is_upcoming'): 
                        logger.info(f"Game {contest_id} is upcoming (no stats yet)")
                        # Import just the schedule without stats
                        translated = self.translator.translate_upcoming_game(game, week_number=week)

                        # Validate and import
                        is_valid, errors = self.translator.validate_translated_data(translated)
                        if not is_valid:
                            logger.error(f"Invalid data for {contest_id}: {errors}")
                            failed.append((contest_id, f"Validation: {errors[0]}"))
                            continue

                        # Import to database (will have NULL scores)
                        if self.game_importer.import_game(translated):
                            imported += 1
                            self.progress.update_week_progress(games_imported=1)
                        continue     



                    if not game_stats['success']:
                        logger.warning(f"No stats for game {contest_id}")
                        failed.append((contest_id, "No stats available"))
                        self.progress.update_week_progress(game_failed=contest_id)
                        continue
                    
                    # Translate to database format
                    translated = self.translator.translate_game_for_db(
                        game, game_stats, week_number=week
                    )
                    
                    # Validate before import
                    is_valid, errors = self.translator.validate_translated_data(translated)
                    if not is_valid:
                        logger.error(f"Invalid data for {contest_id}: {errors}")
                        failed.append((contest_id, f"Validation: {errors[0]}"))
                        continue
                    
                    # Import to database
                    if self.game_importer.import_game(translated):
                        imported += 1
                        self.progress.update_week_progress(games_imported=1)
                        
                except DuplicateGameError:
                    skipped += 1
                    if stop_on_duplicate:
                        logger.info("Hit duplicate game, stopping")
                        break
                        
                except Exception as e:
                    logger.error(f"Failed to import {contest_id}: {str(e)}")
                    failed.append((contest_id, str(e)))
                    self.progress.update_week_progress(game_failed=contest_id)

            # Add this right after calculating the failed count:
            if len(failed) > 3:
                logger.warning(f"⚠️ High failure rate: {len(failed)} games failed in week {week}")
                logger.warning("Failed games: " + str(failed[:5]))  # Show first 5 failures
                # Could add retry logic here in future

            
            # 4. Complete week tracking
            elapsed = time.time() - self.start_time
            self.progress.complete_week(year, week, imported, failed)
            
            # Log summary
            logger.info(f"Week {week} Complete:")
            logger.info(f"  Imported: {imported} games")
            logger.info(f"  Skipped: {skipped} games")
            logger.info(f"  Failed: {len(failed)} games")
            logger.info(f"  Time: {elapsed:.1f} seconds")
            logger.info(f"  API Calls: {self.api_calls}")
            
            # FIX: Actually mark the week as complete if successful!
            if imported > 0 or skipped > 0:  # Consider it complete if we got any games
                self.progress.complete_week(year, week, imported, failed)
                logger.info(f"✅ Marked {year} Week {week} as complete in progress tracker")
            
            return {
                'success': True,
                'imported': imported,
                'skipped': skipped,
                'failed': failed,
                'elapsed': elapsed
            }
            
        except Exception as e:
            logger.error(f"Week import failed with exception: {e}")
            return {'success': False, 'error': str(e)}