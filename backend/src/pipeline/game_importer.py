"""
Game Importer - Handles database insertion of games and statistics
Integrates with Team Manager to resolve team IDs before insertion
"""
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from sqlalchemy import text

# Import your models
from src.database.games_model import Game
from src.database.team_game_stats_model import TeamGameStats
from src.pipeline.team_manager import TeamManager

logger = logging.getLogger(__name__)

class GameImporter:
    """
    Imports games and statistics into the database.
    
    Key responsibilities:
    1. Resolve team IDs using Team Manager
    2. Create Game records with proper foreign keys
    3. Create TeamGameStats records for both teams
    4. Handle duplicates by raising errors (fail fast)
    5. Track failed games for later retry
    """
    def __init__(self, db_connection, team_manager: TeamManager):
        """
        Initialize the Game Importer.
        
        Args:
            db_connection: DatabaseConnection instance
            team_manager: TeamManager instance for resolving team IDs
        """
        self.db = db_connection
        self.team_manager = team_manager
        
        # Track import statistics
        self.games_imported = 0
        self.games_skipped = 0
        self.games_failed = []  # List of (contest_id, error) tuples
        
        # Configuration
        self.batch_size = 1  # how many we are importing at once
        self.stop_on_duplicate = True  # Raise error on duplicates

    def import_game(self, translated_data: dict) -> bool:
        """
        Import a single game with its statistics.
        
        This is the main entry point for importing a game.
        Uses a transaction to ensure all-or-nothing insertion.
        
        Args:
            translated_data: Dict from StatsTranslator.translate_game_for_db()
                Expected structure:
                {
                    'game': {...game data...},
                    'team_stats': [...team statistics...]
                }
        
        Returns:
            bool: True if successful, False if failed (and marked for retry)
        
        Raises:
            DuplicateGameError: If game already exists in database
        """
        game_data = translated_data.get('game', {})
        team_stats_list = translated_data.get('team_stats', [])
        contest_id = game_data.get('contest_id')

        logger.info(f"Importing game {contest_id}: {game_data.get('home_team_name')} vs {game_data.get('away_team_name')}")

        try:
            with self.db.get_session() as session:
                # Check for duplicate BEFORE starting work
                existing_game = session.query(Game).filter(
                    Game.contest_id == contest_id).first()
                
                if existing_game:
                    # Check if this is a scheduled game being updated with results
                    if existing_game.home_score is None and existing_game.away_score is None:
                        # Game was scheduled, now has results - UPDATE it
                        logger.info(f"Game {contest_id} was scheduled, updating with results...")
                        success = self._update_game_with_results(session, existing_game, translated_data)
                        if success:
                            session.commit()
                            self.games_imported += 1
                            return True
                        else:
                            session.rollback()
                            self.games_failed.append((contest_id, "Update failed"))
                            return False
                    else:
                        # Game already has scores - true duplicate
                        error_msg = f"Game {contest_id} already exists with scores"
                        logger.error(error_msg)
                        
                        if self.stop_on_duplicate:
                            raise DuplicateGameError(error_msg)
                        else:
                            self.games_skipped += 1
                            return False

            # Resolve team IDs using Team Manager
            home_team_id, away_team_id = self._resolve_team_ids(
                    session, 
                    game_data
            )

            # Update game data with resolved IDs
            game_data['home_team_id'] = home_team_id
            game_data['away_team_id'] = away_team_id

            # Create Game record
            game = self._create_game_record(session, game_data)

            # Create TeamGameStats records
            self._create_team_stats_records(
                session, 
                game.id,
                home_team_id,
                away_team_id,
                team_stats_list
            )

            # Commit the transaction
            session.commit()
            
            self.games_imported += 1
            logger.info(f"Successfully imported game {contest_id}")
            return True

        except DuplicateGameError:
            # Re-raise duplicate errors to stop processing
            raise
            
        except Exception as e:
            # Mark for retry and continue
            logger.error(f"Failed to import game {contest_id}: {str(e)}")
            self.games_failed.append((contest_id, str(e)))
            return False



    def _update_game_with_results(self, session: Session, existing_game: Game, translated_data: Dict) -> bool:
        """
        Update an existing scheduled game with final scores and stats.
        Used when a game that was previously imported as scheduled is now completed.
        
        Args:
            session: Database session
            existing_game: The existing Game record to update
            translated_data: Translated data with scores and stats
            
        Returns:
            bool: True if successful
        """
        game_data = translated_data.get('game', {})
        team_stats_list = translated_data.get('team_stats', [])
        
        logger.info(f"Updating scheduled game {existing_game.contest_id} with results")
        
        # Update game record
        existing_game.home_score = game_data.get('home_score')
        existing_game.away_score = game_data.get('away_score')
        existing_game.status = game_data.get('status', 'F')
        
        # Create team stats (they didn't exist before)
        self._create_team_stats_records(
            session,
            existing_game.id,
            existing_game.home_team_id,
            existing_game.away_team_id,
            team_stats_list
        )
        
        logger.info(f"Successfully updated game {existing_game.contest_id} with scores and stats")
        return True

    

    def _game_exists(self, session: Session, contest_id: str) -> bool:
        """
        Check if a game already exists in the database.
        
        Args:
            session: Database session
            contest_id: NCAA contest ID
            
        Returns:
            bool: True if game exists
        """
        existing = session.query(Game).filter(
            Game.contest_id == contest_id
        ).first()
        
        return existing is not None

    def _resolve_team_ids(self, session: Session, game_data: Dict) -> Tuple[int, int]:
        """
        Resolve team names/seonames to database IDs.
        
        Args:
            session: Database session
            game_data: Game dictionary with team information
            
        Returns:
            Tuple of (home_team_id, away_team_id)
        """
        # Build team data dictionaries for Team Manager
        home_team_data = {
            'seoname': game_data.get('home_team_seoname', ''),
            'nameShort': game_data.get('home_team_name'),
            'teamId': ''  # NCAA ID would go here if we had it
        }
        
        away_team_data = {
            'seoname': game_data.get('away_team_seoname', ''),
            'nameShort': game_data.get('away_team_name'),
            'teamId': ''  # NCAA ID would go here if we had it
        }
        
        # Use Team Manager to find or create teams
        home_team_id, _ = self.team_manager.find_or_create_team(session, home_team_data)
        away_team_id, _ = self.team_manager.find_or_create_team(session, away_team_data)
        
        return home_team_id, away_team_id

    def _create_game_record(self, session: Session, game_data: dict) -> Game:
        """
        Create a Game record in the database.
        
        Args:
            session: Database session
            game_data: Dictionary with game information
            
        Returns:
            Game: The created Game object
        """
        
        game = Game(
            contest_id=game_data['contest_id'],
            game_date=game_data.get('game_date'),
            year=game_data.get('year'),
            week=game_data.get('week'),
            home_team_id=game_data['home_team_id'],
            away_team_id=game_data['away_team_id'],
            home_score=game_data.get('home_score'),
            away_score=game_data.get('away_score'),
            status=game_data.get('status', 'F'),  # Default to Final
            # Add any other fields your Game model has
        )
        
        session.add(game)
        session.flush()  # Get the game.id without committing
        
        logger.debug(f"Created game record #{game.id} for contest {game.contest_id}")
        return game

    def _create_team_stats_records(
        self, 
        session: Session, 
        game_id: int,
        home_team_id: int,
        away_team_id: int,
        team_stats_list: List[Dict]
    ):
        """
        Create TeamGameStats records for both teams.
        
        Args:
            session: Database session
            game_id: ID of the game record
            home_team_id: ID of home team
            away_team_id: ID of away team
            team_stats_list: List of team statistics dictionaries
        """
        # Match stats to teams based on is_home flag
        home_stats = next((s for s in team_stats_list if s.get('is_home')), {})
        away_stats = next((s for s in team_stats_list if not s.get('is_home')), {})

        # Create home team stats
        if home_stats:
            home_record = TeamGameStats(
                game_id=game_id,
                team_id=home_team_id,
                opponent_id=away_team_id,
                is_home=True,
                # Map all the statistics fields
                **self._map_stats_fields(home_stats)
            )
            session.add(home_record)
            logger.debug(f"Created home team stats for team {home_team_id}")

        # Create away team stats
        if away_stats:
            away_record = TeamGameStats(
                game_id=game_id,
                team_id=away_team_id,
                opponent_id=home_team_id,
                is_home=False,
                # Map all the statistics fields
                **self._map_stats_fields(away_stats)
            )
            session.add(away_record)
            logger.debug(f"Created away team stats for team {away_team_id}")

    def _map_stats_fields(self, stats: Dict) -> Dict:
        """
        Map translated stats to TeamGameStats model fields.
        
        Removes non-stats fields and ensures all values are appropriate types.
        
        Args:
            stats: Dictionary of statistics
            
        Returns:
            Dict with only the statistical fields
        """ 
        # Remove non-statistical fields
        excluded_fields = {'team_name', 'is_home'}
        
        # Create new dict with only stats fields
        mapped_stats = {}
        for key, value in stats.items():
            if key not in excluded_fields:
                # Ensure numeric fields are properly typed
                if value == '' or value is None:
                    mapped_stats[key] = None
                else:
                    mapped_stats[key] = value
        
        return mapped_stats

    def import_week(
        self, 
        week_games: List[Dict],
        week_stats: Dict[str, Dict],
        week_number: int
    ) -> Dict:
        """
        Import all games from a week.
        
        Args:
            week_games: List of games from get_week_games()
            week_stats: Dict mapping contest_id -> game stats
            week_number: Week number for this set of games
            
        Returns:
            Dict with import results:
            {
                'success': bool,
                'imported': count,
                'failed': [(contest_id, error), ...],
                'skipped': count
            }
        """
        logger.info(f"Starting import of {len(week_games)} games for week {week_number}")

        # Reset counters for this week
        self.games_imported = 0
        self.games_skipped = 0
        self.games_failed = []

        for game in week_games:
            contest_id = str(game.get('contestId'))

            try:
                # Get stats for this game
                game_stats = week_stats.get(contest_id, {})
                
                if not game_stats or not game_stats.get('success'):
                    logger.warning(f"No stats available for game {contest_id}, marking for retry")
                    self.games_failed.append((contest_id, "No stats available"))
                    continue

                # Translate using StatsTranslator
                # (This would be done by the pipeline, but showing the flow here)
                translated = {
                    'game': self._extract_game_data(game, week_number),
                    'team_stats': game_stats.get('team_stats', [])
                }
                
                # Import the game
                self.import_game(translated)

            except DuplicateGameError as e:
                logger.error(f"Stopping import: {e}")
                break  # Stop processing on duplicate
                
            except Exception as e:
                logger.error(f"Unexpected error importing game {contest_id}: {e}")
                self.games_failed.append((contest_id, str(e)))   

        # Return summary
        return {
            'success': len(self.games_failed) == 0,
            'imported': self.games_imported,
            'failed': self.games_failed,
            'skipped': self.games_skipped,
            'total': len(week_games)
        }


    def _extract_game_data(self, week_game: Dict, week_number: int) -> Dict:
        """
        Extract game data from week schedule format.
        Helper method to bridge between API format and our database format.
        """
        home_team = next((t for t in week_game.get('teams', []) if t.get('isHome')), {})
        away_team = next((t for t in week_game.get('teams', []) if not t.get('isHome')), {})
        
        return {
            'contest_id': str(week_game.get('contestId')),
            'game_date': self._parse_date(week_game.get('startDate')),
            'week': week_number,
            'home_team_name': home_team.get('nameShort'),
            'away_team_name': away_team.get('nameShort'),
            'home_team_seoname': home_team.get('seoname'),
            'away_team_seoname': away_team.get('seoname'),
            'home_score': home_team.get('score'),
            'away_score': away_team.get('score'),
            'status': week_game.get('gameState', 'F'),
        }

    def _parse_date(self, date_string: str):
        """Parse NCAA date format."""
        if not date_string:
            return None
        try:
            return datetime.strptime(date_string, "%m/%d/%Y")
        except ValueError:
            return None

    # def delete_week(self, year: int, week: int) -> int:
    #     """
    #     Delete all games from a specific week.
        
    #     Useful for re-importing a week if there were issues.
    #     Cascading deletes will remove associated TeamGameStats records.
        
    #     Args:
    #         year: Season year
    #         week: Week number
            
    #     Returns:
    #         int: Number of games deleted
    #     """
    #     with self.db.get_session() as session:
    #         games = session.query(Game).filter(
    #             Game.year == year,
    #             Game.week == week
    #         ).all()
            
    #         count = len(games)
            
    #         for game in games:
    #             logger.info(f"Deleting game {game.contest_id} from {year} week {week}")
    #             session.delete(game)
            
    #         session.commit()
    #         logger.info(f"Deleted {count} games from {year} week {week}")
            
    #         return count

    def delete_week(self, year: int, week: int) -> int:
        """Delete all games from a specific week using raw SQL."""
        with self.db.get_session() as session:
            # Count first for return value
            result = session.execute(
                text("SELECT COUNT(*) FROM games WHERE year = :year AND week = :week"),
                {"year": year, "week": week}
            )
            count = result.scalar()
            
            if count > 0:
                # Raw SQL delete - CASCADE handles team_game_stats
                session.execute(
                    text("DELETE FROM games WHERE year = :year AND week = :week"),
                    {"year": year, "week": week}
                )
                session.commit()
                logger.info(f"Deleted {count} games from {year} week {week}")
            
            return count

    def get_import_stats(self) -> Dict:
        """
        Get current import statistics.
        
        Returns:
            Dict with import metrics
        """
        return {
            'games_imported': self.games_imported,
            'games_skipped': self.games_skipped,
            'games_failed': len(self.games_failed),
            'failed_details': self.games_failed,
            'batch_size': self.batch_size
        }

    def set_batch_size(self, size: int):
        """
        Adjust batch size for transaction grouping.
        
        Args:
            size: Number of games per transaction (1-50)
        """
        if 1 <= size <= 50:
            self.batch_size = size
            logger.info(f"Batch size set to {size} games per transaction")
        else:
            logger.warning(f"Invalid batch size {size}, keeping current size {self.batch_size}")


    def get_failed_games(self) -> List[Tuple[str, str]]:
        """
        Get list of games that failed to import.
        
        Returns:
            List of (contest_id, error_message) tuples
        """
        return self.games_failed.copy()

    def retry_failed_games(self) -> Dict:
        """
        Attempt to retry all failed games.
        
        Returns:
            Dict with retry results
        """
        if not self.games_failed:
            logger.info("No failed games to retry")
            return {'retried': 0, 'success': 0}
        
        logger.info(f"Retrying {len(self.games_failed)} failed games")
        
        failed_copy = self.games_failed.copy()
        self.games_failed = []  # Clear the list
        
        retried = 0
        success = 0
        
        for contest_id, original_error in failed_copy:
            retried += 1
            # This would need the actual game data to retry
            # For now, just tracking the concept
            logger.info(f"Would retry game {contest_id} (original error: {original_error})")
        
        return {
            'retried': retried,
            'success': success,
            'still_failed': len(self.games_failed)
        }


class DuplicateGameError(Exception):
    """Raised when attempting to import a game that already exists."""
    pass
        
        