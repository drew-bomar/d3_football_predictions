"""
Test the Game Importer with real NCAA data
This integrates all components: API → Translator → Team Manager → Game Importer
"""

import sys
import logging
from pathlib import Path
from datetime import datetime

# Add src to path
sys.path.append(str(Path(__file__).parent.parent))

from src.database.connection import DatabaseConnection
from src.pipeline.ncaa_api_client import NCAAAPIClient
from src.pipeline.stats_translator import StatsTranslator
from src.pipeline.team_manager import TeamManager
from src.pipeline.game_importer import GameImporter, DuplicateGameError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_single_game_import():
    """
    Test importing a single game with full stats.
    """
    print("\n" + "="*60)
    print("TEST: Single Game Import")
    print("="*60)
    
    # Initialize components
    db = DatabaseConnection()
    api_client = NCAAAPIClient(delay=1.0)
    translator = StatsTranslator()
    team_manager = TeamManager(db)
    game_importer = GameImporter(db, team_manager)
    
    # Use a known game from Week 8, 2024
    # Wisconsin-Whitewater vs Millikin (66-0)
    test_contest_id = 6311135  # From your sample data
    
    print(f"\nFetching game {test_contest_id} from NCAA API...")
    
    # Get the game stats
    game_stats = api_client.get_game_stats(test_contest_id)
    
    if not game_stats.get('success'):
        print(f"❌ Failed to fetch game stats: {game_stats.get('error')}")
        return False
    
    print(f"✅ Got stats for: {game_stats.get('description')}")
    
    # Create mock week game data (normally from get_week_games)
    week_game = {
        'contestId': test_contest_id,
        'startDate': '10/26/2024',
        'gameState': 'F',
        'teams': [
            {
                'nameShort': 'Wis.-Whitewater',
                'seoname': 'wisconsinwhitewater',
                'isHome': True,
                'score': 66
            },
            {
                'nameShort': 'Millikin',
                'seoname': 'millikin',
                'isHome': False,
                'score': 0
            }
        ]
    }
    
    # Translate the data
    print("\nTranslating game data...")
    translated = translator.translate_game_for_db(
        week_game, 
        game_stats,
        week_number=8
    )
    
    # Validate translation
    is_valid, errors = translator.validate_translated_data(translated)
    if not is_valid:
        print(f"❌ Translation validation failed: {errors}")
        return False
    
    print("✅ Translation successful")
    
    # Import the game
    print("\nImporting game to database...")
    try:
        success = game_importer.import_game(translated)
        
        if success:
            print("✅ Game imported successfully!")
            
            # Display import stats
            stats = game_importer.get_import_stats()
            print(f"\nImport Statistics:")
            print(f"  Games imported: {stats['games_imported']}")
            print(f"  Games failed: {stats['games_failed']}")
            print(f"  Games skipped: {stats['games_skipped']}")
        else:
            print("❌ Game import failed")
            print(f"Failed games: {game_importer.get_failed_games()}")
            
    except DuplicateGameError as e:
        print(f"⚠️ Duplicate game detected: {e}")
        print("This game already exists in the database")
    
    # Cleanup
    db.dispose()
    return True


def test_week_deletion():
    """
    Test deleting a week's worth of games.
    """
    print("\n" + "="*60)
    print("TEST: Week Deletion")
    print("="*60)
    
    db = DatabaseConnection()
    team_manager = TeamManager(db)
    game_importer = GameImporter(db, team_manager)
    
    year = 2024
    week = 8
    
    print(f"\nDeleting all games from {year} Week {week}...")
    deleted_count = game_importer.delete_week(year, week)
    
    print(f"✅ Deleted {deleted_count} games")
    
    db.dispose()


def test_duplicate_detection():
    """
    Test that duplicate detection works correctly.
    """
    print("\n" + "="*60)
    print("TEST: Duplicate Detection")
    print("="*60)
    
    db = DatabaseConnection()
    team_manager = TeamManager(db)
    game_importer = GameImporter(db, team_manager)
    
    # Create a simple test game WITH REQUIRED FIELDS
    test_game = {
        'game': {
            'contest_id': 'TEST-12345',
            'game_date': datetime.now(),
            'year': 2024,
            'week': 99,  # Test week
            'home_team_name': 'Mount Union',
            'away_team_name': 'John Carroll',
            'home_team_seoname': 'mountunion',
            'away_team_seoname': 'johncarroll',
            'home_score': 42,  # REQUIRED
            'away_score': 14,  # REQUIRED
            'status': 'F'
        },
        'team_stats': [
            {
                'is_home': True,
                'first_downs': 20,
                'points_scored': 42,      # REQUIRED!
                'points_allowed': 14,     # REQUIRED!
                'total_offense_yards': 450,
                'passing_yards': 250,
                'rushing_yards': 200,
            },
            {
                'is_home': False,
                'first_downs': 10,
                'points_scored': 14,      # REQUIRED!
                'points_allowed': 42,     # REQUIRED!
                'total_offense_yards': 280,
                'passing_yards': 180,
                'rushing_yards': 100,
            }
        ]
    }
    
    print("\nImporting test game first time...")
    try:
        success = game_importer.import_game(test_game)
        if success:
            print("✅ First import successful")
    except DuplicateGameError:
        print("⚠️ Game already exists, skipping first import")
    
    print("\nAttempting duplicate import...")
    try:
        success = game_importer.import_game(test_game)
        print("❌ ERROR: Duplicate was not detected!")
    except DuplicateGameError as e:
        print(f"✅ Duplicate correctly detected: {e}")
    
    # Clean up test game
    print("\nCleaning up test game...")
    with db.get_session() as session:
        from src.database.games_model import Game
        
        # Use a fresh query and let CASCADE handle it
        session.query(Game).filter(
            Game.contest_id == 'TEST-12345'
        ).delete(synchronize_session=False)  # Tell SQLAlchemy not to track
        
        session.commit()
        print("✅ Test game cleaned up")
    
    db.dispose()

def main():
    """
    Run all tests.
    """
    print("\n" + "="*80)
    print("GAME IMPORTER TEST SUITE")
    print("="*80)
    
    # Test 1: Single game import
    test_single_game_import()
    
    # Test 2: Duplicate detection
    test_duplicate_detection()
    
    # Test 3: Week deletion (optional)
    print("\n" + "-"*40)
    response = input("Test week deletion? (y/n): ")
    if response.lower() == 'y':
        test_week_deletion()
    
    print("\n" + "="*80)
    print("✅ All tests complete!")
    print("="*80)


if __name__ == "__main__":
    main()