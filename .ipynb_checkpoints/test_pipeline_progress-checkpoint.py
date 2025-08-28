# test_pipeline_and_progress.py
"""
Test both SimplePipeline and ProgressTracker together.
Includes cleanup option to remove test data afterwards.
"""
from src.pipeline.simple_pipeline import SimplePipeline
from src.database.connection import DatabaseConnection
from sqlalchemy import text
import json
from pathlib import Path

def test_pipeline_with_progress():
    """Test the pipeline and progress tracker with a single week."""
    print("\n" + "="*60)
    print("TESTING: Pipeline + Progress Tracker")
    print("="*60)
    
    # Test parameters
    TEST_YEAR = 2021
    TEST_WEEK = 1
    
    # Initialize pipeline (which includes progress tracker)
    pipeline = SimplePipeline(delay=1.0)
    
    # Show initial state
    print("\n1. INITIAL STATE:")
    print("-" * 40)
    pipeline.progress.print_status()
    
    # Check if progress file exists
    progress_file = Path('import_progress.json')
    print(f"\nProgress file exists: {progress_file.exists()}")
    
    # Run the import
    print(f"\n2. IMPORTING {TEST_YEAR} WEEK {TEST_WEEK}:")
    print("-" * 40)
    print("This will take 1-2 minutes...")
    
    result = pipeline.import_week(TEST_YEAR, TEST_WEEK)
    
    # Check results
    print("\n3. IMPORT RESULTS:")
    print("-" * 40)
    if result['success']:
        print(f"✅ Success! Imported {result['imported']} games")
        print(f"   Skipped: {result['skipped']}")
        print(f"   Failed: {len(result.get('failed', []))}")
        print(f"   Time: {result['elapsed']:.1f} seconds")
    else:
        print(f"❌ Failed: {result.get('error')}")
        return False
    
    # Verify progress tracker saved correctly
    print("\n4. PROGRESS TRACKER VERIFICATION:")
    print("-" * 40)
    
    if progress_file.exists():
        with open(progress_file, 'r') as f:
            saved_progress = json.load(f)
        
        print(f"✅ Progress file saved")
        print(f"   Completed weeks: {saved_progress['completed_weeks']}")
        print(f"   Total games imported: {saved_progress['statistics']['total_games_imported']}")
        
        # Check if our week is marked complete
        week_key = f"{TEST_YEAR}-{TEST_WEEK}"
        if week_key in saved_progress['completed_weeks']:
            print(f"✅ Week {week_key} marked as complete")
        else:
            print(f"❌ Week {week_key} NOT marked as complete")
    else:
        print("❌ Progress file not created!")
    
    # Verify database
    print("\n5. DATABASE VERIFICATION:")
    print("-" * 40)
    
    db = DatabaseConnection()
    with db.get_session() as session:
        games = session.execute(
            text("SELECT COUNT(*) FROM games WHERE year = :year AND week = :week"),
            {"year": TEST_YEAR, "week": TEST_WEEK}
        ).scalar()
        
        stats = session.execute(text("SELECT COUNT(*) FROM team_game_stats")).scalar()
        teams = session.execute(text("SELECT COUNT(*) FROM teams")).scalar()
        
        print(f"Games imported: {games}")
        print(f"Stats records: {stats} (should be ~2x games)")
        print(f"Teams created: {teams}")
        
        # Get a sample game to verify data
        sample = session.execute(text("""
            SELECT g.contest_id, ht.name, g.home_score, g.away_score, at.name
            FROM games g
            JOIN teams ht ON g.home_team_id = ht.id
            JOIN teams at ON g.away_team_id = at.id
            WHERE g.year = :year AND g.week = :week
            LIMIT 1
        """), {"year": TEST_YEAR, "week": TEST_WEEK}).fetchone()
        
        if sample:
            print(f"\nSample game:")
            print(f"  {sample[1]} {sample[2]} - {sample[3]} {sample[4]}")
    
    # Test resume capability
    print("\n6. TESTING RESUME CAPABILITY:")
    print("-" * 40)
    
    # Create new pipeline instance (simulating restart)
    new_pipeline = SimplePipeline()
    pending = new_pipeline.progress.get_pending_weeks(TEST_YEAR, TEST_YEAR)
    
    print(f"Pending weeks for {TEST_YEAR}: {len(pending)}")
    if week_key not in [f"{y}-{w}" for y, w in pending]:
        print(f"✅ Week {TEST_WEEK} correctly not in pending list")
    else:
        print(f"❌ Week {TEST_WEEK} incorrectly still pending")
    
    return True

def cleanup_test_data():
    """Clean up test data from database and progress file."""
    print("\n" + "="*60)
    print("CLEANUP OPTIONS")
    print("="*60)
    
    db = DatabaseConnection()
    
    # Show what would be deleted
    with db.get_session() as session:
        game_count = session.execute(
            text("SELECT COUNT(*) FROM games WHERE year = 2021 AND week = 1")
        ).scalar()
        
        total_games = session.execute(text("SELECT COUNT(*) FROM games")).scalar()
        total_stats = session.execute(text("SELECT COUNT(*) FROM team_game_stats")).scalar()
        total_teams = session.execute(text("SELECT COUNT(*) FROM teams")).scalar()
    
    print(f"\nCurrent database state:")
    print(f"  Test games (2021 Week 1): {game_count}")
    print(f"  Total games: {total_games}")
    print(f"  Total stats: {total_stats}")
    print(f"  Total teams: {total_teams}")
    
    progress_file = Path('import_progress.json')
    print(f"\nProgress file exists: {progress_file.exists()}")
    
    print("\nCleanup options:")
    print("1. Keep everything (continue testing)")
    print("2. Delete ONLY test week (2021 Week 1)")
    print("3. Clear ENTIRE database and progress")
    print("4. Cancel")
    
    choice = input("\nYour choice (1-4): ")
    
    if choice == '2':
        # Delete just test week
        with db.get_session() as session:
            result = session.execute(
                text("DELETE FROM games WHERE year = 2021 AND week = 1"),
            )
            session.commit()
            print(f"✅ Deleted {result.rowcount} games from 2021 Week 1")
        
        # Update progress file
        if progress_file.exists():
            with open(progress_file, 'r') as f:
                progress = json.load(f)
            
            week_key = "2021-1"
            if week_key in progress['completed_weeks']:
                progress['completed_weeks'].remove(week_key)
                with open(progress_file, 'w') as f:
                    json.dump(progress, f, indent=2)
                print(f"✅ Removed {week_key} from progress tracker")
    
    elif choice == '3':
        # Nuclear option
        confirm = input("Type 'DELETE ALL' to confirm: ")
        if confirm == "DELETE ALL":
            with db.get_session() as session:
                session.execute(text("DELETE FROM team_game_stats"))
                session.execute(text("DELETE FROM games"))
                session.execute(text("DELETE FROM teams"))
                session.commit()
                print("✅ Database cleared")
            
            if progress_file.exists():
                progress_file.unlink()
                print("✅ Progress file deleted")
    
    elif choice == '1':
        print("Keeping all data")
    else:
        print("Cancelled")

if __name__ == "__main__":
    # Run the test
    success = test_pipeline_with_progress()
    
    if success:
        print("\n" + "="*60)
        print("✅ ALL TESTS PASSED!")
        print("="*60)
        
        # Offer cleanup
        cleanup = input("\nRun cleanup? (y/n): ")
        if cleanup.lower() == 'y':
            cleanup_test_data()
    else:
        print("\n❌ Tests failed - check errors above")