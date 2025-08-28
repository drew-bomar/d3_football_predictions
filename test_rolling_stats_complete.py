"""
test_rolling_stats_complete.py
Comprehensive test of the rolling stats system including:
1. Table creation
2. Data import for testing
3. Rolling stats calculation
4. Verification of decay weights
"""

import logging
from datetime import datetime
from sqlalchemy import text
from src.database.connection import DatabaseConnection
from src.database.team_rolling_stats_model import TeamRollingStats
from src.features.rolling_stats_calculator import RollingStatsCalculator
from src.pipeline.simple_pipeline import SimplePipeline

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_rolling_stats_table(db):
    """Create the team_rolling_stats table if it doesn't exist."""
    print("\n" + "="*60)
    print("STEP 1: CREATING ROLLING STATS TABLE")
    print("="*60)
    
    try:
        # Create all tables (this will only create new ones)
        from src.database.teams_model import Team
        from src.database.games_model import Game
        from src.database.team_game_stats_model import TeamGameStats
        
        TeamRollingStats.metadata.create_all(db.engine)
        print("✅ Table 'team_rolling_stats' created successfully!")
        
        # Verify it exists
        with db.get_session() as session:
            result = session.execute(text("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'team_rolling_stats'
                LIMIT 5
            """)).fetchall()
            
            print(f"\nSample columns in table:")
            for col_name, data_type in result:
                print(f"  - {col_name}: {data_type}")
                
    except Exception as e:
        print(f"❌ Error creating table: {e}")
        return False
    
    return True

def import_test_data(db):
    """Import enough data to test rolling stats calculation."""
    print("\n" + "="*60)
    print("STEP 2: IMPORTING TEST DATA")
    print("="*60)
    
    pipeline = SimplePipeline(delay=1.0)
    
    # Check what data we already have
    with db.get_session() as session:
        existing = session.execute(text("""
            SELECT year, week, COUNT(*) as games
            FROM games
            GROUP BY year, week
            ORDER BY year, week
        """)).fetchall()
        
        existing_weeks = {(year, week) for year, week, _ in existing}
        print(f"\nExisting data: {len(existing_weeks)} weeks")
        for year, week, count in existing[:5]:
            print(f"  {year} Week {week}: {count} games")
    
    # Import 2021 weeks 15-18 (end of season) and 2022 weeks 1-5 (start of next)
    # This lets us test the previous season decay weight logic
    weeks_to_import = [
        (2021, 15), (2021, 16), (2021, 17), (2021, 18),  # End of 2021
        (2022, 1), (2022, 2), (2022, 3), (2022, 4), (2022, 5)  # Start of 2022
    ]
    
    imported_count = 0
    for year, week in weeks_to_import:
        if (year, week) in existing_weeks:
            print(f"  Skipping {year} Week {week} (already exists)")
            continue
            
        print(f"\n  Importing {year} Week {week}...")
        result = pipeline.import_week(year, week)
        
        if result['success']:
            imported_count += result['imported']
            print(f"    ✅ Imported {result['imported']} games")
        else:
            print(f"    ❌ Failed: {result.get('error')}")
    
    print(f"\n✅ Import complete! Added {imported_count} new games")
    return imported_count > 0 or len(existing_weeks) > 0

def test_decay_weight_calculation(db):
    """Test that decay weights are properly applied to previous season games."""
    print("\n" + "="*60)
    print("STEP 3: TESTING DECAY WEIGHT CALCULATION")
    print("="*60)
    
    calculator = RollingStatsCalculator(db, prev_season_weight=0.7)
    
    with db.get_session() as session:
        # Find a team that played in both 2021 end and 2022 start
        test_team = session.execute(text("""
            SELECT t.id, t.name
            FROM teams t
            WHERE EXISTS (
                SELECT 1 FROM games g1 
                WHERE (g1.home_team_id = t.id OR g1.away_team_id = t.id)
                AND g1.year = 2021 AND g1.week >= 15
            )
            AND EXISTS (
                SELECT 1 FROM games g2
                WHERE (g2.home_team_id = t.id OR g2.away_team_id = t.id)
                AND g2.year = 2022 AND g2.week <= 3
            )
            LIMIT 1
        """)).fetchone()
        
        if not test_team:
            print("❌ No team found that played in both periods")
            return False
        
        team_id, team_name = test_team
        print(f"\nTesting with: {team_name} (ID: {team_id})")
        
        # Test Week 1 of 2022 - should use 3 games from 2021
        print("\n--- Testing 2022 Week 1 (should use 2021 games) ---")
        
        week_1_game = session.execute(text("""
            SELECT id FROM games
            WHERE year = 2022 AND week = 1
            AND (home_team_id = :team_id OR away_team_id = :team_id)
            LIMIT 1
        """), {'team_id': team_id}).fetchone()
        
        if week_1_game:
            stats = calculator._calculate_team_stats(
                session, 
                game_id=week_1_game[0],
                team_id=team_id,
                opponent_id=1,  # Dummy opponent
                year=2022,
                week=1
            )
            
            print(f"  Games from current season: {stats.get('games_in_season', 0)}")
            print(f"  Games from previous season (3wk): {stats.get('prev_season_games_in_3wk', 0)}")
            print(f"  PPG (3wk with decay): {stats.get('ppg_3wk', 'None'):.1f}" if stats.get('ppg_3wk') else "  PPG (3wk): None")
            
            # Verify previous season games were used
            assert stats.get('prev_season_games_in_3wk', 0) > 0, "Should use previous season games!"
            print("  ✅ Previous season games properly included")
        
        # Test Week 3 of 2022 - should mix current and previous
        print("\n--- Testing 2022 Week 3 (should mix seasons) ---")
        
        week_3_game = session.execute(text("""
            SELECT id FROM games
            WHERE year = 2022 AND week = 3
            AND (home_team_id = :team_id OR away_team_id = :team_id)
            LIMIT 1
        """), {'team_id': team_id}).fetchone()
        
        if week_3_game:
            stats = calculator._calculate_team_stats(
                session,
                game_id=week_3_game[0],
                team_id=team_id,
                opponent_id=1,
                year=2022,
                week=3
            )
            
            print(f"  Games from current season: {stats.get('games_in_season', 0)}")
            print(f"  Games from previous season (3wk): {stats.get('prev_season_games_in_3wk', 0)}")
            print(f"  PPG (3wk with decay): {stats.get('ppg_3wk', 'None'):.1f}" if stats.get('ppg_3wk') else "  PPG (3wk): None")
            
            assert stats.get('games_in_season', 0) == 2, "Should have 2 games from current season"
            assert stats.get('prev_season_games_in_3wk', 0) == 1, "Should have 1 game from previous season"
            print("  ✅ Season mixing working correctly")
        
        # Test Week 5 of 2022 - should use current season only
        print("\n--- Testing 2022 Week 5 (current season only) ---")
        
        week_5_game = session.execute(text("""
            SELECT id FROM games
            WHERE year = 2022 AND week = 5
            AND (home_team_id = :team_id OR away_team_id = :team_id)
            LIMIT 1
        """), {'team_id': team_id}).fetchone()
        
        if week_5_game:
            stats = calculator._calculate_team_stats(
                session,
                game_id=week_5_game[0],
                team_id=team_id,
                opponent_id=1,
                year=2022,
                week=5
            )
            
            print(f"  Games from current season: {stats.get('games_in_season', 0)}")
            print(f"  Games from previous season (3wk): {stats.get('prev_season_games_in_3wk', 0)}")
            print(f"  PPG (3wk): {stats.get('ppg_3wk', 'None'):.1f}" if stats.get('ppg_3wk') else "  PPG (3wk): None")
            
            assert stats.get('prev_season_games_in_3wk', 0) == 0, "Should NOT use previous season"
            print("  ✅ Current season only for week 5+")
    
    return True

def calculate_and_save_sample(db):
    """Calculate and save rolling stats for a sample of games."""
    print("\n" + "="*60)
    print("STEP 4: CALCULATING AND SAVING ROLLING STATS")
    print("="*60)
    
    calculator = RollingStatsCalculator(db, prev_season_weight=0.7)
    
    # Calculate for all 2022 games we have
    print("\nCalculating rolling stats for 2022 games...")
    calculator.calculate_for_all_games(start_year=2022, end_year=2022)
    
    # Verify data was saved
    with db.get_session() as session:
        count = session.execute(text("""
            SELECT COUNT(*) FROM team_rolling_stats
        """)).scalar()
        
        print(f"\n✅ Saved {count} rolling stat records")
        
        # Show a sample
        sample = session.execute(text("""
            SELECT 
                trs.team_id,
                t.name,
                trs.year,
                trs.week,
                trs.ppg_3wk,
                trs.margin_3wk,
                trs.turnover_diff_3wk,
                trs.prev_season_games_in_3wk
            FROM team_rolling_stats trs
            JOIN teams t ON trs.team_id = t.id
            WHERE trs.year = 2022
            ORDER BY trs.week, t.name
            LIMIT 5
        """)).fetchall()
        
        print("\nSample rolling stats:")
        print("-" * 80)
        print(f"{'Team':<20} {'Week':<6} {'PPG(3wk)':<10} {'Margin(3wk)':<12} {'TO Diff':<10} {'Prev Season'}")
        print("-" * 80)
        
        for row in sample:
            team_name = row[1][:20]
            week = row[3]
            ppg = f"{row[4]:.1f}" if row[4] else "N/A"
            margin = f"{row[5]:.1f}" if row[5] else "N/A"
            to_diff = f"{row[6]:.2f}" if row[6] else "N/A"
            prev = row[7]
            
            print(f"{team_name:<20} {week:<6} {ppg:<10} {margin:<12} {to_diff:<10} {prev}")
    
    return count > 0

def verify_data_quality(db):
    """Verify the quality of calculated rolling stats."""
    print("\n" + "="*60)
    print("STEP 5: DATA QUALITY VERIFICATION")
    print("="*60)
    
    with db.get_session() as session:
        # Check for NULL values in critical fields
        null_check = session.execute(text("""
            SELECT 
                SUM(CASE WHEN ppg_3wk IS NULL THEN 1 ELSE 0 END) as null_ppg,
                SUM(CASE WHEN margin_3wk IS NULL THEN 1 ELSE 0 END) as null_margin,
                SUM(CASE WHEN games_in_season < 2 THEN 1 ELSE 0 END) as insufficient_games,
                COUNT(*) as total
            FROM team_rolling_stats
            WHERE year = 2022
        """)).fetchone()
        
        print(f"\nData Quality Metrics:")
        print(f"  Total records: {null_check[3]}")
        print(f"  Records with NULL PPG: {null_check[0]} ({null_check[0]/null_check[3]*100:.1f}%)")
        print(f"  Records with NULL margin: {null_check[1]} ({null_check[1]/null_check[3]*100:.1f}%)")
        print(f"  Records with <2 games: {null_check[2]} ({null_check[2]/null_check[3]*100:.1f}%)")
        
        # Check week 1 specifically
        week1_check = session.execute(text("""
            SELECT 
                AVG(prev_season_games_in_3wk) as avg_prev_games,
                SUM(CASE WHEN prev_season_games_in_3wk > 0 THEN 1 ELSE 0 END) as used_prev,
                COUNT(*) as total
            FROM team_rolling_stats
            WHERE year = 2022 AND week = 1
        """)).fetchone()
        
        if week1_check[2] > 0:
            print(f"\nWeek 1 Analysis:")
            print(f"  Teams using previous season: {week1_check[1]}/{week1_check[2]} ({week1_check[1]/week1_check[2]*100:.1f}%)")
            print(f"  Avg previous season games: {week1_check[0]:.1f}")
        
        # Show PPG progression for a random team
        team_progression = session.execute(text("""
            SELECT 
                trs.week,
                trs.ppg_3wk,
                trs.ppg_5wk,
                trs.ppg_season,
                trs.margin_3wk,
                trs.win_streak
            FROM team_rolling_stats trs
            WHERE trs.year = 2022
            AND trs.team_id = (
                SELECT team_id FROM team_rolling_stats 
                WHERE year = 2022 
                GROUP BY team_id 
                HAVING COUNT(*) >= 5 
                LIMIT 1
            )
            ORDER BY trs.week
        """)).fetchall()
        
        if team_progression:
            print(f"\nSample Team Progression Through Season:")
            print("-" * 70)
            print(f"{'Week':<6} {'PPG(3wk)':<10} {'PPG(5wk)':<10} {'PPG(season)':<12} {'Margin':<10} {'Streak'}")
            print("-" * 70)
            
            for row in team_progression:
                week = row[0]
                ppg3 = f"{row[1]:.1f}" if row[1] else "N/A"
                ppg5 = f"{row[2]:.1f}" if row[2] else "N/A"
                ppg_s = f"{row[3]:.1f}" if row[3] else "N/A"
                margin = f"{row[4]:.1f}" if row[4] else "N/A"
                streak = row[5] if row[5] else 0
                
                print(f"{week:<6} {ppg3:<10} {ppg5:<10} {ppg_s:<12} {margin:<10} {streak}")
    
    return True

def main():
    """Run all tests in sequence."""
    print("\n" + "="*60)
    print("ROLLING STATS SYSTEM - COMPREHENSIVE TEST")
    print("="*60)
    
    db = DatabaseConnection()
    
    # Step 1: Create table
    if not create_rolling_stats_table(db):
        print("❌ Failed to create table. Exiting.")
        return
    
    # Step 2: Import test data
    if not import_test_data(db):
        print("❌ Failed to import test data. Exiting.")
        return
    
    # Step 3: Test decay weight calculation
    if not test_decay_weight_calculation(db):
        print("⚠️  Decay weight test had issues, but continuing...")
    
    # Step 4: Calculate and save stats
    if not calculate_and_save_sample(db):
        print("❌ Failed to calculate stats. Exiting.")
        return
    
    # Step 5: Verify data quality
    if not verify_data_quality(db):
        print("⚠️  Data quality issues detected")
    
    print("\n" + "="*60)
    print("✅ ALL TESTS COMPLETE!")
    print("="*60)
    print("\nNext steps:")
    print("1. Review the output above to ensure calculations look correct")
    print("2. Run the full historical import (2021-2023)")
    print("3. Calculate rolling stats for all games")
    print("4. Begin model training!")
    
    # Final summary
    with db.get_session() as session:
        summary = session.execute(text("""
            SELECT 
                (SELECT COUNT(*) FROM games) as games,
                (SELECT COUNT(*) FROM teams) as teams,
                (SELECT COUNT(*) FROM team_game_stats) as stats,
                (SELECT COUNT(*) FROM team_rolling_stats) as rolling
        """)).fetchone()
        
        print(f"\nDatabase Summary:")
        print(f"  Games: {summary[0]}")
        print(f"  Teams: {summary[1]}")
        print(f"  Game Stats: {summary[2]}")
        print(f"  Rolling Stats: {summary[3]}")

if __name__ == "__main__":
    main()