"""
staged_historical_import.py
Import D3 football data season by season with health checks and confirmation
"""
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.pipeline.simple_pipeline import SimplePipeline
from src.features.rolling_stats_calculator import RollingStatsCalculator
from src.database.connection import DatabaseConnection
from sqlalchemy import text
import time

def health_check(db, year):
    """Run health check on imported data for a specific year."""
    print(f"\n{'='*60}")
    print(f"HEALTH CHECK FOR {year}")
    print('='*60)
    
    with db.get_session() as session:
        # Basic counts
        games = session.execute(text("""
            SELECT COUNT(*) FROM games WHERE year = :year
        """), {'year': year}).scalar()
        
        stats = session.execute(text("""
            SELECT COUNT(*) FROM team_game_stats tgs
            JOIN games g ON tgs.game_id = g.id
            WHERE g.year = :year
        """), {'year': year}).scalar()
        
        # Games per week
        by_week = session.execute(text("""
            SELECT week, COUNT(*) as count
            FROM games 
            WHERE year = :year
            GROUP BY week
            ORDER BY week
        """), {'year': year}).fetchall()
        
        # Failed imports (games without stats)
        no_stats = session.execute(text("""
            SELECT COUNT(*) FROM games g
            LEFT JOIN team_game_stats tgs ON g.id = tgs.game_id
            WHERE g.year = :year AND tgs.id IS NULL
        """), {'year': year}).scalar()
        
        # Average scores to check data quality
        avg_scores = session.execute(text("""
            SELECT 
                ROUND(AVG(home_score), 1) as avg_home,
                ROUND(AVG(away_score), 1) as avg_away,
                ROUND(AVG(home_score + away_score), 1) as avg_total
            FROM games
            WHERE year = :year AND home_score IS NOT NULL
        """), {'year': year}).fetchone()
    
    # Display results
    print(f"\nGames imported: {games}")
    print(f"Stats records: {stats} (should be ~2x games)")
    print(f"Games missing stats: {no_stats}")
    
    print(f"\nGames by week:")
    week_list = [f"W{w}:{c}" for w, c in by_week]
    for i in range(0, len(week_list), 10):
        print(f"  {' '.join(week_list[i:i+10])}")
    
    if avg_scores[0]:
        print(f"\nAverage scores:")
        print(f"  Home: {avg_scores[0]}, Away: {avg_scores[1]}, Total: {avg_scores[2]}")
    
    # Quality checks
    issues = []
    if games < 1000:  # D3 has ~1200-1500 games per season
        issues.append(f"Low game count ({games} - expected ~1200+)")
    if stats < games * 1.9:  # Should be 2x games
        issues.append(f"Missing stats records")
    if no_stats > 10:
        issues.append(f"{no_stats} games have no statistics")
    if avg_scores[2] and (avg_scores[2] < 30 or avg_scores[2] > 70):
        issues.append(f"Unusual average score: {avg_scores[2]}")
    
    if issues:
        print(f"\n⚠️  POTENTIAL ISSUES:")
        for issue in issues:
            print(f"  - {issue}")
        return False
    else:
        print(f"\n✅ {year} data looks healthy!")
        return True

def import_season(pipeline, year, start_week=1, end_week=15):
    """Import a single season with progress tracking."""
    print(f"\n{'='*60}")
    print(f"IMPORTING {year} SEASON")
    print('='*60)
    
    total_imported = 0
    total_skipped = 0
    total_failed = 0
    start_time = time.time()
    
    for week in range(start_week, end_week + 1):
        print(f"\nWeek {week}...")
        result = pipeline.import_week(year, week)
        
        imported = result.get('imported', 0)
        skipped = result.get('skipped', 0)
        failed = len(result.get('failed', []))
        
        total_imported += imported
        total_skipped += skipped
        total_failed += failed
        
        print(f"  Imported: {imported}, Skipped: {skipped}, Failed: {failed}")
        
        # Show some failed games if any
        if failed > 0 and failed <= 3:
            for game_id, error in result['failed'][:3]:
                print(f"    Failed: {game_id} - {error[:50]}")
        
        time.sleep(1)  # Be nice to NCAA servers
    
    elapsed = time.time() - start_time
    print(f"\n{year} Season Complete:")
    print(f"  Total imported: {total_imported}")
    print(f"  Total skipped: {total_skipped}")
    print(f"  Total failed: {total_failed}")
    print(f"  Time: {elapsed/60:.1f} minutes")
    
    return total_imported > 0

def main():
    """Main execution with staged imports and confirmations."""
    print("\n" + "="*60)
    print("STAGED HISTORICAL IMPORT")
    print("="*60)
    
    pipeline = SimplePipeline(delay=1.0)
    db = DatabaseConnection()
    
    # Check current state
    with db.get_session() as session:
        existing = session.execute(text("""
            SELECT year, COUNT(*) as games
            FROM games
            GROUP BY year
            ORDER BY year
        """)).fetchall()
        
        if existing:
            print("\nCurrent data in database:")
            for year, count in existing:
                print(f"  {year}: {count} games")
    
    # Import each season with confirmation
    seasons = [2021, 2022, 2023, 2024,2025]
    
    for year in seasons:
        print(f"\n{'='*60}")
        print(f"Ready to import {year} season (weeks 1-15)")
        print(f"Estimated time: 30-40 minutes")
        
        response = input(f"\nProceed with {year}? (yes/skip/quit): ").lower()
        
        if response == 'quit':
            print("Stopping import process.")
            break
        elif response == 'skip':
            print(f"Skipping {year}")
            continue
        elif response != 'yes':
            print("Invalid response. Use 'yes', 'skip', or 'quit'")
            continue
        
        # Import the season
        success = import_season(pipeline, year)
        
        if not success:
            print(f"\n❌ Issues importing {year}")
            if input("Continue anyway? (yes/no): ").lower() != 'yes':
                break
        
        # Run health check
        healthy = health_check(db, year)
        
        if not healthy:
            print(f"\n⚠️  {year} data may have issues")
            response = input("Continue to next season? (yes/no): ").lower()
            if response != 'yes':
                break
    
    # After all imports, offer to calculate rolling stats
    print(f"\n{'='*60}")
    print("IMPORT PHASE COMPLETE")
    print('='*60)
    
    with db.get_session() as session:
        total_games = session.execute(text("SELECT COUNT(*) FROM games")).scalar()
        years_with_data = session.execute(text("""
            SELECT year FROM games GROUP BY year ORDER BY year
        """)).fetchall()
    
    print(f"\nTotal games in database: {total_games}")
    print(f"Years with data: {', '.join(str(y[0]) for y in years_with_data)}")
    
    if len(years_with_data) >= 2:
        print("\nReady to calculate rolling stats for 2022-2025")
        print("This will take ~10-15 minutes")
        
        if input("\nCalculate rolling stats now? (yes/no): ").lower() == 'yes':
            calculator = RollingStatsCalculator(db)
            print("\nCalculating rolling stats...")
            calculator.calculate_for_all_games(start_year=2022, end_year=2025)
            
            with db.get_session() as session:
                rolling_count = session.execute(text("""
                    SELECT COUNT(*) FROM team_rolling_stats
                """)).scalar()
            
            print(f"✅ Created {rolling_count} rolling stat records!")
    
    print("\n✅ Process complete!")

if __name__ == "__main__":
    main()