import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.features.rolling_stats_calculator import RollingStatsCalculator
from src.database.connection import DatabaseConnection

def calculate_rolling_stats(start_year: int, end_year: int):
    """Calculate rolling stats for a range of years"""
    db = DatabaseConnection()
    calc = RollingStatsCalculator(db)
    
    print(f"Calculating rolling stats for {start_year}-{end_year}...")
    print("This may take a few minutes...\n")
    
    calc.calculate_for_all_games(start_year=start_year, end_year=end_year)
    
    print("\n✅ Rolling stats calculation complete!")
    
    # Verify
    with db.get_session() as session:
        from src.database.team_rolling_stats_model import TeamRollingStats
        from src.database.games_model import Game
        
        for year in range(start_year, end_year + 1):
            game_count = session.query(Game).filter(Game.year == year).count()
            stats_count = session.query(TeamRollingStats).filter(
                TeamRollingStats.year == year
            ).count()
            
            # Each game should have 2 rolling stats records (one per team)
            expected = game_count * 2
            print(f"  {year}: {stats_count}/{expected} rolling stats records")
            if stats_count < expected:
                print(f"    ⚠️  Missing {expected - stats_count} records")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/calculate_rolling_stats.py <year> [end_year]")
        print("Examples:")
        print("  python scripts/calculate_rolling_stats.py 2025")
        print("  python scripts/calculate_rolling_stats.py 2022 2025")
        sys.exit(1)
    
    start_year = int(sys.argv[1])
    end_year = int(sys.argv[2]) if len(sys.argv) > 2 else start_year
    
    calculate_rolling_stats(start_year, end_year)