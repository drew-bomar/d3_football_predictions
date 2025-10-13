import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.database.connection import DatabaseConnection
from src.database.games_model import Game
from src.database.teams_model import Team
from src.database.team_rolling_stats_model import TeamRollingStats

def investigate_missing_stats(year: int, week: int):
    """
    Investigate why certain teams are missing rolling stats.
    
    For predictions in a given week, we need rolling stats from previous weeks.
    This script checks:
    1. If teams exist in the database
    2. If teams have any games in weeks leading up to prediction week
    3. If rolling stats exist for those games
    """
    db = DatabaseConnection()
    
    # These are the matchups that failed from the output
    failed_matchups = [
        ("Muhlenberg", "Franklin & Marshall"),
        ("Fitchburg St.", "Worcester St."),
        ("Chris. Newport", "VTSU Castleton"),
        ("Wis.-Eau Claire", "Wis.-La Crosse"),
        ("WestConn", "New Haven"),
        ("SUNY Maritime", "Merchant Marine"),
        ("Rowan", "Montclair St."),
        ("Bethel (MN)", "Augsburg"),
        ("Cal Lutheran", "La Verne"),
        ("Pacific (OR)", "Linfield"),
        ("Texas Lutheran", "Centenary (LA)")
    ]
    
    with db.get_session() as session:
        print(f"\n{'='*80}")
        print(f"INVESTIGATING MISSING ROLLING STATS FOR {year} WEEK {week}")
        print(f"{'='*80}\n")
        
        for away_name, home_name in failed_matchups:
            print(f"\n{'-'*80}")
            print(f"MATCHUP: {away_name} @ {home_name}")
            print(f"{'-'*80}")
            
            # Check both teams
            for team_name in [away_name, home_name]:
                print(f"\n  Team: {team_name}")
                
                # 1. Does team exist?
                team = session.query(Team).filter(
                    Team.name == team_name
                ).first()
                
                if not team:
                    print(f"    ❌ Team not found in database!")
                    continue
                
                print(f"    ✅ Team found (ID: {team.id})")
                
                # 2. How many games has this team played this season before this week?
                games_count = session.query(Game).filter(
                    ((Game.home_team_id == team.id) | (Game.away_team_id == team.id)),
                    Game.year == year,
                    Game.week < week  # Games BEFORE prediction week
                ).count()
                
                print(f"    📊 Games played in weeks 1-{week-1}: {games_count}")
                
                if games_count == 0:
                    print(f"    ⚠️  No games played yet - can't calculate rolling stats!")
                    continue
                
                # 3. Do rolling stats exist for this team?
                rolling_stats_count = session.query(TeamRollingStats).filter(
                    TeamRollingStats.team_id == team.id,
                    TeamRollingStats.year == year,
                    TeamRollingStats.week < week
                ).count()
                
                print(f"    📈 Rolling stats records: {rolling_stats_count}")
                
                if rolling_stats_count == 0:
                    print(f"    ❌ No rolling stats calculated!")
                    
                    # Show the actual games
                    games = session.query(Game).filter(
                        ((Game.home_team_id == team.id) | (Game.away_team_id == team.id)),
                        Game.year == year,
                        Game.week < week
                    ).all()
                    
                    print(f"    Games that need rolling stats:")
                    for game in games:
                        home_team = session.query(Team).get(game.home_team_id)
                        away_team = session.query(Team).get(game.away_team_id)
                        print(f"      Week {game.week}: {away_team.name} @ {home_team.name}")
                
                elif rolling_stats_count < games_count:
                    print(f"    ⚠️  Only {rolling_stats_count}/{games_count} stats calculated")
                else:
                    print(f"    ✅ Rolling stats look complete")
        
        print(f"\n{'='*80}")
        print("SUMMARY")
        print(f"{'='*80}")
        print("\nTo fix missing rolling stats, run:")
        print(f"  python -c \"from src.features.rolling_stats_calculator import RollingStatsCalculator; ")
        print(f"from src.database.connection import DatabaseConnection; ")
        print(f"calc = RollingStatsCalculator(DatabaseConnection()); ")
        print(f"calc.calculate_for_all_games(start_year={year}, end_year={year})\"")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python scripts/investigate_missing_stats.py <year> <week>")
        sys.exit(1)
    
    year = int(sys.argv[1])
    week = int(sys.argv[2])
    
    investigate_missing_stats(year, week)