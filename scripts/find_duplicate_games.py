import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.database.connection import DatabaseConnection
from src.database.games_model import Game
from src.database.teams_model import Team
from sqlalchemy import func

def find_duplicate_games(year: int = None, week: int = None):
    """
    Find duplicate games (same teams, year, week).
    A duplicate is defined as games with the same home_team_id, away_team_id, year, and week.
    """
    db = DatabaseConnection()
    
    with db.get_session() as session:
        # Query to find duplicates
        query = session.query(
            Game.year,
            Game.week,
            Game.home_team_id,
            Game.away_team_id,
            func.count(Game.id).label('count')
        ).group_by(
            Game.year,
            Game.week,
            Game.home_team_id,
            Game.away_team_id
        ).having(
            func.count(Game.id) > 1
        )
        
        if year:
            query = query.filter(Game.year == year)
        if week:
            query = query.filter(Game.week == week)
        
        duplicates = query.all()
        
        if not duplicates:
            print("✅ No duplicate games found!")
            return
        
        print(f"\n{'='*80}")
        print(f"FOUND {len(duplicates)} DUPLICATE GAME SETS")
        print(f"{'='*80}\n")
        
        for dup in duplicates:
            year, week, home_id, away_id, count = dup
            
            # Get team names
            home_team = session.query(Team).get(home_id)
            away_team = session.query(Team).get(away_id)
            
            print(f"\n{'-'*80}")
            print(f"DUPLICATE: {away_team.name} @ {home_team.name}")
            print(f"Year: {year}, Week: {week}, Count: {count}")
            print(f"{'-'*80}")
            
            # Get all instances of this game
            games = session.query(Game).filter(
                Game.year == year,
                Game.week == week,
                Game.home_team_id == home_id,
                Game.away_team_id == away_id
            ).all()
            
            print("\nInstances:")
            for i, game in enumerate(games, 1):
                print(f"  {i}. Game ID: {game.id}")
                print(f"     Score: {game.away_score} - {game.home_score}")
                print(f"     Created: {game.created_at}")
                print(f"     Stats ID: {game.stats_id}")
            
            # Ask which to delete
            print("\nWhich game(s) should be deleted?")
            print("  Enter game numbers separated by commas (e.g., '1,2')")
            print("  Or 'skip' to keep all")
            
            response = input("Delete: ").strip().lower()
            
            if response == 'skip':
                print("  Skipping...")
                continue
            
            try:
                indices_to_delete = [int(x.strip()) - 1 for x in response.split(',')]
                games_to_delete = [games[i] for i in indices_to_delete]
                
                confirm = input(f"  Confirm deletion of {len(games_to_delete)} game(s)? (yes/no): ")
                if confirm.lower() == 'yes':
                    for game in games_to_delete:
                        session.delete(game)
                    session.commit()
                    print(f"  ✅ Deleted {len(games_to_delete)} game(s)")
                else:
                    print("  ❌ Cancelled")
            except (ValueError, IndexError) as e:
                print(f"  ❌ Invalid input: {e}")

if __name__ == "__main__":
    year = int(sys.argv[1]) if len(sys.argv) > 1 else None
    week = int(sys.argv[2]) if len(sys.argv) > 2 else None
    
    if year:
        print(f"Searching for duplicates in {year}" + (f" week {week}" if week else ""))
    else:
        print("Searching for duplicates in all years/weeks")
    
    find_duplicate_games(year, week)