from src.database.connection import DatabaseConnection
from sqlalchemy import text

def check_database_state():
    """Check what data currently exists in our database."""
    db = DatabaseConnection()
    
    with db.get_session() as session:
        # Get basic counts first
        teams_count = session.execute(text("SELECT COUNT(*) FROM teams")).scalar()
        games_count = session.execute(text("SELECT COUNT(*) FROM games")).scalar()
        stats_count = session.execute(text("SELECT COUNT(*) FROM team_game_stats")).scalar()
        
        print("=" * 50)
        print("DATABASE STATE CHECK")
        print("=" * 50)
        print(f"Teams: {teams_count}")
        print(f"Games: {games_count}")
        print(f"Stats Records: {stats_count}")
        
        # Only query for more details if we have data
        if games_count > 0:
            # Get games by year/week
            games_by_week = session.execute(text("""
                SELECT year, week, COUNT(*) as game_count 
                FROM games 
                GROUP BY year, week 
                ORDER BY year, week
            """)).fetchall()
            
            print("\nGames by Year/Week:")
            for row in games_by_week:
                print(f"  {row[0]} Week {row[1]}: {row[2]} games")
            
            # Get sample games
            sample_games = session.execute(text("""
                SELECT g.contest_id, g.year, g.week, 
                       ht.name as home_team, at.name as away_team,
                       g.home_score, g.away_score
                FROM games g
                JOIN teams ht ON g.home_team_id = ht.id
                JOIN teams at ON g.away_team_id = at.id
                LIMIT 5
            """)).fetchall()
            
            print("\nSample Games:")
            for game in sample_games:
                print(f"  {game[1]} Week {game[2]}: {game[3]} {game[5]} - {game[6]} {game[4]}")
                print(f"    Contest ID: {game[0]}")
        else:
            print("\nNo games in database yet.")
        
        # Check for any orphaned records
        orphan_stats = session.execute(text("""
            SELECT COUNT(*) 
            FROM team_game_stats tgs
            LEFT JOIN games g ON tgs.game_id = g.id
            WHERE g.id IS NULL
        """)).scalar()
        
        if orphan_stats > 0:
            print(f"\n⚠️  WARNING: Found {orphan_stats} orphaned stat records")
        
        # Get team list
        teams = session.execute(text("""
            SELECT name, slug, ncaa_id 
            FROM teams 
            ORDER BY name
            LIMIT 20
        """)).fetchall()
        
        if teams:
            print(f"\nTeams in database (showing up to 20):")
            for team in teams:
                print(f"  - {team[0]} (slug: {team[1]}, ncaa_id: {team[2]})")
        
        return {
            'teams': teams_count,
            'games': games_count,
            'stats': stats_count,
            'has_data': games_count > 0
        }

def clean_database(year=None, week=None, dry_run=True):
    """Clean database - FIXED VERSION"""
    db = DatabaseConnection()
    
    with db.get_session() as session:
        if year and week:
            # Specific week deletion code...
            pass
        else:
            # Full cleanup - THIS PART NEEDS FIXING
            games = session.execute(text("SELECT COUNT(*) FROM games")).scalar()
            stats = session.execute(text("SELECT COUNT(*) FROM team_game_stats")).scalar()
            teams = session.execute(text("SELECT COUNT(*) FROM teams")).scalar()
            
            print(f"\nFull database cleanup would delete:")
            print(f"  - {games} games")
            print(f"  - {stats} stat records")
            print(f"  - {teams} teams")
            
            # THIS IS THE MISSING PART - it's not doing anything after showing counts!
            if not dry_run and (games > 0 or teams > 0):
                print("\n⚠️  WARNING: This will delete ALL data!")
                confirm = input("Type 'DELETE ALL' to confirm: ")
                if confirm == "DELETE ALL":
                    # Order matters due to foreign keys
                    session.execute(text("DELETE FROM team_game_stats"))
                    session.execute(text("DELETE FROM games"))
                    session.execute(text("DELETE FROM teams"))
                    session.commit()
                    print("✅ Database cleared!")
                else:
                    print("Cancelled")
            elif not dry_run:
                # Even if games is 0, still delete teams if they exist
                if teams > 0:
                    confirm = input("Delete the 1 team? (yes/no): ")
                    if confirm.lower() == 'yes':
                        session.execute(text("DELETE FROM teams"))
                        session.commit()
                        print("✅ Teams cleared!")

def get_detailed_stats():
    """Get more detailed statistics about the data."""
    db = DatabaseConnection()
    
    with db.get_session() as session:
        # Get date range of games
        date_range = session.execute(text("""
            SELECT MIN(game_date), MAX(game_date)
            FROM games
        """)).fetchone()
        
        if date_range and date_range[0]:
            print(f"\nDate Range: {date_range[0]} to {date_range[1]}")
        
        # Get stats completeness
        missing_stats = session.execute(text("""
            SELECT COUNT(*)
            FROM games g
            LEFT JOIN team_game_stats tgs ON g.id = tgs.game_id
            WHERE tgs.id IS NULL
        """)).scalar()
        
        if missing_stats > 0:
            print(f"Games missing stats: {missing_stats}")
        
        # Get average scores
        avg_scores = session.execute(text("""
            SELECT 
                AVG(home_score) as avg_home,
                AVG(away_score) as avg_away,
                AVG(home_score + away_score) as avg_total
            FROM games
            WHERE home_score IS NOT NULL
        """)).fetchone()
        
        if avg_scores and avg_scores[0]:
            print(f"\nAverage Scores:")
            print(f"  Home: {avg_scores[0]:.1f}")
            print(f"  Away: {avg_scores[1]:.1f}")  
            print(f"  Total: {avg_scores[2]:.1f}")

if __name__ == "__main__":
    # Check current state
    state = check_database_state()
    
    # Show detailed stats if we have data
    if state['has_data']:
        get_detailed_stats()
    
    print("\n" + "=" * 50)
    print("CLEANUP OPTIONS:")
    print("=" * 50)
    print("1. Keep this test data (only 1 game)")
    print("2. Clean everything for fresh start")
    print("3. Exit without changes")
    
    choice = input("\nYour choice (1/2/3): ")
    
    if choice == '2':
        clean_database(dry_run=False)  # Clean everything
    elif choice == '1':
        print("Keeping existing data")
    else:
        print("No changes made")