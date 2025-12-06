"""
Test script for the Games table
Run from project root: python test_games_table.py
"""
import sys
from datetime import date
from pathlib import Path

# Add src to path so we can import our modules
sys.path.append(str(Path(__file__).parent))

from src.database.connection import db
from src.database.teams_model import Team
from src.database.games_model import Game

def test_games_table():
    """Test creating and querying games."""
    
    print("Testing Games Table")
    print("=" * 50)
    
    # Create tables
    print("\n1. Creating tables...")
    db.create_tables()
    print("   ✓ Tables created")
    
    with db.get_session() as session:
        # First, we need teams to reference
        print("\n2. Creating test teams...")
        
        # Create or get teams
        delaware = Team.find_or_create(
            session, 
            ncaa_id="del-valley-2024",
            name="Delaware Valley",
            conference="MAC"
        )
        
        albright = Team.find_or_create(
            session,
            ncaa_id="albright-2024", 
            name="Albright",
            conference="MAC"
        )
        
        session.commit()
        print(f"   ✓ Created teams: {delaware.name}, {albright.name}")
        
        # Create a game
        print("\n3. Creating test game...")
        
        game = Game(
            contest_id="6309065",
            game_date=date(2024, 10, 26),
            year=2024,
            week=9,
            home_team_id=delaware.id,
            away_team_id=albright.id,
            home_score=28,
            away_score=0,
            status='final'
        )
        
        session.add(game)
        session.commit()
        
        print(f"   ✓ Created game: {game.contest_id}")
        print(f"     Home: {game.home_team.name} - {game.home_score}")
        print(f"     Away: {game.away_team.name} - {game.away_score}")
        
        # Test our properties
        print("\n4. Testing calculated properties...")
        print(f"   Margin: {game.margin} (positive = home won)")
        print(f"   Total points: {game.total_points}")
        print(f"   Winner: {game.winner}")
        
        # Query the game
        print("\n5. Testing queries...")
        
        # Find by contest_id
        found_game = session.query(Game).filter_by(contest_id="6309065").first()
        print(f"   ✓ Found game by contest_id: {found_game.contest_id}")
        
        # Find week 9 games
        week_9_games = session.query(Game).filter_by(year=2024, week=9).all()
        print(f"   ✓ Found {len(week_9_games)} game(s) in week 9")
        
        # Find Delaware Valley's home games
        dv_home_games = session.query(Game).filter_by(home_team_id=delaware.id).all()
        print(f"   ✓ Delaware Valley has {len(dv_home_games)} home game(s)")
    
    print("\n✅ All tests passed!")

if __name__ == "__main__":
    test_games_table()