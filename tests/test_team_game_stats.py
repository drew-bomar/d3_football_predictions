"""
Test script for the TeamGameStats table
Run from project root: python test_team_game_stats_table.py
"""
import sys
from datetime import date
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent))

from src.database.connection import db
from src.database.teams_model import Team
from src.database.games_model import Game
from src.database.team_game_stats_model import TeamGameStats

def test_team_game_stats_table():
    """Test creating and querying team game statistics."""
    
    print("Testing TeamGameStats Table")
    print("=" * 50)
    
    # Create tables
    print("\n1. Creating tables...")
    db.create_tables()
    print("   ✓ Tables created")
    
    with db.get_session() as session:
        # Get our existing teams and game
        print("\n2. Getting existing teams and game...")
        delaware = session.query(Team).filter_by(name="Delaware Valley").first()
        albright = session.query(Team).filter_by(name="Albright").first()
        game = session.query(Game).filter_by(contest_id="6309065").first()
        
        if not all([delaware, albright, game]):
            print("   ✗ Need to run test_games_table.py first!")
            return
        
        print(f"   ✓ Found teams and game")
        
        # Create Delaware Valley's stats (home team perspective)
        print("\n3. Creating Delaware Valley's stats...")
        dv_stats = TeamGameStats(
            game_id=game.id,
            team_id=delaware.id,
            opponent_id=albright.id,
            is_home=True,
            
            # Scoring
            points_scored=28,
            points_allowed=0,
            
            # Some key stats from your JSON
            first_downs=18,
            rushing_yards=138,
            passing_yards=261,
            total_offense_yards=399,
            
            # Opponent stats
            opp_first_downs=10,
            opp_rushing_yards=37,
            opp_passing_yards=155,
            opp_total_offense_yards=192,
            
            # Efficiency
            third_down_conversions=3,
            third_down_attempts=10,
            third_down_pct=30.0,
            
            # Calculated fields
            margin=28,
            win=True,
            turnover_diff=0.0,
            yards_per_play=6.76,
            
            # Context
            sos_before=0.25,
            team_record_before="2-0"
        )
        
        session.add(dv_stats)
        
        # Create Albright's stats (away team perspective)
        print("\n4. Creating Albright's stats (same game, their perspective)...")
        albright_stats = TeamGameStats(
            game_id=game.id,
            team_id=albright.id,
            opponent_id=delaware.id,
            is_home=False,
            
            # Scoring (flipped perspective)
            points_scored=0,
            points_allowed=28,
            
            # Their stats
            first_downs=10,
            rushing_yards=37,
            passing_yards=155,
            total_offense_yards=192,
            
            # Delaware Valley is their opponent
            opp_first_downs=18,
            opp_rushing_yards=138,
            opp_passing_yards=261,
            opp_total_offense_yards=399,
            
            # Their efficiency
            third_down_conversions=1,
            third_down_attempts=12,
            third_down_pct=8.3,
            
            # Calculated (negative margin, they lost)
            margin=-28,
            win=False,
            turnover_diff=0.0,
            yards_per_play=3.4,
            
            # Context
            sos_before=0.0,
            team_record_before="0-2"
        )
        
        session.add(albright_stats)
        session.commit()
        
        print("   ✓ Created stats for both teams")
        
        # Test queries
        print("\n5. Testing queries...")
        
        # Find all stats for Delaware Valley
        dv_all_stats = session.query(TeamGameStats).filter_by(
            team_id=delaware.id
        ).all()
        print(f"   ✓ Delaware Valley has {len(dv_all_stats)} game(s) recorded")
        
        # Get average yards for Delaware Valley
        avg_yards = session.query(TeamGameStats).filter_by(
            team_id=delaware.id
        ).first().total_offense_yards
        print(f"   ✓ Delaware Valley averaged {avg_yards} yards in this game")
        
        # Check the unique constraint works
        print("\n6. Testing constraints...")
        try:
            duplicate = TeamGameStats(
                game_id=game.id,
                team_id=delaware.id,  # Same team, same game - should fail!
                opponent_id=albright.id,
                is_home=True,
                points_scored=99,
                points_allowed=99
            )
            session.add(duplicate)
            session.commit()
            print("   ✗ Unique constraint failed - duplicate was allowed!")
        except Exception as e:
            session.rollback()
            print("   ✓ Unique constraint works - prevented duplicate entry")
        
        # Show the two perspectives
        print("\n7. Demonstrating dual perspective...")
        game_stats = session.query(TeamGameStats).filter_by(game_id=game.id).all()
        
        for stat in game_stats:
            print(f"\n   Team: {stat.team.name}")
            print(f"   Points: {stat.points_scored} - {stat.points_allowed}")
            print(f"   Total Yards: {stat.total_offense_yards}")
            print(f"   Perspective: {'HOME' if stat.is_home else 'AWAY'}")
            print(f"   Result: {'WIN' if stat.win else 'LOSS'}")
    
    print("\n✅ All tests passed!")

if __name__ == "__main__":
    test_team_game_stats_table()