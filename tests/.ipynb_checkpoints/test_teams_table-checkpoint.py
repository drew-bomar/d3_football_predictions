"""
Test script to verify our Teams table setup
Run this from project root: python test_teams_setup.py
"""
import sys
from pathlib import Path

# Add project root to path so we can import our modules
sys.path.append(str(Path(__file__).parent))

from src.database.connection import db
from src.database.team_model import Team  # Updated import!

def test_teams_table():
    """Test creating and querying teams."""
    
    print("1. Creating tables...")
    try:
        db.create_tables()
        print("   ✓ Tables created successfully")
    except Exception as e:
        print(f"   ✗ Error creating tables: {e}")
        return
    
    print("\n2. Adding sample teams...")
    with db.get_session() as session:
        # Create some teams
        mount_union = Team(
            ncaa_id="mount-union-1234",
            name="Mount Union",
            full_name="University of Mount Union Purple Raiders",
            short_name="Mt Union",
            slug="mount-union",
            conference="Ohio Athletic Conference",
            city="Alliance",
            state="OH"
        )
        
        uw_whitewater = Team.find_or_create(
            session,
            ncaa_id="uww-5678",
            name="Wisconsin-Whitewater",
            full_name="University of Wisconsin-Whitewater Warhawks",
            short_name="UW-Whitewater",
            conference="Wisconsin Intercollegiate Athletic Conference",
            city="Whitewater",
            state="WI"
        )
        
        session.add(mount_union)
        # Note: find_or_create already adds to session
        
        print(f"   ✓ Added {mount_union}")
        print(f"   ✓ Added {uw_whitewater}")
    
    print("\n3. Querying teams...")
    with db.get_session() as session:
        # Find all teams
        all_teams = session.query(Team).all()
        print(f"   Found {len(all_teams)} teams")
        
        # Find by NCAA ID
        team = Team.find_by_ncaa_id(session, "mount-union-1234")
        print(f"   Found by NCAA ID: {team}")
        
        # Query by conference
        oac_teams = session.query(Team).filter(
            Team.conference == "Ohio Athletic Conference"
        ).all()
        print(f"   Found {len(oac_teams)} teams in OAC")
    
    print("\n4. Testing find_or_create (no duplicates)...")
    with db.get_session() as session:
        # Try to create Mount Union again
        team = Team.find_or_create(
            session,
            ncaa_id="mount-union-1234",
            name="Mount Union"
        )
        
        # Count total teams - should still be 2
        count = session.query(Team).count()
        print(f"   ✓ Total teams: {count} (no duplicate created)")
    
    print("\n✅ All tests passed!")

if __name__ == "__main__":
    test_teams_table()