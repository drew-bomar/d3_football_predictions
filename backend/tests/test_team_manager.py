"""
Test script for the Team Manager component
Run this to verify team resolution and caching work correctly
"""

import sys
import logging
from pathlib import Path

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from src.database.connection import DatabaseConnection
from src.pipeline.team_manager import TeamManager

# Configure logging to see what's happening
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_team_manager():
    """
    Test the Team Manager with real NCAA data examples.
    """
    # Initialize database connection
    db = DatabaseConnection()
    
    # Create Team Manager
    team_manager = TeamManager(db)
    
    print("\n" + "="*60)
    print("TEAM MANAGER TEST")
    print("="*60)
    
    # Test data from your Week 8 games
    test_teams = [
        {
            'seoname': 'wisconsinwhitewater',
            'nameShort': 'Wis.-Whitewater',
            'teamId': '967'
        },
        {
            'seoname': 'millikin',
            'nameShort': 'Millikin',
            'teamId': '432'
        },
        {
            'seoname': 'mountunion',
            'nameShort': 'Mount Union',
            'teamId': '456'
        },
        {
            'seoname': 'wisconsinwhitewater',  # Duplicate to test caching
            'nameShort': 'Wis.-Whitewater',
            'teamId': '967'
        }
    ]
    
    print(f"\nTesting with {len(test_teams)} team lookups...")
    print("-" * 40)
    
    with db.get_session() as session:
        for i, team_data in enumerate(test_teams, 1):
            print(f"\n{i}. Processing: {team_data['nameShort']}")
            print(f"   Seoname: {team_data['seoname']}")
            
            # Find or create the team
            team_id, was_created = team_manager.find_or_create_team(session, team_data)
            
            if was_created:
                print(f"   ✅ CREATED new team with ID: {team_id}")
            else:
                print(f"   ✅ FOUND existing team with ID: {team_id}")
        
        # Commit the transaction
        session.commit()
    
    # Test cache lookup (without database)
    print("\n" + "-"*40)
    print("Testing cache lookups (no database)...")
    print("-"*40)
    
    test_lookups = [
        'wisconsinwhitewater',
        'mountunion',
        'unknownteam'  # Should return None
    ]
    
    for identifier in test_lookups:
        team_id = team_manager.resolve_team_id(identifier)
        if team_id:
            print(f"✅ Cache hit: '{identifier}' -> Team ID {team_id}")
        else:
            print(f"❌ Cache miss: '{identifier}' not found")
    
    # Display cache statistics
    print("\n" + "="*60)
    print("CACHE STATISTICS")
    print("="*60)
    
    stats = team_manager.get_cache_stats()
    for key, value in stats.items():
        print(f"{key:20}: {value}")
    
    # Test bulk ensure teams
    print("\n" + "="*60)
    print("TESTING BULK ENSURE TEAMS")
    print("="*60)
    
    # Simulate a week's worth of games
    fake_games = [
        {
            'contestId': '1',
            'teams': [
                {'seoname': 'hope', 'nameShort': 'Hope', 'teamId': '123'},
                {'seoname': 'adrian', 'nameShort': 'Adrian', 'teamId': '124'}
            ]
        },
        {
            'contestId': '2',
            'teams': [
                {'seoname': 'mountunion', 'nameShort': 'Mount Union', 'teamId': '456'},
                {'seoname': 'johncarroll', 'nameShort': 'John Carroll', 'teamId': '457'}
            ]
        }
    ]
    
    with db.get_session() as session:
        team_mapping = team_manager.bulk_ensure_teams(session, fake_games)
        session.commit()
        
        print(f"\nProcessed {len(fake_games)} games")
        print(f"Team mapping contains {len(team_mapping)} teams")
        print("\nSample mappings:")
        for seoname in ['hope', 'adrian', 'mountunion']:
            if seoname in team_mapping:
                print(f"  {seoname:20} -> Team ID {team_mapping[seoname]}")
    
    # Final statistics
    print("\n" + "="*60)
    print("FINAL STATISTICS")
    print("="*60)
    
    final_stats = team_manager.get_cache_stats()
    for key, value in final_stats.items():
        print(f"{key:20}: {value}")
    
    print("\n✅ Team Manager test complete!")
    
    # Cleanup
    db.dispose()


if __name__ == "__main__":
    test_team_manager()