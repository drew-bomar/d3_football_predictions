# test_api_client.py
"""
Test script for NCAA API Client
Run this to verify we can fetch data successfully
"""

import logging
import json
from pipeline.optimized_pipeline.ncaa_api_client import NCAAAPIClient

# Set up logging to see what's happening
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def test_week_fetch():
    """Test fetching a week's games"""
    print("\n" + "="*60)
    print("TEST 1: Fetching Week Schedule")
    print("="*60)
    
    client = NCAAAPIClient()
    
    # Test with 2024 Week 8 (from your example)
    result = client.get_week_games(2024, 8)
    
    if result['success']:
        print(f"✅ Successfully fetched Week 8 games!")
        print(f"Found {len(result['games'])} games\n")
        
        # Show first 3 games as examples
        games_to_test = []
        for i, game in enumerate(result['games'][:3]):
            # Find home and away teams
            home = next((t for t in game.get('teams', []) if t.get('isHome')), {})
            away = next((t for t in game.get('teams', []) if not t.get('isHome')), {})
            
            print(f"Game {i+1}:")
            print(f"  Contest ID: {game.get('contestId')}")
            print(f"  {away.get('nameShort', 'Unknown')} @ {home.get('nameShort', 'Unknown')}")
            print(f"  Score: {away.get('score', 'N/A')} - {home.get('score', 'N/A')}")
            print(f"  Status: {game.get('gameState', 'Unknown')}")
            print(f"  Date: {game.get('startDate', 'Unknown')}")
            
            # Save contest ID for stats test
            if game.get('contestId'):
                games_to_test.append(game.get('contestId'))
            print()
        
        return games_to_test
    else:
        print(f"❌ Failed to fetch games: {result.get('error')}")
        return []

def test_game_stats(contest_id: int):
    """Test fetching stats for a specific game"""
    print("\n" + "="*60)
    print(f"TEST 2: Fetching Game Stats for Contest {contest_id}")
    print("="*60)
    
    client = NCAAAPIClient()
    
    result = client.get_game_stats(contest_id)
    
    if result['success']:
        print(f"✅ Successfully fetched stats for game {contest_id}!")
        print(f"\nGame Info:")
        print(f"  Description: {result.get('description', 'Unknown')}")
        print(f"  Status: {result.get('status', 'Unknown')} - {result.get('period', '')}")
        
        # Show stats for each team
        for team_stats in result.get('team_stats', []):
            print(f"\n{team_stats.get('team_name')} {'(Home)' if team_stats.get('is_home') else '(Away)'}:")
            print(f"  Total Yards: {team_stats.get('total_yards', 'N/A')}")
            print(f"  First Downs: {team_stats.get('first_downs', 'N/A')}")
            print(f"  Passing: {team_stats.get('passing_completions', 'N/A')}/{team_stats.get('passing_attempts', 'N/A')} for {team_stats.get('passing_yards', 'N/A')} yards")
            print(f"  Rushing: {team_stats.get('rushing_attempts', 'N/A')} att for {team_stats.get('rushing_yards', 'N/A')} yards")
            print(f"  Third Downs: {team_stats.get('third_down_conversions', 'N/A')}/{team_stats.get('third_down_attempts', 'N/A')}")
            print(f"  Turnovers: {team_stats.get('fumbles_lost', 'N/A')} fumbles, {team_stats.get('passing_interceptions', 'N/A')} INTs")
            print(f"  Penalties: {team_stats.get('penalties', 'N/A')} for {team_stats.get('penalty_yards', 'N/A')} yards")
        
        return True
    else:
        print(f"❌ Failed to fetch stats: {result.get('error')}")
        return False

def test_specific_game():
    """Test with a known game ID from your examples"""
    print("\n" + "="*60)
    print("TEST 3: Testing with Known Game (Wis.-Whitewater vs Wis.-Stevens Point)")
    print("="*60)
    
    client = NCAAAPIClient()
    
    # Using the contest ID from your example
    contest_id = 6308940
    result = client.get_game_stats(contest_id)
    
    if result['success']:
        print(f"✅ Successfully fetched known game!")
        print(f"\nGame: {result.get('description')}")
        
        # Verify we got the right teams
        teams = result.get('teams', [])
        team_names = [t.get('nameShort') for t in teams]
        print(f"Teams found: {', '.join(team_names)}")
        
        # Check that we have stats for both teams
        stats_count = len(result.get('team_stats', []))
        print(f"Stats records: {stats_count} (should be 2)")
        
        # Show a sample of the data structure
        if result.get('team_stats'):
            first_team = result['team_stats'][0]
            print(f"\nSample stats structure for {first_team.get('team_name')}:")
            print(f"  Stats fields available: {len(first_team)} fields")
            
            # Show first 5 non-None stats
            non_none_stats = [(k, v) for k, v in first_team.items() 
                             if v is not None and k not in ['team_id', 'team_name', 'is_home']][:5]
            for key, value in non_none_stats:
                print(f"    {key}: {value}")
    else:
        print(f"❌ Failed to fetch known game: {result.get('error')}")

def save_sample_data():
    """Save sample data for development reference"""
    print("\n" + "="*60)
    print("SAVING SAMPLE DATA FOR REFERENCE")
    print("="*60)
    
    client = NCAAAPIClient()
    
    # Get one week's data
    week_result = client.get_week_games(2024, 8)
    
    if week_result['success'] and week_result['games']:
        # Save week data
        with open('sample_week_data.json', 'w') as f:
            json.dump(week_result, f, indent=2)
        print(f"✅ Saved week data to sample_week_data.json")
        
        # Get stats for first game
        first_game_id = week_result['games'][0].get('contestId')
        if first_game_id:
            stats_result = client.get_game_stats(first_game_id)
            
            if stats_result['success']:
                with open('sample_game_stats.json', 'w') as f:
                    json.dump(stats_result, f, indent=2)
                print(f"✅ Saved game stats to sample_game_stats.json")

if __name__ == "__main__":
    print("\n" + "="*60)
    print("NCAA API CLIENT TEST SUITE")
    print("="*60)
    
    # Test 1: Get week games
    contest_ids = test_week_fetch()
    
    # Test 2: Get stats for first game found
    if contest_ids:
        test_game_stats(contest_ids[0])
    
    # Test 3: Test with known game
    test_specific_game()
    
    # Optional: Save sample data for reference
    print("\nDo you want to save sample data files for reference? (y/n): ", end="")
    response = input().strip().lower()
    if response == 'y':
        save_sample_data()
    
    print("\n" + "="*60)
    print("ALL TESTS COMPLETE!")
    print("="*60)