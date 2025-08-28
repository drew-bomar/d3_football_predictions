"""
NCAA Team Stats Explorer
Explore the structure of the teamStats.json endpoint
"""

import requests
import json
from pprint import pprint

def explore_team_stats(contest_id: str = "6308650"):
    """
    Explore the team stats JSON structure for a specific game.
    """
    url = f"https://data.ncaa.com/casablanca/game/{contest_id}/teamStats.json"
    
    print(f"Fetching team stats from: {url}")
    print("="*80)
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        data = response.json()
        
        # Save raw JSON for inspection
        with open(f'team_stats_{contest_id}.json', 'w') as f:
            json.dump(data, f, indent=2)
        print(f"\nSaved raw JSON to team_stats_{contest_id}.json")
        
        # Explore the structure
        print("\n1. TOP LEVEL KEYS:")
        print("-"*40)
        for key in data.keys():
            print(f"  - {key}: {type(data[key])}")
        
        # Teams info
        if 'teams' in data:
            print("\n2. TEAMS:")
            print("-"*40)
            for i, team in enumerate(data['teams']):
                print(f"  Team {i+1}:")
                for key, value in team.items():
                    print(f"    {key}: {value}")
        
        # Status info
        if 'status' in data:
            print(f"\n3. GAME STATUS: {data['status']}")
        
        # Statistical categories - THIS IS THE MAIN DATA
        if 'statCategories' in data:
            print(f"\n4. STATISTICAL CATEGORIES: {len(data['statCategories'])} categories")
            print("-"*40)
            
            # Show first 10 categories in detail
            for i, category in enumerate(data['statCategories'][:10]):
                print(f"\n  Category {i+1}:")
                for key, value in category.items():
                    print(f"    {key}: {value}")
            
            # List all category names
            print("\n5. ALL STAT CATEGORIES:")
            print("-"*40)
            for cat in data['statCategories']:
                name = cat.get('name', 'Unknown')
                home_val = cat.get('homeValue', 'N/A')
                away_val = cat.get('awayValue', 'N/A')
                print(f"  {name:30} | Home: {home_val:>10} | Away: {away_val:>10}")
        
        return data
        
    except Exception as e:
        print(f"Error: {e}")
        return None


def test_multiple_games():
    """
    Test multiple games to see consistency in data structure.
    """
    # Get some game IDs from week 9
    from ncaa_api_scraper import NCAAAPIScraper
    
    scraper = NCAAAPIScraper()
    games = scraper.get_week_games(2024, 9)
    
    print("\nTesting multiple games for consistency...")
    print("="*80)
    
    # Test first 3 games
    for i, game in enumerate(games[:3]):
        if game.get('contest_id'):
            print(f"\nGame {i+1}: {game['away_team']} @ {game['home_team']}")
            print(f"Contest ID: {game['contest_id']}")
            
            stats = scraper.get_team_stats(game['contest_id'])
            if stats and 'stats' in stats:
                print(f"  Found {len(stats['stats'])} statistical fields")
                # Show a few key stats
                for key in ['home_total_offense', 'away_total_offense', 
                           'home_total_plays', 'away_total_plays']:
                    if key in stats['stats']:
                        print(f"  {key}: {stats['stats'][key]}")


def create_stats_mapping():
    """
    Create a mapping of all possible stat categories for reference.
    """
    # This will help us understand what stats are available
    contest_id = "6308650"
    data = explore_team_stats(contest_id)
    
    if data and 'statCategories' in data:
        print("\n\nSTAT CATEGORY MAPPING FOR DATA PROCESSING:")
        print("="*80)
        print("# Use these field names in your data processor:\n")
        
        for cat in data['statCategories']:
            name = cat.get('name', '')
            clean_name = name.lower().replace(' ', '_').replace('-', '_').replace('/', '_')
            print(f"'{clean_name}',  # {name}")


if __name__ == "__main__":
    # First explore a single game in detail
    explore_team_stats("6308650")
    
    # Then test multiple games
    print("\n\n")
    test_multiple_games()
    
    # Create mapping for data processor
    print("\n\n")
    create_stats_mapping()