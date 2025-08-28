"""
Test the updated NCAA stats parser
"""

from ncaa_api_scraper import NCAAAPIScraper
import json
from pprint import pprint

def test_stats_parser():
    """Test parsing team stats with the actual JSON structure."""
    
    scraper = NCAAAPIScraper()
    
    # Test with the game you provided
    contest_id = "6308650"
    print(f"Testing stats parser for game: {contest_id}")
    print("="*60)
    
    # Get the stats
    stats = scraper.get_team_stats(contest_id)
    
    if stats:
        print("\n1. TEAMS INFO:")
        print("-"*40)
        pprint(stats.get('teams_info', {}))
        
        print("\n2. PARSED STATISTICS:")
        print("-"*40)
        
        # Group stats by team
        home_stats = {}
        away_stats = {}
        
        for key, value in stats.get('stats', {}).items():
            if key.startswith('home_'):
                home_stats[key.replace('home_', '')] = value
            elif key.startswith('away_'):
                away_stats[key.replace('away_', '')] = value
        
        print("\nHOME TEAM STATS:")
        for key, value in sorted(home_stats.items()):
            print(f"  {key:30} = {value}")
        
        print("\nAWAY TEAM STATS:")
        for key, value in sorted(away_stats.items()):
            print(f"  {key:30} = {value}")
        
        # Check for key stats that data_processor.py expects
        print("\n3. KEY STATS CHECK:")
        print("-"*40)
        
        expected_stats = [
            'total_offense', 'rushing', 'passing', 
            'third_down_conversions', 'penalties_yards',
            'fumbles', 'fumbles_lost'
        ]
        
        for stat in expected_stats:
            home_key = f'home_{stat}'
            away_key = f'away_{stat}'
            home_val = stats.get('stats', {}).get(home_key, 'NOT FOUND')
            away_val = stats.get('stats', {}).get(away_key, 'NOT FOUND')
            print(f"{stat:25} -> Home: {home_val:10} | Away: {away_val:10}")
        
        # Save parsed stats for reference
        with open('parsed_team_stats.json', 'w') as f:
            json.dump(stats, f, indent=2)
        print("\nSaved parsed stats to 'parsed_team_stats.json'")
        
    else:
        print("Failed to get stats")


def test_full_game_data():
    """Test getting complete game data with scores and stats."""
    
    scraper = NCAAAPIScraper()
    
    print("\n\nTesting full game data retrieval...")
    print("="*60)
    
    # Get games for a week
    games = scraper.get_week_games(2024, 9)
    
    if games:
        # Test with first game
        game = games[0]
        print(f"\nGame: {game['away_team']} @ {game['home_team']}")
        print(f"Score: {game['away_score']} - {game['home_score']}")
        print(f"Contest ID: {game['contest_id']}")
        
        # Get detailed stats
        if game['contest_id']:
            stats = scraper.get_team_stats(game['contest_id'])
            if stats:
                print("\nStats retrieved successfully!")
                print(f"Total stats fields: {len(stats.get('stats', {}))}")
            else:
                print("\nFailed to get stats")


if __name__ == "__main__":
    # Test the parser
    test_stats_parser()
    
    # Test full workflow
    test_full_game_data()