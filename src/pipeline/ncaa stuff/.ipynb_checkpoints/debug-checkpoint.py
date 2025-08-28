"""
Debug script to verify home/away team assignment
"""

from ncaa_api_scraper import NCAAAPIScraper
import json

def debug_game_data():
    """Check the actual home/away assignment in the data."""
    
    scraper = NCAAAPIScraper()
    
    # Get first game
    games = scraper.get_week_games(2024, 9)
    game = games[0]
    
    print("RAW GAME DATA FROM GRAPHQL:")
    print("="*60)
    print(json.dumps(game, indent=2))
    
    print("\n\nPARSED GAME INFO:")
    print("="*60)
    print(f"Contest ID: {game['contest_id']}")
    print(f"Home Team: {game['home_team']} (ID: {game['home_team_id']})")
    print(f"Away Team: {game['away_team']} (ID: {game['away_team_id']})")
    print(f"Home Score: {game['home_score']}")
    print(f"Away Score: {game['away_score']}")
    print(f"Home Winner: {game['home_winner']}")
    print(f"Away Winner: {game['away_winner']}")
    
    # Get team stats
    stats = scraper.get_team_stats(game['contest_id'])
    
    print("\n\nTEAM STATS META INFO:")
    print("="*60)
    print(json.dumps(stats.get('teams_info', {}), indent=2))
    
    # Analyze
    print("\n\nANALYSIS:")
    print("="*60)
    print(f"Game displayed as: {game['away_team']} @ {game['home_team']}")
    print(f"Scores: {game['away_score']} - {game['home_score']}")
    
    if int(game['away_score']) > int(game['home_score']):
        print(f"Winner: {game['away_team']} (away team) with {game['away_score']} points")
    else:
        print(f"Winner: {game['home_team']} (home team) with {game['home_score']} points")
    
    print(f"\nBut GraphQL says:")
    print(f"  home_winner: {game['home_winner']}")
    print(f"  away_winner: {game['away_winner']}")


if __name__ == "__main__":
    debug_game_data()