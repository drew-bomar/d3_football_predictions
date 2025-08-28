"""
Test processing a single game through the NCAA pipeline
Shows all extracted data: scores, records, and statistics
"""

import json
from ncaa_api_scraper import NCAAAPIScraper
from ncaa_game_data_merger import NCAAGameDataMerger
import logging

# Set up detailed logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

def test_single_game_complete():
    """
    Test processing one game completely through the pipeline.
    Shows all data we can extract.
    """
    scraper = NCAAAPIScraper()
    merger = NCAAGameDataMerger()
    
    print("=" * 80)
    print("NCAA D3 FOOTBALL - SINGLE GAME PROCESSING TEST")
    print("=" * 80)
    
    # Get games from a recent week
    year = 2024
    week = 9
    print(f"\nFetching games for {year} Week {week}...")
    
    games = scraper.get_week_games(year, week)
    
    if not games:
        print("ERROR: No games found!")
        return
    
    # First, let's see what statuses we have
    print(f"\nGame statuses found:")
    statuses = {}
    for game in games:
        status = game.get('status', 'unknown')
        statuses[status] = statuses.get(status, 0) + 1
    
    for status, count in statuses.items():
        print(f"  {status}: {count} games")
    
    # Pick the first game with a valid contest_id
    # Try different status values that might indicate completed games
    test_game = None
    for game in games:
        if game.get('contest_id'):
            # Accept any game that has scores
            if game.get('home_score') is not None and game.get('away_score') is not None:
                test_game = game
                break
    
    if not test_game:
        # Just take the first game with a contest_id
        for game in games:
            if game.get('contest_id'):
                test_game = game
                print(f"\nWARNING: Using game with status '{game.get('status')}' - may not have final stats")
                break
    
    if not test_game:
        print("ERROR: No games with contest_id found!")
        return
    
    print(f"\nSelected Game: {test_game['away_team']} @ {test_game['home_team']}")
    print(f"Contest ID: {test_game['contest_id']}")
    print(f"Date: {test_game['start_date']} at {test_game['start_time']}")
    
    # Display basic game info
    print("\n" + "-" * 40)
    print("BASIC GAME INFO (from GraphQL):")
    print("-" * 40)
    print(f"Status: {test_game['status']}")
    print(f"Home Team: {test_game['home_team']} (ID: {test_game['home_team_id']})")
    print(f"Away Team: {test_game['away_team']} (ID: {test_game['away_team_id']})")
    print(f"Home Score: {test_game['home_score']}")
    print(f"Away Score: {test_game['away_score']}")
    print(f"Home Winner: {test_game['home_winner']}")
    print(f"Away Winner: {test_game['away_winner']}")
    
    # Get detailed team statistics
    print(f"\nFetching detailed statistics...")
    team_stats = scraper.get_team_stats(test_game['contest_id'])
    
    if not team_stats:
        print("ERROR: Could not fetch team statistics!")
        return
    
    # Show teams info from stats endpoint
    print("\n" + "-" * 40)
    print("TEAM INFO (from teamStats endpoint):")
    print("-" * 40)
    for team_type, info in team_stats.get('teams_info', {}).items():
        print(f"\n{team_type.upper()} Team:")
        for key, value in info.items():
            print(f"  {key}: {value}")
    
    # Show a sample of statistics
    print("\n" + "-" * 40)
    print("GAME STATISTICS (sample):")
    print("-" * 40)
    
    # Organize stats by category for better display
    stats = team_stats.get('stats', {})
    
    # Key statistics to display
    key_stats = [
        ('first_downs', 'First Downs'),
        ('total_offense', 'Total Offense'),
        ('net_yards_rushing', 'Rushing Yards'),
        ('net_yards_passing', 'Passing Yards'),
        ('third_down_conversions', 'Third Down Conv'),
        ('third_down_pct', 'Third Down %'),
        ('turnovers', 'Turnovers'),
        ('fumbles', 'Fumbles'),
        ('fumbles_lost', 'Fumbles Lost'),
        ('interceptions', 'Interceptions'),
        ('penalties_number', 'Penalties'),
        ('penalties_yards', 'Penalty Yards'),
        ('time_of_possession', 'Time of Poss')
    ]
    
    print(f"\n{'Statistic':<20} {'Home':<15} {'Away':<15}")
    print("-" * 50)
    
    for stat_key, stat_name in key_stats:
        home_val = stats.get(f'home_{stat_key}', 'N/A')
        away_val = stats.get(f'away_{stat_key}', 'N/A')
        print(f"{stat_name:<20} {str(home_val):<15} {str(away_val):<15}")
    
    # Now merge the data
    print("\n" + "-" * 40)
    print("MERGED GAME DATA:")
    print("-" * 40)
    
    merged = merger.merge_game_data(test_game, team_stats)
    
    # Display merged results
    print(f"\nGame Summary:")
    print(f"  Final Score: {merged['away_team']} {merged['away_score']} - "
          f"{merged['home_score']} {merged['home_team']}")
    print(f"  Margin: {merged['margin']} (positive = home team won)")
    print(f"  Total Points: {merged['total_points']}")
    
    # Show records (will be 0-0 since this is first game processed)
    print(f"\nRecords Before Game:")
    print(f"  {merged['home_team']}: {merged['home_record_before']['record_str']}")
    print(f"  {merged['away_team']}: {merged['away_record_before']['record_str']}")
    
    # Update records after this game
    merger.update_team_records(merged)
    
    # Get updated records
    home_record_after = merger._get_team_record(merged['home_team_id'])
    away_record_after = merger._get_team_record(merged['away_team_id'])
    
    print(f"\nRecords After Game:")
    print(f"  {merged['home_team']}: {home_record_after['record_str']}")
    print(f"  {merged['away_team']}: {away_record_after['record_str']}")
    
    # Display all available stats (for verification)
    print("\n" + "-" * 40)
    print("ALL AVAILABLE STATISTICS:")
    print("-" * 40)
    
    all_stats = list(stats.keys())
    all_stats.sort()
    
    print(f"\nTotal statistics available: {len(all_stats)}")
    print("\nComplete list:")
    for i in range(0, len(all_stats), 3):
        row_stats = all_stats[i:i+3]
        print("  " + " | ".join(f"{s:<25}" for s in row_stats))
    
    # Save to file for inspection
    output_file = "test_game_output.json"
    with open(output_file, 'w') as f:
        json.dump({
            'basic_game_info': test_game,
            'team_stats': team_stats,
            'merged_data': merged,
            'all_stat_keys': all_stats
        }, f, indent=2)
    
    print(f"\n\nComplete data saved to: {output_file}")
    
    return merged


def verify_stat_mapping():
    """
    Verify that NCAA stats map correctly to data_processor expectations.
    """
    print("\n" + "=" * 80)
    print("STAT MAPPING VERIFICATION")
    print("=" * 80)
    
    # Expected stats from data_processor.py
    expected_stats = [
        'final_score', 'first_downs', 'total_offense', 'net_yards_passing', 
        'net_yards_rushing', 'third_down_pct', 'third_down_conversions', 
        'third_down_att', 'fumbles', 'fumbles_lost', 'interceptions',
        'interception_return_yards', 'penalties_number', 'punts_number',
        'sacks_number', 'total_return_yards'
    ]
    
    print("\nStats expected by data_processor.py:")
    for stat in expected_stats:
        print(f"  - {stat}")
    
    # Run single game test to get actual stats
    merged = test_single_game_complete()
    
    if merged:
        print("\n\nChecking stat availability:")
        stats = merged.get('stats', {})
        
        for expected in expected_stats:
            # Check both home and away versions
            home_key = f'home_{expected}'
            away_key = f'away_{expected}'
            
            home_found = home_key in stats
            away_found = away_key in stats
            
            status = "✓" if (home_found and away_found) else "✗"
            print(f"  {status} {expected}: ", end="")
            
            if home_found and away_found:
                print(f"Found (home={stats[home_key]}, away={stats[away_key]})")
            else:
                # Look for similar keys
                similar = [k for k in stats.keys() if expected in k]
                if similar:
                    print(f"Similar keys found: {similar}")
                else:
                    print("NOT FOUND")


if __name__ == "__main__":
    # Test single game processing
    test_single_game_complete()
    
    # Verify stat mapping
    print("\n\n")
    verify_stat_mapping()