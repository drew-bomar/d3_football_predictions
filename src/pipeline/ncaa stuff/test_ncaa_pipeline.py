"""
Test NCAA Complete Pipeline
Tests the NCAA API scraper and game data merger together
"""

import json
import time
from datetime import datetime
from pprint import pprint

from ncaa_api_scraper import NCAAAPIScraper
from ncaa_game_data_merger import NCAAGameDataMerger


def test_single_game():
    """Test processing a single game through the complete pipeline."""
    print("="*80)
    print("TEST 1: Single Game Processing")
    print("="*80)
    
    # Initialize components
    scraper = NCAAAPIScraper()
    merger = NCAAGameDataMerger()
    
    # Get games for week 9
    print("\n1. Fetching games from GraphQL...")
    games = scraper.get_week_games(2024, 9)
    print(f"   Found {len(games)} games")
    
    if not games:
        print("   ERROR: No games found!")
        return
    
    # Take first game
    game = games[0]
    print(f"\n2. Processing first game:")
    print(f"   {game['away_team']} @ {game['home_team']}")
    print(f"   Contest ID: {game['contest_id']}")
    print(f"   Scores: {game['away_score']} - {game['home_score']}")
    
    # Get team stats
    print(f"\n3. Fetching team stats...")
    start_time = time.time()
    team_stats = scraper.get_team_stats(game['contest_id'])
    fetch_time = time.time() - start_time
    print(f"   Fetch time: {fetch_time:.2f} seconds")
    
    if team_stats:
        print(f"   Found {len(team_stats.get('stats', {}))} stat fields")
    else:
        print("   ERROR: No stats found!")
        return
    
    # Merge data
    print(f"\n4. Merging game data...")
    merged = merger.merge_game_data(game, team_stats)
    
    # Display merged result
    print(f"\n5. Merged Game Data:")
    print(f"   Teams: {merged['away_team']} @ {merged['home_team']}")
    print(f"   Score: {merged['away_score']} - {merged['home_score']}")
    print(f"   Margin: {merged['margin']}")
    print(f"   Total Points: {merged['total_points']}")
    
    # Show some key stats
    print(f"\n   Key Statistics:")
    key_stats = [
        ('total_offense', 'Total Offense'),
        ('third_down_pct', '3rd Down %'),
        ('turnovers', 'Turnovers'),
        ('penalties_yards', 'Penalty Yards')
    ]
    
    for stat_key, stat_name in key_stats:
        home_key = f'home_{stat_key}'
        away_key = f'away_{stat_key}'
        home_val = merged['stats'].get(home_key, 'N/A')
        away_val = merged['stats'].get(away_key, 'N/A')
        print(f"   {stat_name:15} - Home: {home_val:>8} | Away: {away_val:>8}")
    
    # Save for inspection
    with open('test_single_game_merged.json', 'w') as f:
        json.dump(merged, f, indent=2)
    print(f"\n   Saved to: test_single_game_merged.json")
    
    return merged


def test_week_processing():
    """Test processing multiple games with record tracking."""
    print("\n\n" + "="*80)
    print("TEST 2: Week Processing with Record Tracking")
    print("="*80)
    
    # Initialize components
    scraper = NCAAAPIScraper()
    merger = NCAAGameDataMerger()
    
    # Process a small subset of games
    print("\n1. Fetching Week 9 games...")
    games = scraper.get_week_games(2024, 9)
    
    # Limit to first 5 games for testing
    test_games = games[:5]
    print(f"   Testing with first {len(test_games)} games")
    
    # Process games
    print("\n2. Processing games...")
    successful = 0
    failed = 0
    total_time = 0
    
    merged_games = []
    
    for i, game in enumerate(test_games):
        print(f"\n   Game {i+1}/{len(test_games)}: {game['away_team']} @ {game['home_team']}")
        
        try:
            # Get records BEFORE the game
            home_record_before = merger._get_team_record(game['home_team_id'])
            away_record_before = merger._get_team_record(game['away_team_id'])
            
            # Fetch stats
            start = time.time()
            team_stats = scraper.get_team_stats(game['contest_id'])
            fetch_time = time.time() - start
            total_time += fetch_time
            
            if team_stats:
                # Merge data
                merged = merger.merge_game_data(game, team_stats)
                merged_games.append(merged)
                
                # Update records
                merger.update_team_records(merged)
                
                # Get records AFTER the game
                home_record_after = merger._get_team_record(game['home_team_id'])
                away_record_after = merger._get_team_record(game['away_team_id'])
                
                print(f"      ✓ Success ({fetch_time:.2f}s)")
                print(f"      Score: {game['away_score']} - {game['home_score']}")
                print(f"      Records before: {game['away_team']} ({away_record_before['record_str']}) @ "
                      f"{game['home_team']} ({home_record_before['record_str']})")
                print(f"      Records after:  {game['away_team']} ({away_record_after['record_str']}) @ "
                      f"{game['home_team']} ({home_record_after['record_str']})")
                
                successful += 1
            else:
                print(f"      ✗ Failed to get stats")
                failed += 1
                
        except Exception as e:
            print(f"      ✗ Error: {e}")
            failed += 1
        
        # Rate limit
        if i < len(test_games) - 1:
            time.sleep(1)
    
    # Summary
    print(f"\n3. Processing Summary:")
    print(f"   Successful: {successful}")
    print(f"   Failed: {failed}")
    print(f"   Average fetch time: {total_time/len(test_games):.2f}s")
    
    # Show team records
    print(f"\n4. Team Records After Processing:")
    teams_shown = 0
    for team_id, record in merger.team_records.items():
        if teams_shown >= 5:  # Show first 5 teams
            break
        team_name = record['opponents'][0]['opponent_name'] if record['opponents'] else team_id
        print(f"   {team_name}: {record['wins']}-{record['losses']}")
        teams_shown += 1
    
    # Save results
    with open('test_week_processing.json', 'w') as f:
        json.dump({
            'games': merged_games,
            'team_records': merger.team_records
        }, f, indent=2)
    print(f"\n   Saved to: test_week_processing.json")
    
    return merged_games, merger


def test_multi_week_records():
    """Test processing multiple weeks to see record tracking."""
    print("\n\n" + "="*80)
    print("TEST 3: Multi-Week Record Tracking")
    print("="*80)
    
    # Initialize components
    scraper = NCAAAPIScraper()
    merger = NCAAGameDataMerger()
    
    # Process weeks 1-3 (just 2 games per week for speed)
    weeks_to_test = [1, 2, 3]
    games_per_week = 2
    
    all_games = []
    
    for week in weeks_to_test:
        print(f"\n\nProcessing Week {week}...")
        
        # Get games
        games = scraper.get_week_games(2024, week)[:games_per_week]
        
        # Process with merger
        week_games = merger.process_week_games(
            games,
            lambda cid: scraper.get_team_stats(cid)
        )
        
        all_games.extend(week_games)
        
        print(f"   Processed {len(week_games)} games")
        
        # Show a game with records
        if week_games:
            game = week_games[0]
            print(f"   Example: {game['away_team']} ({game['away_record_before']['record_str']}) @ "
                  f"{game['home_team']} ({game['home_record_before']['record_str']})")
            print(f"   SOS - Home: {game['home_sos']:.3f}, Away: {game['away_sos']:.3f}")
    
    # Final summary
    print(f"\n\nFinal Summary:")
    print(f"   Total games processed: {len(all_games)}")
    print(f"   Teams tracked: {len(merger.team_records)}")
    
    # Show teams with most games
    sorted_teams = sorted(merger.team_records.items(), 
                         key=lambda x: x[1]['wins'] + x[1]['losses'], 
                         reverse=True)
    
    print(f"\n   Teams with most games:")
    for team_id, record in sorted_teams[:5]:
        total_games = record['wins'] + record['losses']
        if total_games > 0:
            win_pct = record['wins'] / total_games
            sos = merger.calculate_strength_of_schedule(team_id)
            print(f"   {team_id}: {record['wins']}-{record['losses']} "
                  f"({win_pct:.3f}) SOS: {sos:.3f}")


def main():
    """Run all tests."""
    print("NCAA API SCRAPER + MERGER PIPELINE TEST")
    print("="*80)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Test 1: Single game
    single_game = test_single_game()
    
    # Test 2: Week processing
    week_games, merger = test_week_processing()
    
    # Test 3: Multi-week (commented out by default due to time)
    # Uncomment to test multi-week record tracking
    # test_multi_week_records()
    
    print(f"\n\nCompleted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)


if __name__ == "__main__":
    main()