"""
Integrated test showing the complete NCAA data pipeline
"""

from ncaa_api_scraper import NCAAAPIScraper
from ncaa_game_data_merger import NCAAGameDataMerger
import json
import logging
import time

logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def test_integrated_pipeline():
    """
    Test the complete pipeline from NCAA API to data_processor format.
    """
    print("=" * 80)
    print("INTEGRATED NCAA PIPELINE TEST")
    print("=" * 80)
    
    # Initialize components
    scraper = NCAAAPIScraper()
    merger = NCAAGameDataMerger()
    
    # Step 1: Get games for a week
    year, week = 2024, 9
    print(f"\nStep 1: Fetching games for {year} Week {week}...")
    games = scraper.get_week_games(year, week)
    
    if not games:
        print("ERROR: No games found!")
        return
    
    print(f"Found {len(games)} games")
    
    # Find a game with complete data
    test_game = None
    for game in games:
        if (game.get('contest_id') and 
            game.get('home_score') is not None and 
            game.get('away_score') is not None):
            test_game = game
            break
    
    if not test_game:
        print("ERROR: No complete games found!")
        return
    
    print(f"\nSelected game: {test_game['away_team']} @ {test_game['home_team']}")
    print(f"Score: {test_game['away_score']} - {test_game['home_score']}")
    
    # Step 2: Fetch team stats
    print(f"\nStep 2: Fetching team stats for contest {test_game['contest_id']}...")
    team_stats = scraper.get_team_stats(test_game['contest_id'])
    
    if not team_stats:
        print("ERROR: Could not fetch team stats!")
        return
    
    # Step 3: Show parsed results
    print("\nStep 3: Parsed team stats results...")
    
    print(f"Teams found:")
    for team_type, info in team_stats['teams_info'].items():
        print(f"  {team_type}: {info['name']} (ID: {info['id']})")
    
    print(f"\nTotal stats parsed: {len(team_stats['stats'])}")
    
    # Step 4: Create merged game data
    print("\nStep 4: Creating merged game data...")
    
    # Merge with game info
    merged_game = merger.merge_game_data(test_game, team_stats)
    
    # Add year and week
    merged_game['year'] = year
    merged_game['week'] = week
    
    # Show records before game (should be 0-0 for first game)
    print(f"\nRecords BEFORE game:")
    print(f"  {merged_game['home_team']}: {merged_game['home_record_before']['record_str']}")
    print(f"  {merged_game['away_team']}: {merged_game['away_record_before']['record_str']}")
    
    # Update team records based on this game
    merger.update_team_records(merged_game)
    
    # Get updated records
    home_record_after = merger._get_team_record(merged_game['home_team_id'])
    away_record_after = merger._get_team_record(merged_game['away_team_id'])
    
    print(f"\nRecords AFTER game:")
    print(f"  {merged_game['home_team']}: {home_record_after['record_str']}")
    print(f"  {merged_game['away_team']}: {away_record_after['record_str']}")
    
    # Test multiple games to see record progression
    print("\n" + "=" * 50)
    print("Testing record tracking with multiple games...")
    
    # Get a few more games and process them
    games_to_process = games[:5]  # Process first 5 games
    print(f"\nProcessing {len(games_to_process)} games to track records...")
    
    for i, game in enumerate(games_to_process):
        if game.get('contest_id') and game.get('home_score') is not None:
            print(f"\nGame {i+1}: {game['away_team']} @ {game['home_team']} "
                  f"({game['away_score']}-{game['home_score']})")
            
            # Get stats if we don't already have them
            if game == test_game:
                # Already have stats for test game
                game_stats = team_stats
            else:
                time.sleep(1)  # Rate limit
                game_stats = scraper.get_team_stats(game['contest_id'])
                
            if game_stats:
                merged = merger.merge_game_data(game, game_stats)
                
                # Show records before
                print(f"  Before: {game['home_team']} {merged['home_record_before']['record_str']}, "
                      f"{game['away_team']} {merged['away_record_before']['record_str']}")
                
                # Update records
                merger.update_team_records(merged)
                
                # Show records after
                home_after = merger._get_team_record(game['home_team_id'])
                away_after = merger._get_team_record(game['away_team_id'])
                print(f"  After:  {game['home_team']} {home_after['record_str']}, "
                      f"{game['away_team']} {away_after['record_str']}")
    
    # Show final records for all teams we've seen
    print("\n" + "=" * 50)
    print("Final team records after processing games:")
    teams_by_record = []
    for team_id, record in merger.team_records.items():
        # Find team name from one of the games
        team_name = team_id
        for game in games_to_process:
            if game.get('home_team_id') == team_id:
                team_name = game['home_team']
                break
            elif game.get('away_team_id') == team_id:
                team_name = game['away_team']
                break
        
        teams_by_record.append((team_name, record['wins'], record['losses']))
    
    # Sort by wins descending
    teams_by_record.sort(key=lambda x: x[1], reverse=True)
    
    for team_name, wins, losses in teams_by_record[:10]:  # Show first 10
        print(f"  {team_name}: {wins}-{losses}")
    
    # Step 5: Verify required stats
    print("\nStep 5: Verifying required stats for data_processor...")
    
    required_stats = [
        'final_score', 'first_downs', 'total_offense', 'net_yards_passing',
        'net_yards_rushing', 'third_down_pct', 'third_down_conversions',
        'third_down_att', 'fumbles', 'fumbles_lost', 'interceptions',
        'interception_return_yards', 'penalties_number', 'punts_number',
        'sacks_number', 'total_return_yards'
    ]
    
    stats = merged_game.get('stats', {})
    
    print("\nStat availability check:")
    missing_stats = []
    available_stats = []
    
    for stat in required_stats:
        home_key = f'home_{stat}'
        away_key = f'away_{stat}'
        
        # Special handling for final_score
        if stat == 'final_score':
            if 'home_score' in merged_game and 'away_score' in merged_game:
                print(f"  ✓ {stat}: Available (from game info)")
                available_stats.append(stat)
                # Add to stats dict for consistency
                stats['home_final_score'] = merged_game['home_score']
                stats['away_final_score'] = merged_game['away_score']
            else:
                print(f"  ✗ {stat}: MISSING")
                missing_stats.append(stat)
        else:
            if home_key in stats and away_key in stats:
                print(f"  ✓ {stat}: {stats[away_key]} - {stats[home_key]} (away - home)")
                available_stats.append(stat)
            else:
                print(f"  ✗ {stat}: MISSING")
                missing_stats.append(stat)
                
                # Look for similar keys
                similar = [k for k in stats.keys() if stat.replace('_', '') in k.replace('_', '')]
                if similar:
                    print(f"     Similar found: {similar[:3]}")
    
    # Summary
    print(f"\n" + "=" * 50)
    print(f"SUMMARY:")
    print(f"  Available stats: {len(available_stats)}/{len(required_stats)}")
    print(f"  Missing stats: {len(missing_stats)}")
    
    if missing_stats:
        print(f"\nMissing stats that may need alternatives:")
        for stat in missing_stats:
            print(f"  - {stat}")
    
    # Save complete output
    output = {
        'game_info': test_game,
        'team_stats': team_stats,
        'merged_game': merged_game,
        'verification': {
            'required_stats': required_stats,
            'available_stats': available_stats,
            'missing_stats': missing_stats
        }
    }
    
    with open('integrated_test_output.json', 'w') as f:
        json.dump(output, f, indent=2)
    
    print(f"\nComplete output saved to: integrated_test_output.json")
    
    # Step 6: Show how to convert to team-game format
    print("\n" + "=" * 50)
    print("Step 6: Converting to team-game format for data_processor...")
    
    # Create two records (one per team)
    home_game = create_team_game_record(merged_game, 'home', 'away')
    away_game = create_team_game_record(merged_game, 'away', 'home')
    
    print(f"\nHome team game record sample:")
    print(f"  team: {home_game['team']}")
    print(f"  opponent: {home_game['opponent']}")
    print(f"  final_score: {home_game.get('final_score', 'N/A')}")
    print(f"  opp_final_score: {home_game.get('opp_final_score', 'N/A')}")
    print(f"  first_downs: {home_game.get('first_downs', 'N/A')}")
    
    return merged_game


def create_team_game_record(merged_game: dict, team_perspective: str, opp_perspective: str) -> dict:
    """
    Create a team-game record from the merged game data.
    
    Args:
        merged_game: The merged game data
        team_perspective: 'home' or 'away' - the team this record is for
        opp_perspective: 'away' or 'home' - the opponent
    
    Returns:
        Team-game record in data_processor format
    """
    stats = merged_game.get('stats', {})
    
    # Get team names
    team_name = merged_game.get(f'{team_perspective}_team')
    opp_name = merged_game.get(f'{opp_perspective}_team')
    
    # Create base record
    team_game = {
        'year': merged_game.get('year'),
        'week': merged_game.get('week'),
        'game_id': merged_game.get('contest_id'),
        'team': team_name,
        'opponent': opp_name,
        'scraped_at': merged_game.get('date')
    }
    
    # Add final scores
    if f'{team_perspective}_score' in merged_game:
        team_game['final_score'] = merged_game[f'{team_perspective}_score']
    if f'{opp_perspective}_score' in merged_game:
        team_game['opp_final_score'] = merged_game[f'{opp_perspective}_score']
    
    # Map all stats
    for key, value in stats.items():
        if key.startswith(f'{team_perspective}_'):
            # This team's stat
            stat_name = key.replace(f'{team_perspective}_', '')
            team_game[stat_name] = value
        elif key.startswith(f'{opp_perspective}_'):
            # Opponent's stat
            stat_name = key.replace(f'{opp_perspective}_', '')
            team_game[f'opp_{stat_name}'] = value
    
    # Calculate derived fields
    if 'final_score' in team_game and 'opp_final_score' in team_game:
        try:
            score = int(team_game['final_score'])
            opp_score = int(team_game['opp_final_score'])
            team_game['points_scored'] = score
            team_game['points_allowed'] = opp_score
            team_game['margin'] = score - opp_score
            team_game['win'] = 1 if score > opp_score else 0
        except (ValueError, TypeError):
            pass
    
    return team_game


if __name__ == "__main__":
    test_integrated_pipeline()