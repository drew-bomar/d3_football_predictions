"""
Test NCAA pipeline with weeks 6-9 to verify rolling stats and calculated fields
"""

import logging
import pandas as pd
import json
from pathlib import Path
import shutil
from datetime import datetime
import pickle

from pipeline_manager import NCAASeasonPipeline

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_four_weeks_pipeline():
    """Test weeks 6-9 with full verification of calculated fields."""
    
    print("\n" + "=" * 80)
    print("TESTING 4-WEEK PIPELINE (Weeks 6-9)")
    print("=" * 80)
    
    test_dir = "test_weeks_6_9"
    
    # Clean existing directory
    if Path(test_dir).exists():
        print(f"Cleaning existing directory: {test_dir}")
        shutil.rmtree(test_dir)
    
    # Create pipeline
    pipeline = NCAASeasonPipeline(data_dir=test_dir, delay=0.5)
    
    print("\nStarting collection of weeks 6-9...")
    print("This will take approximately 10-15 minutes\n")
    
    start_time = datetime.now()
    
    # Collect weeks 6-9
    result = pipeline.collect_season(2024, start_week=6, end_week=9)
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds() / 60
    
    print(f"\n{'=' * 60}")
    print("COLLECTION COMPLETE")
    print(f"{'=' * 60}")
    
    print(f"\nSummary:")
    print(f"  Weeks processed: {result.get('weeks_processed', 0)}")
    print(f"  Total games: {result.get('successful_games', 0)}")
    print(f"  Failed games: {result.get('failed_games', 0)}")
    print(f"  Success rate: {result.get('successful_games', 0) / result.get('total_games', 1) * 100:.1f}%")
    print(f"  Time taken: {duration:.1f} minutes")
    print(f"  Teams tracked: {result.get('teams_seen', 0)}")
    
    # Test opponent tracking and SOS
    test_opponent_tracking(test_dir)
    
    # Test rolling stats progression
    test_rolling_stats_progression(test_dir)
    
    # Test calculated fields
    test_calculated_fields_in_data(test_dir)
    
    # Final summary
    generate_test_summary(test_dir)
    
    return result


def test_opponent_tracking(test_dir):
    """Test opponent tracking and SOS calculations."""
    
    print(f"\n{'=' * 80}")
    print("TESTING OPPONENT TRACKING & STRENGTH OF SCHEDULE")
    print(f"{'=' * 80}")
    
    # Load the final checkpoint to examine team records
    checkpoint_file = Path(test_dir) / "checkpoints" / "2024_week_09_checkpoint.pkl"
    
    if not checkpoint_file.exists():
        print("ERROR: Checkpoint file not found!")
        return
    
    with open(checkpoint_file, 'rb') as f:
        checkpoint = pickle.load(f)
    
    team_records = checkpoint['merger_state']['team_records']
    
    # Pick a few teams to examine in detail
    teams_to_examine = []
    for team_id, record in team_records.items():
        if len(record['opponents']) >= 3:  # Teams that played at least 3 games
            teams_to_examine.append(team_id)
            if len(teams_to_examine) >= 3:
                break
    
    print(f"\nExamining {len(teams_to_examine)} teams with 3+ games:")
    
    for team_id in teams_to_examine:
        record = team_records[team_id]
        print(f"\n{'=' * 60}")
        print(f"TEAM: {team_id}")
        print(f"Record: {record['wins']}-{record['losses']}")
        print(f"{'=' * 60}")
        
        print(f"\nOpponent History:")
        print(f"{'Date':<12} {'Opponent':<25} {'Location':<8} {'Result':<6} {'Score':<10} {'Opp Record':<10}")
        print("-" * 80)
        
        for i, opp in enumerate(record['opponents']):
            date = opp.get('date', 'N/A')[:10]  # Just date part
            opp_name = opp.get('opponent_name', 'Unknown')[:24]  # Truncate long names
            location = opp.get('location', 'N/A')
            result = opp.get('result', 'N/A')
            score = f"{opp.get('score', 0)}-{opp.get('opponent_score', 0)}"
            opp_record = opp.get('opponent_record_at_time', 'N/A')
            
            print(f"{date:<12} {opp_name:<25} {location:<8} {result:<6} {score:<10} {opp_record:<10}")
        
        # Calculate and show SOS
        print(f"\nStrength of Schedule Calculation:")
        
        total_opp_wins = 0
        total_opp_games = 0
        opp_records_current = []
        
        for opp in record['opponents']:
            opp_id = opp['opponent_id']
            if opp_id in team_records:
                opp_record = team_records[opp_id]
                opp_wins = opp_record['wins']
                opp_losses = opp_record['losses']
                opp_total = opp_wins + opp_losses
                
                total_opp_wins += opp_wins
                total_opp_games += opp_total
                
                opp_records_current.append(f"{opp['opponent_name']}: {opp_wins}-{opp_losses}")
        
        print(f"  Current opponent records:")
        for opp_rec in opp_records_current:
            print(f"    {opp_rec}")
        
        if total_opp_games > 0:
            sos = total_opp_wins / total_opp_games
            print(f"\n  SOS = Total Opponent Wins / Total Opponent Games")
            print(f"  SOS = {total_opp_wins} / {total_opp_games} = {sos:.3f}")
        else:
            print(f"\n  SOS = 0 (no opponent games)")
    
    # Show overall SOS rankings
    print(f"\n{'=' * 60}")
    print("TOP 10 STRENGTH OF SCHEDULE RANKINGS")
    print(f"{'=' * 60}")
    
    sos_rankings = []
    for team_id, record in team_records.items():
        if record['wins'] + record['losses'] >= 2:  # At least 2 games
            # Calculate SOS
            total_opp_wins = 0
            total_opp_games = 0
            
            for opp in record['opponents']:
                opp_id = opp['opponent_id']
                if opp_id in team_records:
                    opp_record = team_records[opp_id]
                    total_opp_wins += opp_record['wins']
                    total_opp_games += opp_record['wins'] + opp_record['losses']
            
            if total_opp_games > 0:
                sos = total_opp_wins / total_opp_games
                sos_rankings.append({
                    'team': team_id,
                    'record': f"{record['wins']}-{record['losses']}",
                    'sos': sos,
                    'games': len(record['opponents'])
                })
    
    sos_rankings.sort(key=lambda x: x['sos'], reverse=True)
    
    print(f"\n{'Rank':<6} {'Team':<30} {'Record':<10} {'Games':<8} {'SOS':<8}")
    print("-" * 62)
    
    for i, team in enumerate(sos_rankings[:10], 1):
        print(f"{i:<6} {team['team']:<30} {team['record']:<10} {team['games']:<8} {team['sos']:<8.3f}")


def test_rolling_stats_progression(test_dir):
    """Show how rolling stats develop week by week for example teams."""
    
    print(f"\n{'=' * 80}")
    print("ROLLING STATS PROGRESSION - WEEK BY WEEK")
    print(f"{'=' * 80}")
    
    # We'll track a couple of teams through all 4 weeks
    teams_to_track = ['Mount Union', 'North Central (IL)']  # High-scoring teams likely to play all weeks
    
    # Load rolling stats for each week
    weekly_data = {}
    for week in range(6, 10):
        rolling_file = Path(test_dir) / "processed" / f"2024_week_{week:02d}_current_rolling.csv"
        if rolling_file.exists():
            df = pd.read_csv(rolling_file)
            weekly_data[week] = df
    
    # For each tracked team, show progression
    for team_name in teams_to_track:
        print(f"\n{'=' * 60}")
        print(f"TEAM: {team_name}")
        print(f"{'=' * 60}")
        
        found_team = False
        
        for week in range(6, 10):
            if week not in weekly_data:
                continue
            
            df = weekly_data[week]
            team_data = df[df['team'] == team_name]
            
            if not team_data.empty:
                found_team = True
                row = team_data.iloc[0]
                
                print(f"\nWeek {week} vs {row['opponent']}:")
                
                # Get the actual game score from team_games file
                team_games_file = Path(test_dir) / "processed" / f"2024_week_{week:02d}_team_games.json"
                if team_games_file.exists():
                    with open(team_games_file, 'r') as f:
                        games = json.load(f)
                    
                    # Find this team's game
                    for game in games:
                        if game['team'] == team_name and game['opponent'] == row['opponent']:
                            print(f"  Game Result: {game['final_score']}-{game['opp_final_score']} ", end="")
                            if game.get('win', 0) == 1:
                                print("(W)")
                            else:
                                print("(L)")
                            print(f"  Game Stats: {game['total_offense']} yards, "
                                  f"{game['third_down_conversions']}/{game['third_down_att']} 3rd downs")
                            break
                
                # Show rolling averages
                print(f"\n  Rolling Averages:")
                
                # 3-week averages
                if 'final_score_3wk' in row:
                    games_3wk = int(row.get('games_in_window_3wk', 0))
                    print(f"    3-week: {row['final_score_3wk']:.1f} pts/game, "
                          f"{row['total_offense_3wk']:.1f} yds/game "
                          f"({games_3wk} games)")
                    
                    if 'turnover_diff_3wk' in row:
                        print(f"            TO Diff: {row['turnover_diff_3wk']:.1f}, "
                              f"YPP: {row.get('yards_per_play_3wk', 0):.1f}")
                
                # 5-week averages (should start appearing in week 6 if they played in weeks 2-5)
                if 'final_score_5wk' in row and week >= 7:
                    games_5wk = int(row.get('games_in_window_5wk', 0))
                    print(f"    5-week: {row['final_score_5wk']:.1f} pts/game, "
                          f"{row['total_offense_5wk']:.1f} yds/game "
                          f"({games_5wk} games)")
                
                # Season averages
                if 'final_score_season' in row:
                    games_season = int(row.get('games_played_season', 0))
                    print(f"    Season: {row['final_score_season']:.1f} pts/game, "
                          f"{row['total_offense_season']:.1f} yds/game "
                          f"({games_season} games)")
        
        if not found_team:
            print(f"  Team not found in data - may not have played in these weeks")


def test_calculated_fields_in_data(test_dir):
    """Verify calculated fields are present and populated."""
    
    print(f"\n{'=' * 80}")
    print("CALCULATED FIELDS VERIFICATION")
    print(f"{'=' * 80}")
    
    # Check a week 9 team game file
    team_games_file = Path(test_dir) / "processed" / "2024_week_09_team_games.json"
    
    if not team_games_file.exists():
        print("ERROR: Week 9 team games file not found!")
        return
    
    with open(team_games_file, 'r') as f:
        games = json.load(f)
    
    # Check several games
    print(f"\nChecking calculated fields in {len(games)} team-game records...")
    
    fields_to_check = ['turnover_diff', 'yards_per_play', 'third_down_rate', 'pass_rush_ratio']
    field_counts = {field: 0 for field in fields_to_check}
    field_examples = {field: [] for field in fields_to_check}
    
    for game in games:
        for field in fields_to_check:
            if field in game and game[field] != 0:
                field_counts[field] += 1
                if len(field_examples[field]) < 3:  # Keep first 3 examples
                    field_examples[field].append({
                        'team': game['team'],
                        'value': game[field]
                    })
    
    print(f"\nField population summary:")
    for field in fields_to_check:
        pct = field_counts[field] / len(games) * 100 if games else 0
        print(f"  {field}: {field_counts[field]}/{len(games)} ({pct:.1f}%) non-zero values")
        
        if field_examples[field]:
            print(f"    Examples:")
            for ex in field_examples[field]:
                print(f"      {ex['team']}: {ex['value']:.3f}")
    
    # Check rolling stats include calculated fields
    rolling_file = Path(test_dir) / "2024_season_rolling_stats.csv"
    
    if rolling_file.exists():
        df = pd.read_csv(rolling_file)
        
        print(f"\n\nRolling stats calculated fields:")
        calc_rolling_fields = [
            'turnover_diff_3wk', 'yards_per_play_3wk', 'third_down_rate_3wk',
            'turnover_diff_5wk', 'yards_per_play_5wk', 'third_down_rate_5wk'
        ]
        
        for field in calc_rolling_fields:
            if field in df.columns:
                non_zero = df[df[field] != 0]
                print(f"  {field}: {len(non_zero)}/{len(df)} ({len(non_zero)/len(df)*100:.1f}%) non-zero")


def generate_test_summary(test_dir):
    """Generate final test summary."""
    
    print(f"\n{'=' * 80}")
    print("FINAL TEST SUMMARY")
    print(f"{'=' * 80}")
    
    # Count files created
    all_files = list(Path(test_dir).rglob("*"))
    file_types = {
        'json': len([f for f in all_files if f.suffix == '.json']),
        'csv': len([f for f in all_files if f.suffix == '.csv']),
        'pkl': len([f for f in all_files if f.suffix == '.pkl']),
        'txt': len([f for f in all_files if f.suffix == '.txt'])
    }
    
    print(f"\nFiles created:")
    for ftype, count in file_types.items():
        print(f"  {ftype}: {count} files")
    
    # Check key outputs
    key_files = {
        "Season rolling stats": "2024_season_rolling_stats.csv",
        "Rolling stats summary": "2024_rolling_stats_summary.txt",
        "Season report": "2024_season_report.txt"
    }
    
    print(f"\nKey outputs:")
    for desc, filename in key_files.items():
        file_path = Path(test_dir) / filename
        if file_path.exists():
            size_mb = file_path.stat().st_size / 1024 / 1024
            print(f"  ✓ {desc}: {size_mb:.2f} MB")
        else:
            print(f"  ✗ {desc}: MISSING")
    
    # Load and show final rolling stats summary
    summary_file = Path(test_dir) / "2024_rolling_stats_summary.txt"
    if summary_file.exists():
        print(f"\n\nTop teams from rolling stats summary:")
        with open(summary_file, 'r') as f:
            lines = f.readlines()
            
        # Find and print top 5 scoring teams
        in_scoring = False
        count = 0
        for line in lines:
            if "Top 10 Teams - Scoring" in line:
                in_scoring = True
                continue
            if in_scoring and line.strip() and not line.startswith("-"):
                print(f"  {line.strip()}")
                count += 1
                if count >= 5:
                    break


if __name__ == "__main__":
    # Run the comprehensive test
    result = test_four_weeks_pipeline()
    
    print("\n\nTest complete!")
    print("Check the test_weeks_6_9 directory for all output files.")
