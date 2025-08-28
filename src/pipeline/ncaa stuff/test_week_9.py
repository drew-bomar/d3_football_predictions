"""
Test the pipeline with Week 9 which has more games
"""

from pipeline_manager import NCAASeasonPipeline
import logging
from pathlib import Path
import shutil
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def test_week_9():
    """Test with Week 9 which should have ~100+ games."""
    print("\n" + "="*80)
    print("Testing Pipeline with Week 9 (2024)")
    print("="*80)
    
    # Clean test directory
    test_dir = "test_week9_data"
    if Path(test_dir).exists():
        shutil.rmtree(test_dir)
    
    # Create pipeline with shorter delay for testing
    pipeline = NCAASeasonPipeline(data_dir=test_dir, delay=0.5)
    
    # Collect just week 9
    print("\nCollecting Week 9 only (this will take a few minutes)...")
    print("Note: Processing ~100+ games with API calls\n")
    
    result = pipeline.collect_season(2024, start_week=9, end_week=9)
    
    print(f"\n{'='*60}")
    print("COLLECTION COMPLETE")
    print(f"{'='*60}")
    
    print(f"\nSummary:")
    print(f"  Total games found: {result.get('total_games', 0)}")
    print(f"  Successfully processed: {result.get('successful_games', 0)}")
    print(f"  Failed: {result.get('failed_games', 0)}")
    print(f"  Teams tracked: {result.get('teams_seen', 0)}")
    print(f"  Time taken: {result.get('collection_time', 0)/60:.1f} minutes")
    
    # Check output files
    print(f"\nChecking output files...")
    
    # Team games
    team_games_file = Path(test_dir) / "processed" / "2024_week_09_team_games.json"
    if team_games_file.exists():
        import json
        with open(team_games_file, 'r') as f:
            team_games = json.load(f)
        print(f"✓ Team games file: {len(team_games)} records")
        
        # Show sample game
        if team_games:
            sample = team_games[0]
            print(f"\n  Sample game:")
            print(f"    {sample['team']} vs {sample['opponent']}")
            print(f"    Score: {sample.get('final_score', 'N/A')} - {sample.get('opp_final_score', 'N/A')}")
            print(f"    SOS: {sample.get('sos_current', 'N/A'):.3f}")
            print(f"    Record before: {sample.get('record_before', 'N/A')}")
    
    # Rolling stats
    rolling_file = Path(test_dir) / "processed" / "2024_week_09_current_rolling.csv"
    if rolling_file.exists():
        rolling_df = pd.read_csv(rolling_file)
        print(f"\n✓ Rolling stats file: {rolling_df.shape[0]} records")
        
        # Note about Week 9 rolling stats
        print(f"\n  Note: Week 9 is first week, so rolling averages will be 0")
        print(f"  This is expected - rolling stats need previous weeks' data")
        
        # Show columns available
        print(f"\n  Columns in rolling stats: {len(rolling_df.columns)}")
        print(f"  Sample columns: {list(rolling_df.columns[:10])}...")
    
    # Check final report
    report_file = Path(test_dir) / "2024_season_report.txt"
    if report_file.exists():
        print(f"\n✓ Season report generated")
        
        # Show top of report
        with open(report_file, 'r') as f:
            lines = f.readlines()[:20]
        
        print(f"\n  First few lines of report:")
        for line in lines[:10]:
            print(f"    {line.rstrip()}")
    
    print(f"\n{'='*60}")
    print("TEST COMPLETE")
    print(f"{'='*60}")
    
    # Optional: test with weeks 8-9 to see rolling stats work
    user_input = input("\nTest weeks 8-9 to see rolling stats develop? (y/n): ")
    if user_input.lower() == 'y':
        test_multiple_weeks()


def test_multiple_weeks():
    """Test weeks 8-9 to see rolling stats actually calculate."""
    print("\n" + "="*80)
    print("Testing Weeks 8-9 for Rolling Stats")
    print("="*80)
    
    test_dir = "test_weeks89_data"
    if Path(test_dir).exists():
        shutil.rmtree(test_dir)
    
    pipeline = NCAASeasonPipeline(data_dir=test_dir, delay=0.5)
    
    print("\nCollecting Weeks 8-9 (this will take several minutes)...")
    result = pipeline.collect_season(2024, start_week=8, end_week=9)
    
    print(f"\nCollection complete:")
    print(f"  Games processed: {result.get('successful_games', 0)}")
    
    # Check Week 9 rolling stats - should have Week 8 data now
    rolling_file = Path(test_dir) / "processed" / "2024_week_09_current_rolling.csv"
    if rolling_file.exists():
        rolling_df = pd.read_csv(rolling_file)
        
        # Look for non-zero rolling stats
        if 'final_score_3wk' in rolling_df.columns:
            teams_with_data = rolling_df[rolling_df['final_score_3wk'] > 0]
            
            if not teams_with_data.empty:
                print(f"\n✓ Rolling stats now working!")
                print(f"  Teams with rolling data: {len(teams_with_data)}")
                
                # Show top scorers
                top_5 = teams_with_data.nlargest(5, 'final_score_3wk')
                print(f"\n  Top 5 by rolling average:")
                for _, team in top_5.iterrows():
                    print(f"    {team['team']}: {team['final_score_3wk']:.1f} pts/game")
            else:
                print(f"\n⚠️  Still no rolling data - may need more weeks")


def quick_test_small_week():
    """Quick test with Week 15 which might have fewer games."""
    print("\nTesting with Week 15 (likely fewer games)...")
    
    pipeline = NCAASeasonPipeline(data_dir="test_week15", delay=0.5)
    result = pipeline.collect_season(2024, start_week=15, end_week=15)
    
    print(f"Week 15 results: {result.get('successful_games', 0)} games processed")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "multi":
        test_multiple_weeks()
    elif len(sys.argv) > 1 and sys.argv[1] == "small":
        quick_test_small_week()
    else:
        test_week_9()