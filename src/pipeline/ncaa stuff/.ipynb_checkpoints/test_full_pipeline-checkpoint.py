"""
Test script for NCAA pipeline with rolling statistics integration
Tests various scenarios including single week, resume capability, and full season
"""

import logging
import json
import pandas as pd
from pathlib import Path
import shutil
from datetime import datetime

# Import pipeline components
from pipeline_manager import NCAASeasonPipeline
from ncaa_translation_layer import NCAAToProcessorTranslator
from data_processor import D3DataProcessor

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_single_week():
    """Test collecting a single week with rolling stats."""
    print("\n" + "="*80)
    print("TEST 1: Single Week Collection with Rolling Stats")
    print("="*80)
    
    # Use test directory
    test_dir = "test_ncaa_data_single"
    
    # Clean up any existing test data
    if Path(test_dir).exists():
        shutil.rmtree(test_dir)
    
    try:
        # Create pipeline
        pipeline = NCAASeasonPipeline(data_dir=test_dir, delay=1.0)
        
        # Collect just week 1 of 2024
        result = pipeline.collect_season(2024, start_week=1, end_week=1)
        
        print(f"\nCollection Summary:")
        print(f"  Games processed: {result.get('successful_games', 0)}")
        print(f"  Failed games: {result.get('failed_games', 0)}")
        print(f"  Teams seen: {result.get('teams_seen', 0)}")
        
        # Only check files if games were processed
        if result.get('successful_games', 0) > 0:
            processed_dir = Path(test_dir) / "processed"
        
        # Check for team games file
        team_games_file = processed_dir / "2024_week_01_team_games.json"
        if team_games_file.exists():
            with open(team_games_file, 'r') as f:
                team_games = json.load(f)
            print(f"\nTeam games saved: {len(team_games)} records")
            
            # Show sample
            if team_games:
                sample = team_games[0]
                print(f"\nSample team game record:")
                print(f"  Team: {sample['team']} vs {sample['opponent']}")
                print(f"  Score: {sample.get('final_score', 'N/A')} - {sample.get('opp_final_score', 'N/A')}")
                print(f"  SOS: {sample.get('sos_current', 'N/A')}")
        
        # Check for rolling stats file
        rolling_file = processed_dir / "2024_week_01_rolling_stats.csv"
        if rolling_file.exists():
            rolling_df = pd.read_csv(rolling_file)
            print(f"\nRolling stats saved: {rolling_df.shape[0]} records")
            
            # Note: Week 1 won't have any rolling averages (no previous games)
            print("\nNote: Week 1 has no previous games, so rolling averages will be 0")
        else:
            print("\nNo rolling stats file found (expected for week 1)")
        
        return True
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        return False


def test_multiple_weeks():
    """Test collecting multiple weeks to see rolling stats develop."""
    print("\n" + "="*80)
    print("TEST 2: Multiple Weeks (1-3) to See Rolling Stats")
    print("="*80)
    
    test_dir = "test_ncaa_data_multi"
    
    if Path(test_dir).exists():
        shutil.rmtree(test_dir)
    
    try:
        pipeline = NCAASeasonPipeline(data_dir=test_dir, delay=1.0)
        
        # Collect weeks 1-3
        result = pipeline.collect_season(2024, start_week=1, end_week=3)
        
        print(f"\nCollection Summary:")
        print(f"  Weeks processed: {result['weeks_processed']}")
        print(f"  Total games: {result['successful_games']}")
        print(f"  Teams tracked: {result['teams_seen']}")
        
        # Check week 3 rolling stats
        rolling_file = Path(test_dir) / "processed" / "2024_week_03_current_rolling.csv"
        if rolling_file.exists():
            rolling_df = pd.read_csv(rolling_file)
            
            print(f"\nWeek 3 Rolling Stats:")
            print(f"  Records with data: {rolling_df.shape[0]}")
            
            # Find teams with best 3-week averages
            if 'final_score_3wk' in rolling_df.columns:
                top_scorers = rolling_df.nlargest(5, 'final_score_3wk')[
                    ['team', 'opponent', 'final_score_3wk', 'win_pct_3wk', 'games_in_window_3wk']
                ]
                
                print(f"\nTop 5 teams by 3-week scoring average:")
                for _, row in top_scorers.iterrows():
                    games = int(row.get('games_in_window_3wk', 0))
                    print(f"  {row['team']}: {row['final_score_3wk']:.1f} pts/game "
                          f"({row['win_pct_3wk']:.0%} win rate, {games} games)")
        
        # Check SOS development
        checkpoint_file = Path(test_dir) / "checkpoints" / "2024_week_03_checkpoint.pkl"
        if checkpoint_file.exists():
            import pickle
            with open(checkpoint_file, 'rb') as f:
                checkpoint = pickle.load(f)
            
            team_records = checkpoint['merger_state']['team_records']
            
            # Find teams with highest SOS
            sos_list = []
            for team_id, record in team_records.items():
                if record['opponents']:
                    # Calculate current SOS
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
                        team_name = record['opponents'][0].get('opponent_name', team_id)
                        sos_list.append((team_name, sos, f"{record['wins']}-{record['losses']}"))
            
            sos_list.sort(key=lambda x: x[1], reverse=True)
            
            print(f"\nTop 5 teams by Strength of Schedule:")
            for name, sos, record in sos_list[:5]:
                print(f"  {name} ({record}): SOS = {sos:.3f}")
        
        return True
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        return False


def test_resume_capability():
    """Test resuming after interruption."""
    print("\n" + "="*80)
    print("TEST 3: Resume Capability")
    print("="*80)
    
    test_dir = "test_ncaa_data_resume"
    
    if Path(test_dir).exists():
        shutil.rmtree(test_dir)
    
    try:
        # First, collect weeks 1-2
        print("\nPhase 1: Collecting weeks 1-2...")
        pipeline = NCAASeasonPipeline(data_dir=test_dir, delay=1.0)
        pipeline.collect_season(2024, start_week=1, end_week=2)
        
        # Check that we have week 2 data
        week2_file = Path(test_dir) / "processed" / "2024_week_02_team_games.json"
        if week2_file.exists():
            print("✓ Week 2 data saved successfully")
        
        # Simulate interruption - create new pipeline instance
        print("\nPhase 2: Simulating interruption and resume...")
        pipeline2 = NCAASeasonPipeline(data_dir=test_dir, delay=1.0)
        
        # Try to collect weeks 1-4 (should resume from week 3)
        result = pipeline2.collect_season(2024, start_week=1, end_week=4)
        
        print(f"\nResume Summary:")
        print(f"  Weeks processed: {result['weeks_processed']}")
        print(f"  Total games: {result['successful_games']}")
        
        # Verify week 4 has proper rolling stats with history
        rolling_file = Path(test_dir) / "processed" / "2024_week_04_current_rolling.csv"
        if rolling_file.exists():
            rolling_df = pd.read_csv(rolling_file)
            
            # Check that we have 3-week averages
            sample_team = rolling_df.iloc[0]
            print(f"\nSample team rolling stats (Week 4):")
            print(f"  Team: {sample_team['team']}")
            print(f"  3-week avg score: {sample_team.get('final_score_3wk', 'N/A')}")
            print(f"  Games in 3-week window: {sample_team.get('games_in_window_3wk', 'N/A')}")
            
            # Verify we have data from previous weeks
            cumulative_file = Path(test_dir) / "checkpoints" / "2024_week_04_cumulative_games.json"
            if cumulative_file.exists():
                with open(cumulative_file, 'r') as f:
                    cumulative = json.load(f)
                print(f"\n✓ Cumulative data maintained: {len(cumulative)} total games")
        
        return True
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        return False


def test_translation_layer():
    """Test the NCAA to data_processor translation."""
    print("\n" + "="*80)
    print("TEST 4: Translation Layer")
    print("="*80)
    
    # Create sample NCAA format data
    sample_games = [
        {
            'year': 2024,
            'week': 5,
            'game_id': 'test123',
            'team': 'Centre',
            'opponent': 'Southwestern',
            'date': '2024-09-28',
            'location': 'away',
            'final_score': 38,
            'opp_final_score': 28,
            'first_downs': 18,
            'opp_first_downs': 15,
            'net_yards_rushing': 55,
            'opp_net_yards_rushing': 108,
            'net_yards_passing': 307,
            'opp_net_yards_passing': 226,
            'total_offense': 362,
            'opp_total_offense': 334,
            'third_down_conversions': '6',
            'third_down_att': '15',
            'third_down_pct': '40.0',
            'fumbles': '1',
            'fumbles_lost': '1',
            'penalties_number': '0',
            'punts_number': '4',
            'sos_before': 0.425,
            'sos_current': 0.438,
            'record_before': '3-1'
        },
        {
            'year': 2024,
            'week': 5,
            'game_id': 'test124',
            'team': 'Southwestern',
            'opponent': 'Centre',
            'date': '2024-09-28',
            'location': 'home',
            'final_score': 28,
            'opp_final_score': 38,
            'first_downs': 15,
            'opp_first_downs': 18,
            'net_yards_rushing': 108,
            'opp_net_yards_rushing': 55,
            'net_yards_passing': 226,
            'opp_net_yards_passing': 307,
            'total_offense': 334,
            'opp_total_offense': 362,
            'third_down_conversions': '10',
            'third_down_att': '17',
            'third_down_pct': '58.8',
            'fumbles': '3',
            'fumbles_lost': '2',
            'penalties_number': '5',
            'punts_number': '4',
            'sos_before': 0.500,
            'sos_current': 0.487,
            'record_before': '2-2'
        }
    ]
    
    # Test translation
    translator = NCAAToProcessorTranslator()
    translated_df = translator.translate_team_games(sample_games)
    
    print(f"\nTranslation Results:")
    print(f"  Input games: {len(sample_games)}")
    print(f"  Output records: {translated_df.shape[0]}")
    print(f"  Columns: {translated_df.shape[1]}")
    
    # Check key fields
    print(f"\nSample translated record:")
    if not translated_df.empty:
        sample = translated_df.iloc[0]
        print(f"  Team: {sample['team']} vs {sample['opponent']}")
        print(f"  Points scored/allowed: {sample['points_scored']}/{sample['points_allowed']}")
        print(f"  Margin: {sample['margin']}")
        print(f"  Win: {sample['win']}")
        
        # Check that all required stats are present
        required_stats = [
            'final_score', 'first_downs', 'total_offense', 'net_yards_passing',
            'net_yards_rushing', 'third_down_pct', 'fumbles', 'penalties_number'
        ]
        
        missing = []
        for stat in required_stats:
            if stat not in translated_df.columns:
                missing.append(stat)
        
        if missing:
            print(f"\n⚠️  Missing stats: {missing}")
        else:
            print(f"\n✓ All required stats present")
    
    # Test with data processor
    print(f"\nTesting with D3DataProcessor...")
    processor = D3DataProcessor()
    
    try:
        # Add missing stats
        translated_df = translator.add_missing_stats(translated_df)
        translated_df = translator.prepare_for_rolling_stats(translated_df)
        
        # Try to calculate rolling stats
        rolling_df = processor.calculate_rolling_stats(translated_df)
        
        if not rolling_df.empty:
            print(f"✓ Rolling stats calculated: {rolling_df.shape[0]} records")
        else:
            print(f"⚠️  No rolling stats generated (expected for single week)")
        
        return True
        
    except Exception as e:
        logger.error(f"Data processor test failed: {e}")
        return False


def run_all_tests():
    """Run all pipeline tests."""
    print("\n" + "="*80)
    print("NCAA PIPELINE TEST SUITE")
    print("="*80)
    
    tests = [
        ("Single Week Collection", test_single_week),
        ("Multiple Weeks with Rolling Stats", test_multiple_weeks),
        ("Resume Capability", test_resume_capability),
        ("Translation Layer", test_translation_layer)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        try:
            print(f"\nRunning: {test_name}")
            success = test_func()
            results.append((test_name, success))
        except Exception as e:
            logger.error(f"Test {test_name} crashed: {e}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "="*80)
    print("TEST SUMMARY")
    print("="*80)
    
    passed = sum(1 for _, success in results if success)
    total = len(results)
    
    for test_name, success in results:
        status = "✓ PASSED" if success else "✗ FAILED"
        print(f"{test_name:<40} {status}")
    
    print(f"\nTotal: {passed}/{total} tests passed")
    
    # Clean up test directories
    print("\nCleaning up test directories...")
    for test_dir in ["test_ncaa_data_single", "test_ncaa_data_multi", "test_ncaa_data_resume"]:
        if Path(test_dir).exists():
            shutil.rmtree(test_dir)
            print(f"  Removed {test_dir}")


def quick_test():
    """Quick test with just one week."""
    print("\nRunning quick test with Week 1 only...")
    
    pipeline = NCAASeasonPipeline(data_dir="quick_test_data", delay=1.0)
    
    # Just process one week
    result = pipeline.collect_season(2024, start_week=1, end_week=1)
    
    print(f"\nQuick test complete:")
    print(f"  Games processed: {result.get('successful_games', 0)}")
    print(f"  Time taken: {result.get('collection_time', 0)/60:.1f} minutes")
    
    # Check for output files
    output_dir = Path("quick_test_data")
    if output_dir.exists():
        files = list(output_dir.rglob("*"))
        print(f"  Files created: {len(files)}")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "quick":
            quick_test()
        elif sys.argv[1] == "single":
            test_single_week()
        elif sys.argv[1] == "multi":
            test_multiple_weeks()
        elif sys.argv[1] == "resume":
            test_resume_capability()
        elif sys.argv[1] == "translate":
            test_translation_layer()
        else:
            print(f"Unknown test: {sys.argv[1]}")
            print("Options: quick, single, multi, resume, translate, or no argument for all tests")
    else:
        run_all_tests()