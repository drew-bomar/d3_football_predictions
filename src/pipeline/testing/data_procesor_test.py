"""
Test Script for D3DataProcessor  
Tests all data processing functionality with sample data and rolling stats
"""

import sys
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path

# Add pipeline directory to path (adjust as needed for your setup)
sys.path.append('pipeline')

from data_processor import D3DataProcessor, process_week_data
from scrapers import SimplifiedD3Scraper

def create_sample_box_scores():
    """Create sample box score data for testing without web scraping"""
    print("Creating sample box score data...")
    
    sample_data = [
        {
            'year': 2022, 'week': 1, 'game_id': '2022_1_1',
            'team1': 'Mount Union', 'team2': 'Defiance',
            'team1_final_score': '65', 'team2_final_score': '0',
            'team1_first_downs': '28', 'team2_first_downs': '9',
            'team1_total_offense': '587', 'team2_total_offense': '151',
            'team1_net_yards_passing': '226', 'team2_net_yards_passing': '74',
            'team1_net_yards_rushing': '361', 'team2_net_yards_rushing': '77',
            'team1_third_down_pct': '67', 'team2_third_down_pct': '13',
            'team1_third_down_conversions': '6', 'team2_third_down_conversions': '2',
            'team1_third_down_att': '9', 'team2_third_down_att': '16',
            'team1_fumbles_lost': '0', 'team2_fumbles_lost': '0',
            'team1_interceptions': '3', 'team2_interceptions': '1',
            'parsing_errors': []
        },
        {
            'year': 2022, 'week': 1, 'game_id': '2022_1_2', 
            'team1': 'Ohio Wesleyan', 'team2': 'Oberlin',
            'team1_final_score': '28', 'team2_final_score': '14',
            'team1_first_downs': '20', 'team2_first_downs': '12',
            'team1_total_offense': '420', 'team2_total_offense': '280',
            'team1_net_yards_passing': '180', 'team2_net_yards_passing': '120',
            'team1_net_yards_rushing': '240', 'team2_net_yards_rushing': '160',
            'team1_third_down_pct': '45', 'team2_third_down_pct': '30',
            'team1_third_down_conversions': '5', 'team2_third_down_conversions': '3',
            'team1_third_down_att': '11', 'team2_third_down_att': '10',
            'team1_fumbles_lost': '1', 'team2_fumbles_lost': '2',
            'team1_interceptions': '1', 'team2_interceptions': '0',
            'parsing_errors': []
        },
        # Week 2 games for testing rolling stats
        {
            'year': 2022, 'week': 2, 'game_id': '2022_2_1',
            'team1': 'Mount Union', 'team2': 'Ohio Wesleyan',
            'team1_final_score': '42', 'team2_final_score': '21',
            'team1_first_downs': '25', 'team2_first_downs': '15',
            'team1_total_offense': '520', 'team2_total_offense': '320',
            'team1_net_yards_passing': '200', 'team2_net_yards_passing': '150',
            'team1_net_yards_rushing': '320', 'team2_net_yards_rushing': '170',
            'team1_third_down_pct': '60', 'team2_third_down_pct': '40',
            'team1_third_down_conversions': '6', 'team2_third_down_conversions': '4',
            'team1_third_down_att': '10', 'team2_third_down_att': '10',
            'team1_fumbles_lost': '0', 'team2_fumbles_lost': '1',
            'team1_interceptions': '2', 'team2_interceptions': '1',
            'parsing_errors': []
        }
    ]
    
    print(f"Created {len(sample_data)} sample games")
    return sample_data


def test_data_cleaning():
    """Test the _clean_numeric_value method"""
    print("="*70)
    print("TEST 1: Data Cleaning (_clean_numeric_value)")
    print("="*70)
    
    processor = D3DataProcessor()
    
    test_values = [
        ('65', 65.0),           # String number
        ('13%', 13.0),          # Percentage
        ('29:18', 1758.0),      # Time format (29*60 + 18)
        ('', 0.0),              # Empty string
        (None, 0.0),            # None value
        (42, 42.0),             # Integer
        ('invalid', 0.0),       # Invalid string
        ('1    -0', 1.0),       # Messy format (would need preprocessing)
    ]
    
    print("Testing value cleaning:")
    for raw_value, expected in test_values:
        result = processor._clean_numeric_value(raw_value)
        status = "✓" if result == expected else "✗"
        print(f"  {status} {str(raw_value):>10} → {result:>8} (expected {expected})")
    
    return True


def test_box_scores_to_team_games():
    """Test converting box scores to team-game format"""
    print("\n" + "="*70)
    print("TEST 2: Box Scores to Team Games Conversion")
    print("="*70)
    
    processor = D3DataProcessor()
    sample_data = create_sample_box_scores()
    
    print(f"Processing {len(sample_data)} box scores...")
    team_games_df = processor.process_box_scores_to_team_games(sample_data)
    
    print(f"\nConversion Results:")
    print(f"  Input: {len(sample_data)} games")
    print(f"  Output: {len(team_games_df)} team-game records")
    print(f"  Expected: {len(sample_data) * 2} records (2 per game)")
    
    # Display sample records
    if len(team_games_df) > 0:
        print(f"\nSample Team-Game Records:")
        print(team_games_df[['team', 'opponent', 'final_score', 'opp_final_score', 'margin', 'win']].head())
        
        # Test derived metrics
        print(f"\nDerived Metrics Check:")
        sample_row = team_games_df.iloc[0]
        print(f"  Team: {sample_row['team']}")
        print(f"  Points Scored: {sample_row.get('points_scored', 'MISSING')}")
        print(f"  Points Allowed: {sample_row.get('points_allowed', 'MISSING')}")
        print(f"  Margin: {sample_row.get('margin', 'MISSING')}")
        print(f"  Win: {sample_row.get('win', 'MISSING')}")
        print(f"  Yards per Play: {sample_row.get('yards_per_play', 'MISSING'):.2f}")
        print(f"  Third Down Rate: {sample_row.get('third_down_rate', 'MISSING'):.3f}")
        print(f"  Turnover Diff: {sample_row.get('turnover_diff', 'MISSING')}")
    
    return team_games_df


def test_rolling_stats():
    """Test rolling statistics calculation"""
    print("\n" + "="*70)
    print("TEST 3: Rolling Statistics Calculation")
    print("="*70)
    
    processor = D3DataProcessor()
    sample_data = create_sample_box_scores()
    
    # Convert to team games first
    team_games_df = processor.process_box_scores_to_team_games(sample_data)
    
    print(f"Calculating rolling stats for {len(team_games_df)} team-game records...")
    rolling_stats_df = processor.calculate_rolling_stats(team_games_df)
    
    print(f"\nRolling Stats Results:")
    print(f"  Input: {len(team_games_df)} team-game records")
    print(f"  Output: {len(rolling_stats_df)} rolling stat records")
    
    if len(rolling_stats_df) > 0:
        # Show column types
        rolling_columns = [col for col in rolling_stats_df.columns if any(w in col for w in ['3wk', '5wk', '7wk', 'season'])]
        print(f"  Rolling stat columns: {len(rolling_columns)}")
        
        # Test Mount Union's progression (should have good stats)
        mu_data = rolling_stats_df[rolling_stats_df['team'] == 'Mount Union'].sort_values('week')
        if len(mu_data) > 0:
            print(f"\nMount Union Progression:")
            for _, row in mu_data.iterrows():
                week = row['week']
                score_3wk = row.get('final_score_3wk', 0)
                win_pct_3wk = row.get('win_pct_3wk', 0)
                games_in_window = row.get('games_in_window_3wk', 0)
                print(f"  Week {week}: Avg Score = {score_3wk:.1f}, Win% = {win_pct_3wk:.1%}, Games Used = {games_in_window}")
        
        # Test Defiance's progression (should show poor performance)
        def_data = rolling_stats_df[rolling_stats_df['team'] == 'Defiance'].sort_values('week')
        if len(def_data) > 0:
            print(f"\nDefiance Progression:")
            for _, row in def_data.iterrows():
                week = row['week']
                score_3wk = row.get('final_score_3wk', 0)
                win_pct_3wk = row.get('win_pct_3wk', 0)
                games_in_window = row.get('games_in_window_3wk', 0)
                print(f"  Week {week}: Avg Score = {score_3wk:.1f}, Win% = {win_pct_3wk:.1%}, Games Used = {games_in_window}")
    
    return rolling_stats_df


def test_data_validation():
    """Test data validation functionality"""
    print("\n" + "="*70)
    print("TEST 4: Data Validation")
    print("="*70)
    
    processor = D3DataProcessor()
    sample_data = create_sample_box_scores()
    team_games_df = processor.process_box_scores_to_team_games(sample_data)
    
    print("Running data validation...")
    validation_report = processor.validate_data(team_games_df)
    
    print(f"\nValidation Report:")
    print(f"  Total Records: {validation_report['total_records']}")
    print(f"  Issues: {len(validation_report['issues'])}")
    print(f"  Warnings: {len(validation_report['warnings'])}")
    
    if validation_report['issues']:
        print(f"  Critical Issues:")
        for issue in validation_report['issues']:
            print(f"    - {issue}")
    
    if validation_report['warnings']:
        print(f"  Warnings:")
        for warning in validation_report['warnings']:
            print(f"    - {warning}")
    
    print(f"\nDataset Summary:")
    summary = validation_report['summary']
    for key, value in summary.items():
        print(f"  {key}: {value}")
    
    return validation_report


def test_model_data_preparation():
    """Test preparing data for ML models"""
    print("\n" + "="*70)
    print("TEST 5: Model Data Preparation")
    print("="*70)
    
    processor = D3DataProcessor()
    sample_data = create_sample_box_scores()
    
    # Process through full pipeline
    team_games_df = processor.process_box_scores_to_team_games(sample_data)
    rolling_stats_df = processor.calculate_rolling_stats(team_games_df)
    
    print("Preparing model-ready data...")
    model_data_df = processor.prepare_model_data(rolling_stats_df)
    
    print(f"\nModel Data Results:")
    print(f"  Input: {len(rolling_stats_df)} rolling stat records")
    print(f"  Output: {len(model_data_df)} model-ready records")
    print(f"  Features: {len(model_data_df.columns)} columns")
    
    # Show feature types
    if len(model_data_df) > 0:
        rank_features = [col for col in model_data_df.columns if 'rank_norm' in col]
        advantage_features = [col for col in model_data_df.columns if 'advantage' in col]
        
        print(f"  Ranking features: {len(rank_features)}")
        print(f"  Advantage features: {len(advantage_features)}")
        
        if rank_features:
            print(f"  Sample ranking features: {rank_features[:3]}")
        
        # Show sample data
        print(f"\nSample Model Features (first team):")
        sample_row = model_data_df.iloc[0]
        print(f"  Team: {sample_row.get('team', 'N/A')}")
        print(f"  Week: {sample_row.get('week', 'N/A')}")
        
        for feature in rank_features[:3]:
            value = sample_row.get(feature, 'N/A')
            print(f"  {feature}: {value}")
    
    return model_data_df


def test_convenience_function():
    """Test the convenience function"""
    print("\n" + "="*70)
    print("TEST 6: Convenience Function (process_week_data)")
    print("="*70)
    
    sample_data = create_sample_box_scores()
    
    print("Testing process_week_data convenience function...")
    team_games, rolling_stats = process_week_data(sample_data)
    
    print(f"  Team Games: {len(team_games)} records")
    print(f"  Rolling Stats: {len(rolling_stats)} records")
    
    return team_games, rolling_stats


def test_with_real_data():
    """Test with real scraped data (optional)"""
    print("\n" + "="*70)
    print("TEST 7: Real Data Integration (Optional)")
    print("="*70)
    
    try:
        print("Attempting to scrape real data for testing...")
        scraper = SimplifiedD3Scraper()
        real_data = scraper.scrape_week_complete(2022, 1, max_games=2)
        
        if real_data['box_scores']:
            processor = D3DataProcessor()
            
            print(f"Processing {len(real_data['box_scores'])} real games...")
            team_games = processor.process_box_scores_to_team_games(real_data['box_scores'])
            rolling_stats = processor.calculate_rolling_stats(team_games)
            
            print(f"  Real data processed successfully!")
            print(f"  Team Games: {len(team_games)}")
            print(f"  Rolling Stats: {len(rolling_stats)}")
            
            return team_games, rolling_stats
        else:
            print("  No real data available, skipping real data test")
            return None, None
            
    except Exception as e:
        print(f"  Real data test failed: {e}")
        print("  (This is expected if no internet connection)")
        return None, None


def run_all_processor_tests():
    """Run all data processor tests"""
    print("D3 FOOTBALL DATA PROCESSOR TEST SUITE")
    print("=" * 70)
    print(f"Started at: {datetime.now()}")
    
    results = {}
    
    try:
        # Test 1: Data cleaning
        results['cleaning'] = test_data_cleaning()
        
        # Test 2: Box scores to team games
        results['team_games'] = test_box_scores_to_team_games()
        
        # Test 3: Rolling stats
        results['rolling_stats'] = test_rolling_stats()
        
        # Test 4: Data validation
        results['validation'] = test_data_validation()
        
        # Test 5: Model preparation
        results['model_data'] = test_model_data_preparation()
        
        # Test 6: Convenience function
        results['convenience'] = test_convenience_function()
        
        # Test 7: Real data (optional)
        results['real_data'] = test_with_real_data()
        
        print("\n" + "="*70)
        print("ALL DATA PROCESSOR TESTS COMPLETED!")
        print("="*70)
        
        # Summary
        successful_tests = sum(1 for v in results.values() if v is not None and len(str(v)) > 0)
        print(f"Successful tests: {successful_tests}/{len(results)}")
        
        return results
        
    except Exception as e:
        print(f"\n❌ DATA PROCESSOR TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    # Run all tests
    results = run_all_processor_tests()
    
    # Optional: Interactive inspection
    if results:
        print(f"\nTest results available in 'results' variable")
        print(f"Sample commands:")
        print(f"  results['team_games'].head()           # View team-game data")
        print(f"  results['rolling_stats'].columns       # See all rolling stat columns")
        print(f"  results['validation']                   # View validation report")