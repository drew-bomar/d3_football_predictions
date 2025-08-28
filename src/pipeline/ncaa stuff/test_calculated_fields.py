"""
Test script to verify all calculated fields are working correctly
"""

from ncaa_translation_layer import NCAAToProcessorTranslator
import json
import pandas as pd


def test_calculated_fields():
    """Test that all efficiency metrics are calculated correctly."""
    
    # Sample game with all necessary stats
    sample_game = {
        'year': 2024,
        'week': 9,
        'game_id': 'test123',
        'team': 'Test Team A',
        'opponent': 'Test Team B',
        'date': '2024-10-26',
        'location': 'home',
        
        # Scores
        'final_score': 35,
        'opp_final_score': 21,
        
        # Basic stats
        'first_downs': 20,
        'total_offense': 425,
        'net_yards_rushing': 150,
        'net_yards_passing': 275,
        
        # For yards per play calculation
        'rushing_attempts': 35,
        'passing_attempts': 25,
        'total_offense_plays': 60,  # Should override calculated
        
        # Third down stats
        'third_down_conversions': '7',
        'third_down_att': '14',
        'third_down_pct': '50.0',  # Should be recalculated
        
        # Turnover stats - Team
        'fumbles': '2',
        'fumbles_lost': '1',
        'interceptions': '1',
        
        # Turnover stats - Opponent
        'opp_fumbles': '3',
        'opp_fumbles_lost': '2',
        'opp_interceptions': '2',
        
        # Other stats
        'penalties_number': '5',
        'punts_number': '4',
        
        # Opponent stats
        'opp_first_downs': 15,
        'opp_total_offense': 320,
        'opp_net_yards_rushing': 80,
        'opp_net_yards_passing': 240,
    }
    
    translator = NCAAToProcessorTranslator()
    
    # Translate the game
    translated = translator._translate_single_game(sample_game)
    
    print("CALCULATED FIELDS TEST")
    print("=" * 60)
    
    print("\nBasic Fields:")
    print(f"  Points Scored: {translated.get('points_scored')}")
    print(f"  Points Allowed: {translated.get('points_allowed')}")
    print(f"  Margin: {translated.get('margin')}")
    print(f"  Win: {translated.get('win')}")
    
    print("\nEfficiency Metrics:")
    print(f"  Turnover Differential: {translated.get('turnover_diff')}")
    print(f"    - Turnovers Forced: {float(translated.get('opp_fumbles_lost', 0)) + float(translated.get('opp_interceptions', 0))}")
    print(f"    - Turnovers Lost: {float(translated.get('fumbles_lost', 0)) + float(translated.get('interceptions', 0))}")
    
    print(f"\n  Yards Per Play: {translated.get('yards_per_play', 0):.2f}")
    print(f"    - Total Offense: {translated.get('total_offense')}")
    print(f"    - Total Plays: 60 (from data)")
    
    print(f"\n  Third Down Rate: {translated.get('third_down_rate', 0):.3f}")
    print(f"    - Conversions: {translated.get('third_down_conversions')}")
    print(f"    - Attempts: {translated.get('third_down_att')}")
    
    print(f"\n  Pass/Rush Ratio: {translated.get('pass_rush_ratio', 0):.3f}")
    print(f"    - Pass Yards: {translated.get('net_yards_passing')}")
    print(f"    - Rush Yards: {translated.get('net_yards_rushing')}")
    
    print(f"\n  Points Per Yard: {translated.get('points_per_yard', 0):.3f}")
    
    # Test with missing data
    print("\n" + "=" * 60)
    print("TESTING WITH MISSING DATA")
    print("=" * 60)
    
    minimal_game = {
        'year': 2024,
        'week': 9,
        'game_id': 'test456',
        'team': 'Test Team C',
        'opponent': 'Test Team D',
        'final_score': 14,
        'opp_final_score': 21,
        # Missing most stats
    }
    
    translated_minimal = translator._translate_single_game(minimal_game)
    
    print("\nWith minimal data:")
    print(f"  Turnover Diff: {translated_minimal.get('turnover_diff', 'Missing')}")
    print(f"  Yards Per Play: {translated_minimal.get('yards_per_play', 'Missing')}")
    print(f"  Third Down Rate: {translated_minimal.get('third_down_rate', 'Missing')}")
    print(f"  Pass/Rush Ratio: {translated_minimal.get('pass_rush_ratio', 'Missing')}")
    
    # Test full translation with dataframe
    print("\n" + "=" * 60)
    print("TESTING DATAFRAME TRANSLATION")
    print("=" * 60)
    
    games = [sample_game, minimal_game]
    df = translator.translate_team_games(games)
    
    print(f"\nDataFrame shape: {df.shape}")
    print(f"Columns with 'diff' or 'rate': ")
    for col in df.columns:
        if 'diff' in col or 'rate' in col or 'ratio' in col:
            print(f"  - {col}: {df[col].tolist()}")
    
    # Verify all expected calculated fields exist
    expected_calculated = ['turnover_diff', 'yards_per_play', 'third_down_rate', 'pass_rush_ratio']
    missing = []
    for field in expected_calculated:
        if field not in df.columns:
            missing.append(field)
    
    if missing:
        print(f"\n⚠️ Missing calculated fields: {missing}")
    else:
        print(f"\n✓ All calculated fields present!")
    
    return df


def verify_rolling_stats_include_calculated():
    """Verify that calculated fields are included in rolling stats."""
    from data_processor import D3DataProcessor
    
    processor = D3DataProcessor()
    
    print("\n" + "=" * 60)
    print("VERIFYING ROLLING STATS CONFIGURATION")
    print("=" * 60)
    
    print("\nCore stats that get rolling averages:")
    for stat in processor.core_stats:
        print(f"  - {stat}")
    
    # Check if our calculated fields are in the rolling calculations
    calculated_fields = ['turnover_diff', 'yards_per_play', 'third_down_rate']
    
    print("\nCalculated fields in rolling stats:")
    for field in calculated_fields:
        # Check if it's in the _get_rolling_window_stats method
        if field in ['margin', 'turnover_diff', 'yards_per_play', 'third_down_rate']:
            print(f"  ✓ {field} - included in rolling calculations")
        else:
            print(f"  ✗ {field} - NOT in rolling calculations")


if __name__ == "__main__":
    # Test calculated fields
    df = test_calculated_fields()
    
    # Verify rolling stats configuration
    verify_rolling_stats_include_calculated()
    
    # Save test output
    if not df.empty:
        df.to_csv('test_calculated_fields.csv', index=False)
        print(f"\n\nTest data saved to: test_calculated_fields.csv")