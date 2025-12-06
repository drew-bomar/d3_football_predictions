# test_stats_translator.py
"""
Test script for Stats Translator
Verifies field mapping and data conversion works correctly
"""

import logging
import json
from pipeline.ncaa_api_client import NCAAAPIClient
from pipeline.stats_translator import StatsTranslator

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def test_basic_translation():
    """Test basic translation with real API data"""
    print("\n" + "="*60)
    print("TEST 1: Basic Translation")
    print("="*60)
    
    client = NCAAAPIClient()
    translator = StatsTranslator()
    
    # Get a week's games
    week_result = client.get_week_games(2024, 8)
    
    if not week_result['success']:
        print("❌ Failed to fetch week games")
        return None
    
    # Get first game with a valid contest ID
    game = week_result['games'][0]
    contest_id = game.get('contestId')
    
    print(f"Testing with game {contest_id}")
    
    # Get stats for this game
    stats_result = client.get_game_stats(contest_id)
    
    if not stats_result['success']:
        print("❌ Failed to fetch game stats")
        return None
    
    # Translate the data
    translated = translator.translate_game_for_db(game, stats_result, week_number=8)
    
    print("\n✅ Translation successful!")
    
    # Show game record
    print("\nGame Record:")
    game_rec = translated['game']
    print(f"  Contest ID: {game_rec['contest_id']}")
    print(f"  Date: {game_rec['game_date']}")
    print(f"  Week: {game_rec['week']}")
    print(f"  Home: {game_rec['home_team_name']} (seoname: {game_rec['home_team_seoname']})")
    print(f"  Away: {game_rec['away_team_name']} (seoname: {game_rec['away_team_seoname']})")
    print(f"  Score: {game_rec['away_score']} - {game_rec['home_score']}")
    print(f"  Home Team ID: {game_rec['home_team_id']} (will be set by team_manager)")
    print(f"  Away Team ID: {game_rec['away_team_id']} (will be set by team_manager)")
    
    return translated

def test_stats_mapping(translated_data):
    """Test that stats are properly mapped to database fields"""
    print("\n" + "="*60)
    print("TEST 2: Stats Field Mapping")
    print("="*60)
    
    if not translated_data:
        print("❌ No translated data to test")
        return
    
    team_stats = translated_data['team_stats']
    
    for i, stats in enumerate(team_stats):
        team_name = stats.get('team_name', 'Unknown')
        is_home = "Home" if stats.get('is_home') else "Away"
        
        print(f"\n{team_name} ({is_home}):")
        
        # Check key mapped fields
        print("  Basic Stats:")
        print(f"    First Downs: {stats.get('first_downs')}")
        print(f"    Total Offense: {stats.get('total_offense')} yards")
        print(f"    Total Plays: {stats.get('total_plays')}")
        
        print("  Passing:")
        print(f"    Completions: {stats.get('pass_completions')}/{stats.get('pass_attempts')}")
        print(f"    Yards: {stats.get('pass_yards')}")
        print(f"    TDs: {stats.get('pass_tds')}")
        print(f"    INTs: {stats.get('interceptions')}")
        
        print("  Rushing:")
        print(f"    Attempts: {stats.get('rush_attempts')}")
        print(f"    Yards: {stats.get('rush_yards')}")
        print(f"    TDs: {stats.get('rush_tds')}")
        
        print("  Efficiency:")
        print(f"    3rd Downs: {stats.get('third_down_conversions')}/{stats.get('third_down_attempts')}")
        print(f"    3rd Down %: {stats.get('third_down_pct', 0):.1f}%")
        print(f"    Completion %: {stats.get('completion_pct', 0):.1f}%")

def test_calculated_fields(translated_data):
    """Test that calculated fields are properly generated"""
    print("\n" + "="*60)
    print("TEST 3: Calculated Fields")
    print("="*60)
    
    if not translated_data:
        print("❌ No translated data to test")
        return
    
    team_stats = translated_data['team_stats']
    
    for stats in team_stats:
        team_name = stats.get('team_name', 'Unknown')
        print(f"\n{team_name} Calculated Fields:")
        
        # Check calculated fields exist
        calculated_fields = [
            'completion_pct',
            'third_down_pct',
            'fourth_down_pct',
            'total_touchdowns',
            'turnovers'
        ]
        
        for field in calculated_fields:
            if field in stats:
                print(f"  ✅ {field}: {stats[field]}")
            else:
                print(f"  ❌ {field}: MISSING")

def test_data_validation():
    """Test data validation"""
    print("\n" + "="*60)
    print("TEST 4: Data Validation")
    print("="*60)
    
    translator = StatsTranslator()
    
    # Test with valid data
    valid_data = {
        'game': {
            'contest_id': '123456',
            'home_team_name': 'Test Home',
            'away_team_name': 'Test Away',
        },
        'team_stats': [
            {'total_offense': 400, 'first_downs': 20},
            {'total_offense': 350, 'first_downs': 18}
        ]
    }
    
    is_valid, errors = translator.validate_translated_data(valid_data)
    print(f"Valid data test: {'✅ PASSED' if is_valid else '❌ FAILED'}")
    if errors:
        for error in errors:
            print(f"  - {error}")
    
    # Test with invalid data (missing teams)
    invalid_data = {
        'game': {
            'contest_id': '123456'
        },
        'team_stats': []
    }
    
    is_valid, errors = translator.validate_translated_data(invalid_data)
    print(f"\nInvalid data test: {'✅ Correctly identified' if not is_valid else '❌ FAILED'}")
    if errors:
        print("  Errors found:")
        for error in errors:
            print(f"    - {error}")

def test_specific_game():
    """Test with the known Wis.-Whitewater game"""
    print("\n" + "="*60)
    print("TEST 5: Known Game Translation (Wis.-Whitewater vs Wis.-Stevens Point)")
    print("="*60)
    
    client = NCAAAPIClient()
    translator = StatsTranslator()
    
    # Get the specific week
    week_result = client.get_week_games(2024, 8)
    
    # Find the Whitewater game
    target_game = None
    for game in week_result['games']:
        if game.get('contestId') == 6308940:
            target_game = game
            break
    
    if not target_game:
        print("❌ Could not find target game")
        return
    
    # Get stats
    stats_result = client.get_game_stats(6308940)
    
    # Translate
    translated = translator.translate_game_for_db(target_game, stats_result, week_number=8)
    
    # Validate the blowout score
    game_rec = translated['game']
    print(f"Score verification: {game_rec['away_team_name']} {game_rec['away_score']} - "
          f"{game_rec['home_team_name']} {game_rec['home_score']}")
    
    # Check stats match the blowout
    for stats in translated['team_stats']:
        if stats['team_name'] == 'Wis.-Whitewater':
            print(f"\n{stats['team_name']} dominated with:")
            print(f"  Total Offense: {stats.get('total_offense')} yards")
            print(f"  Passing: {stats.get('pass_yards')} yards")
            print(f"  Rushing: {stats.get('rush_yards')} yards")
            print(f"  Third Downs: {stats.get('third_down_conversions')}/{stats.get('third_down_attempts')}")

def save_translated_sample():
    """Save a sample of translated data for reference"""
    print("\n" + "="*60)
    print("SAVING TRANSLATED SAMPLE")
    print("="*60)
    
    client = NCAAAPIClient()
    translator = StatsTranslator()
    
    # Get one game
    week_result = client.get_week_games(2024, 8)
    if week_result['success'] and week_result['games']:
        game = week_result['games'][0]
        stats_result = client.get_game_stats(game['contestId'])
        
        if stats_result['success']:
            translated = translator.translate_game_for_db(game, stats_result, week_number=8)
            
            with open('sample_translated_data.json', 'w') as f:
                # Convert datetime to string for JSON serialization
                if translated['game'].get('game_date'):
                    translated['game']['game_date'] = str(translated['game']['game_date'])
                json.dump(translated, f, indent=2)
            print("✅ Saved translated data to sample_translated_data.json")

if __name__ == "__main__":
    print("\n" + "="*60)
    print("STATS TRANSLATOR TEST SUITE")
    print("="*60)
    
    # Test 1: Basic translation
    translated = test_basic_translation()
    
    # Test 2: Field mapping
    test_stats_mapping(translated)
    
    # Test 3: Calculated fields
    test_calculated_fields(translated)
    
    # Test 4: Validation
    test_data_validation()
    
    # Test 5: Specific known game
    test_specific_game()
    
    # Optional: Save sample
    print("\nDo you want to save a translated data sample? (y/n): ", end="")
    response = input().strip().lower()
    if response == 'y':
        save_translated_sample()
    
    print("\n" + "="*60)
    print("ALL TESTS COMPLETE!")
    print("="*60)