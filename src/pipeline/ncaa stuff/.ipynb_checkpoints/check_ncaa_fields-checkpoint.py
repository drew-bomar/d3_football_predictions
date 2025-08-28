"""
Check what field names NCAA uses for total plays
"""

import json
from pathlib import Path


def check_ncaa_field_names():
    """Look at actual NCAA data to find the right field names."""
    
    # Look for a recent team games file
    test_dirs = ["test_weeks89_data", "test_weeks_6_9", "test_week9_data"]
    
    team_games_file = None
    for dir_name in test_dirs:
        path = Path(dir_name) / "processed"
        if path.exists():
            # Find any team games file
            files = list(path.glob("*_team_games.json"))
            if files:
                team_games_file = files[0]
                break
    
    if not team_games_file:
        print("No team games file found! Run a test first.")
        return
    
    print(f"Checking file: {team_games_file}")
    
    with open(team_games_file, 'r') as f:
        games = json.load(f)
    
    if not games:
        print("No games in file!")
        return
    
    # Look at first game
    game = games[0]
    
    print(f"\nTeam: {game.get('team')} vs {game.get('opponent')}")
    print(f"\nLooking for total plays field...")
    
    # Find all fields with 'play' in the name
    play_fields = [k for k in game.keys() if 'play' in k.lower()]
    print(f"\nFields containing 'play': {play_fields}")
    
    # Find all fields with 'total' in the name
    total_fields = [k for k in game.keys() if 'total' in k.lower()]
    print(f"\nFields containing 'total': {total_fields}")
    
    # Check specific offense fields
    print(f"\n\nOffense-related fields:")
    offense_fields = {
        'total_offense': game.get('total_offense', 'NOT FOUND'),
        'total_offense_plays': game.get('total_offense_plays', 'NOT FOUND'),
        'total_offense_avg_play': game.get('total_offense_avg_play', 'NOT FOUND'),
        'rushing_attempts': game.get('rushing_attempts', 'NOT FOUND'),
        'passing_attempts': game.get('passing_attempts', 'NOT FOUND'),
        'passing_completions': game.get('passing_completions', 'NOT FOUND')
    }
    
    for field, value in offense_fields.items():
        print(f"  {field}: {value}")
    
    # Calculate yards per play manually
    print(f"\n\nManual calculation:")
    try:
        total_offense = float(game.get('total_offense', 0))
        total_plays = float(game.get('total_offense_plays', 0))
        
        if total_plays > 0:
            ypp = total_offense / total_plays
            print(f"  Total Offense: {total_offense}")
            print(f"  Total Plays: {total_plays}")
            print(f"  Yards Per Play: {ypp:.2f}")
        else:
            # Try from components
            rush_att = float(game.get('rushing_attempts', 0))
            pass_att = float(game.get('passing_attempts', 0))
            total_plays_calc = rush_att + pass_att
            
            if total_plays_calc > 0:
                ypp = total_offense / total_plays_calc
                print(f"  Total Offense: {total_offense}")
                print(f"  Rush Attempts: {rush_att}")
                print(f"  Pass Attempts: {pass_att}")
                print(f"  Total Plays (calculated): {total_plays_calc}")
                print(f"  Yards Per Play: {ypp:.2f}")
    except Exception as e:
        print(f"  Error calculating: {e}")
    
    # Show all numeric fields for reference
    print(f"\n\nAll numeric fields in game data:")
    numeric_fields = []
    for k, v in game.items():
        if isinstance(v, (int, float)) or (isinstance(v, str) and v.replace('.', '').isdigit()):
            numeric_fields.append(k)
    
    numeric_fields.sort()
    for field in numeric_fields[:20]:  # First 20
        print(f"  {field}: {game[field]}")
    
    if len(numeric_fields) > 20:
        print(f"  ... and {len(numeric_fields) - 20} more fields")


if __name__ == "__main__":
    check_ncaa_field_names()