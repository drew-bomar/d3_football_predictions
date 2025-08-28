"""
Test script for the updated NCAA scraper
"""

# First, let's test if we can import the scraper
try:
    from ncaa_scraper import NCAAScraper
    print("✅ Successfully imported NCAAScraper")
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("Make sure to save the NCAA scraper code as 'ncaa_scraper.py' first")
    exit(1)

def test_ncaa_scraper():
    """Test the NCAA scraper with a known good week"""
    
    print("🧪 Testing NCAA D3 Football Scraper")
    print("=" * 50)
    
    # Create scraper instance
    scraper = NCAAScraper(delay=2.0)  # Be extra respectful with delays
    
    # Test with 2024 Week 5 (should have games)
    print(f"\n🏈 Testing 2024 Week 5 (limited to 3 games)...")
    
    try:
        result = scraper.scrape_week_complete(year=2024, week=5, max_games=3)
        
        # Check results
        summary = result['summary']
        box_scores = result['box_scores']
        
        print(f"\n📊 SCRAPING RESULTS:")
        print(f"  Total games found: {summary['total_games']}")
        print(f"  Successful parses: {summary['successful_box_scores']}")
        print(f"  Failed parses: {summary['failed_box_scores']}")
        print(f"  Success rate: {summary['success_rate']:.1%}")
        
        if box_scores:
            print(f"\n🎮 SAMPLE GAMES:")
            for i, game in enumerate(box_scores):
                team1 = game.get('team1', 'Unknown')
                team2 = game.get('team2', 'Unknown') 
                score1 = game.get('team1_final_score', '?')
                score2 = game.get('team2_final_score', '?')
                
                print(f"  Game {i+1}: {team1} {score1} - {score2} {team2}")
                
                # Show any parsing errors
                errors = game.get('parsing_errors', [])
                if errors:
                    print(f"    ⚠️  Errors: {errors}")
                
                # Show what stats we got from team-stats page
                stats_found = [key for key in game.keys() if key.startswith('team1_') and key not in ['team1_final_score']]
                print(f"    📈 Stats categories found: {len(stats_found)}")
                if stats_found:
                    print(f"    Sample stats: {stats_found[:5]}")
        
        else:
            print(f"\n❌ No games found - let's debug...")
            
            # Try manual URL discovery to see what went wrong
            print(f"\n🔍 DEBUGGING - Testing URL discovery manually...")
            game_urls = scraper._discover_game_urls(2024, 5)
            print(f"  Game URLs discovered: {len(game_urls)}")
            if game_urls:
                print(f"  Sample URLs: {game_urls[:3]}")
            else:
                print(f"  ❌ No game URLs found - check scoreboard parsing")
        
    except Exception as e:
        print(f"❌ Error during scraping: {e}")
        import traceback
        traceback.print_exc()

def test_url_discovery_only():
    """Test just the URL discovery part"""
    
    print(f"\n🔍 Testing URL discovery independently...")
    
    scraper = NCAAScraper()
    
    # Test different weeks to see which ones have games
    test_weeks = [
        (2024, 1),   # Season start
        (2024, 5),   # Mid-season
        (2024, 9),   # Late season
    ]
    
    for year, week in test_weeks:
        print(f"\n  Testing {year} Week {week}:")
        try:
            urls = scraper._discover_game_urls(year, week)
            print(f"    Found {len(urls)} game URLs")
            if urls:
                print(f"    Sample: {urls[0]}")
        except Exception as e:
            print(f"    Error: {e}")

def test_single_game():
    """Test parsing a single game in detail"""
    
    print(f"\n🎯 Testing single game parsing...")
    
    scraper = NCAAScraper()
    
    # First get a game URL
    try:
        game_urls = scraper._discover_game_urls(2024, 5)
        if not game_urls:
            print("  ❌ No game URLs found to test with")
            return
        
        test_url = game_urls[0]
        print(f"  Testing with game URL: {test_url}")
        
        # Parse this specific game
        game_data = scraper.parse_game_stats(test_url)
        
        print(f"\n  📋 SINGLE GAME RESULTS:")
        print(f"    Team 1: {game_data.get('team1', 'Not found')}")
        print(f"    Team 2: {game_data.get('team2', 'Not found')}")
        print(f"    Score: {game_data.get('team1_final_score', '?')}-{game_data.get('team2_final_score', '?')}")
        print(f"    Parsing errors: {game_data.get('parsing_errors', [])}")
        
        # Count stats found
        stats_keys = [k for k in game_data.keys() if k.startswith('team1_') or k.startswith('team2_')]
        print(f"    Total stat fields: {len(stats_keys)}")
        
        if len(stats_keys) > 4:  # More than just basic info
            print(f"    ✅ Detailed stats successfully extracted!")
        else:
            print(f"    ⚠️  Only basic stats found - check team-stats page parsing")
        
    except Exception as e:
        print(f"  ❌ Error in single game test: {e}")

if __name__ == "__main__":
    # Run the tests
    test_ncaa_scraper()
    test_url_discovery_only()
    test_single_game()
    
    print(f"\n🏁 Testing complete!")
    print("If you see issues, we can debug and fix them step by step.")