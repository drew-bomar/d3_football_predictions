# Create this file: test_integration.py
from scrapers import D3FootballScraper
from data_processor import D3DataProcessor

def test_complete_integration():
    print("Testing Complete Scraper → Data Processor Integration")
    print("=" * 60)
    
    # Step 1: Scrape real data
    scraper = D3FootballScraper()
    week_data = scraper.scrape_week_complete(2022, 1, max_games=5)
    print(f"Scraped: {len(week_data['box_scores'])} games")
    
    # Step 2: Process to team-games
    processor = D3DataProcessor()
    team_games = processor.process_box_scores_to_team_games(week_data['box_scores'])
    print(f"Team-games: {len(team_games)} records")
    
    # Step 3: Calculate rolling stats
    rolling_stats = processor.calculate_rolling_stats(team_games)
    print(f"Rolling stats: {len(rolling_stats)} records")
    
    # Step 4: Show sample rolling data
    if len(rolling_stats) > 0:
        sample = rolling_stats.iloc[0]
        print(f"\nSample rolling stats for {sample['team']}:")
        print(f"  3-week avg score: {sample.get('final_score_3wk', 'N/A')}")
        print(f"  5-week avg score: {sample.get('final_score_5wk', 'N/A')}")
        print(f"  Games in window: {sample.get('games_in_window_3wk', 'N/A')}")
    
    return team_games, rolling_stats

if __name__ == "__main__":
    team_games, rolling_stats = test_complete_integration()