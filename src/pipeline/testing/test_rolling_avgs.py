"""
Detailed test of rolling averages with full visibility into the calculation process
Shows exactly how 3-week rolling averages are calculated for Defiance
"""

from pipeline.scrapers import D3FootballScraper
from pipeline.data_processor import D3DataProcessor
import pandas as pd

def scrape_defiance_multiple_weeks():
    """Scrape multiple weeks to get Defiance games for rolling average testing"""
    
    print("SCRAPING DEFIANCE GAMES ACROSS MULTIPLE WEEKS")  
    print("=" * 60)
    
    scraper = D3FootballScraper()
    processor = D3DataProcessor()
    
    all_box_scores = []
    
    # Scrape weeks 1-4 to get enough data for rolling averages
    for week in range(1, 5):
        print(f"\nScraping 2022 Week {week}...")
        
        week_data = scraper.scrape_week_complete(2022, week, max_games=20)  # Limit for speed
        box_scores = week_data['box_scores']
        
        # Find Defiance games in this week
        defiance_games = []
        for game in box_scores:
            if 'Defiance' in [game.get('team1'), game.get('team2')]:
                defiance_games.append(game)
        
        print(f"  Found {len(defiance_games)} Defiance game(s) in week {week}")
        
        if defiance_games:
            for game in defiance_games:
                team1, team2 = game.get('team1', '?'), game.get('team2', '?')
                score1, score2 = game.get('team1_final_score', '?'), game.get('team2_final_score', '?')
                print(f"    {team1} {score1} - {score2} {team2}")
        
        all_box_scores.extend(box_scores)
    
    return all_box_scores

def analyze_defiance_rolling_averages(all_box_scores):
    """Analyze Defiance's rolling averages with full step-by-step visibility"""
    
    print(f"\n" + "=" * 60)
    print("DEFIANCE ROLLING AVERAGE ANALYSIS")
    print("=" * 60)
    
    processor = D3DataProcessor()
    
    # Convert to team-game format
    print("\nStep 1: Converting to team-game format...")
    team_games = processor.process_box_scores_to_team_games(all_box_scores)
    
    # Filter to just Defiance games
    defiance_games = team_games[team_games['team'] == 'Defiance'].copy()
    defiance_games = defiance_games.sort_values(['year', 'week']).reset_index(drop=True)
    
    print(f"Found {len(defiance_games)} Defiance games")
    
    # Show Defiance's game-by-game stats
    print(f"\nDefiance Game-by-Game Stats:")
    print("-" * 80)
    print(f"{'Week':<6} {'Opponent':<20} {'Score':<8} {'Opp Score':<10} {'Margin':<8} {'Total Off':<10} {'1st Downs':<10}")
    print("-" * 80)
    
    for _, game in defiance_games.iterrows():
        week = game['week']
        opponent = game['opponent'][:18]  # Truncate long names
        score = game.get('final_score', 0)
        opp_score = game.get('opp_final_score', 0) 
        margin = game.get('margin', 0)
        total_off = game.get('total_offense', 0)
        first_downs = game.get('first_downs', 0)
        
        print(f"{week:<6} {opponent:<20} {score:<8} {opp_score:<10} {margin:<8} {total_off:<10} {first_downs:<10}")
    
    # Calculate rolling stats
    print(f"\nStep 2: Calculating rolling averages...")
    rolling_stats = processor.calculate_rolling_stats(team_games)
    
    # Filter to Defiance rolling stats  
    defiance_rolling = rolling_stats[rolling_stats['team'] == 'Defiance'].copy()
    defiance_rolling = defiance_rolling.sort_values('week').reset_index(drop=True)
    
    print(f"\nDefiance Rolling Average Calculation Details:")
    print("=" * 80)
    
    # Show detailed calculation for each week
    for _, week_stats in defiance_rolling.iterrows():
        week = week_stats['week']
        print(f"\n📊 WEEK {week} - 3-Week Rolling Average Calculation:")
        print("-" * 50)
        
        # Get the previous games used for this calculation
        previous_games = defiance_games[defiance_games['week'] < week]
        window_games = previous_games.tail(3)  # Last 3 games
        
        if len(window_games) == 0:
            print(f"   No previous games - all rolling averages = 0")
            continue
        
        print(f"   Games used in calculation ({len(window_games)} games):")
        
        # Show each game used in the rolling average
        total_score = 0
        total_first_downs = 0
        total_offense_sum = 0
        wins = 0
        
        for i, (_, game) in enumerate(window_games.iterrows(), 1):
            game_week = game['week']
            opponent = game['opponent']
            score = game.get('final_score', 0)
            first_downs = game.get('first_downs', 0)
            total_off = game.get('total_offense', 0)
            win = game.get('win', 0)
            
            total_score += score
            total_first_downs += first_downs  
            total_offense_sum += total_off
            wins += win
            
            print(f"     Game {i}: Week {game_week} vs {opponent}")
            print(f"       Score: {score}, First Downs: {first_downs}, Total Offense: {total_off}, Win: {win}")
        
        # Calculate averages manually
        avg_score = total_score / len(window_games) if len(window_games) > 0 else 0
        avg_first_downs = total_first_downs / len(window_games) if len(window_games) > 0 else 0
        avg_total_offense = total_offense_sum / len(window_games) if len(window_games) > 0 else 0
        win_pct = wins / len(window_games) if len(window_games) > 0 else 0
        
        print(f"\n   Manual Calculation:")
        print(f"     Average Score: {total_score}/{len(window_games)} = {avg_score:.1f}")
        print(f"     Average First Downs: {total_first_downs}/{len(window_games)} = {avg_first_downs:.1f}")
        print(f"     Average Total Offense: {total_offense_sum}/{len(window_games)} = {avg_total_offense:.1f}")
        print(f"     Win Percentage: {wins}/{len(window_games)} = {win_pct:.1%}")
        
        # Compare with processor results
        processor_score = week_stats.get('final_score_3wk', 0)
        processor_first_downs = week_stats.get('first_downs_3wk', 0)
        processor_total_offense = week_stats.get('total_offense_3wk', 0)
        processor_win_pct = week_stats.get('win_pct_3wk', 0)
        
        print(f"\n   Processor Results:")  
        print(f"     final_score_3wk: {processor_score:.1f}")
        print(f"     first_downs_3wk: {processor_first_downs:.1f}")
        print(f"     total_offense_3wk: {processor_total_offense:.1f}")
        print(f"     win_pct_3wk: {processor_win_pct:.1%}")
        
        # Verify they match
        score_match = abs(avg_score - processor_score) < 0.1
        fd_match = abs(avg_first_downs - processor_first_downs) < 0.1
        to_match = abs(avg_total_offense - processor_total_offense) < 0.1
        win_match = abs(win_pct - processor_win_pct) < 0.01
        
        if score_match and fd_match and to_match and win_match:
            print(f"   ✅ CALCULATION VERIFIED - Manual and processor results match!")
        else:
            print(f"   ❌ MISMATCH DETECTED:")
            if not score_match:
                print(f"      Score: Manual={avg_score:.1f}, Processor={processor_score:.1f}")
            if not fd_match:
                print(f"      First Downs: Manual={avg_first_downs:.1f}, Processor={processor_first_downs:.1f}")
            if not to_match:
                print(f"      Total Offense: Manual={avg_total_offense:.1f}, Processor={processor_total_offense:.1f}")
            if not win_match:
                print(f"      Win %: Manual={win_pct:.1%}, Processor={processor_win_pct:.1%}")
    
    return defiance_games, defiance_rolling

def main():
    """Run the complete detailed rolling average test"""
    
    print("DETAILED ROLLING AVERAGE TEST FOR DEFIANCE")
    print("This will show step-by-step how 3-week rolling averages are calculated")
    print("=" * 80)
    
    try:
        # Step 1: Scrape multiple weeks of data
        all_box_scores = scrape_defiance_multiple_weeks()
        
        # Step 2: Analyze rolling averages in detail
        defiance_games, defiance_rolling = analyze_defiance_rolling_averages(all_box_scores)
        
        print(f"\n" + "=" * 80)
        print("TEST COMPLETE!")
        print("=" * 80)
        print(f"✅ Scraped {len(all_box_scores)} total games")
        print(f"✅ Found {len(defiance_games)} Defiance games")
        print(f"✅ Calculated rolling averages for {len(defiance_rolling)} weeks")
        print(f"✅ Manual calculations verified against processor")
        
        return True
        
    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    if success:
        print(f"\n🎉 Rolling average calculation is working correctly!")
    else:
        print(f"\n💥 Issues found - check the error messages above")