"""
verify_rolling_stats.py
Verify that rolling stats calculations are correct, especially previous season usage
"""
from src.database.connection import DatabaseConnection
from sqlalchemy import text

def verify_previous_season_usage():
    """Check how many teams are using previous season data in early weeks."""
    db = DatabaseConnection()
    
    print("\n" + "="*60)
    print("VERIFYING PREVIOUS SEASON USAGE")
    print("="*60)
    
    with db.get_session() as session:
        # Check 2022 Week 1 - should use 2021 games
        week1_stats = session.execute(text("""
            SELECT 
                COUNT(*) as total_teams,
                SUM(CASE WHEN prev_season_games_in_3wk > 0 THEN 1 ELSE 0 END) as using_prev,
                AVG(prev_season_games_in_3wk) as avg_prev_games,
                AVG(games_in_season) as avg_current_games
            FROM team_rolling_stats
            WHERE year = 2022 AND week = 1
        """)).fetchone()
        
        print(f"\n2022 Week 1 (should use 2021 games):")
        print(f"  Total teams: {week1_stats[0]}")
        print(f"  Using previous season: {week1_stats[1]} ({week1_stats[1]/week1_stats[0]*100:.1f}%)")
        print(f"  Avg previous season games used: {week1_stats[2]:.2f}")
        print(f"  Avg current season games: {week1_stats[3]:.2f} (should be 0)")
        
        # Check progression through early weeks
        print(f"\nProgression of previous season usage:")
        print("-" * 50)
        print(f"{'Week':<6} {'Teams':<8} {'Using Prev':<12} {'Avg Prev':<10} {'Avg Current'}")
        print("-" * 50)
        
        for week in range(1, 6):
            stats = session.execute(text("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN prev_season_games_in_3wk > 0 THEN 1 ELSE 0 END) as using_prev,
                    AVG(prev_season_games_in_3wk) as avg_prev,
                    AVG(games_in_season) as avg_current
                FROM team_rolling_stats
                WHERE year = 2022 AND week = :week
            """), {'week': week}).fetchone()
            
            pct = stats[1]/stats[0]*100 if stats[0] > 0 else 0
            print(f"{week:<6} {stats[0]:<8} {stats[1]:<4} ({pct:>4.1f}%) {stats[2]:>8.2f} {stats[3]:>10.2f}")

def verify_specific_team():
    """Deep dive into a specific team's rolling stats progression."""
    db = DatabaseConnection()
    
    print("\n" + "="*60)
    print("SPECIFIC TEAM VERIFICATION")
    print("="*60)
    
    with db.get_session() as session:
        # Find a team that played in both 2021 end and 2022 start
        team = session.execute(text("""
            SELECT t.id, t.name
            FROM teams t
            WHERE EXISTS (
                SELECT 1 FROM games g 
                WHERE (g.home_team_id = t.id OR g.away_team_id = t.id)
                AND g.year = 2021 AND g.week >= 10
            )
            AND EXISTS (
                SELECT 1 FROM games g
                WHERE (g.home_team_id = t.id OR g.away_team_id = t.id)
                AND g.year = 2022 AND g.week <= 5
            )
            LIMIT 1
        """)).fetchone()
        
        if not team:
            print("No suitable team found")
            return
        
        team_id, team_name = team
        print(f"\nAnalyzing: {team_name} (ID: {team_id})")
        
        # Get their actual games from end of 2021
        print(f"\n{team_name}'s last games of 2021:")
        last_2021_games = session.execute(text("""
            SELECT 
                g.week,
                g.game_date,
                CASE WHEN g.home_team_id = :team_id THEN g.home_score ELSE g.away_score END as score,
                CASE WHEN g.home_team_id = :team_id THEN g.away_score ELSE g.home_score END as opp_score
            FROM games g
            WHERE (g.home_team_id = :team_id OR g.away_team_id = :team_id)
            AND g.year = 2021
            ORDER BY g.week DESC
            LIMIT 3
        """), {'team_id': team_id}).fetchall()
        
        for week, date, score, opp_score in last_2021_games:
            print(f"  Week {week}: {score}-{opp_score} (on {date})")
        
        # Calculate what their stats SHOULD be for 2022 Week 1
        if last_2021_games:
            manual_ppg = sum(row[2] for row in last_2021_games) / len(last_2021_games)
            manual_papg = sum(row[3] for row in last_2021_games) / len(last_2021_games)
            manual_margin = manual_ppg - manual_papg
            
            # Apply decay weight (0.7)
            weighted_ppg = (sum(row[2] * 0.7 for row in last_2021_games)) / (len(last_2021_games) * 0.7)
            
            print(f"\nManual calculation for 2022 Week 1:")
            print(f"  Simple average PPG: {manual_ppg:.1f}")
            print(f"  Weighted PPG (0.7 decay): {weighted_ppg:.1f}")
            print(f"  Margin: {manual_margin:.1f}")
        
        # Get their actual rolling stats for early 2022
        print(f"\n{team_name}'s rolling stats in 2022:")
        print("-" * 70)
        print(f"{'Week':<6} {'PPG(3wk)':<10} {'Margin':<10} {'Prev Games':<12} {'Current Games'}")
        print("-" * 70)
        
        rolling_stats = session.execute(text("""
            SELECT 
                week,
                ppg_3wk,
                margin_3wk,
                prev_season_games_in_3wk,
                games_in_season
            FROM team_rolling_stats
            WHERE team_id = :team_id
            AND year = 2022
            AND week <= 5
            ORDER BY week
        """), {'team_id': team_id}).fetchall()
        
        for week, ppg, margin, prev_games, current_games in rolling_stats:
            ppg_str = f"{ppg:.1f}" if ppg else "N/A"
            margin_str = f"{margin:.1f}" if margin else "N/A"
            print(f"{week:<6} {ppg_str:<10} {margin_str:<10} {prev_games:<12} {current_games}")

def verify_calculation_accuracy():
    """Spot check some calculations against raw data."""
    db = DatabaseConnection()
    
    print("\n" + "="*60)
    print("CALCULATION ACCURACY CHECK")
    print("="*60)
    
    with db.get_session() as session:
        # Get a random team's Week 5 stats (should be all current season)
        sample = session.execute(text("""
            SELECT 
                trs.team_id,
                t.name,
                trs.ppg_3wk,
                trs.margin_3wk,
                trs.games_in_3wk
            FROM team_rolling_stats trs
            JOIN teams t ON trs.team_id = t.id
            WHERE trs.year = 2022 
            AND trs.week = 5
            AND trs.games_in_3wk = 3
            LIMIT 1
        """)).fetchone()
        
        if sample:
            team_id, team_name, ppg_3wk, margin_3wk, games_3wk = sample
            print(f"\nVerifying {team_name}'s Week 5 stats:")
            print(f"  Stored PPG (3wk): {ppg_3wk:.1f}")
            print(f"  Stored Margin (3wk): {margin_3wk:.1f}")
            
            # Manually calculate from raw game data
            raw_games = session.execute(text("""
                SELECT 
                    g.week,
                    CASE WHEN g.home_team_id = :team_id THEN g.home_score ELSE g.away_score END as score,
                    CASE WHEN g.home_team_id = :team_id THEN g.away_score ELSE g.home_score END as opp_score
                FROM games g
                WHERE (g.home_team_id = :team_id OR g.away_team_id = :team_id)
                AND g.year = 2022
                AND g.week >= 2 AND g.week <= 4
                ORDER BY g.week
            """), {'team_id': team_id}).fetchall()
            
            print(f"\nActual games (weeks 2-4):")
            total_scored = 0
            total_allowed = 0
            for week, score, opp_score in raw_games:
                print(f"  Week {week}: {score}-{opp_score}")
                total_scored += score
                total_allowed += opp_score
            
            if raw_games:
                calc_ppg = total_scored / len(raw_games)
                calc_papg = total_allowed / len(raw_games)
                calc_margin = calc_ppg - calc_papg
                
                print(f"\nManual calculation:")
                print(f"  PPG: {calc_ppg:.1f} (stored: {ppg_3wk:.1f}) {'✅' if abs(calc_ppg - ppg_3wk) < 0.5 else '❌'}")
                print(f"  Margin: {calc_margin:.1f} (stored: {margin_3wk:.1f}) {'✅' if abs(calc_margin - margin_3wk) < 0.5 else '❌'}")

def main():
    """Run all verification checks."""
    verify_previous_season_usage()
    verify_specific_team()
    verify_calculation_accuracy()
    
    print("\n" + "="*60)
    print("VERIFICATION COMPLETE")
    print("="*60)
    print("\nIf percentages increase from Week 1→3 then decrease Week 4→5,")
    print("the previous season decay is working correctly!")

if __name__ == "__main__":
    main()