"""
db_inspector.py - Enhanced version with rolling stats and predictions support
Provides comprehensive database state information and cleanup options
"""
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.database.connection import DatabaseConnection
from sqlalchemy import text
from datetime import datetime

def check_database_state():
    """Check what data currently exists in our database including rolling stats and predictions."""
    db = DatabaseConnection()
    
    with db.get_session() as session:
        # Get basic counts
        teams_count = session.execute(text("SELECT COUNT(*) FROM teams")).scalar()
        games_count = session.execute(text("SELECT COUNT(*) FROM games")).scalar()
        stats_count = session.execute(text("SELECT COUNT(*) FROM team_game_stats")).scalar()
        
        # Check if rolling stats table exists and get count
        try:
            rolling_count = session.execute(text("SELECT COUNT(*) FROM team_rolling_stats")).scalar()
            rolling_exists = True
        except:
            rolling_count = 0
            rolling_exists = False
        
        # Check if predictions table exists and get count
        try:
            predictions_count = session.execute(text("SELECT COUNT(*) FROM predictions")).scalar()
            predictions_exists = True
        except:
            predictions_count = 0
            predictions_exists = False
        
        print("=" * 60)
        print("DATABASE STATE CHECK")
        print("=" * 60)
        print(f"Teams: {teams_count}")
        print(f"Games: {games_count}")
        print(f"Game Stats: {stats_count}")
        print(f"Rolling Stats: {rolling_count} {'✅' if rolling_exists else '❌ (table not created)'}")
        print(f"Predictions: {predictions_count} {'✅' if predictions_exists else '❌ (table not created)'}")
        
        # Games by year/week summary
        if games_count > 0:
            games_by_year = session.execute(text("""
                SELECT year, COUNT(DISTINCT week) as weeks, COUNT(*) as games
                FROM games
                GROUP BY year
                ORDER BY year
            """)).fetchall()
            
            print("\n📊 GAMES BY YEAR:")
            print("-" * 40)
            for year, weeks, games in games_by_year:
                print(f"  {year}: {weeks} weeks, {games} games")
            
            # Detailed week breakdown for each year
            print("\n📅 DETAILED WEEK BREAKDOWN:")
            print("-" * 40)
            
            for year, _, _ in games_by_year:
                weeks_detail = session.execute(text("""
                    SELECT week, COUNT(*) as game_count
                    FROM games
                    WHERE year = :year
                    GROUP BY week
                    ORDER BY week
                """), {'year': year}).fetchall()
                
                week_list = [f"W{w}:{c}" for w, c in weeks_detail]
                print(f"  {year}: {', '.join(week_list)}")
        
        # Predictions analysis
        if predictions_exists and predictions_count > 0:
            print("\n🎯 PREDICTIONS ANALYSIS:")
            print("-" * 40)
            
            # Overall stats
            pred_overall = session.execute(text("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(CASE WHEN was_correct IS NOT NULL THEN 1 END) as evaluated,
                    COUNT(CASE WHEN was_correct = TRUE THEN 1 END) as correct,
                    ROUND(AVG(CASE WHEN was_correct IS NOT NULL THEN 
                        CASE WHEN was_correct THEN 1.0 ELSE 0.0 END 
                    END) * 100, 1) as accuracy
                FROM predictions
            """)).fetchone()
            
            print(f"  Overall: {pred_overall[0]} predictions")
            print(f"    Evaluated: {pred_overall[1]}")
            if pred_overall[3]:
                print(f"    Accuracy: {pred_overall[3]}%")
            
            # By year/week
            pred_by_week = session.execute(text("""
                SELECT year, week,
                       COUNT(*) as total,
                       COUNT(CASE WHEN was_correct IS NOT NULL THEN 1 END) as evaluated,
                       ROUND(AVG(CASE WHEN was_correct IS NOT NULL THEN 
                           CASE WHEN was_correct THEN 1.0 ELSE 0.0 END 
                       END) * 100, 1) as accuracy
                FROM predictions
                GROUP BY year, week
                ORDER BY year, week
            """)).fetchall()
            
            print("\n  By Week:")
            for year, week, total, evaluated, accuracy in pred_by_week:
                eval_str = f"{evaluated} eval" if evaluated > 0 else "not eval"
                acc_str = f", {accuracy}% acc" if accuracy else ""
                print(f"    {year} W{week}: {total} preds ({eval_str}{acc_str})")
            
            # Sample predictions
            sample_preds = session.execute(text("""
                SELECT p.year, p.week,
                       COALESCE(ht.short_name, ht.name) as home_team,
                       COALESCE(at.short_name, at.name) as away_team,
                       COALESCE(pw.short_name, pw.name) as predicted_winner,
                       p.home_win_prob,
                       p.was_correct,
                       p.actual_home_score,
                       p.actual_away_score
                FROM predictions p
                JOIN teams ht ON p.home_team_id = ht.id
                JOIN teams at ON p.away_team_id = at.id
                JOIN teams pw ON p.predicted_winner_id = pw.id
                ORDER BY p.created_at DESC
                LIMIT 3
            """)).fetchall()
            
            if sample_preds:
                print("\n  Sample Predictions (recent):")
                for pred in sample_preds:
                    prob = pred[5] * 100 if pred[5] else 0
                    winner_name = pred[4]
                    score = f"{pred[7]}-{pred[8]}" if pred[7] is not None else "Not played"
                    correct = "✅" if pred[6] else ("❌" if pred[6] is not None else "⏳")
                    print(f"    {pred[0]} W{pred[1]}: {pred[2]} vs {pred[3]}")
                    print(f"      Predicted: {winner_name} ({prob:.1f}%) | {score} {correct}")
        
        # Rolling stats analysis
        if rolling_exists and rolling_count > 0:
            print("\n📈 ROLLING STATS ANALYSIS:")
            print("-" * 40)
            
            # Coverage by year
            rolling_coverage = session.execute(text("""
                SELECT 
                    year,
                    COUNT(DISTINCT week) as weeks_covered,
                    COUNT(DISTINCT team_id) as teams_covered,
                    COUNT(*) as total_records,
                    AVG(games_in_season) as avg_games_used
                FROM team_rolling_stats
                GROUP BY year
                ORDER BY year
            """)).fetchall()
            
            for year, weeks, teams, records, avg_games in rolling_coverage:
                print(f"  {year}:")
                print(f"    Weeks: {weeks}, Teams: {teams}")
                print(f"    Records: {records}, Avg games used: {avg_games:.1f}")
            
            # Data quality check
            quality = session.execute(text("""
                SELECT 
                    SUM(CASE WHEN ppg_3wk IS NULL THEN 1 ELSE 0 END) as null_ppg,
                    SUM(CASE WHEN margin_3wk IS NULL THEN 1 ELSE 0 END) as null_margin,
                    SUM(CASE WHEN prev_season_games_in_3wk > 0 THEN 1 ELSE 0 END) as used_prev_season,
                    COUNT(*) as total
                FROM team_rolling_stats
            """)).fetchone()
            
            print(f"\n  Data Quality:")
            print(f"    NULL PPG (3wk): {quality[0]}/{quality[3]} ({quality[0]/quality[3]*100:.1f}%)")
            print(f"    NULL Margin: {quality[1]}/{quality[3]} ({quality[1]/quality[3]*100:.1f}%)")
            print(f"    Used prev season: {quality[2]} records")
        
        # Sample teams
        print("\n🏈 SAMPLE TEAMS:")
        print("-" * 40)
        teams_sample = session.execute(text("""
            SELECT 
                t.name,
                t.slug,
                COUNT(DISTINCT g.id) as games_played,
                COUNT(DISTINCT trs.id) as rolling_records
            FROM teams t
            LEFT JOIN games g ON t.id IN (g.home_team_id, g.away_team_id)
            LEFT JOIN team_rolling_stats trs ON t.id = trs.team_id
            GROUP BY t.id, t.name, t.slug
            ORDER BY games_played DESC
            LIMIT 5
        """)).fetchall()
        
        for name, slug, games, rolling in teams_sample:
            print(f"  {name[:25]:<25} Games: {games:>3}, Rolling: {rolling:>3}")
        
        # Check for data issues
        print("\n⚠️  DATA INTEGRITY CHECKS:")
        print("-" * 40)
        
        # Orphaned stats
        orphan_stats = session.execute(text("""
            SELECT COUNT(*) 
            FROM team_game_stats tgs
            LEFT JOIN games g ON tgs.game_id = g.id
            WHERE g.id IS NULL
        """)).scalar()
        
        # Games without stats
        games_no_stats = session.execute(text("""
            SELECT COUNT(*)
            FROM games g
            LEFT JOIN team_game_stats tgs ON g.id = tgs.game_id
            WHERE tgs.id IS NULL
        """)).scalar()
        
        # Rolling stats without games
        if rolling_exists:
            orphan_rolling = session.execute(text("""
                SELECT COUNT(*)
                FROM team_rolling_stats trs
                LEFT JOIN games g ON trs.game_id = g.id
                WHERE g.id IS NULL
            """)).scalar()
        else:
            orphan_rolling = 0
        
        # Unevaluated predictions
        if predictions_exists:
            unevaluated_preds = session.execute(text("""
                SELECT COUNT(*)
                FROM predictions
                WHERE was_correct IS NULL
            """)).scalar()
        else:
            unevaluated_preds = 0
        
        issues = []
        if orphan_stats > 0:
            issues.append(f"  ❌ {orphan_stats} orphaned game stats")
        if games_no_stats > 0:
            issues.append(f"  ❌ {games_no_stats} games missing stats")
        if orphan_rolling > 0:
            issues.append(f"  ❌ {orphan_rolling} orphaned rolling stats")
        if unevaluated_preds > 0:
            issues.append(f"  ⚠️  {unevaluated_preds} predictions not evaluated")
        
        if issues:
            for issue in issues:
                print(issue)
        else:
            print("  ✅ No data integrity issues found")
        
        return {
            'teams': teams_count,
            'games': games_count,
            'stats': stats_count,
            'rolling': rolling_count,
            'predictions': predictions_count,
            'has_data': games_count > 0,
            'has_rolling': rolling_exists and rolling_count > 0,
            'has_predictions': predictions_exists and predictions_count > 0
        }

def clean_database(year=None, week=None, dry_run=True):
    """Clean database with options for specific data removal."""
    db = DatabaseConnection()
    
    with db.get_session() as session:
        if year and week:
            # Specific week deletion
            games = session.execute(text("""
                SELECT COUNT(*) FROM games 
                WHERE year = :year AND week = :week
            """), {'year': year, 'week': week}).scalar()
            
            stats = session.execute(text("""
                SELECT COUNT(*) FROM team_game_stats tgs
                JOIN games g ON tgs.game_id = g.id
                WHERE g.year = :year AND g.week = :week
            """), {'year': year, 'week': week}).scalar()
            
            try:
                rolling = session.execute(text("""
                    SELECT COUNT(*) FROM team_rolling_stats trs
                    JOIN games g ON trs.game_id = g.id
                    WHERE g.year = :year AND g.week = :week
                """), {'year': year, 'week': week}).scalar()
            except:
                rolling = 0
            
            try:
                predictions = session.execute(text("""
                    SELECT COUNT(*) FROM predictions
                    WHERE year = :year AND week = :week
                """), {'year': year, 'week': week}).scalar()
            except:
                predictions = 0
            
            print(f"\nDeleting {year} Week {week} would remove:")
            print(f"  - {games} games")
            print(f"  - {stats} stat records")
            print(f"  - {rolling} rolling stat records")
            print(f"  - {predictions} predictions")
            
            if not dry_run and games > 0:
                confirm = input(f"Delete {year} Week {week}? (yes/no): ")
                if confirm.lower() == 'yes':
                    # Order matters due to foreign keys
                    try:
                        session.execute(text("""
                            DELETE FROM predictions
                            WHERE year = :year AND week = :week
                        """), {'year': year, 'week': week})
                    except:
                        pass
                    
                    session.execute(text("""
                        DELETE FROM team_rolling_stats 
                        WHERE game_id IN (
                            SELECT id FROM games WHERE year = :year AND week = :week
                        )
                    """), {'year': year, 'week': week})
                    
                    session.execute(text("""
                        DELETE FROM team_game_stats 
                        WHERE game_id IN (
                            SELECT id FROM games WHERE year = :year AND week = :week
                        )
                    """), {'year': year, 'week': week})
                    
                    session.execute(text("""
                        DELETE FROM games WHERE year = :year AND week = :week
                    """), {'year': year, 'week': week})
                    
                    session.commit()
                    print(f"✅ Deleted {year} Week {week}")
        
        elif year and not week:
            # Full year deletion
            games = session.execute(text("""
                SELECT COUNT(*) FROM games WHERE year = :year
            """), {'year': year}).scalar()
            
            try:
                predictions = session.execute(text("""
                    SELECT COUNT(*) FROM predictions WHERE year = :year
                """), {'year': year}).scalar()
            except:
                predictions = 0
            
            print(f"\nDeleting entire year {year} would remove:")
            print(f"  - {games} games (and associated stats)")
            print(f"  - {predictions} predictions")
            
            if not dry_run and games > 0:
                confirm = input(f"Delete ALL of {year}? (yes/no): ")
                if confirm.lower() == 'yes':
                    try:
                        session.execute(text("""
                            DELETE FROM predictions WHERE year = :year
                        """), {'year': year})
                    except:
                        pass
                    
                    session.execute(text("""
                        DELETE FROM team_rolling_stats 
                        WHERE year = :year
                    """), {'year': year})
                    
                    session.execute(text("""
                        DELETE FROM team_game_stats 
                        WHERE game_id IN (SELECT id FROM games WHERE year = :year)
                    """), {'year': year})
                    
                    session.execute(text("""
                        DELETE FROM games WHERE year = :year
                    """), {'year': year})
                    
                    session.commit()
                    print(f"✅ Deleted year {year}")
        
        else:
            # Full cleanup
            games = session.execute(text("SELECT COUNT(*) FROM games")).scalar()
            stats = session.execute(text("SELECT COUNT(*) FROM team_game_stats")).scalar()
            teams = session.execute(text("SELECT COUNT(*) FROM teams")).scalar()
            
            try:
                rolling = session.execute(text("SELECT COUNT(*) FROM team_rolling_stats")).scalar()
            except:
                rolling = 0
            
            try:
                predictions = session.execute(text("SELECT COUNT(*) FROM predictions")).scalar()
            except:
                predictions = 0
            
            print(f"\nFull database cleanup would delete:")
            print(f"  - {games} games")
            print(f"  - {stats} game stat records")
            print(f"  - {rolling} rolling stat records")
            print(f"  - {predictions} predictions")
            print(f"  - {teams} teams")
            
            if not dry_run:
                print("\n⚠️  WARNING: This will delete ALL data!")
                confirm = input("Type 'DELETE ALL' to confirm: ")
                if confirm == "DELETE ALL":
                    # Order matters due to foreign keys
                    try:
                        session.execute(text("DELETE FROM predictions"))
                    except:
                        pass
                    
                    try:
                        session.execute(text("DELETE FROM team_rolling_stats"))
                    except:
                        pass  # Table might not exist
                    
                    session.execute(text("DELETE FROM team_game_stats"))
                    session.execute(text("DELETE FROM games"))
                    session.execute(text("DELETE FROM teams"))
                    session.commit()
                    print("✅ Database cleared!")
                else:
                    print("Cancelled")

def clean_rolling_stats_only():
    """Clean only the rolling stats table."""
    db = DatabaseConnection()
    
    with db.get_session() as session:
        try:
            count = session.execute(text("SELECT COUNT(*) FROM team_rolling_stats")).scalar()
            
            if count > 0:
                print(f"\nThis will delete {count} rolling stat records")
                confirm = input("Delete all rolling stats? (yes/no): ")
                
                if confirm.lower() == 'yes':
                    session.execute(text("DELETE FROM team_rolling_stats"))
                    session.commit()
                    print("✅ Rolling stats cleared!")
            else:
                print("No rolling stats to delete")
        except Exception as e:
            print(f"Error: {e}")

def clean_predictions_only():
    """Clean only the predictions table."""
    db = DatabaseConnection()
    
    with db.get_session() as session:
        try:
            count = session.execute(text("SELECT COUNT(*) FROM predictions")).scalar()
            
            if count > 0:
                print(f"\nThis will delete {count} prediction records")
                confirm = input("Delete all predictions? (yes/no): ")
                
                if confirm.lower() == 'yes':
                    session.execute(text("DELETE FROM predictions"))
                    session.commit()
                    print("✅ Predictions cleared!")
            else:
                print("No predictions to delete")
        except Exception as e:
            print(f"Error: {e}")

def show_menu():
    """Display interactive menu for database management."""
    print("\n" + "=" * 60)
    print("DATABASE MANAGEMENT MENU")
    print("=" * 60)
    print("1. Check current state")
    print("2. Clean specific week (e.g., 2022 Week 5)")
    print("3. Clean entire year")
    print("4. Clean ONLY rolling stats")
    print("5. Clean ONLY predictions")
    print("6. Clean EVERYTHING (nuclear option)")
    print("7. Exit")
    print("-" * 60)
    
    return input("Your choice (1-7): ")

def main():
    """Main entry point with interactive menu."""
    while True:
        choice = show_menu()
        
        if choice == '1':
            check_database_state()
            
        elif choice == '2':
            year = input("Enter year: ")
            week = input("Enter week: ")
            try:
                clean_database(int(year), int(week), dry_run=False)
            except ValueError:
                print("Invalid year or week")
                
        elif choice == '3':
            year = input("Enter year to delete: ")
            try:
                clean_database(int(year), dry_run=False)
            except ValueError:
                print("Invalid year")
                
        elif choice == '4':
            clean_rolling_stats_only()
            
        elif choice == '5':
            clean_predictions_only()
            
        elif choice == '6':
            clean_database(dry_run=False)
            
        elif choice == '7':
            print("Goodbye!")
            break
            
        else:
            print("Invalid choice")
        
        if choice != '7':
            input("\nPress Enter to continue...")

if __name__ == "__main__":
    # Check state first
    state = check_database_state()
    
    # Ask if user wants interactive menu
    if input("\nEnter interactive menu? (y/n): ").lower() == 'y':
        main()
    else:
        print("\nRun 'python db_inspector.py' again for menu")