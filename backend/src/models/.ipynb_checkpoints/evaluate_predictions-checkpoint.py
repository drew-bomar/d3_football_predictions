# evaluate_predictions.py
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))


from src.database.connection import DatabaseConnection
from sqlalchemy import text
import pandas as pd

def evaluate_week(year: int, week: int, model_name: str = 'logistic_baseline'):
    db = DatabaseConnection()
    
    with db.get_session() as session:
        # Update predictions with results (same as before)
        update_query = text("""
            UPDATE predictions p
            SET actual_winner_id = CASE 
                    WHEN g.home_score > g.away_score THEN p.home_team_id 
                    ELSE p.away_team_id 
                END,
                actual_home_score = g.home_score,
                actual_away_score = g.away_score,
                was_correct = (
                    (p.predicted_winner_id = p.home_team_id AND g.home_score > g.away_score) OR
                    (p.predicted_winner_id = p.away_team_id AND g.away_score > g.home_score)
                ),
                evaluated_at = NOW()
            FROM games g
            WHERE p.year = g.year AND p.week = g.week
                AND p.home_team_id = g.home_team_id
                AND p.away_team_id = g.away_team_id
                AND p.year = :year AND p.week = :week
                AND p.model_name = :model_name
                AND g.home_score IS NOT NULL
        """)
        session.execute(update_query, {'year': year, 'week': week, 'model_name': model_name})
        session.commit()
        
        # Get detailed results
        results = session.execute(text("""
            SELECT 
                ht.name as home_team,
                at.name as away_team,
                wt.name as predicted_winner,
                awt.name as actual_winner,
                p.confidence,
                p.home_win_prob,
                p.was_correct,
                p.actual_home_score,
                p.actual_away_score
            FROM predictions p
            JOIN teams ht ON p.home_team_id = ht.id
            JOIN teams at ON p.away_team_id = at.id
            JOIN teams wt ON p.predicted_winner_id = wt.id
            LEFT JOIN teams awt ON p.actual_winner_id = awt.id
            WHERE p.year = :year AND p.week = :week 
                AND p.model_name = :model_name
                AND p.was_correct IS NOT NULL
            ORDER BY p.confidence DESC
        """), {'year': year, 'week': week, 'model_name': model_name}).fetchall()
        
        df = pd.DataFrame(results, columns=['home_team', 'away_team', 'predicted_winner', 
                                           'actual_winner', 'confidence', 'home_win_prob',
                                           'was_correct', 'home_score', 'away_score'])
    
    print(f"\n{'='*60}")
    print(f"EVALUATION: {year} Week {week} - {model_name}")
    print('='*60)
    
    # Overall accuracy
    accuracy = df['was_correct'].mean()
    print(f"\nOverall: {df['was_correct'].sum()}/{len(df)} ({accuracy:.1%})")
    
    # By confidence bucket
    print("\nAccuracy by Confidence:")
    for min_c, max_c, label in [(0.9, 1.0, "90-100%"), (0.8, 0.9, "80-90%"), 
                                 (0.7, 0.8, "70-80%"), (0.6, 0.7, "60-70%")]:
        bucket = df[(df['confidence'] >= min_c) & (df['confidence'] < max_c)]
        if len(bucket) > 0:
            print(f"  {label}: {bucket['was_correct'].mean():.1%} ({len(bucket)} games)")
    
    # Biggest misses
    print(f"\nBiggest Misses:")
    misses = df[~df['was_correct']].head(5)
    for _, row in misses.iterrows():
        print(f"  {row['away_team']} @ {row['home_team']}")
        print(f"    Predicted: {row['predicted_winner']} ({row['confidence']:.1%})")
        print(f"    Actual: {row['actual_winner']} ({row['home_score']}-{row['away_score']})")
    
    return df

if __name__ == "__main__":
    year = int(input("Year: "))
    week = int(input("Week: "))
    df = evaluate_week(year, week)