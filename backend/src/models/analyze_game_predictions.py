"""
analyze_predictions.py - See individual game predictions vs actual results

This script shows you exactly what the model predicted for each game
and whether it was right or wrong, helping identify patterns.
"""

import numpy as np
import pandas as pd
import joblib
from pathlib import Path

# Add project root to path
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
a
from src.models.data_prep import GameDataPrep
from src.database.connection import DatabaseConnection
from train_logistic_baseline import LogisticRegressionBaseline

def get_team_names():
    """Fetch team names from database for readable output."""
    db = DatabaseConnection()
    with db.get_session() as session:
        query = """
        SELECT id, name FROM teams
        ORDER BY id
        """
        teams_df = pd.read_sql(query, session.bind)
    
    return dict(zip(teams_df['id'], teams_df['name']))

def analyze_game_predictions():
    """
    Show detailed game-by-game predictions vs actual results.
    """
    print("\n" + "="*80)
    print("GAME-BY-GAME PREDICTION ANALYSIS")
    print("="*80)
    
    # Load the trained model
    model_path = Path('models/logistic_baseline.pkl')
    if not model_path.exists():
        print("❌ Model not found! Run train_logistic_baseline.py first.")
        return
    
    model = joblib.load(model_path)
    
    # Load data (same as training)
    prep = GameDataPrep()
    data = prep.prepare_full_pipeline(
        start_year=2022,
        end_year=2023,
        min_week=4,
        target_type='home_win',
        test_size=0.2,
        normalize=True
    )
    
    # Get team names for readable output
    print("\nFetching team names...")
    team_names = get_team_names()
    
    # Get predictions
    y_prob = model.model.predict_proba(data['X_test'])[:, 1]
    y_pred = (y_prob > 0.5).astype(int)
    y_true = data['y_test']
    
    # Get game details from the test set
    test_indices = data['test_indices']
    test_games = data['df'].iloc[test_indices].copy()
    
    # Add predictions to dataframe
    test_games['predicted_prob'] = y_prob
    test_games['predicted_winner'] = ['HOME' if p > 0.5 else 'AWAY' for p in y_prob]
    test_games['actual_winner'] = ['HOME' if t == 1 else 'AWAY' for t in y_true]
    test_games['correct'] = y_pred == y_true
    test_games['confidence'] = np.where(y_prob > 0.5, y_prob, 1 - y_prob)
    
    # Add team names
    test_games['home_team'] = test_games['home_team_id'].map(team_names)
    test_games['away_team'] = test_games['away_team_id'].map(team_names)
    
    # Calculate upset predictions
    test_games['predicted_upset'] = (
        ((y_prob < 0.5) & (test_games['home_margin_3wk'] > test_games['away_margin_3wk'])) |
        ((y_prob > 0.5) & (test_games['home_margin_3wk'] < test_games['away_margin_3wk']))
    )
    
    print(f"\nTotal test games: {len(test_games)}")
    print(f"Overall accuracy: {test_games['correct'].mean():.1%}")
    
    # ============================================================
    # HIGH CONFIDENCE PREDICTIONS
    # ============================================================
    print("\n" + "="*60)
    print("HIGH CONFIDENCE PREDICTIONS (>80% confident)")
    print("="*60)
    
    high_conf = test_games[test_games['confidence'] > 0.8].sort_values('confidence', ascending=False)
    
    if len(high_conf) > 0:
        print(f"\nFound {len(high_conf)} high confidence games")
        print(f"Accuracy on these: {high_conf['correct'].mean():.1%}\n")
        
        for idx, game in high_conf.head(10).iterrows():
            result = "✅" if game['correct'] else "❌"
            print(f"{result} Week {game['week']}: {game['away_team']} @ {game['home_team']}")
            print(f"   Predicted: {game['predicted_winner']} ({game['confidence']:.1%} conf)")
            print(f"   Actual: {game['actual_winner']} (Score: {game['home_score']}-{game['away_score']})")
            if game['predicted_upset']:
                print(f"   🎯 UPSET PICK!")
            print()
    
    # ============================================================
    # BIGGEST MISSES (High confidence but wrong)
    # ============================================================
    print("\n" + "="*60)
    print("BIGGEST MISSES (Confident but wrong)")
    print("="*60)
    
    big_misses = test_games[
        (~test_games['correct']) & (test_games['confidence'] > 0.7)
    ].sort_values('confidence', ascending=False)
    
    if len(big_misses) > 0:
        print(f"\nFound {len(big_misses)} high-confidence misses\n")
        
        for idx, game in big_misses.head(5).iterrows():
            print(f"❌ Week {game['week']}: {game['away_team']} @ {game['home_team']}")
            print(f"   Predicted: {game['predicted_winner']} ({game['confidence']:.1%} conf)")
            print(f"   Actual: {game['actual_winner']} (Score: {game['home_score']}-{game['away_score']})")
            print(f"   Home 3wk margin: {game['home_margin_3wk']:.1f}")
            print(f"   Away 3wk margin: {game['away_margin_3wk']:.1f}")
            print()
    
    # ============================================================
    # UPSET PREDICTIONS
    # ============================================================
    print("\n" + "="*60)
    print("UPSET PREDICTIONS (Picked underdog based on margin)")
    print("="*60)
    
    upsets = test_games[test_games['predicted_upset']]
    
    if len(upsets) > 0:
        print(f"\nPredicted {len(upsets)} upsets")
        print(f"Accuracy on upset picks: {upsets['correct'].mean():.1%}\n")
        
        for idx, game in upsets.head(5).iterrows():
            result = "✅" if game['correct'] else "❌"
            print(f"{result} Week {game['week']}: {game['away_team']} @ {game['home_team']}")
            print(f"   Predicted: {game['predicted_winner']} ({game['confidence']:.1%} conf)")
            print(f"   Actual: {game['actual_winner']} (Score: {game['home_score']}-{game['away_score']})")
            print()
    
    # ============================================================
    # CLOSEST CALLS (Near 50/50)
    # ============================================================
    print("\n" + "="*60)
    print("CLOSEST CALLS (45-55% confidence)")
    print("="*60)
    
    close_calls = test_games[
        (test_games['predicted_prob'] >= 0.45) & 
        (test_games['predicted_prob'] <= 0.55)
    ].sort_values('predicted_prob')
    
    if len(close_calls) > 0:
        print(f"\nFound {len(close_calls)} toss-up games")
        print(f"Accuracy on these: {close_calls['correct'].mean():.1%}\n")
        
        for idx, game in close_calls.head(5).iterrows():
            result = "✅" if game['correct'] else "❌"
            prob_str = f"{game['predicted_prob']:.1%} home"
            print(f"{result} Week {game['week']}: {game['away_team']} @ {game['home_team']}")
            print(f"   Model said: {prob_str} (basically a toss-up)")
            print(f"   Actual: {game['actual_winner']} (Score: {game['home_score']}-{game['away_score']})")
            print()
    
    # ============================================================
    # PERFORMANCE BY WEEK
    # ============================================================
    print("\n" + "="*60)
    print("PERFORMANCE BY WEEK")
    print("="*60)
    
    weekly_acc = test_games.groupby('week').agg({
        'correct': 'mean',
        'confidence': 'mean',
        'game_id': 'count'
    }).rename(columns={'game_id': 'games'})
    
    print("\nWeek | Games | Accuracy | Avg Confidence")
    print("-" * 45)
    for week, row in weekly_acc.iterrows():
        print(f"{week:4.0f} | {row['games']:5.0f} | {row['correct']:7.1%} | {row['confidence']:7.1%}")
    
    # ============================================================
    # STATISTICAL SUMMARY
    # ============================================================
    print("\n" + "="*60)
    print("STATISTICAL SUMMARY")
    print("="*60)
    
    print(f"\nAccuracy by confidence level:")
    print(f"  90%+ confident: {test_games[test_games['confidence'] > 0.9]['correct'].mean():.1%} ({len(test_games[test_games['confidence'] > 0.9])} games)")
    print(f"  80-90% confident: {test_games[(test_games['confidence'] >= 0.8) & (test_games['confidence'] < 0.9)]['correct'].mean():.1%} ({len(test_games[(test_games['confidence'] >= 0.8) & (test_games['confidence'] < 0.9)])} games)")
    print(f"  70-80% confident: {test_games[(test_games['confidence'] >= 0.7) & (test_games['confidence'] < 0.8)]['correct'].mean():.1%} ({len(test_games[(test_games['confidence'] >= 0.7) & (test_games['confidence'] < 0.8)])} games)")
    print(f"  60-70% confident: {test_games[(test_games['confidence'] >= 0.6) & (test_games['confidence'] < 0.7)]['correct'].mean():.1%} ({len(test_games[(test_games['confidence'] >= 0.6) & (test_games['confidence'] < 0.7)])} games)")
    print(f"  50-60% confident: {test_games[(test_games['confidence'] >= 0.5) & (test_games['confidence'] < 0.6)]['correct'].mean():.1%} ({len(test_games[(test_games['confidence'] >= 0.5) & (test_games['confidence'] < 0.6)])} games)")
    
    print(f"\nHome vs Away predictions:")
    home_preds = test_games[test_games['predicted_winner'] == 'HOME']
    away_preds = test_games[test_games['predicted_winner'] == 'AWAY']
    print(f"  Predicted HOME: {len(home_preds)} times, {home_preds['correct'].mean():.1%} accurate")
    print(f"  Predicted AWAY: {len(away_preds)} times, {away_preds['correct'].mean():.1%} accurate")
    
    # Save detailed results
    output_file = 'test_game_predictions.csv'
    test_games[['week', 'home_team', 'away_team', 'home_score', 'away_score', 
                'predicted_winner', 'confidence', 'actual_winner', 'correct']].to_csv(output_file, index=False)
    print(f"\n💾 Detailed predictions saved to {output_file}")
    
    return test_games

if __name__ == "__main__":
    results = analyze_game_predictions()