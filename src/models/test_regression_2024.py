"""
test_2024_predictions.py - Test the trained model on 2024 season data
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

import numpy as np
import pandas as pd
import joblib
from pathlib import Path
from sklearn.metrics import accuracy_score, confusion_matrix

from src.models.data_prep import GameDataPrep
from src.models.train_logistic_baseline import LogisticRegressionBaseline

def test_on_2024():
    """Test the 2023-trained model on 2024 data."""
    
    print("\n" + "="*60)
    print("TESTING ON 2024 SEASON")
    print("="*60)
    
   # Load trained model
    model = joblib.load('models/logistic_baseline.pkl')
    
    prep = GameDataPrep()
    df = prep.fetch_game_data(start_year=2024, end_year=2024, min_week=4)
    X_test, feature_names = prep.create_feature_matrix(df)
    targets = prep.create_targets(df)
    y_test = targets['home_win']
    
    # Get the scaler from training data
    training_data = prep.prepare_full_pipeline(
        start_year=2022,
        end_year=2023,
        min_week=4,
        target_type='home_win',
        test_size=0.2,
        normalize=True
    )
    
    # Apply training scaler to 2024 data
    X_test = training_data['scaler'].transform(X_test)
    
    if len(X_test) == 0:
        print("No 2024 data found! Import weeks 1-5 first.")
        return
    
    # Make predictions
    y_pred = model.model.predict(X_test)
    y_prob = model.model.predict_proba(X_test)[:, 1]
    
    # Calculate accuracy
    accuracy = accuracy_score(y_test, y_pred)
    
    print(f"\n📊 2024 Season Results (Weeks 4-5):")
    print(f"  Games tested: {len(y_test)}")
    print(f"  Accuracy: {accuracy:.1%}")
    print(f"  Home win rate: {y_test.mean():.1%}")
    
    # Confusion matrix
    cm = confusion_matrix(y_test, y_pred)
    print(f"\n📋 Confusion Matrix:")
    print(f"                 Predicted")
    print(f"                 Away  Home")
    print(f"  Actual Away  [{cm[0,0]:4d}, {cm[0,1]:4d}]")
    print(f"  Actual Home  [{cm[1,0]:4d}, {cm[1,1]:4d}]")
    
    # Confidence analysis
    high_conf = y_prob[(y_prob > 0.8) | (y_prob < 0.2)]
    if len(high_conf) > 0:
        high_conf_mask = (y_prob > 0.8) | (y_prob < 0.2)
        high_conf_acc = ((y_pred[high_conf_mask] == y_test[high_conf_mask]).mean())
        print(f"\n🎯 High confidence games (>80%): {len(high_conf)}")
        print(f"  Accuracy on these: {high_conf_acc:.1%}")
    
    # Week by week
    df = pd.DataFrame({'week': [g.week for g in df.itertuples()], 'correct': (y_pred == y_test)})
    weekly = df.assign(correct=(y_pred == y_test)).groupby('week')['correct'].agg(['mean', 'count'])
    
    print(f"\n📅 Week-by-week accuracy:")
    for week, row in weekly.iterrows():
        print(f"  Week {week}: {row['mean']:.1%} ({int(row['count'])} games)")
    
    print("\n" + "="*60)
    if accuracy > 0.7:
        print("✅ Model generalizes well to 2024!")
    elif accuracy > 0.6:
        print("⚠️  Some degradation but still predictive")
    else:
        print("❌ Significant performance drop - investigate why")
    
    return accuracy

if __name__ == "__main__":
    test_on_2024()