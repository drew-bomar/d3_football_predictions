"""
save_scaler.py - One-time script to save the scaler for API use

Run once: python save_scaler.py
"""

import joblib
from pathlib import Path

# Add project root to path
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.models.data_prep import GameDataPrep

def main():
    print("Extracting scaler and feature names from training pipeline...")
    
    # Run the same pipeline used during training
    prep = GameDataPrep()
    data = prep.prepare_full_pipeline(
        start_year=2022,
        end_year=2024,
        min_week=4,
        target_type='home_win',
        test_year=2024,
        normalize=True
    )
    
    # Save scaler and feature names
    model_dir = Path('models')
    model_dir.mkdir(exist_ok=True)
    
    scaler_path = model_dir / 'scaler.pkl'
    joblib.dump({
        'scaler': data['scaler'],
        'feature_names': data['feature_names']
    }, scaler_path)
    
    print(f"✓ Saved scaler and {len(data['feature_names'])} feature names to {scaler_path}")
    
    # Verify it loads correctly
    loaded = joblib.load(scaler_path)
    print(f"✓ Verified: scaler type = {type(loaded['scaler']).__name__}")
    print(f"✓ Verified: {len(loaded['feature_names'])} features")

if __name__ == "__main__":
    main()