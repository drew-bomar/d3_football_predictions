"""
Check what's actually in the test CSV file
"""

import pandas as pd
import os

def check_test_csv():
    """Look at the actual CSV output from the test."""
    
    csv_file = "test_calculated_fields.csv"
    
    if not os.path.exists(csv_file):
        print(f"File {csv_file} not found!")
        return
    
    df = pd.read_csv(csv_file)
    
    print(f"CSV Shape: {df.shape}")
    print(f"\nColumns in CSV:")
    print(list(df.columns))
    
    print(f"\n\nFirst row data (selected fields):")
    if len(df) > 0:
        row = df.iloc[0]
        
        fields_to_check = [
            'team', 'opponent', 'final_score', 'total_offense',
            'yards_per_play', 'turnover_diff', 'third_down_rate',
            'pass_rush_ratio', 'total_offense_plays', 'rushing_attempts',
            'passing_attempts'
        ]
        
        for field in fields_to_check:
            if field in df.columns:
                print(f"  {field}: {row[field]}")
            else:
                print(f"  {field}: NOT IN DATAFRAME")
    
    # Check if yards_per_play column exists and has values
    if 'yards_per_play' in df.columns:
        print(f"\n\nyards_per_play values in dataframe: {df['yards_per_play'].tolist()}")
    else:
        print("\n\nyards_per_play column NOT FOUND in dataframe!")

if __name__ == "__main__":
    check_test_csv()