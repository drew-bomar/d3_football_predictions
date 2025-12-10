# save as debug_features.py, run from backend/
import joblib

# Check what features the scaler expects
scaler_data = joblib.load('models/scaler.pkl')
feature_names = scaler_data['feature_names']

print(f"Total features: {len(feature_names)}")
print("\nFirst 20 features:")
for f in feature_names[:20]:
    print(f"  {f}")

print("\nELO-related features:")
for f in feature_names:
    if 'elo' in f.lower():
        print(f"  {f}")

print("\nMatchup features (no home_/away_ prefix):")
for f in feature_names:
    if not f.startswith('home_') and not f.startswith('away_'):
        print(f"  {f}")