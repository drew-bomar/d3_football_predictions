# D3 Football Predictions Project

## Overall Goals

- Build a predictive model for D3 football game outcomes
- Develop a robust data pipeline for NCAA game data ingestion
- Create interpretable features that capture team performance and trends
- Establish baseline models and progressively improve prediction accuracy
- Explore advanced ML models (XGBoost, Deep Forest) for better performance

## Completed Key Features

### Data Pipeline
- **NCAA API Integration** (src/pipeline/ncaa_api_client.py): Fetches game schedules and detailed statistics from NCAA API
- **Stats Translation Layer** (src/pipeline/stats_translator.py): Translates NCAA API format to database schema
- **Game Importer** (src/pipeline/game_importer.py): Imports games with duplicate detection and validation
- **Simple Pipeline** (src/pipeline/simple_pipeline.py): Orchestrates full import workflow with progress tracking
- **Historical Importer** (src/pipeline/historical_importer.py): Batch imports for historical seasons

### Database Schema
- PostgreSQL database with models for:
  - Teams (src/database/teams_model.py)
  - Games (src/database/games_model.py)
  - Team game statistics (src/database/team_game_stats_model.py)
  - Rolling statistics (src/database/team_rolling_stats_model.py)
  - Predictions (src/database/predictions_model.py)
  - ELO ratings (game_elos table)

### Feature Engineering
- **Rolling Statistics Calculator** (src/features/rolling_stats_calculator.py):
  - 3-week and 5-week rolling windows
  - Previous season data with decay weights (0.7) for early weeks
  - Offensive metrics (PPG, yards per play, total yards)
  - Defensive metrics (opponent YPP, opponent total yards)
  - Efficiency metrics (3rd down %, turnover differential)
  - Trend/momentum indicators (PPG trend, margin trend, win streaks)
  - Consistency metrics (standard deviation)
  - Strength of schedule placeholders

- **ELO Rating System** (src/features/elo_calculator.py):
  - Classic ELO implementation for D3 football
  - Home field advantage adjustment (65 ELO points)
  - Margin of victory multiplier with diminishing returns
  - Season-to-season regression (75% carryover)
  - Historical ELO calculation for all games (2022-2024)
  - ELO features integrated into rolling stats:
    - Current ELO rating
    - ELO change over 3/5 week windows (momentum)
    - Average opponent ELO (true strength of schedule)

### Machine Learning Models
- **Logistic Regression Baseline** (src/models/train_logistic_baseline.py):
  - Trained on 2022-2023 data, tested on 2024
  - Probability calibration using isotonic regression (5-fold CV)
  - Feature importance analysis and interpretability
  - Visualization suite (ROC curves, calibration plots, feature importance)
  - Model serialization for predictions
  - Saved models in models/ directory

### Data Preparation
- **Data Prep Pipeline** (src/models/data_prep.py):
  - Feature construction from rolling stats
  - Home/away matchup differentials
  - Train/test splitting by year
  - Feature normalization
  - Metadata tracking

### Testing & Validation
- Weekly prediction testing framework
- Model evaluation tools (src/models/evaluate_predictions.py)
- Analysis tools (src/models/analyze_game_predictions.py)

### Utilities
- **Database Inspector** (scripts/db_inspector.py): Inspect database state and verify data
- **Rolling Stats Verifier** (scripts/verify_rolling_stats.py): Validate rolling statistics calculations

## Project Structure (DRY - Single Source of Truth)

```
d3-football-predictions/
├── src/
│   ├── database/          # Database models and connections
│   ├── pipeline/          # Data import (NCAA API → Database)
│   ├── features/          # Feature engineering (rolling stats, ELO)
│   └── models/            # ML training, predictions, evaluation
├── scripts/               # Utility scripts for inspection/validation
├── models/                # Trained model artifacts (.pkl files)
├── notebooks/             # Jupyter notebooks for exploration
└── tests/                 # Unit tests
```

### Canonical Locations for Each Function
1. **Game Import** → `src/pipeline/simple_pipeline.py` + `game_importer.py`
2. **Database Operations** → `src/database/*_model.py`
3. **Rolling Stats Calculation** → `src/features/rolling_stats_calculator.py`
4. **ELO Calculation** → `src/features/elo_calculator.py`
5. **Model Training** → `src/models/train_logistic_baseline.py`
6. **Generate Predictions** → `src/models/weekly_predictor.py`
7. **Evaluate Predictions** → `src/models/evaluate_predictions.py`
8. **Model Analysis** → `src/models/analyze_game_predictions.py`

## Current Status (Updated: 2025-10-08)

### ✅ Recently Completed
- **Repository Cleanup**: Removed 45+ redundant files, organized utilities into scripts/
- **Generalized Prediction Script**: Created `scripts/predict_week.py` that can generate predictions for any week
  - Supports backtesting mode (`--backtest`) for model evaluation
  - Automatic import checking and data validation
  - Saves predictions to database with proper metadata
- **Unified Evaluation Script**: Merged evaluation tools into `src/models/evaluate_predictions.py`
  - Confidence bucket analysis (shows accuracy by confidence ranges)
  - Detailed reporting with biggest misses and best predictions
  - Support for evaluating single weeks or all predictions
- **2025 Week 5 & 6 Predictions**: Generated and evaluated predictions for weeks 5-6
  - Week 5: 49 predictions (57.1% accuracy)
  - Week 6: 98 predictions (63.3% accuracy)
  - Overall: 147 predictions (61.2% accuracy)

### 🚨 Critical Issue Identified: Model Calibration Drift

**Problem**: Model shows severe overconfidence on 2025 predictions
- 90-100% confidence predictions: Only **76.5%** accurate (expected 97%) - **20.5% gap**
- 80-90% confidence predictions: Only **64.4%** accurate (expected 87%) - **22.6% gap**
- Model was trained on 2022-2023 data, applied to 2025 (2-year gap)
- Model IS calibrated (isotonic regression, 5-fold CV) but calibration is outdated

**Root Causes Identified**:
1. **Temporal drift**: 2-year gap between training data (2022-2023) and application (2025)
2. **Distributional shift**: Home field advantage weakened in 2025 (margin: 3.5 → 1.6 points)
3. **Small sample size**: Only 147 predictions, but calibration gap is too large to be just variance

**Analysis Performed**:
- ✅ Checked model was actually calibrated (it is - using isotonic regression)
- ✅ Verified calibration worked on 2023 test set (90%+ predictions hit 94.8%)
- ✅ Compared home field advantage: 2022-2023 avg 3.49 pt margin, 2025 only 1.58 pt
- ✅ Confirmed NOT just early season variance (similar patterns across weeks 5-6)
- ✅ Verified complete 2024 data availability (all core weeks 6-12 have full scores)

### 🔧 Currently Working On: Model Retraining

**Next Immediate Action**: Retrain model on 2022-2024 data to reduce temporal gap and improve calibration

## Todo

### 🔥 Immediate Priority (DO THIS FIRST)
- [ ] **Retrain model on 2022-2024 data** with proper chronological split
  - Train on: 2022-2023
  - Test on: 2024 (chronological split, no data leakage)
  - Focus on weeks 6-12 evaluation (core regular season)
  - Run: `python -m src.models.train_logistic_baseline`
  - Expected improvements:
    - More recent training data (reduces 2-year gap to 1-year)
    - Better calibration on 2024 test set
    - More accurate confidence intervals

- [ ] **Evaluate new model calibration** on 2024 test data
  - Check calibration curves by confidence bucket
  - Verify 90%+ predictions hit ~90% accuracy
  - Compare performance on core weeks (6-12) vs early/late season
  - Save calibration analysis results

- [ ] **Regenerate 2025 week 5 predictions** with retrained model
  - Delete old week 5 predictions from database
  - Run: `python scripts/predict_week.py 2025 5 --backtest --save-to-db`
  - Compare accuracy with old model

- [ ] **Re-evaluate 2025 predictions** with new model
  - Run: `python -m src.models.evaluate_predictions --all`
  - Check if calibration gaps improved
  - Document before/after comparison

### Immediate Next Steps
- [x] Clean up repository (removed old_pipeline/, ncaa stuff/, testing/, .ipynb_checkpoints)
- [x] Create weekly prediction workflow pipeline
- [ ] Document model versioning and performance benchmarks
- [ ] Set up automated model retraining schedule (monthly or per-season)

### Short-term (Next 2-3 Weeks)
- [ ] **Generate predictions for weeks 7-10** using retrained model
  - Core regular season weeks - most important for accuracy
  - Compare week-over-week calibration stability
- [ ] **Build calibration monitoring dashboard**
  - Track confidence bucket accuracy over time
  - Alert if calibration drift exceeds threshold (>10% gap)
  - Visualize temporal trends in prediction quality
- [ ] **Analyze model failure modes**
  - Which team types are hardest to predict? (top-tier, evenly matched, etc.)
  - Do predictions get worse for certain conferences?
  - Early vs late season accuracy patterns

### Medium-term (Next Month)
- [ ] **Consider additional calibration techniques**
  - Temperature scaling on recent data
  - Platt scaling for more flexible calibration
  - Ensemble calibration methods
- [ ] **Train and evaluate XGBoost model**
  - Capture non-linear feature interactions
  - Compare against logistic baseline
  - Feature importance analysis
  - Check if calibration is better out-of-the-box
- [ ] **Train and evaluate Deep Forest model**
  - Explore ensemble methods
  - Performance comparison
- [ ] **Hyperparameter tuning for all models**
  - Grid search on validation set (2023)
  - Optimize for both accuracy AND calibration
- [ ] **Create model comparison framework**
  - Standardized metrics across all models
  - A/B testing infrastructure

### Long-term
- [ ] **Develop web API** for model predictions
  - RESTful endpoints for weekly predictions
  - Historical prediction lookup
  - Model confidence visualization
- [ ] **Automated weekly prediction pipeline**
  - Cron job to check for new games
  - Auto-generate predictions when data available
  - Email/Slack notifications with results
- [ ] **Model retraining automation**
  - Monthly retraining schedule during season
  - Automatic calibration validation
  - Rollback mechanism if new model performs worse

## Key Scripts & Usage

### Generate Predictions for Any Week
```bash
# Generate predictions for upcoming games
python scripts/predict_week.py 2025 7 --save-to-db

# Backtest on already-played games
python scripts/predict_week.py 2025 5 --backtest --save-to-db

# Use specific model version
python scripts/predict_week.py 2025 7 --model-name logistic_calibrated --model-version v2 --save-to-db
```

### Evaluate Model Performance
```bash
# Evaluate all predictions in database
python -m src.models.evaluate_predictions --all

# Evaluate specific week
python -m src.models.evaluate_predictions 2025 6

# Evaluate specific model
python -m src.models.evaluate_predictions 2025 6 --model-name logistic_calibrated --model-version v1
```

### Retrain Model
```bash
# Retrain on 2022-2024 data (uses 2024 as test year)
python -m src.models.train_logistic_baseline
```

## Model Performance Summary

### Current Model (Trained on 2022-2023)
- **Training**: 2022-2023 seasons
- **Test Set**: 2023 holdout
- **2023 Test Accuracy**: ~60-65% (needs verification)
- **2023 Test Calibration**: Good (90%+ predictions hit 94.8%)

### 2025 Production Performance (Weeks 5-6)
- **Overall Accuracy**: 61.2% (90/147 correct)
- **Week 5**: 57.1% (28/49 correct)
- **Week 6**: 63.3% (62/98 correct)
- **Calibration Issues**:
  - 90-100% confidence: 76.5% actual (20.5% gap)
  - 80-90% confidence: 64.4% actual (22.6% gap)
  - 70-80% confidence: 64.1% actual (10.7% gap)

### Why Accuracy is Decent but Calibration is Off
The model correctly identifies which team is favored, but is too confident in its predictions. For betting or decision-making where probabilities matter, the calibration issue is critical even though raw accuracy is acceptable.
