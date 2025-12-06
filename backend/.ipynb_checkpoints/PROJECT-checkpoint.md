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

## Currently Working On

- ✅ **Repository Cleanup**: Removed 45+ redundant files, organized utilities into scripts/
- **Next**: Document model performance benchmarks and establish weekly prediction workflow

## Todo

### Immediate Next Steps
- [x] Clean up repository (removed old_pipeline/, ncaa stuff/, testing/, .ipynb_checkpoints)
- [ ] Document model versioning and performance benchmarks
- [ ] Create weekly prediction workflow pipeline
- [ ] Set up structured testing framework for ongoing validation

### Short-term (Next 2-3 Weeks)
- [ ] Collect and analyze model predictions for upcoming weeks
- [ ] Build data quality monitoring dashboard
- [ ] Track prediction accuracy over time
- [ ] Identify model weaknesses and edge cases

### Medium-term (Next Month)
- [ ] Train and evaluate XGBoost model
  - Capture non-linear feature interactions
  - Compare against logistic baseline
  - Feature importance analysis
- [ ] Train and evaluate Deep Forest model
  - Explore ensemble methods
  - Performance comparison
- [ ] Hyperparameter tuning for all models
- [ ] Create model comparison framework

### Long-term
- [ ] development web api for where users can see model predictions
- [ ] automated weekly predictions
