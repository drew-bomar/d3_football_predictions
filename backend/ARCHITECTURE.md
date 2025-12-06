# D3 Football Predictions - Architecture Guide

## Overview
This project predicts D3 football game outcomes using machine learning. The codebase follows DRY principles with a clear separation of concerns.

## Directory Structure

```
d3-football-predictions/
├── src/                    # Source code (organized by function)
│   ├── database/           # Database models and connection
│   ├── pipeline/           # Data import from NCAA API
│   ├── features/           # Feature engineering
│   └── models/             # ML training, predictions, evaluation
├── scripts/                # Utility scripts
├── models/                 # Trained model artifacts (.pkl)
├── notebooks/              # Jupyter notebooks for exploration
├── tests/                  # Unit tests
├── data/                   # Data storage
│   ├── raw/                # Raw data
│   └── processed/          # Processed data
└── [config files]          # PROJECT.md, requirements.txt, etc.
```

## Canonical Locations (Single Source of Truth)

### 1. Game Import Pipeline
**Purpose:** Fetch games from NCAA API and store in database

**Files:**
- `src/pipeline/simple_pipeline.py` - Main orchestrator for weekly imports
- `src/pipeline/historical_importer.py` - Batch import for historical data
- `src/pipeline/game_importer.py` - Database insertion logic
- `src/pipeline/ncaa_api_client.py` - NCAA API wrapper
- `src/pipeline/stats_translator.py` - Translate NCAA format to DB schema
- `src/pipeline/team_manager.py` - Team resolution and creation
- `src/pipeline/progress_tracker.py` - Import progress tracking

**Usage:**
```python
from src.pipeline.simple_pipeline import SimplePipeline

# Import a specific week
pipeline = SimplePipeline()
result = pipeline.import_week(year=2024, week=7)

# Import historical data
from src.pipeline.historical_importer import HistoricalImporter
importer = HistoricalImporter()
importer.import_range(start_year=2022, end_year=2024)
```

### 2. Database Operations
**Purpose:** ORM models and database connection

**Files:**
- `src/database/connection.py` - Database connection management
- `src/database/teams_model.py` - Teams table
- `src/database/games_model.py` - Games table
- `src/database/team_game_stats_model.py` - Per-game statistics
- `src/database/team_rolling_stats_model.py` - Rolling statistics
- `src/database/predictions_model.py` - Model predictions

**Usage:**
```python
from src.database.connection import DatabaseConnection
from src.database.games_model import Game

db = DatabaseConnection()
with db.get_session() as session:
    games = session.query(Game).filter(Game.year == 2024).all()
```

### 3. Rolling Statistics Calculation
**Purpose:** Calculate team performance metrics over time

**File:** `src/features/rolling_stats_calculator.py`

**Features:**
- 3-week and 5-week rolling windows
- Previous season decay weights (0.7)
- Offensive, defensive, efficiency metrics
- Trend/momentum indicators
- ELO-based features

**Usage:**
```python
from src.features.rolling_stats_calculator import RollingStatsCalculator
from src.database.connection import DatabaseConnection

db = DatabaseConnection()
calculator = RollingStatsCalculator(db)
calculator.calculate_for_all_games(start_year=2022, end_year=2024)
```

### 4. ELO Ratings
**Purpose:** Track team strength over time using ELO system

**File:** `src/features/elo_calculator.py`

**Features:**
- Classic ELO with home field advantage (65 points)
- Margin of victory multiplier
- Season-to-season regression (75%)

**Usage:**
```python
from src.features.elo_calculator import ELOCalculator

calculator = ELOCalculator()
elo_df = calculator.calculate_historical_elos(2022, 2024)
calculator.save_elos_to_database(elo_df)
```

### 5. Model Training
**Purpose:** Train machine learning models

**File:** `src/models/train_logistic_baseline.py`

**Current Models:**
- Logistic Regression (baseline) with isotonic calibration

**Usage:**
```bash
# Train model
python src/models/train_logistic_baseline.py

# Output: models/logistic_regression.pkl
```

### 6. Generate Predictions
**Purpose:** Predict upcoming games

**File:** `src/models/weekly_predictor.py`

**Usage:**
```python
from src.models.weekly_predictor import WeeklyPredictor

predictor = WeeklyPredictor(model_path='models/logistic_regression.pkl')
predictions = predictor.predict_week(year=2024, week=7)
predictor.save_to_database(predictions)
```

### 7. Evaluate Predictions
**Purpose:** Compare predictions to actual results

**Files:**
- `src/models/evaluate_predictions.py` - Overall evaluation metrics
- `src/models/analyze_game_predictions.py` - Detailed game-by-game analysis

**Usage:**
```python
from src.models.evaluate_predictions import evaluate_week

metrics = evaluate_week(year=2024, week=7)
# Returns: accuracy, log loss, calibration metrics
```

### 8. Model Analysis
**Purpose:** Understand model performance and feature importance

**File:** `src/models/analyze_game_predictions.py`

**Capabilities:**
- Feature importance analysis
- Confidence calibration
- Error analysis by game characteristics

## Utility Scripts

Located in `scripts/`:

- `db_inspector.py` - Inspect database state, verify data completeness
- `verify_rolling_stats.py` - Validate rolling statistics calculations

**Usage:**
```bash
python scripts/db_inspector.py
python scripts/verify_rolling_stats.py --year 2024 --week 7
```

## Weekly Prediction Workflow

Complete workflow for generating predictions each week:

```bash
# 1. Check what data is missing
python src/models/import_checker.py

# 2. Import missing week data
python -c "from src.pipeline.simple_pipeline import SimplePipeline; \
           p = SimplePipeline(); p.import_week(2024, 7)"

# 3. Calculate rolling stats (if needed)
python -c "from src.features.rolling_stats_calculator import RollingStatsCalculator; \
           from src.database.connection import DatabaseConnection; \
           calc = RollingStatsCalculator(DatabaseConnection()); \
           calc.calculate_for_all_games(2024, 2024)"

# 4. Generate predictions
python -c "from src.models.weekly_predictor import WeeklyPredictor; \
           p = WeeklyPredictor('models/logistic_regression.pkl'); \
           p.predict_week(2024, 8)"

# 5. After games complete, evaluate
python -c "from src.models.evaluate_predictions import evaluate_week; \
           evaluate_week(2024, 8)"
```

## Data Flow

```
NCAA API
   ↓
[simple_pipeline.py] → Fetch & translate game data
   ↓
[game_importer.py] → Store in PostgreSQL
   ↓
[rolling_stats_calculator.py] → Calculate features
   ↓
[train_logistic_baseline.py] → Train model
   ↓
[weekly_predictor.py] → Generate predictions
   ↓
PostgreSQL (predictions table)
   ↓
[evaluate_predictions.py] → Compare to results
```

## Adding New Models

To add a new model (e.g., XGBoost):

1. Create `src/models/train_xgboost.py` following the pattern in `train_logistic_baseline.py`
2. Use `src/models/data_prep.py` for consistent data preparation
3. Save model to `models/xgboost_model.pkl`
4. Update `weekly_predictor.py` to support new model
5. Compare performance using `evaluate_predictions.py`

## Database Schema

Key tables:
- `teams` - Team information
- `games` - Game results and metadata
- `team_game_stats` - Per-game statistics for each team
- `team_rolling_stats` - Calculated rolling features
- `game_elos` - ELO ratings before/after each game
- `predictions` - Model predictions with confidence

## Configuration

**Database:** Configure in `src/database/connection.py`
**NCAA API:** Client in `src/pipeline/ncaa_api_client.py`
**Model Parameters:** Edit training scripts in `src/models/`

## Best Practices

1. **Never duplicate code** - Use the canonical files listed above
2. **Test scripts go in `tests/`** - Don't create ad-hoc test files in root
3. **Utilities go in `scripts/`** - Keep root directory clean
4. **One function, one location** - Follow the Single Source of Truth principle
5. **Document changes** - Update PROJECT.md and this file when adding features

## Development Guidelines

- All imports from NCAA API go through `simple_pipeline.py`
- All database queries use the ORM models in `src/database/`
- All feature calculations happen in `src/features/`
- All ML code lives in `src/models/`
- Keep notebooks for exploration only, not production code

## Troubleshooting

**Import fails:**
```bash
python scripts/db_inspector.py  # Check database state
```

**Rolling stats incorrect:**
```bash
python scripts/verify_rolling_stats.py --year 2024 --week 7
```

**Model performance issues:**
```python
from src.models.analyze_game_predictions import analyze_predictions
analyze_predictions(year=2024, week=7)
```
