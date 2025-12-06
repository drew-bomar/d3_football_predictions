#!/usr/bin/env python3
"""
predict_week.py - Generate predictions for any week

Usage:
    python scripts/predict_week.py <year> <week> [--save-to-db]

Examples:
    python scripts/predict_week.py 2025 5
    python scripts/predict_week.py 2025 6 --save-to-db
"""

import sys
import argparse
import logging
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.models.weekly_predictor import WeeklyPredictor
from src.models.week_importer import WeekImporter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """Generate predictions for a specified week."""
    parser = argparse.ArgumentParser(
        description='Generate predictions for a specific week',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/predict_week.py 2025 5
  python scripts/predict_week.py 2025 6 --save-to-db
  python scripts/predict_week.py 2025 7 --save-to-db --model-name logistic_baseline --model-version v1
        """
    )

    parser.add_argument('year', type=int, help='Season year (e.g., 2025)')
    parser.add_argument('week', type=int, help='Week number (e.g., 5)')
    parser.add_argument('--save-to-db', action='store_true',
                       help='Save predictions to database')
    parser.add_argument('--model-name', type=str, default='logistic_calibrated',
                       help='Model name for database (default: logistic_calibrated)')
    parser.add_argument('--model-version', type=str, default='v1',
                       help='Model version for database (default: v1)')
    parser.add_argument('--skip-import-check', action='store_true',
                       help='Skip checking/importing missing weeks')
    parser.add_argument('--backtest', action='store_true',
                       help='Include already-played games (for model evaluation)')

    args = parser.parse_args()

    # Validate inputs
    if args.year < 2022 or args.year > 2030:
        print(f"Error: Year {args.year} seems invalid")
        sys.exit(1)

    if args.week < 1 or args.week > 15:
        print(f"Error: Week {args.week} must be between 1 and 15")
        sys.exit(1)

    print(f"\n{'='*60}")
    print(f"PREDICTION GENERATOR - {args.year} Week {args.week}")
    print(f"{'='*60}")

    # Step 1: Check if we have necessary data
    if not args.skip_import_check:
        print("\nStep 1: Checking for required data...")
        importer = WeekImporter()

        data_ready = importer.ensure_data_ready(args.year, args.week)

        if not data_ready:
            print("\n⚠️  Warning: Some required data may be missing.")
            response = input("Continue anyway? (y/n): ").strip().lower()
            if response != 'y':
                print("Prediction cancelled.")
                sys.exit(0)
    else:
        print("\nSkipping import check (--skip-import-check specified)")

    # Step 2: Generate predictions
    print(f"\nStep 2: Generating predictions for {args.year} Week {args.week}...")
    if args.backtest:
        print("   Backtesting mode: Using rolling stats through week {} to predict already-played games".format(args.week - 1))
    try:
        predictor = WeeklyPredictor()
        predictions = predictor.predict_week(args.year, args.week, include_played=args.backtest)

        if not predictions:
            print(f"\nNo games found for {args.year} Week {args.week}")
            print("This could mean:")
            print("  1. The week hasn't been scheduled yet")
            print("  2. All games have already been played")
            print("  3. The API is not returning data for this week")
            sys.exit(0)

        # Step 3: Display predictions
        predictor.format_predictions(predictions)

        # Step 4: Optionally save to database
        if args.save_to_db:
            print(f"\nStep 3: Saving predictions to database...")
            saved_count = predictor.save_predictions_to_db(
                predictions,
                model_name=args.model_name,
                model_version=args.model_version
            )
            print(f"✅ Saved {saved_count} predictions to database")
            print(f"   Model: {args.model_name} ({args.model_version})")
        else:
            print("\nPredictions not saved to database (use --save-to-db to save)")

        # Summary
        print(f"\n{'='*60}")
        print("SUMMARY")
        print(f"{'='*60}")
        print(f"Total games: {len(predictions)}")
        print(f"Successfully predicted: {sum(1 for p in predictions if not p.get('error'))}")
        print(f"Errors: {sum(1 for p in predictions if p.get('error'))}")

        if args.save_to_db:
            print(f"Database records: {saved_count}")

        print("\n✅ Done!")

    except FileNotFoundError as e:
        print(f"\n❌ Error: {e}")
        print("\nMake sure you have trained a model first:")
        print("  python -m src.models.train_logistic_baseline")
        sys.exit(1)

    except Exception as e:
        logger.exception("Prediction failed")
        print(f"\n❌ Error generating predictions: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
