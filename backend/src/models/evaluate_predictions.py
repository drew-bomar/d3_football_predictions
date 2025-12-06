#!/usr/bin/env python3
"""
evaluate_predictions.py - Evaluate model predictions against actual results

Matches predictions with actual game outcomes and calculates accuracy metrics.

Usage:
    python -m src.models.evaluate_predictions <year> <week> [--model-name MODEL] [--model-version VERSION]
    python -m src.models.evaluate_predictions --all

Examples:
    python -m src.models.evaluate_predictions 2025 5
    python -m src.models.evaluate_predictions 2025 6 --model-name logistic_calibrated
    python -m src.models.evaluate_predictions --all
"""

import sys
import os
import argparse
from datetime import datetime
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.database.connection import DatabaseConnection
from sqlalchemy import text
import pandas as pd

def evaluate_week(year: int, week: int, model_name: str = None, model_version: str = None):
    """
    Evaluate predictions for a specific week.

    Returns:
        DataFrame with evaluation results
    """
    db = DatabaseConnection()

    with db.get_session() as session:
        # Update predictions with results
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
                AND (:model_name IS NULL OR p.model_name = :model_name)
                AND (:model_version IS NULL OR p.model_version = :model_version)
                AND g.home_score IS NOT NULL
        """)
        session.execute(update_query, {
            'year': year,
            'week': week,
            'model_name': model_name,
            'model_version': model_version
        })
        session.commit()

        # Get detailed results
        results = session.execute(text("""
            SELECT
                p.model_name,
                p.model_version,
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
                AND (:model_name IS NULL OR p.model_name = :model_name)
                AND (:model_version IS NULL OR p.model_version = :model_version)
                AND p.was_correct IS NOT NULL
            ORDER BY p.confidence DESC
        """), {
            'year': year,
            'week': week,
            'model_name': model_name,
            'model_version': model_version
        }).fetchall()

        df = pd.DataFrame(results, columns=[
            'model_name', 'model_version', 'home_team', 'away_team',
            'predicted_winner', 'actual_winner', 'confidence', 'home_win_prob',
            'was_correct', 'home_score', 'away_score'
        ])

    if len(df) == 0:
        print(f"\n⚠️  No evaluated predictions found for {year} Week {week}")
        return df

    model_display = f"{df['model_name'].iloc[0]} ({df['model_version'].iloc[0]})" if len(df) > 0 else "N/A"

    print(f"\n{'='*80}")
    print(f"EVALUATION: {year} Week {week} - {model_display}")
    print('='*80)

    # Overall accuracy
    accuracy = df['was_correct'].mean()
    total = len(df)
    correct = df['was_correct'].sum()
    print(f"\nOverall Accuracy: {correct}/{total} ({accuracy:.1%})")

    # By confidence bucket
    print("\nAccuracy by Confidence Bucket:")
    confidence_buckets = [
        (0.9, 1.0, "90-100%"),
        (0.8, 0.9, "80-90%"),
        (0.7, 0.8, "70-80%"),
        (0.6, 0.7, "60-70%"),
        (0.5, 0.6, "50-60%")
    ]

    for min_c, max_c, label in confidence_buckets:
        bucket = df[(df['confidence'] >= min_c) & (df['confidence'] < max_c)]
        if len(bucket) > 0:
            bucket_acc = bucket['was_correct'].mean()
            print(f"  {label:8s}: {bucket_acc:.1%} ({bucket['was_correct'].sum()}/{len(bucket)} games)")

    # Biggest upsets (high confidence misses)
    print(f"\nBiggest Misses (High Confidence Incorrect):")
    misses = df[~df['was_correct']].head(5)
    for i, row in enumerate(misses.iterrows(), 1):
        row = row[1]
        print(f"\n{i}. {row['away_team']} @ {row['home_team']}")
        print(f"   Predicted: {row['predicted_winner']} ({row['confidence']:.1%} confidence)")
        print(f"   Actual: {row['actual_winner']} won {row['home_score']}-{row['away_score']}")

    # Best predictions (high confidence correct)
    print(f"\nBest Predictions (High Confidence Correct):")
    best = df[df['was_correct']].head(5)
    for i, row in enumerate(best.iterrows(), 1):
        row = row[1]
        print(f"\n{i}. {row['away_team']} @ {row['home_team']}")
        print(f"   Predicted: {row['predicted_winner']} ({row['confidence']:.1%} confidence)")
        print(f"   Actual: {row['actual_winner']} won {row['home_score']}-{row['away_score']}")

    return df

def evaluate_all_predictions():
    """Evaluate all predictions in the database."""
    db = DatabaseConnection()

    with db.get_session() as session:
        # Get all unique year/week/model combinations that have predictions
        query = text("""
            SELECT DISTINCT year, week, model_name, model_version
            FROM predictions
            ORDER BY year, week, model_name, model_version
        """)

        result = session.execute(query)
        combinations = [(row[0], row[1], row[2], row[3]) for row in result]

    if not combinations:
        print("No predictions found in database.")
        return

    print(f"\nFound {len(combinations)} prediction sets to evaluate:")
    for year, week, model_name, model_version in combinations:
        print(f"  - {year} Week {week}: {model_name} ({model_version})")

    all_results = []

    for year, week, model_name, model_version in combinations:
        df = evaluate_week(year, week, model_name, model_version)
        if len(df) > 0:
            all_results.append(df)

    if not all_results:
        return

    # Overall summary
    combined_df = pd.concat(all_results, ignore_index=True)

    print(f"\n{'='*80}")
    print("OVERALL SUMMARY")
    print(f"{'='*80}")

    total_predictions = len(combined_df)
    total_correct = combined_df['was_correct'].sum()
    overall_accuracy = combined_df['was_correct'].mean()

    print(f"\nTotal Predictions Evaluated: {total_predictions}")
    print(f"Total Correct: {total_correct}")
    print(f"Overall Accuracy: {overall_accuracy:.1%}")

    print("\nBy Week:")
    for year, week, model_name, model_version in combinations:
        week_df = combined_df[
            (combined_df['model_name'] == model_name) &
            (combined_df['model_version'] == model_version)
        ]
        if len(week_df) > 0:
            week_acc = week_df['was_correct'].mean()
            week_correct = week_df['was_correct'].sum()
            week_total = len(week_df)
            print(f"  {year} Week {week} ({model_name} {model_version}): "
                  f"{week_correct}/{week_total} ({week_acc:.1%})")

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Evaluate model predictions against actual game results',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument('year', type=int, nargs='?',
                       help='Season year (e.g., 2025)')
    parser.add_argument('week', type=int, nargs='?',
                       help='Week number (e.g., 5)')
    parser.add_argument('--model-name', type=str,
                       help='Filter by model name')
    parser.add_argument('--model-version', type=str,
                       help='Filter by model version')
    parser.add_argument('--all', action='store_true',
                       help='Evaluate all predictions in database')

    args = parser.parse_args()

    if args.all:
        evaluate_all_predictions()
    elif args.year and args.week:
        evaluate_week(args.year, args.week, args.model_name, args.model_version)
    else:
        parser.print_help()
        sys.exit(1)

if __name__ == "__main__":
    main()