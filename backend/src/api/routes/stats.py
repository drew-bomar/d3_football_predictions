"""
stats.py - roputes for model performance statistics

stub endpoints for now to mimic the behavior of actual responses so we can move forward to front-edn dev
will return to create actual SQL queries
"""

from fastapi import APIRouter, Query
from src.api.schemas import (
    AccuracyResponse,
    SeasonAccuracy,
    CalibrationResponse,
    CalibrationBucket,
    GamesByBucketResponse,
    BucketGame,
    TeamInfo
)
from src.api.routes.predictions import calculate_confidence_bucket
from datetime import date

router = APIRouter(
    prefix= "/api/stats",
    tags = ["stats"]
)

@router.get("/accuracy", response_model=AccuracyResponse)
def get_accuracy():
    """
    Get overall model accuracy and accuracy by season

    Stub Implementation for now
    """
    return AccuracyResponse(
        overall={
            "games": 816,
            "correct": 532,
            "accuracy": 0.652
        },
        by_season=[
            SeasonAccuracy(year=2025, games=816, correct=532, accuracy=0.652)
        ],
        current_season=2025
    )

@router.get("/calibration", response_model = CalibrationResponse)
def get_calibration():
    """
    Get calibration data showing predicted vs actual accuracy for each confidence bucket

    Stub endpoint for now
    """
    return CalibrationResponse(
        buckets=[
            CalibrationBucket(
                bucket_min=0.50,
                bucket_max=0.59,
                label="50-59",
                games=150,
                mean_predicted=0.545,
                actual_accuracy=0.53
            ),
            CalibrationBucket(
                bucket_min=0.60,
                bucket_max=0.69,
                label="60-69",
                games=200,
                mean_predicted=0.645,
                actual_accuracy=0.62
            ),
            CalibrationBucket(
                bucket_min=0.70,
                bucket_max=0.79,
                label="70-79",
                games=180,
                mean_predicted=0.745,
                actual_accuracy=0.71
            ),
            CalibrationBucket(
                bucket_min=0.80,
                bucket_max=0.89,
                label="80-89",
                games=120,
                mean_predicted=0.84,
                actual_accuracy=0.80
            ),
            CalibrationBucket(
                bucket_min=0.90,
                bucket_max=1.00,
                label="90-100",
                games=166,
                mean_predicted=0.95,
                actual_accuracy=0.92
            )
        ]
    )

@router.get("/games-by-bucket", response_model = GamesByBucketResponse)
def get_games_by_bucket(
    bucket_min : float = Query(..., description = "Minimum Confidence eg(0.70)"), #... means theres no default it must be provided
    bucket_max : float = Query(..., description = "Maximum Confidence eg(0.79)")
): 
    """
    Return every game from the current season that was predicted within this confidence bucket

    Stub Endpoint for noq
    """
    # Create a label from the bucket range
    label = f"{int(bucket_min * 100)}-{int(bucket_max * 100)}"
    
    # Return placeholder games
    return GamesByBucketResponse(
        bucket_min=bucket_min,
        bucket_max=bucket_max,
        label=label,
        games=[
            BucketGame(
                game_id=1001,
                year=2025,
                week=8,
                date=date(2025, 10, 19),
                home_team=TeamInfo(id=698, name="Wis.-Whitewater", slug="wis-whitewater", conference=None),
                away_team=TeamInfo(id=872, name="Wis.-Eau Claire", slug="wis-eau-claire", conference=None),
                predicted_winner="home",
                confidence=0.74,
                home_win_prob=0.74,
                away_win_prob=0.26,
                home_score=35,
                away_score=21,
                actual_winner="home",
                was_correct=True
            ),
            BucketGame(
                game_id=1002,
                year=2025,
                week=9,
                date=date(2025, 10, 26),
                home_team=TeamInfo(id=772, name="Gettysburg", slug="gettysburg", conference=None),
                away_team=TeamInfo(id=884, name="Muhlenberg", slug="muhlenberg", conference=None),
                predicted_winner="away",
                confidence=0.72,
                home_win_prob=0.28,
                away_win_prob=0.72,
                home_score=14,
                away_score=28,
                actual_winner="away",
                was_correct=True
            )
        ]
    )