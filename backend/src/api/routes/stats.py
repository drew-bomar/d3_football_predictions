"""
stats.py - roputes for model performance statistics

stub endpoints for now to mimic the behavior of actual responses so we can move forward to front-edn dev
will return to create actual SQL queries
"""

from fastapi import APIRouter, Query, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text

from src.api.dependencies import get_db
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
def get_accuracy(db: Session = Depends(get_db)):
    """
    Get overall model accuracy and accuracy by season
    """

    overall_query = text("""
        SELECT 
            COUNT(*) as games,
            SUM(CASE WHEN was_correct THEN 1 ELSE 0 END) as correct
        FROM predictions
        WHERE was_correct IS NOT NULL        
                        """)
    
    overall_result = db.execute(overall_query).fetchone()

    overall_games = overall_result.games or 0
    overall_correct  = overall_result.correct or 0
    overall_accuracy= round(overall_correct / overall_games, 4) if overall_games > 0 else 0

    by_season_query = text("""
        SELECT 
            year,
            COUNT(*) as games,
            SUM(CASE WHEN was_correct THEN 1 ELSE 0 END) as correct
        FROM predictions
        WHERE was_correct IS NOT NULL
        GROUP BY year
        ORDER BY year
        """)
    
    by_season_result = db.execute(by_season_query).fetchall()

    #create SeasonAccuracy objects with query results
    by_season = [
        SeasonAccuracy(
            year = row.year,
            games = row.games,
            correct = row.correct,
            accuracy= round(row.correct / row.games, 4) if row.games > 0 else 0
        )
        for row in by_season_result
    ]

    current_season = max(row.year for row in by_season_result) if by_season_result else 2025

    return AccuracyResponse(
        overall={
            "games": overall_games,
            "correct": overall_correct,
            "accuracy": overall_accuracy
        },
        by_season= by_season,
        current_season=current_season
    )

@router.get("/calibration", response_model = CalibrationResponse)
def get_calibration(db: Session = Depends(get_db)):
    """
    Get calibration data showing predicted vs actual accuracy for each confidence bucket

    """

    query = text("""
        SELECT
            CASE
                 WHEN confidence >= 0.9 THEN 0.9
                 WHEN confidence >= 0.8 THEN 0.8
                 WHEN confidence >= 0.7 THEN 0.7
                 WHEN confidence >= 0.6 THEN 0.6
                 ELSE 0.5
            END as bucket_min,
            COUNT(*) as games,
            AVG(confidence) as mean_predicted,
            AVG(CASE WHEN was_correct THEN 1.0 ELSE 0.0 END) as actual_accuracy
        FROM predictions
        WHERE was_correct IS NOT NULL
        GROUP by bucket_min
        ORDER by bucket_min    
    """)

    #get all rows the query
    result = db.execute(query).fetchall()

    #create bucket objects
    buckets = []
    for row in result:
        bucket_min = float(row.bucket_min)
        bucket_max = bucket_min + 0.09 if bucket_min < 0.9 else 1.0
        label = f"{int(bucket_min * 100)}-{int(bucket_max * 100)}"

        buckets.append(CalibrationBucket(
            bucket_min = bucket_min,
            bucket_max = bucket_max,
            label=label,
            games=row.games,
            mean_predicted = round(float(row.mean_predicted), 4),
            actual_accuracy = round(float(row.actual_accuracy),4)
        ))

    return CalibrationResponse(buckets=buckets)

@router.get("/games-by-bucket", response_model = GamesByBucketResponse)
def get_games_by_bucket(
    bucket_min : float = Query(..., description = "Minimum Confidence eg(0.70)"), #... means theres no default it must be provided
    bucket_max : float = Query(..., description = "Maximum Confidence eg(0.79)"),
    db: Session = Depends(get_db)
): 
    """
    Return every game from the current season that was predicted within this confidence bucket
    """
    # Create a label from the bucket range
    label = f"{int(bucket_min * 100)}-{int(bucket_max * 100)}"
    
    query = text("""
            SELECT
                g.id as game_id,
                g.year,
                g.week,
                g.game_date,
                g.home_score,
                g.away_score,
                
                ht.id as home_team_id,
                ht.name as home_team_name,
                ht.slug as home_team_slug,
                ht.conference as home_team_conference,
                 
                at.id as away_team_id,
                at.name as away_team_name,
                at.slug as away_team_slug,
                at.conference as away_team_conference,
                 
                p.home_win_prob,
                p.away_win_prob,
                p.confidence,
                p.was_correct
            
            FROM predictions p
            JOIN games g ON
                 p.year = g.year
                 AND p.week = g.week
                 AND p.home_team_id = g.home_team_id
                 AND p.away_team_id = g.away_team_id
            JOIN teams ht ON g.home_team_id = ht.id
            JOIN teams at ON g.away_team_id = at.id
            WHERE p.confidence >= :bucket_min
                AND p.confidence <=:bucket_max
                AND p.was_correct IS NOT NULL
            ORDER BY g.year DESC, g.week DESC, p.confidence DESC
    """)

    result = db.execute(query, {
        "bucket_min": bucket_min,
        "bucket_max": bucket_max
    }).fetchall()

    games = []
    for row in result: #create a bucket game object for each row from query result
        predicted_winner = "home" if row.home_win_prob > 0.5 else "away"

        if row.home_score > row.away_score:
            actual_winner = "home"
        elif row.away_score > row.home_score:
            actual_winner = "away"
        else:
            actual_winner = "tie"

        games.append(BucketGame(
                game_id=row.game_id,
                year=row.year,
                week=row.week,
                date=row.game_date,
                home_team=TeamInfo(
                    id=row.home_team_id,
                    name=row.home_team_name,
                    slug=row.home_team_slug,
                    conference=row.home_team_conference
                ),
                away_team=TeamInfo(
                    id=row.away_team_id,
                    name=row.away_team_name,
                    slug=row.away_team_slug,
                    conference=row.away_team_conference
                ),
                predicted_winner=predicted_winner,
                confidence=round(float(row.confidence), 4),
                home_win_prob=round(float(row.home_win_prob), 4),
                away_win_prob=round(float(row.away_win_prob), 4),
                home_score=row.home_score,
                away_score=row.away_score,
                actual_winner=actual_winner,
                was_correct=row.was_correct
            ))
        
    #return all games for current bucket
    return GamesByBucketResponse(
        bucket_min=bucket_min,
        bucket_max=bucket_max,
        label=label,
        games=games
    )