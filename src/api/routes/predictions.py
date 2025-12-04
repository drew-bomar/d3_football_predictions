"""
predictions.py - routes data for prediction data 
Here we will query the databse and return prediction data matching the API contract
"""
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Optional

from src.api.dependencies import get_db
from src.api.schemas import (
    WeekPredictionsResponse,
    PredictionGame,
    TeamInfo
)

router = APIRouter(
    prefix = "/api",
    tags = ["predictions"]
)

#helper functions
def calculate_confidence_bucket(confidence : float) -> str:
    """""
    Convert a confidence value(0.0-1.0) to a bucket string label
    """""
    pct = confidence * 100

    if pct >= 90:
        return "90-100"
    elif pct >= 80:
        return "80-89"
    elif pct >= 70:
        return "70-79"
    elif pct >= 60:
        return "60-69"
    elif pct >= 50:
        return "50-59"
    

def row_to_prediction(row) -> PredictionGame:
    """
    Transform a database row into a PredictionGame schema

    Raw data from SQL query --> puts it in form of our API contract
    """
    #any completed game will have a home score 
    is_completed = row.home_score is not None

    #created home and away nest teamnInfo objects
    home_team = TeamInfo(
        id=row.home_team_id,
        name=row.home_team_name,
        slug=row.home_team_slug,
        conference=row.home_team_conference
    )

    away_team = TeamInfo(
        id=row.away_team_id,
        name=row.away_team_name,
        slug=row.away_team_slug,
        conference=row.away_team_conference
    )

    predicted_winner = "home" if row.home_win_prob > 0.5 else "away"
    
    #determine actual winner only if game has been completed
    actual_winner = None
    if is_completed:
        if row.home_score > row.away_score:
            actual_winner = "home"
        elif row.away_score > row.home_score:
            actual_winner = "away"
        else:
            actual_winner = "tie"

    was_correct = None
    if is_completed and actual_winner:
        was_correct = (predicted_winner == actual_winner)
    
    is_upset = None
    if was_correct is not None:
        is_upset = (not was_correct) and (row.confidence >= 0.70)
    
    key_advantages = []
    if row.key_factors:
        key_advantages = [adv.strip() for adv in row.key_factors.split (',') if adv.strip()]

    return PredictionGame(
        game_id = row.game_id,
        date = row.game_date,
        status = "completed" if is_completed else "upcoming",
        home_team = home_team,
        away_team = away_team,
        predicted_winner = predicted_winner,
        home_win_prob = round(row.home_win_prob, 4),
        away_win_prob = round(row.away_win_prob, 4),
        confidence = round(row.confidence, 4),
        confidence_bucket = calculate_confidence_bucket(row.confidence),
        key_advantages = key_advantages,
        home_score=row.home_score,
        away_score=row.away_score,
        actual_winner=actual_winner,
        was_correct=was_correct,
        is_upset=is_upset
    )

@router.get("/debug/db-test")
def test_db(db: Session = Depends(get_db)):
    result = db.execute(text("SELECT COUNT(*) FROM predictions WHERE year = 2025"))
    count = result.scalar()
    return {"predictions_count": count}


@router.get("/predictions/{year}/{week}", response_model= WeekPredictionsResponse)
def get_week_predictions(
    year : int,
    week: int,
    db: Session = Depends(get_db)
 ):
    """
    Get all predictions for a specific week

    Path Parameters:
        year: Season year (e.g., 2024)
        week: Week number (e.g., 7)
    
    Returns:
        WeekPredictionsResponse with all predictions for that week

    """
    #---------Query the Database---------
    # This query joins three tables:
    # - predictions: Our model's predictions
    # - games: The actual game info (scores, dates)
    # - teams (twice): Once for home team, once for away team

    print(f"DEBUG: Received request for year = {year} and week = {week}")

    query = text("""
        SELECT    
            -- Game Info
            g.id as game_id,
            g.game_date,
            g.home_score,
            g.away_score,
            
            -- Home team info
            ht.id as home_team_id,
            ht.name as home_team_name,
            ht.slug as home_team_slug,
            ht.conference as home_team_conference,
            
            -- Away Team Info
            at.id as away_team_id,
            at.name as away_team_name,
            at.slug as away_team_slug,
            at.conference as away_team_conference,
                 
            --Prediction Info
            p.home_win_prob,
            p.away_win_prob,
            p.confidence,
            p.key_factors

        FROM predictions P  
        JOIN games g ON
            p.year = g.year
            AND p.week = g.week
            AND p.home_team_id = g.home_team_id
            AND p.away_team_id = g.away_team_id
        JOIN teams ht ON g.home_team_id = ht.id
        JOIN teams at on g.away_team_id = at.id
        WHERE p.year = :year AND p.week = :week
        ORDER BY p.confidence DESC
    """)

    #excute the query with our parameters
    result = db.execute(query,{"year": year, "week": week})
    rows = result.fetchall()

    print(f"DEBUG: Query returned {len(rows)} rows")

    #handle no results
    if not rows:
        raise HTTPException(
            status_code = 404,
            detail = f"No predictions found for {year} week {week}"
        )

    predictions = [row_to_prediction(row) for row in rows]

    games_count = len(predictions)

    #count correct predictions only for completed games
    completed_predictions = [p for p in predictions if p.was_correct is not None]
    correct_count = sum(1 for p in completed_predictions if p.was_correct)

    #check accuracy 
    accuracy = None
    if completed_predictions:
        accuracy = round(correct_count / len(completed_predictions), 4)

    #build response according to schema.py
    return WeekPredictionsResponse(
        year = year,
        week = week,
        games_count = games_count,
        correct_count = correct_count,
        accuracy = accuracy,
        predictions=predictions
)
