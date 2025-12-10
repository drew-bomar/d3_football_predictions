"""
schemas.py - Pydantic models that define our API response shapes

They serve to both validate that our data has the correct types and auto-generate API documentation
Guarantees that our API always returns predictably structured data

Each class here corresponds to JSON object shape in our API contract
"""
from pydantic import BaseModel
from typing import Optional
from datetime import date

class TeamInfo(BaseModel):
    """
    Basic information associated with any team in a response

    This is a nested object, anytime a response whats to reference a team, it will be a TeamInfo object
    """
    id : int
    name : str
    slug: str
    conference : Optional[str] = None   #some teams won't have this data

class PredictionGame(BaseModel):
    """
    A single predicted game with all of it's details

    Many fields are optional to handle upcoming games
    """
    game_id : int
    date : date
    status : str        #"completed" or "upcoming"

    home_team : TeamInfo
    away_team : TeamInfo

    predicted_winner : str   #"home" or "away"
    home_win_prob : float       # 0.0-1.0
    away_win_prob : float 
    confidence : float
    confidence_bucket : str   #example 70-79

    key_advantages : list[str]

    #will all be none if hasn't be played 
    home_score : Optional[int] = None
    away_score : Optional[int] = None
    actual_winner : Optional[str] = None
    was_correct : Optional[bool] = None
    is_upset : Optional[bool] = None


class WeekPredictionsResponse(BaseModel):
    """
    full response for GET /api/prediction/{year}/{week}
    """
    year: int
    week: int
    games_count : int
    correct_count : int
    accuracy : Optional[float] = None #none if no games have been completed yet
    predictions: list[PredictionGame]

class SeasonAccuracy(BaseModel):
    """
    Accuracy stats for a single season
    """
    year : int
    games : int
    correct : int
    accuracy : float

class AccuracyResponse(BaseModel):
    """
    Response for GET /api/stats/accuracy
    """
    overall : dict      #contains overall stats of number of games, number correct, and accuracy across all seasons
    by_season: list[SeasonAccuracy]
    current_season : int

class CalibrationBucket(BaseModel):
    """
    A single confidence bucket to be used for the calibration chart
    """
    bucket_min: float
    bucket_max: float
    label : str
    games: int
    mean_predicted: float
    actual_accuracy: float

class CalibrationResponse(BaseModel):
    """
    Response for GET /api/stats/calibration
    """
    buckets: list[CalibrationBucket]
    
class BucketGame(BaseModel):
    """
    A game returned by the calibration bucket pop-up
    """
    game_id : int
    year : int
    week : int
    home_team : TeamInfo
    away_team : TeamInfo
    predicted_winner: str
    confidence : float
    home_win_prob: float
    away_win_prob: float 
    home_score: int
    away_score: int
    actual_winner: str
    was_correct: bool

class GamesByBucketResponse(BaseModel):
    """
    Response for GET /api/stats/games_by_bucket
    """
    bucket_min : float
    bucket_max : float
    label : str
    games : list[BucketGame]

class SimulationResponse(BaseModel):
    """
    Response for GET /api/simulate
    """
    home_team: TeamInfo
    away_team: TeamInfo
    predicted_winner: str
    home_win_prob: float
    away_win_prob: float
    confidence: float
    confidence_bucket: str

class SeasonMeta(BaseModel):
    """
    Metadata about a seasonn's available predictions
    """
    year: int
    weeks_with_predictions : list[int]

class MetaResponse(BaseModel):
    """
    Response for GET /api/meta
    General metadata about available data
    """
    teams: list[TeamInfo]
    team_count: int
    seasons: list[SeasonMeta]

