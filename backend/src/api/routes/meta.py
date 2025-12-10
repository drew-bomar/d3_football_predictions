"""
meta.py - Metadata endpoints for available teams, seasons, and weeks

Populates dropdowns and understanding what data exists
"""
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text

from src.api.dependencies import get_db
from src.api.schemas import MetaResponse, TeamInfo, SeasonMeta

router = APIRouter(
    prefix = "/api",
    tags=["meta"]
)

@router.get("/meta", response_model = MetaResponse)
def get_meta(db: Session = Depends(get_db)):
    """
    Get metadata about available teams and predictions.
    
    Returns:
        - All teams (sorted alphabetically)
        - Seasons and which weeks have predictions
    """

    teams_query = text("""
            SELECT id, name, slug, conference
            FROM teams
            ORDER BY name ASC              
        """)
    teams_result = db.execute(teams_query).fetchall()

    teams = [
        TeamInfo(
            id=row.id,
            name=row.name,
            slug=row.slug,
            conference=row.conference
        )
        for row in teams_result
    ]

    seasons_query = text("""
        SELECT DISTINCT year, week
        FROM predictions
        ORDER BY year DESC, week ASC
        """ )
    
    seasons_result = db.execute(seasons_query).fetchall()

    # Group weeks by year
    seasons_dict = {}
    for row in seasons_result:
        if row.year not in seasons_dict:
            seasons_dict[row.year] = []
        seasons_dict[row.year].append(row.week)

    seasons = [
        SeasonMeta(year=year, weeks_with_predictions=weeks)
        for year, weeks in sorted(seasons_dict.items(), reverse=True)
    ]

    return MetaResponse(
        teams=teams,
        team_count=len(teams),
        seasons=seasons
    )

