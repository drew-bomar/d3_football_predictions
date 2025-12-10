"""
dependencies.py - shared dependencies for FastAPI routes
For FastAPI, a "dependecy" is a something that gets prepared before
your route runs and its cleaned up after, ex is database connections

This files creates a resuable database session that any route can use
"""

import os
from typing import Generator
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from functools import lru_cache
from src.models.matchup_predictor import MatchupPredictor


#check environment variable first, then fall back to loacl default
DATABASE_URL = os.getenv(
    'DATABASE_URL',
    'postgresql://d3_user:databaseguy824@localhost/d3_football'
)

#create SQLAlchemy engine, the core connection to your database, manages your "pool" of connections
engine = create_engine(DATABASE_URL)

#create session factory, each call to SessionLocal() creates a new database session
SessionLocal = sessionmaker(autocommit =False, autoflush= False, bind = engine)

@lru_cache()
def get_predictor() -> MatchupPredictor:
    """Load predictor once, reuse for all requests."""
    return MatchupPredictor()

def get_db() -> Generator[Session, None, None]:
    """
    Dependency that provides a database session to routes.
    
    How it works:
    1. Creates a new session when a request comes in
    2. Yields it to the route function
    3. Closes it after the route finishes (even if there's an error)
    
    Usage in a route:
        @app.get("/something")
        def my_route(db: Session = Depends(get_db)):
            results = db.query(SomeModel).all()
            return results
    
    The 'yield' keyword makes this a generator - everything before yield
    runs before the route, everything after runs when the route completes.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
        