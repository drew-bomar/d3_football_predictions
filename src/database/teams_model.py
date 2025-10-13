"""
SQLAlchemy models for D3 Football Predictions
Starting with the Teams table - the foundation of our database
"""
from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, DateTime, Boolean,
    UniqueConstraint, Index, text
)
from sqlalchemy.dialects.postgresql import UUID
from src.database.connection import Base

class Team(Base):
    """
    Represents a D3 football team.
    
    Design decisions:
    1. We use both an internal ID and NCAA's ID because:
       - Our ID gives us control and consistency
       - NCAA ID lets us match with external data
    
    2. Separate name fields for flexibility:
       - name: Common name (e.g., "Mount Union")
       - full_name: Official name (e.g., "University of Mount Union Purple Raiders")
       - short_name: For displays (e.g., "Mt Union")
    
    3. Timestamps for data quality:
       - Know when data was added/updated
       - Essential for debugging data issues
    """

    # Table name in PostgreSQL
    __tablename__ = 'teams'

    # Primary key - using integer for simplicity and performance
    id = Column(Integer, primary_key=True)

    #ncaa's id for this team based on their api
    #not unique because a team may have a different ID across seasons
    ncaa_id = Column(String(50), nullable=True, index=True)

    #team names - various formats for different uses
    name = Column(String(50), nullable=False)
    full_name = Column(String(200))
    short_name = Column(String(50))

    # URL-friendly identifier (e.g., "mount-union")
    # Useful for web routes: /teams/mount-union
    slug = Column(String(100), unique=True, nullable=False)

    # Conference tracking
    conference = Column(String(100))
    division = Column(String(20), default='III')  # We're D3 focused but flexible

    # Location information
    city = Column(String(100))
    state = Column(String(2))  # Two-letter state code

    #status tracking
    is_active = Column(Boolean, default = True) # sometimes programs get shutdown

    # Audit fields - when was this record created/modified?
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Constraints ensure data integrity at the database level
    __table_args__ = (
        # A team should have only one entry per NCAA ID + name combination
        # This prevents accidental duplicates
        UniqueConstraint('ncaa_id', 'name', name='uq_team_ncaa_id_name'),
        
        # Indexes for query performance
        # We'll often search by name or conference
        Index('idx_team_name', 'name'),
        Index('idx_team_conference', 'conference'),
        Index('idx_team_state', 'state'),
    )

    def __repr__(self):
        """String representation for debugging."""
        return f"<Team(id={self.id}, name='{self.name}', ncaa_id='{self.ncaa_id}')>"

    def __str__(self):
        """Human-readable string representation."""
        return self.name

    @classmethod
    def find_by_ncaa_id(cls, session, ncaa_id):
        """
        Find a team by its NCAA ID.
        
        Class methods like this encapsulate common queries,
        making code more readable and reusable.
        """
        return session.query(cls).filter_by(ncaa_id=ncaa_id).first()

    @classmethod
    def find_or_create(cls, session, ncaa_id, name, **kwargs):
        """
        Find existing team or create new one.
        
        This pattern prevents duplicates when importing data.
        Very common in ETL (Extract, Transform, Load) operations.
        """
        team = cls.find_by_ncaa_id(session, ncaa_id)

        if not team:
            # Create slug from name (e.g., "Mount Union" -> "mount-union")
            slug = kwargs.get('slug') or name.lower().replace(' ', '-')

            team = cls(
                ncaa_id = ncaa_id,
                name = name,
                slug = slug,
                **kwargs
            )
            session.add(team)

        return team







