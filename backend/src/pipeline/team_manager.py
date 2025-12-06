"""
Team Manager - Handles team resolution and caching for the NCAA D3 Football pipeline
This component bridges the gap between NCAA API team data and our database records
"""

import re
import logging
from typing import Dict, Optional, List, Tuple
from datetime import datetime
from sqlalchemy.orm import Session

# Import the Team model (adjust import path as needed)
from src.database.teams_model import Team

logger = logging.getLogger(__name__)

class TeamManager:
    """
    Manages team lookups and creation with intelligent caching.
    
    Key responsibilities:
    1. Resolve NCAA team identifiers to database team IDs
    2. Create new team records when needed
    3. Cache lookups to minimize database queries
    4. Handle team name variations gracefully
    """
    def __init__(self, db_connection):
        """
        Initialize the Team Manager.
        
        Args:
            db_connection: DatabaseConnection instance from connection.py
        """
        self.db = db_connection
        
        # Cache structure: {seoname: team_id}
        self.seoname_cache = {}
        
        # Secondary cache for team names (backup resolution)
        self.name_cache = {}
        
        # Track cache statistics for debugging
        self.cache_hits = 0
        self.cache_misses = 0
        self.teams_created = 0
        
        # Load existing teams into cache on startup
        self._initialize_cache()

    def _initialize_cache(self):
        """
        Load all existing teams from database into cache.
        This runs once at startup to populate our lookup dictionaries.
        """
        with self.db.get_session() as session:
            teams = session.query(Team).filter(Team.is_active == True).all()
            
            for team in teams:
                # Cache by seoname (if available)
                if team.slug:  # Using slug field for seoname
                    self.seoname_cache[team.slug] = team.id
                
                # Also cache by regular name for backup lookups
                if team.name:
                    self.name_cache[team.name.lower()] = team.id
                
                # Cache short_name variant too
                if team.short_name:
                    self.name_cache[team.short_name.lower()] = team.id
            
            logger.info(f"Initialized team cache with {len(teams)} teams")
            logger.debug(f"Seoname cache size: {len(self.seoname_cache)}")
            logger.debug(f"Name cache size: {len(self.name_cache)}")

    # def find_or_create_team(self, session: Session, team_data: dict) -> Tuple[int, bool]:
    #     """
    #     Find existing team or create new one.
        
    #     This is the main method called during game import.
    #     It tries multiple strategies to find a team before creating a new one.
        
    #     Args:
    #         session: SQLAlchemy session (from the game importer's transaction)
    #         team_data: Dict with team info from NCAA API
    #             Expected keys: 'seoname', 'nameShort', 'teamId' (NCAA's ID)
        
    #     Returns:
    #         Tuple of (team_id, was_created)
    #     """
    #     seoname = team_data.get('seoname', '').lower()
    #     name_short = team_data.get('nameShort', '')
    #     ncaa_id = str(team_data.get('teamId', ''))

    #     # Strategy 1: Check seoname cache (fastest), don't need to create
    #     if seoname and seoname in self.seoname_cache:
    #         self.cache_hits += 1
    #         logger.debug(f"Cache hit for seoname: {seoname}")
    #         return self.seoname_cache[seoname], False

    #     # Strategy 2: Check name cache (backup)
    #     name_lower = name_short.lower()
    #     if name_lower in self.name_cache:
    #         self.cache_hits += 1
    #         logger.debug(f"Cache hit for name: {name_short}")
    #         team_id = self.name_cache[name_lower]

    #         if seoname:
    #             self.seoname_cache[seoname] = team_id

    #         return team_id, False

    #     # Strategy 3: Database lookup (cache miss)
    #     self.cache_misses += 1
    #     logger.debug(f"Cache miss for team: {name_short} (seoname: {seoname})")

    #     if name_short != "Trinity (TX)":
    #         logger.warning(f"Processing {name_short} (seoname: {seoname})")
        
    #     # Try to find by NCAA ID first
    #     team = None
    #     if ncaa_id:
    #         team = session.query(Team).filter(Team.ncaa_id == ncaa_id).first()
        
    #     # If not found by NCAA ID, try by slug (seoname)
    #     if not team and seoname:
    #         team = session.query(Team).filter(Team.slug == seoname).first()
        
    #     # If still not found, try by name
    #     if not team and name_short:
    #         team = session.query(Team).filter(Team.name == name_short).first()
    #         if team and team.name != name_short:
    #             logger.error(f"WRONG TEAM! Asked for {name_short}, got {team.name}")
            
    #     # If team exists, update cache and return
    #     if team:
    #         logger.info(f"Found existing team in database: {team.name}")
            
    #         # Update all caches
    #         if seoname:
    #             self.seoname_cache[seoname] = team.id
    #         self.name_cache[name_lower] = team.id
            
    #         # Update team's NCAA ID if it changed
    #         if ncaa_id and team.ncaa_id != ncaa_id:
    #             logger.warning(f"Updating NCAA ID for {team.name}: {team.ncaa_id} -> {ncaa_id}")
    #             team.ncaa_id = ncaa_id
    #             team.updated_at = datetime.utcnow()
            
    #         return team.id, False

    #     # Strategy 4: Create new team
    #     logger.info(f"Creating new team: {name_short} (seoname: {seoname})")
        
    #     team = Team(
    #         ncaa_id=ncaa_id,
    #         name=name_short,
    #         short_name=name_short,
    #         slug=seoname if seoname else self._generate_slug(name_short),
    #         is_active=True
    #     )
        
    #     session.add(team)
    #     session.flush() #get the ID without commiting

    #     # Update caches with new team
    #     if seoname:
    #         self.seoname_cache[seoname] = team.id
    #     self.name_cache[name_lower] = team.id
        
    #     self.teams_created += 1
    #     logger.info(f"Created team #{team.id}: {team.name}")
        
    #     return team.id, True

    def find_or_create_team(self, session: Session, team_data: dict) -> Tuple[int, bool]:
        seoname = team_data.get('seoname', '').lower()
        name_short = team_data.get('nameShort', '')
        ncaa_id = str(team_data.get('teamId', ''))
        
        # FIX: Don't convert None to string '', keep it as empty
        if not team_data.get('teamId'):
            ncaa_id = None
        else:
            ncaa_id = str(team_data.get('teamId'))
        
        # Debug logging
        if name_short != "Trinity (TX)":
            logger.warning(f"Processing {name_short} (seoname: {seoname}, ncaa_id: {ncaa_id})")
        
        # Strategy 1: Check seoname cache (fastest)
        if seoname and seoname in self.seoname_cache:
            self.cache_hits += 1
            logger.debug(f"Cache hit for seoname: {seoname}")
            return self.seoname_cache[seoname], False
        
        # Strategy 2: Check name cache
        name_lower = name_short.lower()
        if name_lower in self.name_cache:
            self.cache_hits += 1
            logger.debug(f"Cache hit for name: {name_short}")
            team_id = self.name_cache[name_lower]
            if seoname:
                self.seoname_cache[seoname] = team_id
            return team_id, False
        
        # Strategy 3: Database lookup
        self.cache_misses += 1
        
        # CRITICAL FIX: Only query by NCAA ID if it's not None or empty
        team = None
        if ncaa_id and ncaa_id != '':
            team = session.query(Team).filter(Team.ncaa_id == ncaa_id).first()
            if team:
                logger.debug(f"Found by NCAA ID: {team.name}")
        
        # Try by slug if not found
        if not team and seoname:
            team = session.query(Team).filter(Team.slug == seoname).first()
            if team:
                logger.debug(f"Found by slug: {team.name}")
        
        # Try by name if still not found
        if not team and name_short:
            team = session.query(Team).filter(Team.name == name_short).first()
            if team:
                logger.debug(f"Found by name: {team.name}")
        
        # If team exists, update cache and return
        if team:
            logger.info(f"Found existing team in database: {team.name}")
            # Update caches
            if seoname:
                self.seoname_cache[seoname] = team.id
            self.name_cache[name_lower] = team.id
            
            # Update NCAA ID if we have one and it's different
            if ncaa_id and team.ncaa_id != ncaa_id:
                logger.warning(f"Updating NCAA ID for {team.name}: {team.ncaa_id} -> {ncaa_id}")
                team.ncaa_id = ncaa_id
                team.updated_at = datetime.utcnow()

            return team.id, False

        # Strategy 4: Create new team
        logger.info(f"Creating new team: {name_short} (seoname: {seoname})")
       
        team = Team(
           ncaa_id=ncaa_id,  # This will be None if empty, not ''
           name=name_short,
           short_name=name_short,
           slug=seoname if seoname else self._generate_slug(name_short),
           is_active=True
        )
        
        session.add(team)
        session.flush()  # Get the ID without committing
        
        # Update caches with new team
        if seoname:
           self.seoname_cache[seoname] = team.id
        self.name_cache[name_lower] = team.id
        
        self.teams_created += 1
        logger.info(f"Created team #{team.id}: {team.name}")
        
        return team.id, True

    
    def _generate_slug(self, name: str) -> str:
        """
        Generate a URL-friendly slug from a team name.
        
        Examples:
            "Mount Union" -> "mount-union"
            "Wis.-Whitewater" -> "wis-whitewater"
            "St. John's (Minn.)" -> "st-johns-minn"
        """
        # Convert to lowercase
        slug = name.lower()
        
        # Remove parentheses and their contents
        slug = re.sub(r'\([^)]*\)', '', slug)
        
        # Replace non-alphanumeric characters with hyphens
        slug = re.sub(r'[^a-z0-9]+', '-', slug)
        
        # Remove leading/trailing hyphens
        slug = slug.strip('-')
        
        return slug

    def resolve_team_id(self, team_identifier: str) -> Optional[int]:
        """
        Quick lookup to get team ID from seoname or name.
        
        This is a read-only method that doesn't create teams.
        Used when you need a team ID quickly without database access.
        
        Args:
            team_identifier: seoname or team name
            
        Returns:
            team_id if found in cache, None otherwise
        """
        identifier_lower = team_identifier.lower()
        
        # Check seoname cache first
        if identifier_lower in self.seoname_cache:
            self.cache_hits += 1
            return self.seoname_cache[identifier_lower]
        
        # Check name cache
        if identifier_lower in self.name_cache:
            self.cache_hits += 1
            return self.name_cache[identifier_lower]
        
        # Not in cache
        self.cache_misses += 1
        return None

    def bulk_ensure_teams(self, session: Session, games_data: List[Dict]) -> Dict[str, int]:
        """Pre-process all teams from a list of games - FIXED VERSION."""
        teams_to_process = {}
        
        # Extract unique teams from all games
        for game in games_data:
            for team in game.get('teams', []):
                seoname = team.get('seoname', '').lower()
                
                if not seoname:  # Skip empty seonames
                    continue
                    
                # Skip if already in cache
                if seoname in self.seoname_cache:
                    continue
                
                # Skip if we've already seen this team in this batch
                if seoname in teams_to_process:
                    continue
                
                teams_to_process[seoname] = {
                    'seoname': seoname,
                    'nameShort': team.get('nameShort'),
                    'teamId': team.get('teamId')
                }
        
        logger.info(f"Processing {len(teams_to_process)} new teams")
        
        # Process each team
        team_mapping = {}
        for seoname, team_data in teams_to_process.items():
            team_id, was_created = self.find_or_create_team(session, team_data)
            team_mapping[seoname] = team_id
            
            if was_created:
                logger.debug(f"Created team: {team_data.get('nameShort')}")
        
        # DON'T FLUSH/COMMIT HERE - Let the main transaction handle it
        
        # Add existing cached teams to the mapping
        team_mapping.update(self.seoname_cache)
        
        logger.info(f"Bulk ensure complete: {self.teams_created} teams created")
        
        return team_mapping

    def get_team_by_id(self, session: Session, team_id: int) -> Optional[Team]:
        """
        Retrieve a team object by ID.
        Simple helper method for when you need the full team object.
        """
        return session.query(Team).filter(Team.id == team_id).first()

    def get_cache_stats(self) -> Dict:
        """
        Return cache performance statistics.
        Useful for monitoring and optimization.
        """
        total_lookups = self.cache_hits + self.cache_misses
        hit_rate = (self.cache_hits / total_lookups * 100) if total_lookups > 0 else 0
        
        return {
            'cache_hits': self.cache_hits,
            'cache_misses': self.cache_misses,
            'hit_rate': f"{hit_rate:.1f}%",
            'teams_created': self.teams_created,
            'cached_teams': len(self.seoname_cache)
        }
    
    def clear_cache(self):
        """
        Clear all caches and reset statistics.
        Useful for testing or if you suspect cache corruption.
        """
        self.seoname_cache.clear()
        self.name_cache.clear()
        self.cache_hits = 0
        self.cache_misses = 0
        self.teams_created = 0
        logger.info("Team cache cleared")
        
        # Reload from database
        self._initialize_cache()

    
        