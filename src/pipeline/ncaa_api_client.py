"""
NCAA API Client - Fetches game data from NCAA's GraphQL endpoint
Handles the two main queries we need:
1. Week schedule (all games for a week)
2. Game statistics (detailed stats for one game)
"""

import requests
import json
import time
import logging
from typing import Dict, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class NCAAAPIClient:
    """
    Clean interface to NCAA's GraphQL API for D3 football data.
    
    This class only fetches and returns data - no database operations.
    All data transformation happens in other modules.
    """

    def __init__(self, delay : float = 1.0):
        """
        Initialize the API client with rate limiting.
        
        Args:
            delay: Seconds to wait between API calls (be respectful!)
        """
        self.delay = delay
        self.base_url = "https://sdataprod.ncaa.com/" 
        
        #headers to make look like a normal browser request
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json'
        }

        # Query hash for the GetContests GraphQL query (found in your examples)
        self.contests_query_hash = "c1bd3e9f56889ebca2937ecf24a2d62ccbe771939687b5ef258a51a2110c1d57"
        self.team_stats_query_hash = "b41348ee662d9236483167395b16bb6ab36b12e2908ef6cd767685ea8a2f59bd"
        
        self.last_request_time = 0

    def _rate_limit(self):
        """
        Ensure we don't hammer the NCAA API(don't want to get blocked).
        Waits if necessary to maintain our delay between requests.
        """
        current_time=time.time()
        time_since_last = current_time - self.last_request_time

        if time_since_last < self.delay:
            sleep_time = self.delay - time_since_last
            logger.debug(f"Rate Limiting: sleeping {sleep_time:.1f} seconds")
            time.sleep(sleep_time)

        self.last_request_time = time.time()

    def get_week_games(self, year: int, week: int) -> dict:
        """
        Fetch all games for a specific week.
        
        Args:
            year: Season year (e.g., 2024)
            week: Week number (1-15 typically)
            
        Returns:
            Dict containing:
                - 'success': Boolean indicating if request succeeded
                - 'games': List of game dictionaries
                - 'error': Error message if failed
        """
        self._rate_limit

        # Build the GraphQL query parameters
        variables = {
            "sportCode": "MFB",
            "division": 3,
            "seasonYear": year,
            "contestDate": None,
            "week": week
        }

        extensions = {
            "persistedQuery": {
                "version": 1,
                "sha256Hash": self.contests_query_hash
            }
        }

        params = {
            "meta": "GetContests_web",
            "extensions": json.dumps(extensions, separators=(',', ':')),
            "queryName": "GetContests_web", 
            "variables": json.dumps(variables, separators=(',', ':'))
        }

        logger.info(f"Fetching games for {year} Week {week}")

        try:
            response = requests.get(self.base_url, params = params, headers = self.headers)
            response.raise_for_status()

            data = response.json()
            contests = data.get('data', {}).get('contests', [])

            logger.info(f"Successfully fetched {len(contests)} games for Week {week}")

            return {
                'success': True,
                'games': contests,
                'year': year,
                'week': week
            }
        except requests.RequestException as e:
            logger.error(f"API request failed for {year} Week {week}: {e}")
            return {
                'success': False,
                'games': [],
                'error': str(e)
            }
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON response: {e}")
            return {
                'success': False,
                'games': [],
                'error': f"Invalid JSON: {e}"
            }
            
    def get_game_stats(self, contest_id: int) -> dict:
        """
        Fetch detailed statistics for a specific game.
        
        Args:
            contest_id: The NCAA contest ID (e.g., 6308940)
            
        Returns:
            Dict containing:
                - 'success': Boolean indicating if request succeeded
                - 'stats': Dict with detailed team statistics
                - 'error': Error message if failed
        """
        self._rate_limit()

        # Build the GraphQL query for team stats
        variables = {
            "contestId": str(contest_id),
            "staticTestEnv": None  # This is in the URL you provided
        }
        
        extensions = {
            "persistedQuery": {
                "version": 1,
                "sha256Hash": "b41348ee662d9236483167395b16bb6ab36b12e2908ef6cd767685ea8a2f59bd"
            }
        }
        
        params = {
            "meta": "NCAA_GetGamecenterTeamStatsFootballById_web",  # Corrected
            "extensions": json.dumps(extensions, separators=(',', ':')),
            "queryName": "NCAA_GetGamecenterTeamStatsFootballById_web",  # Corrected
            "variables": json.dumps(variables, separators=(',', ':'))
        }
        
        logger.info(f"Fetching stats for game {contest_id}")

        try:
            response= requests.get(self.base_url, params = params, headers=self.headers)
            response.raise_for_status()

            data = response.json()

            boxscore = data.get('data', {}).get('boxscore', {})

            if not boxscore:
                logger.warning(f"No boxscore data found for game {contest_id}")
                return {
                    'success': False,
                    'contest_id': contest_id,
                    'error': 'No boxscore data in response'
                }

            # Extract the key information
            result = {
                'success': True,
                'contest_id': boxscore.get('contestId'),
                'description': boxscore.get('description'),  # "Team A vs Team B"
                'status': boxscore.get('status'),  # "F" for final
                'period': boxscore.get('period'),  # "FINAL"
                'teams': boxscore.get('teams', []),  # Team info with names, colors, etc.
                'team_stats': []  # Will populate with parsed stats
            }

            for team_boxscore in boxscore.get('teamBoxscore', []):
                team_id = team_boxscore.get('teamId')
                stats = team_boxscore.get('teamStats', {})

                # Find the corresponding team info
                team_info = next((t for t in result['teams'] if int(t.get('teamId', 0)) == team_id), {})
                
                    # Flatten the nested stats structure
                parsed_stats = {
                    'team_id': team_id,
                    'team_name': team_info.get('nameShort', 'Unknown'),
                    'is_home': team_info.get('isHome', False),
                    
                    # Basic stats
                    'first_downs': stats.get('firstDowns'),
                    'first_downs_passing': stats.get('firstDownsPassing'),
                    'first_downs_rushing': stats.get('firstDownsRushing'),
                    'first_downs_penalty': stats.get('firstDownsPenalty'),
                    
                    # Third/Fourth down conversions
                    'third_down_conversions': stats.get('thirdDowns'),
                    'third_down_attempts': stats.get('thirdDownAttempts'),
                    'fourth_down_conversions': stats.get('fourthDowns'),
                    'fourth_down_attempts': stats.get('fourthDownAttempts'),
                    
                    # Turnovers
                    'fumbles': stats.get('fumbles'),
                    'fumbles_lost': stats.get('fumblesLost'),
                    
                    # Penalties
                    'penalties': stats.get('penalty'),
                    'penalty_yards': stats.get('penaltyYards'),
                    
                    # Total offense
                    'total_plays': stats.get('teamPlays'),
                    'total_yards': stats.get('teamYards'),
                    'yards_per_play': stats.get('teamAverage'),
                }
                
                # Add passing stats
                passing = stats.get('TeamPassingStats', {})
                parsed_stats.update({
                    'passing_attempts': passing.get('passingAttempts'),
                    'passing_completions': passing.get('passingCompletions'),
                    'passing_yards': passing.get('passingYards'),
                    'passing_tds': passing.get('passingTDs'),
                    'passing_interceptions': passing.get('passingInterceptions'),
                    'passing_long': passing.get('passingLong'),
                })
                
                # Add rushing stats
                rushing = stats.get('TeamRushingStats', {})
                parsed_stats.update({
                    'rushing_attempts': rushing.get('rushingAttempts'),
                    'rushing_yards': rushing.get('rushingYards'),
                    'rushing_tds': rushing.get('rushingTDs'),
                    'rushing_long': rushing.get('rushingLong'),
                })
                
                # Add defensive stats
                defense = stats.get('TeamDefenseStats', {})
                parsed_stats.update({
                    'defense_interceptions': defense.get('defenseInterceptions'),
                    'fumbles_forced': defense.get('fumblesForced'),
                    'fumbles_recovered': defense.get('fumblesRecovered'),
                    'sacks': defense.get('sacks'),
                    'tackles_for_loss': defense.get('lossTackles'),
                    'total_tackles': defense.get('totalTackles'),
                })
                
                # Add special teams stats
                punting = stats.get('TeamPuntingStats', {})
                parsed_stats.update({
                    'punts': punting.get('puntingPunts'),
                    'punt_yards': punting.get('puntingYards'),
                    'punt_average': punting.get('puntingAverage'),
                })
                
                # Add return stats
                kick_returns = stats.get('TeamKickReturnsStats', {})
                parsed_stats.update({
                    'kick_returns': kick_returns.get('kickReturns'),
                    'kick_return_yards': kick_returns.get('kickReturnYards'),
                    'kick_return_average': kick_returns.get('kickReturnAverage'),
                })
                
                punt_returns = stats.get('TeamPuntReturnsStats', {})
                parsed_stats.update({
                    'punt_returns': punt_returns.get('puntReturns'),
                    'punt_return_yards': punt_returns.get('puntReturnYards'),
                    'punt_return_average': punt_returns.get('puntReturnAverage'),
                })
                
                result['team_stats'].append(parsed_stats)
            
            logger.info(f"Successfully parsed stats for game {contest_id}: {result['description']}")
            return result
                
        except requests.RequestException as e:
            logger.error(f"API request failed for game {contest_id}: {e}")
            return {
                'success': False,
                'contest_id': contest_id,
                'error': str(e)
            }
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON response: {e}")
            return {
                'success': False,
                'contest_id': contest_id,
                'error': f"Invalid JSON: {e}"
            }
        except Exception as e:
            logger.error(f"Unexpected error processing game {contest_id}: {e}")
            return {
                'success': False,
                'contest_id': contest_id,
                'error': str(e)
            }
