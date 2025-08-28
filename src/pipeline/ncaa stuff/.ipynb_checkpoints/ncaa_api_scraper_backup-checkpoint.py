"""
NCAA D3 Football API Scraper
Uses the discovered GraphQL API for game lists and JSON endpoints for team stats
"""

import requests
import json
import time
import logging
from typing import Dict, List, Optional
from urllib.parse import quote

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class NCAAAPIScraper:
    """
    Scraper for NCAA D3 football using official API endpoints.
    
    Endpoints:
    - Game list: GraphQL query at sdataprod.ncaa.com
    - Team stats: https://data.ncaa.com/casablanca/game/{game_id}/teamStats.json
    """
    
    def __init__(self, delay: float = 1.0):
        self.delay = delay
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json'
        }
        
        # GraphQL query components
        self.graphql_base = "https://sdataprod.ncaa.com/"
        self.query_hash = "c1bd3e9f56889ebca2937ecf24a2d62ccbe771939687b5ef258a51a2110c1d57"
    
    def get_week_games(self, year: int, week: int) -> List[Dict]:
        """
        Get all games for a specific week using the GraphQL API.
        
        Args:
            year: Season year (e.g., 2024)
            week: Week number (1-15)
            
        Returns:
            List of game dictionaries with contestId, teams, scores, etc.
        """
        # Build compact JSON strings (no spaces after colons/commas)
        variables = f'{{"sportCode":"MFB","division":3,"seasonYear":{year},"contestDate":null,"week":{week}}}'
        extensions = f'{{"persistedQuery":{{"version":1,"sha256Hash":"{self.query_hash}"}}}}'
        
        params = {
            "meta": "GetContests_web",
            "extensions": extensions,
            "queryName": "GetContests_web",
            "variables": variables
        }
        
        logger.info(f"Fetching games for {year} Week {week}")
        
        try:
            response = requests.get(self.graphql_base, params=params, headers=self.headers)
            response.raise_for_status()
            
            data = response.json()
            contests = data.get('data', {}).get('contests', [])
            
            logger.info(f"Found {len(contests)} games for Week {week}")
            
            # Parse the contests into a simpler format
            games = []
            for contest in contests:
                game = self._parse_contest(contest)
                if game:
                    games.append(game)
            
            return games
            
        except Exception as e:
            logger.error(f"Error fetching week {week}: {e}")
            return []
            
            data = response.json()
            contests = data.get('data', {}).get('contests', [])
            
            logger.info(f"Found {len(contests)} games for Week {week}")
            
            # Parse the contests into a simpler format
            games = []
            for contest in contests:
                game = self._parse_contest(contest)
                if game:
                    games.append(game)
            
            return games
            
        except Exception as e:
            logger.error(f"Error fetching week {week}: {e}")
            return []
    
    def _parse_contest(self, contest: Dict) -> Optional[Dict]:
        """
        Parse a contest object from the GraphQL response.
        """
        try:
            teams = contest.get('teams', [])
            if len(teams) < 2:
                return None
            
            # Find home and away teams
            home_team = next((t for t in teams if t.get('isHome')), None)
            away_team = next((t for t in teams if not t.get('isHome')), None)
            
            if not home_team or not away_team:
                return None
            
            game = {
                'contest_id': contest.get('contestId'),
                'game_url': contest.get('url'),
                'status': contest.get('gameState'),
                'start_date': contest.get('startDate'),
                'start_time': contest.get('startTime'),
                
                'home_team': home_team.get('nameShort'),
                'home_team_id': home_team.get('seoname'),
                'home_score': home_team.get('score'),
                'home_winner': home_team.get('isWinner'),
                
                'away_team': away_team.get('nameShort'),
                'away_team_id': away_team.get('seoname'),
                'away_score': away_team.get('score'),
                'away_winner': away_team.get('isWinner'),
                
                'final_message': contest.get('finalMessage'),
                'current_period': contest.get('currentPeriod')
            }
            
            return game
            
        except Exception as e:
            logger.error(f"Error parsing contest: {e}")
            return None
    
    def get_team_stats(self, contest_id: str) -> Optional[Dict]:
        """
        Get detailed team statistics for a specific game.
        
        Args:
            contest_id: The NCAA contest ID
            
        Returns:
            Dict with detailed team statistics
        """
        url = f"https://data.ncaa.com/casablanca/game/{contest_id}/teamStats.json"
        
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            
            data = response.json()
            return self._parse_team_stats(data)
            
        except Exception as e:
            logger.error(f"Error fetching stats for game {contest_id}: {e}")
            return None
    
    def _parse_team_stats(self, data: Dict) -> Dict:
        """
        Parse team statistics JSON into a standardized format.
        
        The teamStats.json structure:
        - meta: contains team information
        - teams: array with each team's statistics
        """
        result = {
            'teams_info': {},
            'stats': {}
        }
        
        # Extract team metadata
        meta_teams = data.get('meta', {}).get('teams', [])
        for team in meta_teams:
            team_type = 'home' if team.get('homeTeam') == 'true' else 'away'
            result['teams_info'][team_type] = {
                'name': team.get('shortname'),
                'id': team.get('id'),
                'seo_name': team.get('seoName'),
                'color': team.get('color')
            }
        
        # Parse statistics for each team
        teams_stats = data.get('teams', [])
        
        for team_data in teams_stats:
            team_id = team_data.get('teamId')
            
            # Determine if this is home or away team
            team_type = None
            for t_type, t_info in result['teams_info'].items():
                if t_info['id'] == team_id:
                    team_type = t_type
                    break
            
            if not team_type:
                continue
            
            # Parse each stat
            for stat_item in team_data.get('stats', []):
                stat_name = stat_item.get('stat', '')
                stat_value = stat_item.get('data', '')
                
                # Clean stat name for use as key
                clean_name = stat_name.lower().replace(' ', '_').replace('-', '_').replace(':', '').replace('.', '')
                
                # Store the main stat value
                result['stats'][f'{team_type}_{clean_name}'] = stat_value
                
                # Handle special cases with compound values
                if 'Number-Lost' in stat_name:
                    # e.g., "Fumbles: Number-Lost" with value "2-1"
                    if '-' in stat_value:
                        parts = stat_value.split('-')
                        if len(parts) == 2:
                            base_name = clean_name.replace('_number_lost', '')
                            result['stats'][f'{team_type}_{base_name}'] = parts[0]
                            result['stats'][f'{team_type}_{base_name}_lost'] = parts[1]
                
                elif 'Number-Yards' in stat_name:
                    # e.g., "Penalties: Number-Yards" with value "6-55"
                    if '-' in stat_value:
                        parts = stat_value.split('-')
                        if len(parts) == 2:
                            base_name = clean_name.replace('_number_yards', '')
                            result['stats'][f'{team_type}_{base_name}_number'] = parts[0]
                            result['stats'][f'{team_type}_{base_name}_yards'] = parts[1]
                
                # Parse breakdown stats if available
                breakdown = stat_item.get('breakdown', [])
                for breakdown_item in breakdown:
                    breakdown_stat = breakdown_item.get('stat', '')
                    breakdown_value = breakdown_item.get('data', '')
                    
                    # Create key combining main stat and breakdown stat
                    breakdown_clean = breakdown_stat.lower().replace(' ', '_').replace('.', '').replace('-', '_')
                    breakdown_key = f'{team_type}_{clean_name}_{breakdown_clean}'
                    
                    result['stats'][breakdown_key] = breakdown_value
        
        # Add calculated fields that data_processor.py expects
        self._add_calculated_fields(result)
        
        return result
    
    def _add_calculated_fields(self, result: Dict):
        """Add calculated fields that match data_processor.py expectations."""
        stats = result['stats']
        
        # Extract scores if available (might need to get from game list)
        # For now, these would need to be added from the contest data
        
        # Add any derived stats
        for team_type in ['home', 'away']:
            # Parse third down conversions (e.g., "3-11" -> 3 conversions, 11 attempts)
            third_down_key = f'{team_type}_third_down_conversions'
            if third_down_key in stats:
                value = stats[third_down_key]
                if '-' in value:
                    parts = value.split('-')
                    if len(parts) == 2:
                        stats[f'{team_type}_third_down_conversions_made'] = parts[0]
                        stats[f'{team_type}_third_down_att'] = parts[1]
                        # Calculate percentage
                        try:
                            made = int(parts[0])
                            attempts = int(parts[1])
                            if attempts > 0:
                                pct = (made / attempts) * 100
                                stats[f'{team_type}_third_down_pct'] = f"{pct:.1f}"
                        except:
                            pass
    
    def scrape_week_complete(self, year: int, week: int, include_stats: bool = True) -> Dict:
        """
        Complete week scraping: get games and optionally their detailed stats.
        
        Args:
            year: Season year
            week: Week number
            include_stats: Whether to fetch detailed stats for each game
            
        Returns:
            Dict with games list and summary
        """
        logger.info(f"Starting complete scrape for {year} Week {week}")
        
        # Get all games for the week
        games = self.get_week_games(year, week)
        
        if not games:
            logger.warning(f"No games found for {year} Week {week}")
            return {'games': [], 'summary': {'total': 0}}
        
        # Optionally get detailed stats
        if include_stats:
            games_with_stats = 0
            
            for i, game in enumerate(games):
                if game.get('contest_id'):
                    logger.info(f"Fetching stats for game {i+1}/{len(games)}: "
                              f"{game['away_team']} @ {game['home_team']}")
                    
                    if i > 0:
                        time.sleep(self.delay)  # Rate limiting
                    
                    stats = self.get_team_stats(game['contest_id'])
                    if stats:
                        game['team_stats'] = stats
                        games_with_stats += 1
            
            summary = {
                'total': len(games),
                'with_stats': games_with_stats,
                'success_rate': games_with_stats / len(games) if games else 0
            }
        else:
            summary = {'total': len(games)}
        
        logger.info(f"Week {week} complete: {len(games)} games found")
        
        return {
            'games': games,
            'summary': summary,
            'year': year,
            'week': week
        }


def test_scraper():
    """Test the NCAA API scraper."""
    scraper = NCAAAPIScraper()
    
    # Test getting games for a week
    print("Testing Week 9, 2024...")
    games = scraper.get_week_games(2024, 9)
    
    print(f"\nFound {len(games)} games")
    
    # Show first few games
    for game in games[:3]:
        print(f"\n{game['away_team']} @ {game['home_team']}")
        print(f"  Score: {game['away_score']} - {game['home_score']}")
        print(f"  Status: {game['status']}")
        print(f"  Contest ID: {game['contest_id']}")
    
    # Test getting stats for one game
    if games and games[0].get('contest_id'):
        print(f"\nTesting stats for game: {games[0]['contest_id']}")
        stats = scraper.get_team_stats(games[0]['contest_id'])
        
        if stats:
            print(f"Stats found for: {stats.get('home_team')} vs {stats.get('away_team')}")
            # Show a few stats
            for key, value in list(stats.get('stats', {}).items())[:5]:
                print(f"  {key}: {value}")
    
    # Test complete week scrape
    print("\n\nTesting complete week scrape (without detailed stats)...")
    result = scraper.scrape_week_complete(2024, 9, include_stats=False)
    print(f"Summary: {result['summary']}")


if __name__ == "__main__":
    test_scraper()