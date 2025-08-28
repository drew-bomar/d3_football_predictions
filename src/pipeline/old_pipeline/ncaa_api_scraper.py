"""
NCAA D3 Football API Scraper - Updated with correct JSON parsing
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
    
    def _parse_team_stats(self, json_data: Dict) -> Dict:
        """
        Parse the team statistics JSON into a standardized format.
        Now correctly handles the actual NCAA JSON structure.
        """
        result = {
            'teams_info': {},
            'stats': {}
        }
        
        # Parse team metadata
        meta_teams = json_data.get('meta', {}).get('teams', [])
        team_id_to_type = {}  # Map team ID to home/away
        
        for team in meta_teams:
            is_home = team.get('homeTeam') == 'true'
            team_type = 'home' if is_home else 'away'
            team_id = team.get('id')
            
            result['teams_info'][team_type] = {
                'name': team.get('shortname'),
                'id': team_id,
                'seo_name': team.get('seoName'),
                'color': team.get('color'),
                'abbr': team.get('sixCharAbbr')
            }
            
            team_id_to_type[team_id] = team_type
        
        # Parse stats for each team
        teams_data = json_data.get('teams', [])
        
        for team_data in teams_data:
            team_id = team_data.get('teamId')
            team_type = team_id_to_type.get(team_id)
            
            if not team_type:
                logger.warning(f"Unknown team ID: {team_id}")
                continue
            
            # Process each stat
            for stat_item in team_data.get('stats', []):
                self._parse_stat_item(stat_item, team_type, result['stats'])
        
        # Add calculated fields and mappings
        self._add_calculated_fields(result)
        self._apply_stat_mappings(result)
        
        return result
    
    def _parse_stat_item(self, stat_item: Dict, team_type: str, stats_dict: Dict):
        """
        Parse a single stat item and its breakdowns.
        """
        stat_name = stat_item.get('stat', '')
        stat_value = stat_item.get('data', '')
        
        # Create clean key name
        clean_name = self._clean_stat_name(stat_name)
        key = f'{team_type}_{clean_name}'
        
        # Handle different stat formats
        if 'Number-Lost' in stat_name:
            # e.g., "Fumbles: Number-Lost" with value "2-1"
            self._parse_number_lost_stat(stat_name, stat_value, team_type, stats_dict)
            
        elif 'Number-Yards' in stat_name:
            # e.g., "Penalties: Number-Yards" with value "6-55"
            self._parse_number_yards_stat(stat_name, stat_value, team_type, stats_dict)
            
        elif stat_name == 'Third-Down Conversions':
            # Special handling for third down
            self._parse_third_down_stat(stat_value, team_type, stats_dict)
            
        elif stat_name == 'Fourth-Down Conversions':
            # Fourth down conversions
            self._parse_fourth_down_stat(stat_value, team_type, stats_dict)
            
        else:
            # Regular stat
            stats_dict[key] = stat_value
        
        # Process breakdown if available
        breakdown = stat_item.get('breakdown', [])
        for breakdown_item in breakdown:
            breakdown_stat = breakdown_item.get('stat', '')
            breakdown_value = breakdown_item.get('data', '')
            
            # Create key for breakdown stat
            breakdown_clean = self._clean_stat_name(breakdown_stat)
            
            # For main stats like "Rushing" or "Passing", use the breakdown as suffix
            if clean_name in ['rushing', 'passing']:
                breakdown_key = f'{team_type}_{clean_name}_{breakdown_clean}'
            else:
                # For other stats, include both main and breakdown names
                breakdown_key = f'{team_type}_{clean_name}_{breakdown_clean}'
            
            stats_dict[breakdown_key] = breakdown_value
    
    def _clean_stat_name(self, stat_name: str) -> str:
        """Convert stat name to clean key format."""
        clean = stat_name.lower()
        clean = clean.replace(' ', '_')
        clean = clean.replace('.', '')
        clean = clean.replace(':', '')
        clean = clean.replace('-', '_')
        clean = clean.replace('__', '_')
        
        # Special replacements
        clean = clean.replace('1st_downs', 'first_downs')
        clean = clean.replace('avg_per_', 'avg_')
        
        return clean.strip('_')
    
    def _parse_number_lost_stat(self, stat_name: str, value: str, team_type: str, stats_dict: Dict):
        """Parse stats like 'Fumbles: Number-Lost' with value '2-1'."""
        base_name = stat_name.split(':')[0].strip().lower()
        
        if '-' in value:
            parts = value.split('-')
            if len(parts) == 2:
                stats_dict[f'{team_type}_{base_name}'] = parts[0].strip()
                stats_dict[f'{team_type}_{base_name}_lost'] = parts[1].strip()
    
    def _parse_number_yards_stat(self, stat_name: str, value: str, team_type: str, stats_dict: Dict):
        """Parse stats like 'Penalties: Number-Yards' with value '6-55'."""
        base_name = stat_name.split(':')[0].strip().lower()
        
        if '-' in value:
            parts = value.split('-')
            if len(parts) == 2:
                stats_dict[f'{team_type}_{base_name}_number'] = parts[0].strip()
                stats_dict[f'{team_type}_{base_name}_yards'] = parts[1].strip()
    
    def _parse_third_down_stat(self, value: str, team_type: str, stats_dict: Dict):
        """Parse third down conversions like '3-11'."""
        if '-' in value:
            parts = value.split('-')
            if len(parts) == 2:
                try:
                    made = int(parts[0].strip())
                    attempts = int(parts[1].strip())
                    
                    stats_dict[f'{team_type}_third_down_conversions'] = str(made)
                    stats_dict[f'{team_type}_third_down_att'] = str(attempts)
                    
                    # Calculate percentage
                    if attempts > 0:
                        pct = (made / attempts) * 100
                        stats_dict[f'{team_type}_third_down_pct'] = f"{pct:.1f}"
                    else:
                        stats_dict[f'{team_type}_third_down_pct'] = "0.0"
                except ValueError:
                    logger.warning(f"Could not parse third down stat: {value}")
                    stats_dict[f'{team_type}_third_down_conversions'] = value
    
    def _parse_fourth_down_stat(self, value: str, team_type: str, stats_dict: Dict):
        """Parse fourth down conversions like '1-2'."""
        if '-' in value:
            parts = value.split('-')
            if len(parts) == 2:
                try:
                    made = int(parts[0].strip())
                    attempts = int(parts[1].strip())
                    
                    stats_dict[f'{team_type}_fourth_down_conversions'] = str(made)
                    stats_dict[f'{team_type}_fourth_down_att'] = str(attempts)
                except ValueError:
                    logger.warning(f"Could not parse fourth down stat: {value}")
                    stats_dict[f'{team_type}_fourth_down_conversions'] = value
    
    def _add_calculated_fields(self, result: Dict):
        """Add calculated fields that data_processor.py expects."""
        stats = result['stats']
        
        # Calculate total return yards for each team
        for team_type in ['home', 'away']:
            kick_key = f'{team_type}_kickoff_returns_yards'
            punt_key = f'{team_type}_punt_returns_yards'
            int_key = f'{team_type}_interception_returns_yards'
            
            total = 0
            if kick_key in stats:
                try:
                    # Handle negative yards (like "1--1")
                    value = stats[kick_key].replace('--', '-')
                    total += int(value)
                except:
                    pass
            
            if punt_key in stats:
                try:
                    value = stats[punt_key].replace('--', '-')
                    total += int(value)
                except:
                    pass
                    
            if int_key in stats:
                try:
                    total += int(stats[int_key])
                except:
                    pass
            
            if total != 0:
                stats[f'{team_type}_total_return_yards'] = str(total)
    
    def _apply_stat_mappings(self, result: Dict):
        """Apply mappings to match data_processor expected names."""
        stats = result['stats']
        
        # Mappings from NCAA names to data_processor names
        mappings = {
            'rushing': 'net_yards_rushing',
            'passing': 'net_yards_passing',
            'passing_interceptions': 'interceptions',
            'interception_returns_yards': 'interception_return_yards',
            'punting_number': 'punts_number',
        }
        
        # Apply mappings
        for team_type in ['home', 'away']:
            for ncaa_name, processor_name in mappings.items():
                ncaa_key = f'{team_type}_{ncaa_name}'
                if ncaa_key in stats:
                    processor_key = f'{team_type}_{processor_name}'
                    stats[processor_key] = stats[ncaa_key]
    
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
    """Test the NCAA API scraper with updated parser."""
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
            print(f"\nTeams found:")
            for team_type, info in stats.get('teams_info', {}).items():
                print(f"  {team_type}: {info['name']}")
            
            # Show a few key stats
            print(f"\nSample stats:")
            sample_keys = [
                'home_first_downs', 'away_first_downs',
                'home_net_yards_rushing', 'away_net_yards_rushing',
                'home_third_down_pct', 'away_third_down_pct'
            ]
            
            for key in sample_keys:
                if key in stats.get('stats', {}):
                    print(f"  {key}: {stats['stats'][key]}")


if __name__ == "__main__":
    test_scraper()