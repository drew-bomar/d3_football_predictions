"""
NCAA Game Data Merger
Combines game info (with scores) from GraphQL with detailed stats from teamStats API
"""

import logging
from typing import Dict, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class NCAAGameDataMerger:
    """
    Merges game information from multiple NCAA API sources.
    
    Combines:
    - Basic game info and scores from GraphQL endpoint
    - Detailed team statistics from teamStats endpoint
    - Tracks team records and opponents for additional metrics
    """
    
    def __init__(self):
        # Track team records across the season
        self.team_records = {}  # {team_id: {'wins': 0, 'losses': 0, 'opponents': []}}
        self.team_elo_ratings = {}  # For future Elo implementation
        
    def merge_game_data(self, game_info: Dict, team_stats: Dict) -> Dict:
        """
        Merge game information with team statistics.
        
        Args:
            game_info: Game data from GraphQL (includes scores)
            team_stats: Detailed stats from teamStats endpoint
            
        Returns:
            Merged game data with all information
        """
        merged = {
            # Game metadata
            'contest_id': game_info.get('contest_id'),
            'date': game_info.get('start_date'),
            'time': game_info.get('start_time'),
            'status': game_info.get('status'),
            
            # Team information
            'home_team': game_info.get('home_team'),
            'home_team_id': game_info.get('home_team_id'),
            'away_team': game_info.get('away_team'),
            'away_team_id': game_info.get('away_team_id'),
            
            # Scores from game_info
            'home_score': game_info.get('home_score'),
            'away_score': game_info.get('away_score'),
            
            # Add all statistics from team_stats
            'stats': team_stats.get('stats', {}),
            
            # Calculate additional fields
            'home_won': game_info.get('home_winner', False),
            'away_won': game_info.get('away_winner', False),
            'margin': None,
            'total_points': None
        }
        
        # Calculate margin and total points
        try:
            home_score = int(merged['home_score']) if merged['home_score'] is not None else 0
            away_score = int(merged['away_score']) if merged['away_score'] is not None else 0
            merged['margin'] = home_score - away_score
            merged['total_points'] = home_score + away_score
        except (ValueError, TypeError):
            logger.warning(f"Could not calculate margin for game {merged['contest_id']}")
        
        # Add team records at time of game (if tracking)
        merged['home_record_before'] = self._get_team_record(merged['home_team_id'])
        merged['away_record_before'] = self._get_team_record(merged['away_team_id'])
        
        return merged
    
    def _get_team_record(self, team_id: str) -> Dict:
        """Get team's current record."""
        if team_id in self.team_records:
            record = self.team_records[team_id]
            return {
                'wins': record['wins'],
                'losses': record['losses'],
                'record_str': f"{record['wins']}-{record['losses']}"
            }
        return {'wins': 0, 'losses': 0, 'record_str': '0-0'}
    
    def update_team_records(self, merged_game: Dict):
        """
        Update team records after processing a game.
        
        Should be called after merge_game_data for each game,
        processing games in chronological order.
        """
        home_id = merged_game['home_team_id']
        away_id = merged_game['away_team_id']
        
        # Initialize records if needed
        for team_id in [home_id, away_id]:
            if team_id not in self.team_records:
                self.team_records[team_id] = {
                    'wins': 0,
                    'losses': 0,
                    'opponents': [],
                    'game_results': []
                }
        
        # Determine winner based on scores (not the winner flags which might be wrong)
        try:
            home_score = int(merged_game['home_score']) if merged_game['home_score'] is not None else 0
            away_score = int(merged_game['away_score']) if merged_game['away_score'] is not None else 0
            
            if home_score > away_score:
                # Home team won
                self.team_records[home_id]['wins'] += 1
                self.team_records[away_id]['losses'] += 1
                home_result = 'W'
                away_result = 'L'
            else:
                # Away team won (or tie, but treating as away win)
                self.team_records[home_id]['losses'] += 1
                self.team_records[away_id]['wins'] += 1
                home_result = 'L'
                away_result = 'W'
                
        except (ValueError, TypeError) as e:
            logger.warning(f"Could not determine winner for game {merged_game.get('contest_id')}: {e}")
            return
        
        # Track opponents and results for SOS calculation
        self.team_records[home_id]['opponents'].append({
            'opponent_id': away_id,
            'opponent_name': merged_game['away_team'],
            'result': home_result,
            'date': merged_game['date']
        })
        
        self.team_records[away_id]['opponents'].append({
            'opponent_id': home_id,
            'opponent_name': merged_game['home_team'],
            'result': away_result,
            'date': merged_game['date']
        })
    
    def calculate_strength_of_schedule(self, team_id: str) -> float:
        """
        Calculate strength of schedule for a team.
        
        Simple version: average winning percentage of opponents
        """
        if team_id not in self.team_records:
            return 0.0
        
        opponents = self.team_records[team_id]['opponents']
        if not opponents:
            return 0.0
        
        total_opponent_wins = 0
        total_opponent_games = 0
        
        for opp in opponents:
            opp_id = opp['opponent_id']
            if opp_id in self.team_records:
                opp_record = self.team_records[opp_id]
                total_opponent_wins += opp_record['wins']
                total_opponent_games += opp_record['wins'] + opp_record['losses']
        
        if total_opponent_games == 0:
            return 0.0
        
        return total_opponent_wins / total_opponent_games
    
    def process_week_games(self, games: List[Dict], stats_fetcher) -> List[Dict]:
        """
        Process all games for a week, merging data and updating records.
        
        Args:
            games: List of games from GraphQL
            stats_fetcher: Function to fetch team stats given contest_id
            
        Returns:
            List of merged game data
        """
        merged_games = []
        
        # Sort games by date/time to ensure proper record tracking
        sorted_games = sorted(games, key=lambda g: (g.get('start_date', ''), g.get('start_time', '')))
        
        for game in sorted_games:
            contest_id = game.get('contest_id')
            if not contest_id:
                logger.warning(f"No contest_id for game: {game}")
                continue
            
            # Fetch team stats
            try:
                team_stats = stats_fetcher(contest_id)
                if team_stats:
                    # Merge data
                    merged = self.merge_game_data(game, team_stats)
                    merged_games.append(merged)
                    
                    # Update records for future games
                    self.update_team_records(merged)
                    
                    # Calculate and add SOS
                    merged['home_sos'] = self.calculate_strength_of_schedule(merged['home_team_id'])
                    merged['away_sos'] = self.calculate_strength_of_schedule(merged['away_team_id'])
                    
                else:
                    logger.warning(f"No stats found for game {contest_id}")
                    
            except Exception as e:
                logger.error(f"Error processing game {contest_id}: {e}")
        
        return merged_games


# Example usage function
def example_usage():
    """Show how to use the merger with the NCAA scraper."""
    from ncaa_api_scraper_final import NCAAAPIScraper
    
    # Initialize scraper and merger
    scraper = NCAAAPIScraper()
    merger = NCAAGameDataMerger()
    
    # Process multiple weeks in order
    for week in range(1, 4):  # Weeks 1-3 as example
        print(f"\nProcessing Week {week}")
        
        # Get games for the week
        games = scraper.get_week_games(2024, week)
        
        # Process with merger (which tracks records)
        merged_games = merger.process_week_games(
            games, 
            lambda cid: scraper.get_team_stats(cid)
        )
        
        print(f"Processed {len(merged_games)} games")
        
        # Show example of first game
        if merged_games:
            game = merged_games[0]
            print(f"\nExample game:")
            print(f"  {game['away_team']} ({game['away_record_before']['record_str']}) @ "
                  f"{game['home_team']} ({game['home_record_before']['record_str']})")
            print(f"  Score: {game['away_score']} - {game['home_score']}")
            print(f"  Home SOS: {game['home_sos']:.3f}, Away SOS: {game['away_sos']:.3f}")
    
    return merger


if __name__ == "__main__":
    merger = example_usage()
    
    # Show final records for a few teams
    print("\n\nFinal team records:")
    for team_id, record in list(merger.team_records.items())[:5]:
        print(f"  Team {team_id}: {record['wins']}-{record['losses']}")
        print(f"    Opponents faced: {len(record['opponents'])}")