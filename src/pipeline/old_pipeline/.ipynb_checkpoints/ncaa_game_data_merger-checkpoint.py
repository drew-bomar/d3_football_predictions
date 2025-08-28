"""
Enhanced NCAA Game Data Merger with opponent tracking and SOS calculation
"""

import logging
from typing import Dict, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class NCAAGameDataMerger:
    """
    Merges game information from multiple NCAA API sources.
    Now includes opponent tracking and strength of schedule calculations.
    """
    
    def __init__(self):
        # Track team records and opponent history
        self.team_records = {}  # {team_id: {'wins': 0, 'losses': 0, 'opponents': []}}
        self.team_elo_ratings = {}  # For future Elo implementation
        
        # Default starting Elo
        self.default_elo = 1500
        
    def merge_game_data(self, game_info: Dict, team_stats: Dict) -> Dict:
        """
        Merge game information with team statistics.
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
        
        # Add team records and SOS at time of game
        merged['home_record_before'] = self._get_team_record(merged['home_team_id'])
        merged['away_record_before'] = self._get_team_record(merged['away_team_id'])
        
        # Calculate SOS before this game
        merged['home_sos_before'] = self.calculate_strength_of_schedule(merged['home_team_id'])
        merged['away_sos_before'] = self.calculate_strength_of_schedule(merged['away_team_id'])
        
        # Add opponent history summary
        merged['home_opponents_before'] = self._get_opponents_summary(merged['home_team_id'])
        merged['away_opponents_before'] = self._get_opponents_summary(merged['away_team_id'])
        
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
    
    def _get_opponents_summary(self, team_id: str) -> Dict:
        """Get summary of opponents faced so far."""
        if team_id not in self.team_records:
            return {
                'count': 0,
                'wins_against': 0,
                'losses_against': 0,
                'opponent_combined_record': '0-0'
            }
        
        team_data = self.team_records[team_id]
        opponents = team_data.get('opponents', [])
        
        wins_against = sum(1 for opp in opponents if opp['result'] == 'W')
        losses_against = sum(1 for opp in opponents if opp['result'] == 'L')
        
        # Calculate combined record of all opponents
        total_opp_wins = 0
        total_opp_losses = 0
        
        for opp in opponents:
            opp_id = opp['opponent_id']
            if opp_id in self.team_records:
                total_opp_wins += self.team_records[opp_id]['wins']
                total_opp_losses += self.team_records[opp_id]['losses']
        
        return {
            'count': len(opponents),
            'wins_against': wins_against,
            'losses_against': losses_against,
            'opponent_combined_record': f"{total_opp_wins}-{total_opp_losses}"
        }
    
    def update_team_records(self, merged_game: Dict):
        """
        Update team records after processing a game.
        Now includes detailed opponent tracking.
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
        
        # Determine winner
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
                # Away team won
                self.team_records[home_id]['losses'] += 1
                self.team_records[away_id]['wins'] += 1
                home_result = 'L'
                away_result = 'W'
                
        except (ValueError, TypeError) as e:
            logger.warning(f"Could not determine winner for game {merged_game.get('contest_id')}: {e}")
            return
        
        # Track detailed opponent information
        game_date = merged_game.get('date', '')
        
        # For home team
        self.team_records[home_id]['opponents'].append({
            'opponent_id': away_id,
            'opponent_name': merged_game['away_team'],
            'result': home_result,
            'score': home_score,
            'opponent_score': away_score,
            'margin': home_score - away_score,
            'date': game_date,
            'location': 'home',
            'opponent_record_at_time': merged_game['away_record_before']['record_str'],
            'game_id': merged_game['contest_id']
        })
        
        # For away team
        self.team_records[away_id]['opponents'].append({
            'opponent_id': home_id,
            'opponent_name': merged_game['home_team'],
            'result': away_result,
            'score': away_score,
            'opponent_score': home_score,
            'margin': away_score - home_score,
            'date': game_date,
            'location': 'away',
            'opponent_record_at_time': merged_game['home_record_before']['record_str'],
            'game_id': merged_game['contest_id']
        })
    
    def calculate_strength_of_schedule(self, team_id: str, games_back: Optional[int] = None) -> float:
        """
        Calculate strength of schedule for a team.
        
        Args:
            team_id: Team identifier
            games_back: If specified, only look at last N games
            
        Returns:
            SOS as opponent winning percentage (0.0 to 1.0)
        """
        if team_id not in self.team_records:
            return 0.0
        
        opponents = self.team_records[team_id]['opponents']
        if not opponents:
            return 0.0
        
        # Optionally limit to recent games
        if games_back:
            opponents = opponents[-games_back:]
        
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
    
    def calculate_strength_of_victory(self, team_id: str) -> float:
        """
        Calculate strength of victory (average win % of defeated opponents).
        """
        if team_id not in self.team_records:
            return 0.0
        
        opponents = self.team_records[team_id]['opponents']
        defeated_opponents = [opp for opp in opponents if opp['result'] == 'W']
        
        if not defeated_opponents:
            return 0.0
        
        total_wins = 0
        total_games = 0
        
        for opp in defeated_opponents:
            opp_id = opp['opponent_id']
            if opp_id in self.team_records:
                opp_record = self.team_records[opp_id]
                total_wins += opp_record['wins']
                total_games += opp_record['wins'] + opp_record['losses']
        
        if total_games == 0:
            return 0.0
        
        return total_wins / total_games
    
    def get_common_opponents(self, team1_id: str, team2_id: str) -> List[Dict]:
        """
        Find common opponents between two teams.
        Useful for comparing teams that haven't played each other.
        """
        if team1_id not in self.team_records or team2_id not in self.team_records:
            return []
        
        team1_opponents = {opp['opponent_id']: opp for opp in self.team_records[team1_id]['opponents']}
        team2_opponents = {opp['opponent_id']: opp for opp in self.team_records[team2_id]['opponents']}
        
        common_ids = set(team1_opponents.keys()) & set(team2_opponents.keys())
        
        common_opponents = []
        for opp_id in common_ids:
            common_opponents.append({
                'opponent_id': opp_id,
                'opponent_name': team1_opponents[opp_id]['opponent_name'],
                'team1_result': team1_opponents[opp_id]['result'],
                'team1_margin': team1_opponents[opp_id]['margin'],
                'team2_result': team2_opponents[opp_id]['result'],
                'team2_margin': team2_opponents[opp_id]['margin']
            })
        
        return common_opponents
    
    def calculate_basic_elo_ratings(self, k_factor: float = 32.0):
        """
        Calculate basic Elo ratings based on games played.
        This is a simplified version - full implementation would update after each game.
        """
        # Initialize all teams with default rating
        for team_id in self.team_records:
            self.team_elo_ratings[team_id] = self.default_elo
        
        # Process all games chronologically
        all_games = []
        for team_id, record in self.team_records.items():
            for opp in record['opponents']:
                # Only add home games to avoid duplicates
                if opp['location'] == 'home':
                    all_games.append({
                        'home_id': team_id,
                        'away_id': opp['opponent_id'],
                        'home_score': opp['score'],
                        'away_score': opp['opponent_score'],
                        'date': opp['date']
                    })
        
        # Sort by date
        all_games.sort(key=lambda x: x['date'])
        
        # Update ratings for each game
        for game in all_games:
            home_id = game['home_id']
            away_id = game['away_id']
            
            if home_id in self.team_elo_ratings and away_id in self.team_elo_ratings:
                # Get current ratings
                home_elo = self.team_elo_ratings[home_id]
                away_elo = self.team_elo_ratings[away_id]
                
                # Calculate expected scores
                home_expected = 1 / (1 + 10 ** ((away_elo - home_elo) / 400))
                away_expected = 1 - home_expected
                
                # Actual results
                if game['home_score'] > game['away_score']:
                    home_actual = 1
                    away_actual = 0
                else:
                    home_actual = 0
                    away_actual = 1
                
                # Update ratings
                self.team_elo_ratings[home_id] = home_elo + k_factor * (home_actual - home_expected)
                self.team_elo_ratings[away_id] = away_elo + k_factor * (away_actual - away_expected)
        
        return self.team_elo_ratings


# Test the enhanced merger
def test_enhanced_features():
    """Test the new SOS and opponent tracking features."""
    from ncaa_api_scraper import NCAAAPIScraper
    
    print("Testing Enhanced Merger Features")
    print("=" * 50)
    
    scraper = NCAAAPIScraper()
    merger = NCAAGameDataMerger()
    
    # Get some games
    games = scraper.get_week_games(2024, 9)[:10]  # First 10 games
    
    # Process them
    for i, game in enumerate(games):
        if game.get('contest_id') and game.get('home_score') is not None:
            print(f"\nProcessing game {i+1}: {game['away_team']} @ {game['home_team']}")
            
            stats = scraper.get_team_stats(game['contest_id'])
            if stats:
                merged = merger.merge_game_data(game, stats)
                merger.update_team_records(merged)
                
                # Show SOS info
                print(f"  {game['home_team']} SOS: {merged['home_sos_before']:.3f}")
                print(f"  {game['away_team']} SOS: {merged['away_sos_before']:.3f}")
    
    # Show final SOS rankings
    print("\n" + "=" * 50)
    print("Final Strength of Schedule Rankings:")
    
    sos_rankings = []
    for team_id, record in merger.team_records.items():
        # Find team name
        team_name = team_id
        for game in games:
            if game.get('home_team_id') == team_id:
                team_name = game['home_team']
                break
            elif game.get('away_team_id') == team_id:
                team_name = game['away_team']
                break
        
        sos = merger.calculate_strength_of_schedule(team_id)
        sov = merger.calculate_strength_of_victory(team_id)
        
        sos_rankings.append({
            'team': team_name,
            'record': f"{record['wins']}-{record['losses']}",
            'sos': sos,
            'sov': sov
        })
    
    # Sort by SOS
    sos_rankings.sort(key=lambda x: x['sos'], reverse=True)
    
    print(f"\n{'Team':<20} {'Record':<8} {'SOS':<8} {'SOV':<8}")
    print("-" * 44)
    for team in sos_rankings[:10]:
        print(f"{team['team']:<20} {team['record']:<8} {team['sos']:<8.3f} {team['sov']:<8.3f}")


if __name__ == "__main__":
    test_enhanced_features()