"""
NCAA Season Pipeline Manager
Handles collection of full seasons of D3 football data with incremental saves and resume capability
"""

import json
import os
import time
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import pickle
import pandas as pd

from ncaa_api_scraper import NCAAAPIScraper
from ncaa_game_data_merger import NCAAGameDataMerger
from data_processor import D3DataProcessor
from ncaa_translation_layer import NCAAToProcessorTranslator, integrate_with_data_processor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class NCAASeasonPipeline:
    """
    Manages collection of full seasons of NCAA D3 football data.
    
    Features:
    - Process weeks in chronological order for accurate record tracking
    - Save progress incrementally after each week
    - Resume from interruptions
    - Convert to data_processor format
    - Track collection statistics
    """
    
    def __init__(self, data_dir: str = "ncaa_data", delay: float = 1.5):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        
        # Create subdirectories
        self.raw_dir = self.data_dir / "raw"
        self.processed_dir = self.data_dir / "processed"
        self.checkpoints_dir = self.data_dir / "checkpoints"
        
        for dir in [self.raw_dir, self.processed_dir, self.checkpoints_dir]:
            dir.mkdir(exist_ok=True)
        
        # Initialize components
        self.scraper = NCAAAPIScraper(delay=delay)
        self.merger = NCAAGameDataMerger()
        self.processor = D3DataProcessor()
        self.translator = NCAAToProcessorTranslator()
        
        # Store cumulative data for rolling stats
        self.cumulative_team_games = []
        
        # Pipeline state
        self.pipeline_state = {
            'seasons_completed': [],
            'current_season': None,
            'current_week': None,
            'total_games_processed': 0,
            'total_games_failed': 0,
            'start_time': None,
            'last_checkpoint': None
        }
        
        # Load existing state if available
        self._load_pipeline_state()
    
    def collect_season(self, year: int, start_week: int = 1, end_week: int = 15) -> Dict:
        """
        Collect data for a full season.
        
        Args:
            year: Season year (e.g., 2024)
            start_week: Starting week (default 1)
            end_week: Ending week (default 15)
            
        Returns:
            Summary statistics of the collection
        """
        logger.info(f"Starting collection for {year} season (weeks {start_week}-{end_week})")
        
        self.pipeline_state['current_season'] = year
        self.pipeline_state['start_time'] = datetime.now().isoformat()
        
        season_summary = {
            'year': year,
            'weeks_processed': 0,
            'total_games': 0,
            'successful_games': 0,
            'failed_games': 0,
            'teams_seen': set(),
            'collection_time': 0
        }
        
        start_time = time.time()
        
        # Check if we're resuming
        resume_week = self._get_resume_week(year)
        if resume_week > start_week:
            logger.info(f"Resuming from week {resume_week}")
            start_week = resume_week
            # Load merger state and cumulative data from checkpoint
            self._load_merger_checkpoint(year, resume_week - 1)
            self._load_cumulative_data(year, resume_week - 1)
        
        # Process each week in order
        for week in range(start_week, end_week + 1):
            self.pipeline_state['current_week'] = week
            
            logger.info(f"\n{'='*60}")
            logger.info(f"Processing {year} Week {week}")
            logger.info(f"{'='*60}")
            
            week_data = self._process_week(year, week)
            
            if week_data:
                season_summary['weeks_processed'] += 1
                season_summary['total_games'] += week_data['total_games']
                season_summary['successful_games'] += week_data['successful_games']
                season_summary['failed_games'] += week_data['failed_games']
                season_summary['teams_seen'].update(week_data['teams_seen'])
                
                # Save checkpoint
                self._save_checkpoint(year, week)
                
                # Update pipeline state
                self.pipeline_state['total_games_processed'] += week_data['successful_games']
                self.pipeline_state['total_games_failed'] += week_data['failed_games']
                self._save_pipeline_state()
                
                logger.info(f"Week {week} complete: {week_data['successful_games']}/{week_data['total_games']} games")
                
                # Only show SOS leaders if we have data
                if self.merger.team_records:
                    logger.info(f"Running SOS leaders: {self._get_sos_leaders(3)}")
            else:
                logger.warning(f"Week {week} failed - no data collected")
            
            # Brief pause between weeks
            if week < end_week:
                time.sleep(2)
        
        # Final processing
        season_summary['collection_time'] = time.time() - start_time
        season_summary['teams_seen'] = len(season_summary['teams_seen'])
        
        # Calculate success rate safely
        if season_summary['total_games'] > 0:
            success_rate = season_summary['successful_games'] / season_summary['total_games']
        else:
            success_rate = 0
        
        # Mark season as complete
        self.pipeline_state['seasons_completed'].append(year)
        self.pipeline_state['current_season'] = None
        self._save_pipeline_state()
        
        # Generate final season report
        self._generate_season_report(year, season_summary)
        
        # Generate final rolling statistics for the complete season
        self._generate_final_rolling_stats(year)
        
        logger.info(f"\nSeason {year} collection complete!")
        logger.info(f"Total time: {season_summary['collection_time']/60:.1f} minutes")
        logger.info(f"Games collected: {season_summary['successful_games']}/{season_summary['total_games']}")
        
        # Clear cumulative data for next season
        self.cumulative_team_games = []
        
        return season_summary
    
    def _process_week(self, year: int, week: int) -> Optional[Dict]:
        """
        Process a single week of games.
        
        Returns:
            Week summary statistics
        """
        week_summary = {
            'year': year,
            'week': week,
            'total_games': 0,
            'successful_games': 0,
            'failed_games': 0,
            'teams_seen': set(),
            'games': []
        }
        
        try:
            # Get games for the week
            games = self.scraper.get_week_games(year, week)
            
            if not games:
                logger.warning(f"No games found for {year} week {week}")
                return None
            
            week_summary['total_games'] = len(games)
            logger.info(f"Found {len(games)} games for week {week}")
            
            # Process each game
            for i, game in enumerate(games):
                if not game.get('contest_id'):
                    logger.debug(f"Skipping game without contest_id")
                    continue
                
                # Skip if no scores (game might be cancelled/postponed)
                if game.get('home_score') is None or game.get('away_score') is None:
                    logger.debug(f"Skipping game without scores: {game.get('contest_id')}")
                    continue
                
                logger.info(f"Processing game {i+1}/{len(games)}: "
                          f"{game['away_team']} @ {game['home_team']} "
                          f"({game['away_score']}-{game['home_score']})")
                
                # Rate limiting
                if i > 0:
                    time.sleep(self.scraper.delay)
                
                # Get team stats
                try:
                    team_stats = self.scraper.get_team_stats(game['contest_id'])
                    
                    if team_stats:
                        # Merge game data
                        merged_game = self.merger.merge_game_data(game, team_stats)
                        merged_game['year'] = year
                        merged_game['week'] = week
                        
                        # Update team records
                        self.merger.update_team_records(merged_game)
                        
                        # Add current SOS to the merged game
                        merged_game['home_sos_current'] = self.merger.calculate_strength_of_schedule(
                            merged_game['home_team_id']
                        )
                        merged_game['away_sos_current'] = self.merger.calculate_strength_of_schedule(
                            merged_game['away_team_id']
                        )
                        
                        week_summary['games'].append(merged_game)
                        week_summary['successful_games'] += 1
                        week_summary['teams_seen'].add(game['home_team'])
                        week_summary['teams_seen'].add(game['away_team'])
                        
                    else:
                        logger.warning(f"No stats returned for game {game['contest_id']}")
                        week_summary['failed_games'] += 1
                        
                except Exception as e:
                    logger.error(f"Error processing game {game['contest_id']}: {e}")
                    week_summary['failed_games'] += 1
            
            # Save week data
            self._save_week_data(year, week, week_summary)
            
            return week_summary
            
        except Exception as e:
            logger.error(f"Error processing week {week}: {e}")
            return None
    
    def _save_week_data(self, year: int, week: int, week_data: Dict):
        """Save processed data for a week including rolling statistics."""
        # Convert sets to lists for JSON serialization
        if 'teams_seen' in week_data and isinstance(week_data['teams_seen'], set):
            week_data['teams_seen'] = list(week_data['teams_seen'])
        
        # Save raw week data
        raw_file = self.raw_dir / f"{year}_week_{week:02d}.json"
        with open(raw_file, 'w') as f:
            json.dump(week_data, f, indent=2)
        
        # Convert to team-game format for data_processor
        if week_data['games']:
            team_games = self._convert_to_team_games(week_data['games'])
            
            # Save team-game format
            processed_file = self.processed_dir / f"{year}_week_{week:02d}_team_games.json"
            with open(processed_file, 'w') as f:
                json.dump(team_games, f, indent=2)
            
            # Add to cumulative data
            self.cumulative_team_games.extend(team_games)
            
            # Calculate rolling statistics using ALL games up to this point
            logger.info(f"Calculating rolling stats with {len(self.cumulative_team_games)} total games")
            
            # Translate to data_processor format
            translated_df = self.translator.translate_team_games(self.cumulative_team_games)
            
            if not translated_df.empty:
                # Add missing stats and prepare
                translated_df = self.translator.add_missing_stats(translated_df)
                translated_df = self.translator.prepare_for_rolling_stats(translated_df)
                
                # Calculate rolling stats
                rolling_stats_df = self.processor.calculate_rolling_stats(translated_df)
                
                # Save rolling stats for this week
                if not rolling_stats_df.empty:
                    rolling_file = self.processed_dir / f"{year}_week_{week:02d}_rolling_stats.csv"
                    rolling_stats_df.to_csv(rolling_file, index=False)
                    
                    # Also save current week's rolling stats separately for easy access
                    current_week_rolling = rolling_stats_df[
                        (rolling_stats_df['year'] == year) & 
                        (rolling_stats_df['week'] == week)
                    ]
                    
                    current_file = self.processed_dir / f"{year}_week_{week:02d}_current_rolling.csv"
                    current_week_rolling.to_csv(current_file, index=False)
                    
                    logger.info(f"Saved rolling stats: {len(current_week_rolling)} records for week {week}")
                    
                    # Log some interesting rolling stats
                    self._log_rolling_stats_summary(current_week_rolling)
    
    def _convert_to_team_games(self, games: List[Dict]) -> List[Dict]:
        """Convert merged games to team-game format for data_processor."""
        team_games = []
        
        for game in games:
            # Create home team's game record
            home_game = self._create_team_game_record(game, 'home', 'away')
            if home_game:
                team_games.append(home_game)
            
            # Create away team's game record
            away_game = self._create_team_game_record(game, 'away', 'home')
            if away_game:
                team_games.append(away_game)
        
        return team_games
    
    def _create_team_game_record(self, merged_game: Dict, team_perspective: str, 
                                opp_perspective: str) -> Dict:
        """Create a single team-game record."""
        stats = merged_game.get('stats', {})
        
        team_game = {
            'year': merged_game.get('year'),
            'week': merged_game.get('week'),
            'game_id': merged_game.get('contest_id'),
            'team': merged_game.get(f'{team_perspective}_team'),
            'opponent': merged_game.get(f'{opp_perspective}_team'),
            'date': merged_game.get('date'),
            'location': team_perspective  # 'home' or 'away'
        }
        
        # Add scores
        if f'{team_perspective}_score' in merged_game:
            team_game['final_score'] = merged_game[f'{team_perspective}_score']
        if f'{opp_perspective}_score' in merged_game:
            team_game['opp_final_score'] = merged_game[f'{opp_perspective}_score']
        
        # Map all stats
        for key, value in stats.items():
            if key.startswith(f'{team_perspective}_'):
                stat_name = key.replace(f'{team_perspective}_', '')
                team_game[stat_name] = value
            elif key.startswith(f'{opp_perspective}_'):
                stat_name = key.replace(f'{opp_perspective}_', '')
                team_game[f'opp_{stat_name}'] = value
        
        # Add SOS and record info
        team_game['sos_before'] = merged_game.get(f'{team_perspective}_sos_before', 0)
        team_game['sos_current'] = merged_game.get(f'{team_perspective}_sos_current', 0)
        team_game['record_before'] = merged_game.get(f'{team_perspective}_record_before', {}).get('record_str', '0-0')
        
        # Calculate derived fields
        if 'final_score' in team_game and 'opp_final_score' in team_game:
            try:
                score = int(team_game['final_score'])
                opp_score = int(team_game['opp_final_score'])
                team_game['points_scored'] = score
                team_game['points_allowed'] = opp_score
                team_game['margin'] = score - opp_score
                team_game['win'] = 1 if score > opp_score else 0
            except (ValueError, TypeError):
                pass

        # Add calculated fields that data_processor expects
        # Turnover differential
        try:
            opp_fumbles_lost = float(team_game.get('opp_fumbles_lost', 0))
            opp_interceptions = float(team_game.get('opp_interceptions', 0))
            fumbles_lost = float(team_game.get('fumbles_lost', 0))
            interceptions = float(team_game.get('interceptions', 0))
            
            team_game['turnover_diff'] = (opp_fumbles_lost + opp_interceptions) - (fumbles_lost + interceptions)
        except (ValueError, TypeError):
            team_game['turnover_diff'] = 0
        
        # Yards per play
        try:
            total_offense = float(team_game.get('total_offense', 0))
            total_plays = float(team_game.get('total_offense_plays', 0))
            
            if total_plays > 0:
                team_game['yards_per_play'] = total_offense / total_plays
            else:
                team_game['yards_per_play'] = 0
        except (ValueError, TypeError):
            team_game['yards_per_play'] = 0
        
        # Third down rate (as decimal, not percentage)
        try:
            conversions = float(team_game.get('third_down_conversions', 0))
            attempts = float(team_game.get('third_down_att', 0))
            
            if attempts > 0:
                team_game['third_down_rate'] = conversions / attempts
            else:
                team_game['third_down_rate'] = 0
        except (ValueError, TypeError):
            team_game['third_down_rate'] = 0
        
        # Pass/rush ratio
        try:
            pass_yards = float(team_game.get('net_yards_passing', 0))
            rush_yards = float(team_game.get('net_yards_rushing', 0))
            total_yards = pass_yards + rush_yards
            
            if total_yards > 0:
                team_game['pass_rush_ratio'] = pass_yards / total_yards
            else:
                team_game['pass_rush_ratio'] = 0.5  # Default to balanced if no yards
        except (ValueError, TypeError):
            team_game['pass_rush_ratio'] = 0.5
   
        return team_game
    
    def _save_checkpoint(self, year: int, week: int):
        """Save checkpoint after completing a week."""
        checkpoint = {
            'year': year,
            'week': week,
            'timestamp': datetime.now().isoformat(),
            'merger_state': {
                'team_records': self.merger.team_records,
                'team_elo_ratings': self.merger.team_elo_ratings
            },
            'games_processed': self.pipeline_state['total_games_processed'],
            'cumulative_games_count': len(self.cumulative_team_games)
        }
        
        checkpoint_file = self.checkpoints_dir / f"{year}_week_{week:02d}_checkpoint.pkl"
        with open(checkpoint_file, 'wb') as f:
            pickle.dump(checkpoint, f)
        
        # Also save cumulative team games for resume capability
        cumulative_file = self.checkpoints_dir / f"{year}_week_{week:02d}_cumulative_games.json"
        with open(cumulative_file, 'w') as f:
            json.dump(self.cumulative_team_games, f)
        
        logger.debug(f"Checkpoint saved for {year} week {week}")
    
    def _load_merger_checkpoint(self, year: int, week: int):
        """Load merger state from checkpoint."""
        checkpoint_file = self.checkpoints_dir / f"{year}_week_{week:02d}_checkpoint.pkl"
        
        if checkpoint_file.exists():
            try:
                with open(checkpoint_file, 'rb') as f:
                    checkpoint = pickle.load(f)
                
                self.merger.team_records = checkpoint['merger_state']['team_records']
                self.merger.team_elo_ratings = checkpoint['merger_state']['team_elo_ratings']
                
                logger.info(f"Loaded checkpoint from week {week} with "
                          f"{len(self.merger.team_records)} teams")
                return True
            except Exception as e:
                logger.error(f"Error loading checkpoint: {e}")
        
        return False
    
    def _load_cumulative_data(self, year: int, week: int):
        """Load cumulative team games from checkpoint."""
        cumulative_file = self.checkpoints_dir / f"{year}_week_{week:02d}_cumulative_games.json"
        
        if cumulative_file.exists():
            try:
                with open(cumulative_file, 'r') as f:
                    self.cumulative_team_games = json.load(f)
                logger.info(f"Loaded {len(self.cumulative_team_games)} cumulative games from checkpoint")
                return True
            except Exception as e:
                logger.error(f"Error loading cumulative data: {e}")
                # Try to rebuild from individual week files
                self._rebuild_cumulative_data(year, week)
        
        return False
    
    def _rebuild_cumulative_data(self, year: int, up_to_week: int):
        """Rebuild cumulative data from individual week files."""
        logger.info(f"Rebuilding cumulative data up to week {up_to_week}")
        self.cumulative_team_games = []
        
        for week in range(1, up_to_week + 1):
            team_games_file = self.processed_dir / f"{year}_week_{week:02d}_team_games.json"
            if team_games_file.exists():
                with open(team_games_file, 'r') as f:
                    week_games = json.load(f)
                    self.cumulative_team_games.extend(week_games)
        
        logger.info(f"Rebuilt cumulative data: {len(self.cumulative_team_games)} games")
    
    def _log_rolling_stats_summary(self, rolling_stats_df: pd.DataFrame):
        """Log interesting rolling statistics summary."""
        if rolling_stats_df.empty:
            return
        
        # Find teams with best 5-week rolling averages
        if 'final_score_5wk' in rolling_stats_df.columns:
            # Build the list of columns to display
            display_cols = ['team', 'opponent', 'final_score_5wk']
            if 'win_pct_5wk' in rolling_stats_df.columns:
                display_cols.append('win_pct_5wk')
            
            top_offensive = rolling_stats_df.nlargest(5, 'final_score_5wk')[display_cols]
            
            logger.info("\nTop 5 teams by 5-week scoring average:")
            for _, row in top_offensive.iterrows():
                win_pct_str = ""
                if 'win_pct_5wk' in row:
                    win_pct_str = f", {row['win_pct_5wk']:.1%} win rate"
                logger.info(f"  {row['team']}: {row['final_score_5wk']:.1f} pts/game{win_pct_str}")
        
        # Show rolling momentum (teams improving)
        if 'final_score_3wk' in rolling_stats_df.columns and 'final_score_season' in rolling_stats_df.columns:
            rolling_stats_df['momentum'] = (
                rolling_stats_df['final_score_3wk'] - rolling_stats_df['final_score_season']
            )
            
            improving = rolling_stats_df.nlargest(3, 'momentum')[['team', 'momentum']]
            if not improving.empty:
                logger.info("\nTeams with strongest momentum (3wk vs season):")
                for _, row in improving.iterrows():
                    logger.info(f"  {row['team']}: +{row['momentum']:.1f} pts/game")
    
    def _get_resume_week(self, year: int) -> int:
        """Determine which week to resume from."""
        # Check for existing week files
        max_week = 0
        for file in self.raw_dir.glob(f"{year}_week_*.json"):
            week_num = int(file.stem.split('_')[-1])
            max_week = max(max_week, week_num)
        
        return max_week + 1 if max_week > 0 else 1
    
    def _save_pipeline_state(self):
        """Save overall pipeline state."""
        state_file = self.data_dir / "pipeline_state.json"
        with open(state_file, 'w') as f:
            json.dump(self.pipeline_state, f, indent=2)
    
    def _load_pipeline_state(self):
        """Load pipeline state if it exists."""
        state_file = self.data_dir / "pipeline_state.json"
        if state_file.exists():
            try:
                with open(state_file, 'r') as f:
                    self.pipeline_state = json.load(f)
                logger.info(f"Loaded pipeline state: {self.pipeline_state['total_games_processed']} games processed")
            except Exception as e:
                logger.warning(f"Could not load pipeline state: {e}")
    
    def _get_sos_leaders(self, top_n: int = 5) -> str:
        """Get current SOS leaders for logging."""
        if not self.merger.team_records:
            return "No teams yet"
        
        # Calculate SOS for all teams
        sos_list = []
        for team_id, record in self.merger.team_records.items():
            if record['wins'] + record['losses'] > 0:
                sos = self.merger.calculate_strength_of_schedule(team_id)
                # Try to get team name from recent games
                team_name = team_id
                
                sos_list.append((team_name, sos, f"{record['wins']}-{record['losses']}"))
        
        # Sort by SOS
        sos_list.sort(key=lambda x: x[1], reverse=True)
        
        # Format top N
        leaders = []
        for name, sos, record in sos_list[:top_n]:
            leaders.append(f"{name} ({record}): {sos:.3f}")
        
        return ", ".join(leaders)
    
    def _generate_season_report(self, year: int, summary: Dict):
        """Generate a comprehensive season report."""
        report_file = self.data_dir / f"{year}_season_report.txt"
        
        with open(report_file, 'w') as f:
            f.write(f"NCAA D3 Football Season Report - {year}\n")
            f.write("=" * 60 + "\n\n")
            
            f.write(f"Collection Summary:\n")
            f.write(f"  Total Games: {summary['successful_games']}/{summary['total_games']}\n")
            if summary['total_games'] > 0:
                f.write(f"  Success Rate: {summary['successful_games']/summary['total_games']*100:.1f}%\n")
            else:
                f.write(f"  Success Rate: N/A (no games found)\n")
            f.write(f"  Teams Tracked: {summary['teams_seen']}\n")
            f.write(f"  Collection Time: {summary['collection_time']/60:.1f} minutes\n\n")
            
            # Final standings
            f.write("Final Standings (by record):\n")
            standings = []
            for team_id, record in self.merger.team_records.items():
                wins = record['wins']
                losses = record['losses']
                win_pct = wins / (wins + losses) if (wins + losses) > 0 else 0
                sos = self.merger.calculate_strength_of_schedule(team_id)
                
                # Get team name
                team_name = team_id
                if record['opponents']:
                    # Look for a home game to get proper team name
                    for opp in record['opponents']:
                        if opp['location'] == 'home':
                            # This team was home, so use the game to find its name
                            # This is a bit hacky but works
                            team_name = team_id
                            break
                
                standings.append({
                    'team': team_name,
                    'wins': wins,
                    'losses': losses,
                    'win_pct': win_pct,
                    'sos': sos
                })
            
            # Sort by win percentage, then by SOS
            standings.sort(key=lambda x: (x['win_pct'], x['sos']), reverse=True)
            
            f.write(f"\n{'Team':<30} {'Record':<10} {'Win %':<8} {'SOS':<8}\n")
            f.write("-" * 56 + "\n")
            
            for team in standings[:25]:  # Top 25
                f.write(f"{team['team']:<30} {team['wins']}-{team['losses']:<10} "
                       f"{team['win_pct']:<8.3f} {team['sos']:<8.3f}\n")
        
        logger.info(f"Season report saved to {report_file}")
    
    def _generate_final_rolling_stats(self, year: int):
        """Generate complete season rolling statistics file."""
        logger.info("Generating final season rolling statistics...")
        
        if not self.cumulative_team_games:
            logger.warning("No cumulative games to process")
            return
        
        # Translate all games
        translated_df = self.translator.translate_team_games(self.cumulative_team_games)
        
        if translated_df.empty:
            logger.warning("No games translated for final stats")
            return
        
        # Prepare and calculate
        translated_df = self.translator.add_missing_stats(translated_df)
        translated_df = self.translator.prepare_for_rolling_stats(translated_df)
        
        # Calculate rolling stats for entire season
        rolling_stats_df = self.processor.calculate_rolling_stats(translated_df)
        
        if not rolling_stats_df.empty:
            # Save complete season rolling stats
            season_file = self.data_dir / f"{year}_season_rolling_stats.csv"
            rolling_stats_df.to_csv(season_file, index=False)
            
            # Generate rolling stats summary report
            summary_file = self.data_dir / f"{year}_rolling_stats_summary.txt"
            
            with open(summary_file, 'w') as f:
                f.write(f"NCAA D3 Football Rolling Statistics Summary - {year}\n")
                f.write("=" * 60 + "\n\n")
                
                # Best teams by various rolling metrics
                metrics = [
                    ('final_score_5wk', 'Scoring (5-week avg)'),
                    ('total_offense_5wk', 'Total Offense (5-week avg)'),
                    ('turnover_diff_5wk', 'Turnover Margin (5-week avg)'),
                    ('third_down_rate_5wk', 'Third Down % (5-week avg)')
                ]
                
                for metric, title in metrics:
                    if metric in rolling_stats_df.columns:
                        f.write(f"\nTop 10 Teams - {title}:\n")
                        f.write("-" * 40 + "\n")
                        
                        # Get final week stats for each team
                        final_week_stats = rolling_stats_df.loc[
                            rolling_stats_df.groupby('team')['week'].idxmax()
                        ]
                        
                        top_teams = final_week_stats.nlargest(10, metric)
                        
                        for idx, (_, team) in enumerate(top_teams.iterrows(), 1):
                            value = team[metric]
                            win_pct = team.get('win_pct_5wk', 0)
                            f.write(f"{idx:2d}. {team['team']:<25} {value:>8.1f} "
                                   f"(Win%: {win_pct:.1%})\n")
            
            logger.info(f"Final rolling stats saved: {season_file}")
            logger.info(f"Rolling stats summary saved: {summary_file}")
        else:
            logger.warning("No rolling stats generated for season")


# Convenience functions
def collect_multiple_seasons(years: List[int], data_dir: str = "ncaa_data"):
    """
    Collect multiple seasons of data.
    
    Args:
        years: List of years to collect (e.g., [2022, 2023, 2024])
        data_dir: Directory to save data
    """
    pipeline = NCAASeasonPipeline(data_dir=data_dir)
    
    results = []
    for year in years:
        logger.info(f"\n{'='*80}")
        logger.info(f"COLLECTING {year} SEASON")
        logger.info(f"{'='*80}\n")
        
        result = pipeline.collect_season(year)
        results.append(result)
        
        # Pause between seasons
        if year != years[-1]:
            logger.info(f"\nPausing 30 seconds before next season...")
            time.sleep(30)
    
    # Generate summary report
    total_games = sum(r['successful_games'] for r in results)
    total_time = sum(r['collection_time'] for r in results)
    
    logger.info(f"\n{'='*80}")
    logger.info(f"COLLECTION COMPLETE")
    logger.info(f"{'='*80}")
    logger.info(f"Seasons collected: {len(years)}")
    logger.info(f"Total games: {total_games}")
    logger.info(f"Total time: {total_time/3600:.1f} hours")
    
    return results


if __name__ == "__main__":
    # Test with one week
    pipeline = NCAASeasonPipeline()
    
    # Collect just week 1 of 2024 as a test
    pipeline.collect_season(2024, start_week=1, end_week=1)