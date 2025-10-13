import logging
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

class RollingStatsCalculator:
    """
    Calculates rolling statistics for team performance.
    
    Key features:
    - Uses previous season data for early weeks with decay weight
    - Never includes the game being predicted (no data leakage)
    - Handles missing data gracefully
    - Calculates trends and momentum indicators
    """
    def __init__(self, db_connection, prev_season_weight: float = 0.7):
        """
        Initialize the calculator.
        
        Args:
            db_connection: Database connection instance
            prev_season_weight: Weight for previous season games (0.7 = 70% weight)
        """
        self.db = db_connection
        self.prev_season_weight = prev_season_weight
        
        # Configuration
        self.windows = [3, 5]  # Calculate 3 and 5 week windows
        self.min_games = 2  # Minimum games needed for any calculation
        
        logger.info(f"Initialized calculator with prev_season_weight={prev_season_weight}")

    def calculate_for_all_games(self, start_year: int = 2022, end_year: int = 2023):
        """
        Calculate rolling stats for all games in the specified years.
        This is the main entry point for batch processing..
        
        Args:
            start_year: First year to calculate
            end_year: Last year to calculate
        """
        logger.info(f"Calculating rolling stats for {start_year}-{end_year}")
        
        with self.db.get_session() as session:
            # Get all games in date range
            games = session.execute(text("""
                SELECT id, year, week, home_team_id, away_team_id
                FROM games
                WHERE year BETWEEN :start_year AND :end_year
                ORDER BY year, week, id
            """), {'start_year': start_year, 'end_year': end_year}).fetchall()
            
            total_games = len(games)
            logger.info(f"Found {total_games} games to process")
            
            processed = 0
            for game in games:
                game_id, year, week, home_id, away_id = game
                
                # Calculate for home team
                home_stats = self._calculate_team_stats(
                    session, game_id, home_id, away_id, year, week
                )
                self._save_stats(session, home_stats)
                
                # Calculate for away team
                away_stats = self._calculate_team_stats(
                    session, game_id, away_id, home_id, year, week
                )
                self._save_stats(session, away_stats)
                
                processed += 1
                if processed % 100 == 0:
                    session.commit()  # Commit in batches
                    logger.info(f"Processed {processed}/{total_games} games")
            
            session.commit()
            logger.info(f"Completed! Processed {processed} games")

    def _calculate_team_stats(
        self, 
        session: Session, 
        game_id: int, 
        team_id: int, 
        opponent_id: int,
        year: int, 
        week: int
    ) -> Dict:
        """
        Calculate all rolling statistics for one team before a specific game.
        
        This is where the magic happens - fetching previous games and
        calculating all the rolling averages.
        """
        stats = {
            'game_id': game_id,
            'team_id': team_id,
            'opponent_id': opponent_id,
            'year': year,
            'week': week
        }
        
        # Determine how many previous season games to use
        prev_season_games_needed = max(0, 3 - (week - 1))
        
        # Get current season games before this week
        current_games = self._get_team_games(
            session, team_id, year, max_week=week-1
        )
        
        # Get previous season games if needed (for early weeks)
        previous_games = []
        if prev_season_games_needed > 0 and year > 2021:
            previous_games = self._get_team_games(
                session, team_id, year-1, max_week = None,
                last_n=prev_season_games_needed
            )
        
        # Combine games for calculations
        all_games = current_games + previous_games
        
        if len(all_games) < self.min_games:
            logger.debug(f"Team {team_id} has only {len(all_games)} games before week {week}")
            return self._create_null_stats(stats)
        
        # Track data quality
        stats['games_in_season'] = len(current_games)
        stats['prev_season_games_in_3wk'] = min(len(previous_games), 3)
        stats['prev_season_games_in_5wk'] = min(len(previous_games), 5)
        
        # Calculate for each window
        for window in self.windows:
            window_stats = self._calculate_window_stats(
                all_games[:window], 
                window,
                prev_season_count=len(previous_games)
            )
            stats.update(window_stats)
        
        # Calculate season stats (current season only)
        if current_games:
            season_stats = self._calculate_season_stats(current_games)
            stats.update(season_stats)
        
        # Calculate momentum indicators
        if len(all_games) >= 6:
            trend_stats = self._calculate_trends(all_games)
            stats.update(trend_stats)
        
        # Calculate strength of schedule
        sos_stats = self._calculate_sos(session, all_games, [3, 5])
        stats.update(sos_stats)

        # Calculate ELO-based features
        elo_stats = self._calculate_elo_features(session, team_id, game_id, all_games, [3, 5])
        stats.update(elo_stats)

        return stats


    def _get_team_games(
        self, 
        session: Session, 
        team_id: int, 
        year: int, 
        max_week: Optional[int] = None,
        last_n: Optional[int] = None
    ) -> List[Dict]:
        """
        Fetch team games with all statistics.
        
        Args:
            max_week: Only get games before this week
            last_n: Get last N games of the season
        """
        base_query = """
            SELECT 
                tgs.*,
                g.week,
                g.year,
                g.game_date
            FROM team_game_stats tgs
            JOIN games g ON tgs.game_id = g.id
            WHERE tgs.team_id = :team_id
                AND g.year = :year
        """
        
        if max_week:
            base_query += " AND g.week <= :max_week"
        
        base_query += " ORDER BY g.week DESC, g.game_date DESC"
        
        if last_n:
            base_query += f" LIMIT {last_n}"
        
        params = {'team_id': team_id, 'year': year}
        if max_week:
            params['max_week'] = max_week
        
        result = session.execute(text(base_query), params)
        
        games = []
        for row in result:
            games.append(dict(row._mapping))
        
        return games

    def _calculate_window_stats(
        self, 
        games: List[Dict], 
        window: int,
        prev_season_count: int = 0
    ) -> Dict:
        """
        Calculate statistics for a specific window with decay weights.
        
        The key innovation here is applying decay weights to previous
        season games while keeping current season at full weight.
        """
        if not games:
            return {}
        
        # Calculate weights for each game
        weights = []
        for i, game in enumerate(games):
            if i >= len(games) - prev_season_count:
                # This is a previous season game
                weights.append(self.prev_season_weight)
            else:
                # Current season game
                weights.append(1.0)
        
        weights = np.array(weights)
        prefix = f"{window}wk"
        
        stats = {
            f'games_in_{prefix}': len(games)
        }
        
        # Helper function for weighted average
        def weighted_avg(values):
            values = np.array([v for v in values if v is not None])
            if len(values) == 0:
                return None
            if len(values) != len(weights):
                # Adjust weights if some values are None
                w = weights[:len(values)]
            else:
                w = weights
            return np.average(values, weights=w)
        
        # Offensive metrics
        stats[f'ppg_{prefix}'] = weighted_avg([g['points_scored'] for g in games])
        stats[f'papg_{prefix}'] = weighted_avg([g['points_allowed'] for g in games])
        
        # Calculate yards per play
        total_yards = sum((g['total_offense_yards'] or 0) * weights[i] 
                         for i, g in enumerate(games))
        total_plays = sum((g['total_offense_plays'] or 0) * weights[i] 
                         for i, g in enumerate(games))
        total_weight = sum(weights)
        
        if total_plays > 0:
            stats[f'ypp_{prefix}'] = total_yards / total_plays
        else:
            stats[f'ypp_{prefix}'] = None
        
        # Total yards per game
        stats[f'total_yards_{prefix}'] = weighted_avg([g['total_offense_yards'] for g in games])
        
        # Third down percentage
        third_conv = sum((g['third_down_conversions'] or 0) * weights[i] 
                        for i, g in enumerate(games))
        third_att = sum((g['third_down_attempts'] or 0) * weights[i] 
                       for i, g in enumerate(games))
        
        if third_att > 0:
            stats[f'third_down_pct_{prefix}'] = (third_conv / third_att) * 100
        else:
            stats[f'third_down_pct_{prefix}'] = None
        
        # Turnover differential per game
        turnovers_lost = [(g.get('fumbles_lost', 0) or 0) + 
                         (g.get('passing_interceptions', 0) or 0) 
                         for g in games]
        turnovers_gained = [(g.get('opp_fumbles_lost', 0) or 0) + 
                           (g.get('defense_interceptions', 0) or 0) 
                           for g in games]
        
        to_diff = [gained - lost for gained, lost in zip(turnovers_gained, turnovers_lost)]
        stats[f'turnover_diff_{prefix}'] = weighted_avg(to_diff)
        
        # Defensive metrics
        opp_yards = sum((g.get('opp_total_offense_yards', 0) or 0) * weights[i] 
                       for i, g in enumerate(games))
        opp_plays = sum((g.get('opp_total_offense_plays', 0) or 0) * weights[i] 
                       for i, g in enumerate(games))
        
        if opp_plays > 0:
            stats[f'opp_ypp_{prefix}'] = opp_yards / opp_plays
        else:
            stats[f'opp_ypp_{prefix}'] = None
        
        stats[f'opp_total_yards_{prefix}'] = weighted_avg([g.get('opp_total_offense_yards') for g in games])
        
        # Margin
        margins = [g['points_scored'] - g['points_allowed'] for g in games]
        stats[f'margin_{prefix}'] = weighted_avg(margins)
        
        # Pass/rush ratio
        pass_yards = sum((g.get('passing_yards', 0) or 0) * weights[i] 
                        for i, g in enumerate(games))
        rush_yards = sum((g.get('rushing_yards', 0) or 0) * weights[i] 
                        for i, g in enumerate(games))
        total_off = pass_yards + rush_yards
        
        if total_off > 0:
            stats[f'pass_ratio_{prefix}'] = pass_yards / total_off
        else:
            stats[f'pass_ratio_{prefix}'] = 0.5  # Default to balanced
        
        # Win tracking
        wins = sum(1 for g in games[:window] if g.get('win'))
        stats[f'last_{window}_wins'] = wins
        
        # Consistency metrics (standard deviation)
        if window == 3:
            ppg_values = [g['points_scored'] for g in games[:3]]
            margin_values = [g['points_scored'] - g['points_allowed'] for g in games[:3]]
            
            if len(ppg_values) >= 2:
                stats['ppg_std_3wk'] = np.std(ppg_values)
                stats['margin_std_3wk'] = np.std(margin_values)
        
        return stats

    def _calculate_season_stats(self, games: List[Dict]) -> Dict:
        """Calculate season-long statistics (current season only)."""
        if not games:
            return {}
        
        # No decay weights for season stats - current season only
        stats = {}
        
        stats['ppg_season'] = np.mean([g['points_scored'] for g in games])
        stats['papg_season'] = np.mean([g['points_allowed'] for g in games])
        stats['margin_season'] = np.mean([g['points_scored'] - g['points_allowed'] for g in games])
        
        # Total yards
        total_yards = sum(g.get('total_offense_yards', 0) or 0 for g in games)
        total_plays = sum(g.get('total_offense_plays', 0) or 0 for g in games)
        
        if total_plays > 0:
            stats['ypp_season'] = total_yards / total_plays
        
        stats['total_yards_season'] = np.mean([g.get('total_offense_yards', 0) for g in games])
        
        return stats

    def _calculate_trends(self, games: List[Dict]) -> Dict:
        """
        Calculate momentum indicators by comparing recent vs previous performance.
        Positive trends indicate improvement.
        """
        recent_3 = games[:3]
        previous_3 = games[3:6]
        
        # PPG trend
        recent_ppg = np.mean([g['points_scored'] for g in recent_3])
        prev_ppg = np.mean([g['points_scored'] for g in previous_3])
        ppg_trend = recent_ppg - prev_ppg
        
        # Margin trend  
        recent_margin = np.mean([g['points_scored'] - g['points_allowed'] for g in recent_3])
        prev_margin = np.mean([g['points_scored'] - g['points_allowed'] for g in previous_3])
        margin_trend = recent_margin - prev_margin
        
        # Defensive trend (negative is better)
        recent_def = np.mean([g['points_allowed'] for g in recent_3])
        prev_def = np.mean([g['points_allowed'] for g in previous_3])
        defensive_trend = recent_def - prev_def
        
        # Win streak
        streak = 0
        for game in games:
            if game.get('win'):
                if streak >= 0:
                    streak += 1
                else:
                    break
            else:
                if streak <= 0:
                    streak -= 1
                else:
                    break
        
        return {
            'ppg_trend': ppg_trend,
            'margin_trend': margin_trend,
            'defensive_trend': defensive_trend,
            'win_streak': streak
        }

    def _calculate_sos(self, session: Session, games: List[Dict], windows: List[int]) -> Dict:
        """
        Calculate strength of schedule for each window.
        Uses opponent win percentage as proxy for strength.
        """
        sos_stats = {}
        
        for window in windows:
            window_games = games[:window] if len(games) >= window else games
            
            if not window_games:
                sos_stats[f'sos_{window}wk'] = None
                continue
            
            # Get opponent records
            opponent_ids = [g['opponent_id'] for g in window_games]
            
            # Simplified SOS - you could enhance this
            # For now, using 0.5 as placeholder
            sos_stats[f'sos_{window}wk'] = 0.5
        
        # Season SOS
        if games:
            sos_stats['sos_season'] = 0.5  # Placeholder
        
        return sos_stats

    def _calculate_elo_features(self, session: Session, team_id: int,
                                game_id: int, games: List[Dict],
                                windows: List[int]) -> Dict:
        """
        Calculate ELO-based features for the team.

        Features include:
        - Current ELO rating
        - ELO changes over windows (momentum)
        - Average opponent ELO (true strength of schedule)
        """
        elo_stats = {}

        # Get team's ELO before this game from game_elos table
        result = session.execute(text("""
            SELECT home_elo_before, away_elo_before, home_team_id
            FROM game_elos
            WHERE game_id = :game_id
        """), {'game_id': game_id}).fetchone()

        if not result:
            # No ELO data available - return nulls
            elo_stats['current_elo'] = None
            for window in windows:
                elo_stats[f'elo_change_{window}wk'] = None
                elo_stats[f'avg_opp_elo_{window}wk'] = None
            return elo_stats

        # Determine which ELO is ours
        home_elo, away_elo, home_team_id = result
        current_elo = home_elo if team_id == home_team_id else away_elo
        elo_stats['current_elo'] = current_elo

        # Get ELO history for windows
        for window in windows:
            window_games = games[:window] if len(games) >= window else games

            if not window_games:
                elo_stats[f'elo_change_{window}wk'] = None
                elo_stats[f'avg_opp_elo_{window}wk'] = None
                continue

            # Get game IDs for this window
            game_ids = [g['game_id'] for g in window_games]

            # Get ELO data for these games
            elo_results = session.execute(text("""
                SELECT
                    ge.game_id,
                    ge.home_team_id,
                    ge.away_team_id,
                    ge.home_elo_before,
                    ge.away_elo_before,
                    ge.home_elo_change,
                    ge.away_elo_change
                FROM game_elos ge
                WHERE ge.game_id = ANY(:game_ids)
                ORDER BY ge.game_id DESC
            """), {'game_ids': game_ids}).fetchall()

            if not elo_results:
                elo_stats[f'elo_change_{window}wk'] = None
                elo_stats[f'avg_opp_elo_{window}wk'] = None
                continue

            # Calculate ELO change (current - oldest in window)
            elo_changes = []
            opp_elos = []

            for elo_row in elo_results:
                gid, h_id, a_id, h_elo_before, a_elo_before, h_change, a_change = elo_row

                if team_id == h_id:
                    elo_changes.append(h_change)
                    opp_elos.append(a_elo_before)
                else:
                    elo_changes.append(a_change)
                    opp_elos.append(h_elo_before)

            # ELO change = sum of all changes in window
            elo_stats[f'elo_change_{window}wk'] = sum(elo_changes) if elo_changes else None

            # Average opponent ELO
            elo_stats[f'avg_opp_elo_{window}wk'] = np.mean(opp_elos) if opp_elos else None

        return elo_stats

    def _create_null_stats(self, base_stats: Dict) -> Dict:
        """Create a stats dict with NULL values when insufficient data."""
        stats = base_stats.copy()
        
        # Add all fields as None
        for window in self.windows:
            prefix = f"{window}wk"
            for field in ['ppg', 'papg', 'ypp', 'opp_ypp', 'margin', 
                         'third_down_pct', 'turnover_diff', 'total_yards',
                         'opp_total_yards', 'pass_ratio', 'sos']:
                stats[f'{field}_{prefix}'] = None
            stats[f'games_in_{prefix}'] = 0
        
        # Season stats
        for field in ['ppg', 'papg', 'ypp', 'margin', 'total_yards', 'sos']:
            stats[f'{field}_season'] = None
        
        # Trends
        for field in ['ppg_trend', 'margin_trend', 'defensive_trend', 'win_streak']:
            stats[field] = None

        # ELO features
        stats['current_elo'] = None
        for window in self.windows:
            stats[f'elo_change_{window}wk'] = None
            stats[f'avg_opp_elo_{window}wk'] = None

        return stats
    
    def _save_stats(self, session: Session, stats: Dict):
        """Save calculated stats to the database."""
        if not stats or stats.get('game_id') is None:
            return
        
        # Skip teams with insufficient data (only has basic IDs)
        if stats.get('games_in_season', 0) == 0:
            return  # Don't save records for teams with no games
        
        # Convert numpy types
        clean_stats = {}
        for key, value in stats.items():
            if isinstance(value, np.number):
                clean_stats[key] = float(value)
            elif isinstance(value, np.ndarray):
                clean_stats[key] = value.tolist()
            else:
                clean_stats[key] = value
        
        # Add timestamps
        from datetime import datetime
        clean_stats['created_at'] = datetime.utcnow()
        clean_stats['updated_at'] = datetime.utcnow()
        
        stats = clean_stats
        
        # Build INSERT statement dynamically based on available fields
        columns = []
        values = []
        
        for key, value in stats.items():
            if value is not None:
                columns.append(key)
                values.append(value)
        
        if not columns:
            return
        
        # Create parameterized query
        column_str = ', '.join(columns)
        param_str = ', '.join([f':{col}' for col in columns])
        
        query = f"""
            INSERT INTO team_rolling_stats ({column_str})
            VALUES ({param_str})
            ON CONFLICT (team_id, game_id) 
            DO UPDATE SET {', '.join([f'{col} = :{col}' for col in columns if col not in ['team_id', 'game_id']])}
        """
        
        try:
            session.execute(text(query), stats)
        except Exception as e:
            logger.error(f"Failed to save stats: {e}")
            logger.debug(f"Stats that failed: {stats}")