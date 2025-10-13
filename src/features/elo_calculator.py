"""
ELO Rating Calculator for D3 Football

Implements an ELO rating system that:
1. Tracks team strength over time
2. Adjusts for margin of victory
3. Carries ratings across seasons with regression
4. Provides true strength of schedule metrics
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

import numpy as np
import pandas as pd
from typing import Dict, Tuple, Optional
from sqlalchemy import text
import logging

from src.database.connection import DatabaseConnection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ELOCalculator:
    """
    Calculate and maintain ELO ratings for D3 football teams.
    """

    # ELO Parameters
    STARTING_ELO = 1500
    K_FACTOR = 32  # How much ratings change per game
    HOME_ADVANTAGE = 65  # ELO points added to home team for prediction
    SEASON_REGRESSION = 0.75  # Carry 75% of previous season's rating

    def __init__(self, db_connection: Optional[DatabaseConnection] = None):
        """Initialize ELO calculator with database connection."""
        self.db = db_connection or DatabaseConnection()
        self.team_elos: Dict[int, float] = {}  # team_id -> current ELO

    def expected_score(self, elo_a: float, elo_b: float, home_advantage: float = 0) -> float:
        """
        Calculate expected score for team A against team B.

        Args:
            elo_a: Team A's ELO rating
            elo_b: Team B's ELO rating
            home_advantage: ELO boost for home team (default 0)

        Returns:
            Expected probability that team A wins (0.0 to 1.0)
        """
        return 1 / (1 + 10 ** ((elo_b - (elo_a + home_advantage)) / 400))

    def margin_multiplier(self, margin: int, winner_elo: float, loser_elo: float) -> float:
        """
        Calculate multiplier based on margin of victory.
        Larger margins = bigger rating changes, but with diminishing returns.

        Args:
            margin: Absolute point differential
            winner_elo: Winner's ELO before the game
            loser_elo: Loser's ELO before the game

        Returns:
            Multiplier for K-factor (typically 1.0 to 3.0)
        """
        # Log scale with cap - diminishing returns for blowouts
        base_multiplier = np.log(abs(margin) + 1) * 2.2

        # Autocorrelation adjustment - upsets get bigger changes
        elo_diff = winner_elo - loser_elo
        if elo_diff < 0:  # Upset
            base_multiplier *= 1.2

        return min(base_multiplier, 3.0)  # Cap at 3x

    def update_elo(self, winner_id: int, loser_id: int,
                   margin: int, is_home_win: bool) -> Tuple[float, float, float, float]:
        """
        Update ELO ratings after a game.

        Args:
            winner_id: Winning team's ID
            loser_id: Losing team's ID
            margin: Point differential (winner - loser)
            is_home_win: True if home team won

        Returns:
            Tuple of (new_winner_elo, new_loser_elo, winner_change, loser_change)
        """
        # Get current ELOs (default to starting ELO if not found)
        winner_elo = self.team_elos.get(winner_id, self.STARTING_ELO)
        loser_elo = self.team_elos.get(loser_id, self.STARTING_ELO)

        # Determine home advantage
        home_adv = self.HOME_ADVANTAGE if is_home_win else -self.HOME_ADVANTAGE

        # Calculate expected outcome
        expected_winner = self.expected_score(winner_elo, loser_elo, home_adv)

        # Actual outcome (winner gets 1, loser gets 0)
        actual_winner = 1.0

        # Margin multiplier
        margin_mult = self.margin_multiplier(margin, winner_elo, loser_elo)

        # Calculate ELO changes
        winner_change = self.K_FACTOR * margin_mult * (actual_winner - expected_winner)
        loser_change = -winner_change

        # Update ratings
        new_winner_elo = winner_elo + winner_change
        new_loser_elo = loser_elo + loser_change

        # Store updated ratings
        self.team_elos[winner_id] = new_winner_elo
        self.team_elos[loser_id] = new_loser_elo

        return new_winner_elo, new_loser_elo, winner_change, loser_change

    def regress_elos_for_new_season(self):
        """
        Regress all ELO ratings toward mean for new season.
        Accounts for roster turnover and program changes.
        """
        for team_id in self.team_elos:
            current_elo = self.team_elos[team_id]
            regressed_elo = (self.SEASON_REGRESSION * current_elo +
                           (1 - self.SEASON_REGRESSION) * self.STARTING_ELO)
            self.team_elos[team_id] = regressed_elo

        logger.info(f"Regressed {len(self.team_elos)} team ELOs for new season")

    def calculate_historical_elos(self, start_year: int = 2022,
                                  end_year: int = 2024) -> pd.DataFrame:
        """
        Calculate ELO ratings for all historical games.

        Args:
            start_year: First year to calculate
            end_year: Last year to calculate

        Returns:
            DataFrame with columns: [game_id, home_team_id, away_team_id,
                                     home_elo_before, away_elo_before,
                                     home_elo_after, away_elo_after,
                                     home_elo_change, away_elo_change]
        """
        logger.info(f"Calculating historical ELOs from {start_year} to {end_year}")

        # Fetch all games chronologically
        with self.db.get_session() as session:
            games = session.execute(text("""
                SELECT id, year, week, home_team_id, away_team_id,
                       home_score, away_score, game_date
                FROM games
                WHERE year >= :start_year AND year <= :end_year
                    AND home_score IS NOT NULL AND away_score IS NOT NULL
                ORDER BY year, week, game_date
            """), {'start_year': start_year, 'end_year': end_year}).fetchall()

        logger.info(f"Processing {len(games)} games...")

        results = []
        current_year = start_year

        for game in games:
            game_id, year, week, home_id, away_id, home_score, away_score, game_date = game

            # Check if new season - regress ELOs
            if year > current_year:
                logger.info(f"Season transition: {current_year} → {year}")
                self.regress_elos_for_new_season()
                current_year = year

            # Get ELOs before game
            home_elo_before = self.team_elos.get(home_id, self.STARTING_ELO)
            away_elo_before = self.team_elos.get(away_id, self.STARTING_ELO)

            # Determine winner and margin
            if home_score > away_score:
                winner_id = home_id
                loser_id = away_id
                margin = home_score - away_score
                is_home_win = True
            else:
                winner_id = away_id
                loser_id = home_id
                margin = away_score - home_score
                is_home_win = False

            # Update ELOs
            new_winner_elo, new_loser_elo, winner_change, loser_change = \
                self.update_elo(winner_id, loser_id, margin, is_home_win)

            # Store results
            if is_home_win:
                home_elo_after = new_winner_elo
                away_elo_after = new_loser_elo
                home_elo_change = winner_change
                away_elo_change = loser_change
            else:
                home_elo_after = new_loser_elo
                away_elo_after = new_winner_elo
                home_elo_change = loser_change
                away_elo_change = winner_change

            results.append({
                'game_id': game_id,
                'year': year,
                'week': week,
                'home_team_id': home_id,
                'away_team_id': away_id,
                'home_elo_before': home_elo_before,
                'away_elo_before': away_elo_before,
                'home_elo_after': home_elo_after,
                'away_elo_after': away_elo_after,
                'home_elo_change': home_elo_change,
                'away_elo_change': away_elo_change
            })

        df = pd.DataFrame(results)
        logger.info(f"Calculated ELOs for {len(df)} games")

        return df

    def save_elos_to_database(self, elo_df: pd.DataFrame):
        """
        Save ELO calculations to database.
        Creates/updates game_elos table.
        """
        with self.db.get_session() as session:
            # Create table if not exists
            session.execute(text("""
                CREATE TABLE IF NOT EXISTS game_elos (
                    game_id INTEGER PRIMARY KEY REFERENCES games(id),
                    home_team_id INTEGER NOT NULL REFERENCES teams(id),
                    away_team_id INTEGER NOT NULL REFERENCES teams(id),
                    home_elo_before FLOAT NOT NULL,
                    away_elo_before FLOAT NOT NULL,
                    home_elo_after FLOAT NOT NULL,
                    away_elo_after FLOAT NOT NULL,
                    home_elo_change FLOAT NOT NULL,
                    away_elo_change FLOAT NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """))

            # Clear existing data
            session.execute(text("DELETE FROM game_elos"))

            # Insert new data
            for _, row in elo_df.iterrows():
                session.execute(text("""
                    INSERT INTO game_elos
                    (game_id, home_team_id, away_team_id,
                     home_elo_before, away_elo_before,
                     home_elo_after, away_elo_after,
                     home_elo_change, away_elo_change)
                    VALUES
                    (:game_id, :home_team_id, :away_team_id,
                     :home_elo_before, :away_elo_before,
                     :home_elo_after, :away_elo_after,
                     :home_elo_change, :away_elo_change)
                """), row.to_dict())

            session.commit()
            logger.info(f"Saved {len(elo_df)} ELO records to database")

    def get_team_current_elo(self, team_id: int) -> float:
        """Get current ELO for a team."""
        return self.team_elos.get(team_id, self.STARTING_ELO)

    def get_top_teams(self, n: int = 25) -> pd.DataFrame:
        """Get top N teams by current ELO."""
        rankings = [(team_id, elo) for team_id, elo in self.team_elos.items()]
        rankings.sort(key=lambda x: x[1], reverse=True)

        return pd.DataFrame(rankings[:n], columns=['team_id', 'elo'])


def main():
    """Calculate and save historical ELOs."""
    print("\n" + "="*60)
    print("D3 FOOTBALL ELO CALCULATOR")
    print("="*60)

    calculator = ELOCalculator()

    # Calculate historical ELOs
    print("\n📊 Calculating historical ELO ratings (2022-2024)...")
    elo_df = calculator.calculate_historical_elos(start_year=2022, end_year=2024)

    # Show some statistics
    print(f"\n✓ Processed {len(elo_df)} games")
    print(f"\n📈 ELO Statistics:")
    print(f"  Average ELO change per game: {elo_df['home_elo_change'].abs().mean():.1f}")
    print(f"  Max ELO change: {elo_df[['home_elo_change', 'away_elo_change']].abs().max().max():.1f}")

    # Show current top teams
    print(f"\n🏆 Top 10 Teams by Current ELO:")
    top_teams = calculator.get_top_teams(10)

    from src.database.teams_model import Team
    db = DatabaseConnection()
    with db.get_session() as session:
        for idx, (team_id, elo) in top_teams.iterrows():
            team = session.query(Team).filter(Team.id == team_id).first()
            team_name = team.name if team else f"Team {team_id}"
            print(f"  {idx+1:2d}. {team_name:30s} ELO: {elo:.0f}")

    # Save to database
    print(f"\n💾 Saving ELO data to database...")
    calculator.save_elos_to_database(elo_df)

    print(f"\n✓ Complete! ELO ratings calculated and saved.")
    print(f"\nNext steps:")
    print(f"  1. Add ELO features to rolling stats")
    print(f"  2. Retrain model with ELO features")
    print(f"  3. Compare performance")


if __name__ == "__main__":
    main()
