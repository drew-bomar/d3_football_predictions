# src/pipeline/progress_tracker.py
import json
import time
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from datetime import datetime

class ProgressTracker:
    """
    Track import progress with detailed statistics.
    
    Key features:
    - Resume capability after interruption
    - Detailed timing statistics
    - Failed game tracking for retry
    """
    def __init__(self, filepath: str = 'import_progress.json'):
        self.filepath = Path(filepath)
        self.progress = self._load_progress()
        self.current_session_start = datetime.now()

    def _load_progress(self) -> Dict:
        """Load existing progress or create new tracking structure."""
        if self.filepath.exists():
            with open(self.filepath, 'r') as f:
                return json.load(f)
        
        return {
            'completed_weeks': [],  # List of "year-week" strings
            'failed_weeks': {},     # Dict of "year-week": error_msg
            'partial_weeks': {},    # Weeks with some failed games
            'statistics': {
                'total_games_imported': 0,
                'total_api_calls': 0,
                'total_import_time_seconds': 0,
                'sessions': []  # List of import sessions with timestamps
            },
            'current_week': None,   # Currently processing
            'last_updated': None
        }

    def start_week(self, year: int, week: int, expected_games: int):
        """Mark a week as starting processing."""
        key = f"{year}-{week}"
        self.progress['current_week'] = {
            'key': key,
            'year': year,
            'week': week,
            'expected_games': expected_games,
            'imported_games': 0,
            'failed_games': [],
            'start_time': time.time()
        }
        self._save()


    def update_week_progress(self, games_imported: int = 0, game_failed: str = None):
        """Update progress for current week."""
        if self.progress['current_week']:
            self.progress['current_week']['imported_games'] += games_imported
            if game_failed:
                self.progress['current_week']['failed_games'].append(game_failed)
            self._save()

    def complete_week(self, year: int, week: int, games_imported: int, 
                     games_failed: List[Tuple[str, str]]):
        """Mark a week as complete with statistics."""
        key = f"{year}-{week}"
        
        # Calculate timing
        elapsed = 0
        if self.progress['current_week']:
            elapsed = time.time() - self.progress['current_week']['start_time']
        
        # Update statistics
        self.progress['statistics']['total_games_imported'] += games_imported
        self.progress['statistics']['total_import_time_seconds'] += elapsed
        
        # Handle completion status
        if games_failed:
            # Partial success
            self.progress['partial_weeks'][key] = {
                'imported': games_imported,
                'failed': games_failed,
                'elapsed_seconds': elapsed
            }
        else:
            # Full success
            if key not in self.progress['completed_weeks']:
                self.progress['completed_weeks'].append(key)
            # Remove from failed if it was there
            if key in self.progress['failed_weeks']:
                del self.progress['failed_weeks'][key]
        
        self.progress['current_week'] = None
        self.progress['last_updated'] = datetime.now().isoformat()
        self._save()


    def get_pending_weeks(self, start_year: int = 2021, 
                         end_year: int = 2024) -> List[Tuple[int, int]]:
        """Get list of weeks that need import (not completed)."""
        pending = []
        
        for year in range(start_year, end_year + 1):
            # D3 typically has 10-15 weeks depending on year
            max_week = 15 if year >= 2022 else 13
            
            for week in range(1, max_week + 1):
                key = f"{year}-{week}"
                if key not in self.progress['completed_weeks']:
                    pending.append((year, week))
        
        return pending

        
    def get_statistics(self) -> Dict:
        """Get detailed import statistics."""
        stats = self.progress['statistics'].copy()
        
        # Calculate averages
        if stats['total_games_imported'] > 0:
            stats['avg_seconds_per_game'] = (
                stats['total_import_time_seconds'] / stats['total_games_imported']
            )
        
        # Add completion percentage
        total_expected = len(self.get_pending_weeks()) + len(self.progress['completed_weeks'])
        if total_expected > 0:
            stats['completion_percentage'] = (
                len(self.progress['completed_weeks']) / total_expected * 100
            )
        
        return stats

    def estimate_remaining_time(self) -> float:
        """Estimate time to complete remaining imports."""
        pending = self.get_pending_weeks()
        stats = self.get_statistics()
        
        if 'avg_seconds_per_game' in stats:
            # Assume ~115 games per week
            estimated_seconds = len(pending) * 115 * stats['avg_seconds_per_game']
            return estimated_seconds / 60  # Return in minutes
        
        # Fallback: 2 minutes per week
        return len(pending) * 2

    def _save(self):
        """Save progress to file."""
        with open(self.filepath, 'w') as f:
            json.dump(self.progress, f, indent=2)

    def print_status(self):
        """Print formatted status report."""
        print("\n" + "=" * 60)
        print("IMPORT PROGRESS STATUS")
        print("=" * 60)
        
        completed = len(self.progress['completed_weeks'])
        failed = len(self.progress['failed_weeks'])
        partial = len(self.progress['partial_weeks'])
        pending = len(self.get_pending_weeks())
        
        print(f"Completed Weeks: {completed}")
        print(f"Failed Weeks: {failed}")
        print(f"Partial Weeks: {partial}")
        print(f"Pending Weeks: {pending}")
        
        stats = self.get_statistics()
        print(f"\nTotal Games Imported: {stats.get('total_games_imported', 0)}")
        
        if stats.get('avg_seconds_per_game'):
            print(f"Avg Time per Game: {stats['avg_seconds_per_game']:.2f} seconds")
        
        if stats.get('completion_percentage'):
            print(f"Overall Progress: {stats['completion_percentage']:.1f}%")
        
        remaining = self.estimate_remaining_time()
        print(f"Estimated Time Remaining: {remaining:.1f} minutes")
        
        if self.progress['current_week']:
            current = self.progress['current_week']
            print(f"\nCurrently Processing: {current['key']}")
            print(f"  Games: {current['imported_games']}/{current['expected_games']}")


    
    