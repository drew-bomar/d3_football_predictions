import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.pipeline.simple_pipeline import SimplePipeline

if len(sys.argv) != 3:
    print("Usage: python scripts/import_week.py <year> <week>")
    sys.exit(1)

year = int(sys.argv[1])
week = int(sys.argv[2])

pipeline = SimplePipeline()
result = pipeline.import_week(year, week)

# Handle different return structures
if isinstance(result, dict):
    imported = result.get('games_imported', 'unknown')
    failed = result.get('games_failed', 'unknown')
    print(f"✅ Import complete: {imported} games imported, {failed} failed")
else:
    print(f"✅ Import complete (check logs for details)")