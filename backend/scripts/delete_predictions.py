import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.database.connection import DatabaseConnection
from sqlalchemy import text

def delete_predictions(year: int, week: int, model_version: str = None):
    """Delete predictions using raw SQL to avoid ORM relationship issues"""
    db = DatabaseConnection()
    
    with db.get_session() as session:
        # Count first
        count_sql = "SELECT COUNT(*) FROM predictions WHERE year = :year AND week = :week"
        params = {'year': year, 'week': week}
        
        if model_version:
            count_sql += " AND model_version = :model_version"
            params['model_version'] = model_version
        
        count = session.execute(text(count_sql), params).scalar()
        print(f"Found {count} predictions to delete")
        
        if count > 0:
            response = input(f"Delete {count} predictions for {year} week {week}? (yes/no): ")
            if response.lower() == 'yes':
                delete_sql = "DELETE FROM predictions WHERE year = :year AND week = :week"
                if model_version:
                    delete_sql += " AND model_version = :model_version"
                
                session.execute(text(delete_sql), params)
                session.commit()
                print(f"✅ Deleted {count} predictions")
            else:
                print("❌ Cancelled")
        else:
            print("No predictions to delete")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python scripts/delete_predictions_sql.py <year> <week> [model_version]")
        sys.exit(1)
    
    year = int(sys.argv[1])
    week = int(sys.argv[2])
    model_version = sys.argv[3] if len(sys.argv) > 3 else None
    
    delete_predictions(year, week, model_version)