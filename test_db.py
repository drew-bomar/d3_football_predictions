from src.database.connection import DatabaseConnection
from sqlalchemy import text

db = DatabaseConnection()
with db.get_session() as session:
    # What predictions exist?
    result = session.execute(text("""
        SELECT year, week, COUNT(*) as count 
        FROM predictions 
        GROUP BY year, week 
        ORDER BY year, week
    """))
    print("Predictions in database:")
    for row in result:
        print(f"  {row[0]} Week {row[1]}: {row[2]} predictions")
