from sqlalchemy import create_engine
import os

# You can also use environment variables for security
DB_USER = "d3_user"
DB_PASS = "your_password_here"  # Replace with your actual password
DB_NAME = "d3_football"

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@localhost/{DB_NAME}"

engine = create_engine(DATABASE_URL)

try:
    conn = engine.connect()
    print("✅ Connected to PostgreSQL successfully!")
    print(f"📊 Database: {DB_NAME}")
    conn.close()
except Exception as e:
    print(f"❌ Connection failed: {e}")