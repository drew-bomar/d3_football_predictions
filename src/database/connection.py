"""
Database connection management for D3 Football Predictions
This module handles all database connections using SQLAlchemy
"""
import os
from contextlib import contextmanager
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.pool import NullPool, QueuePool
from sqlalchemy.ext.declarative import declarative_base

Base  = declarative_base()

class DatabaseConnection:
    """
    Manages database connections with proper pooling and error handling.
    
    Why use a class? It gives us:
    1. A single place to configure all database settings
    2. Reusable connection logic
    3. Easy testing (we can mock this class)
    """
    def __init__(self, database_url = None):
        """
        Initialize the database connection.
        
        Args:
            database_url: PostgreSQL connection string. If None, reads from environment.
        """
        # Get database URL from environment or parameter
        self.database_url = database_url or os.getenv(
            'DATABASE_URL',
            'postgresql://d3_user:databaseguy824@localhost/d3_football'
        )

        # Engine is the core interface to the database
        # It manages the connection pool and translates SQLAlchemy commands to SQL
        self.engine = None

        # Session factory creates new database sessions (think: conversations with the DB)
        self.Session = None

        # Initialize the connection
        self._init_engine()


    def _init_engine(self):
        """
        Create the database engine with proper pooling configuration.
        
        Connection pooling is crucial for performance:
        - Reuses connections instead of creating new ones (expensive!)
        - Limits total connections to prevent overwhelming the database
        - Handles connection recycling and health checks
        """

        self.engine = create_engine(
            self.database_url,
            #pool settings tuned for our use case
            pool_size = 5,       # Number of connections to maintain in pool
            max_overflow = 10,   # Maximum overflow connections allowed
            pool_timeout = 30,   # Seconds to wait before timing out
            pool_recycle = 1800, # Recycle connections after 30 minutes

            #performance settings
            echo = False,        # Set True to see all SQL queries (debugging)
            future = True,       # Use SQLAlchemy 2.0 style

            # PostgreSQL specific settings
            connect_args={
                "options": "-c timezone=utc"  # Always use UTC in database
            }
        )

        # Add connection event listeners for debugging and monitoring
        @event.listens_for(self.engine, "connect")
        def receive_connect(dbapi_connection, connection_record):
            """Log when we receive a new connection"""
            connection_record.info['pid'] = os.getpid()

        @event.listens_for(self.engine, "checkout")
        def receive_checkout(dbapi_connection, connection_record, connection_proxy):
            """Log when a connection is checked out from the pool"""
            pid = os.getpid()
            if connection_record.info['pid'] != pid:
                # Connection record belongs to a different process
                # We need to invalidate it to avoid issues
                connection_record.connection = connection_proxy.connection = None
                raise Exception( 
                    f"Connection belongs to pid {connection_record.info['pid']}, "
                    f"but we're in pid {pid}"
                )

        #create session factory
        #scoped_session ensures thread-safety (important for web apps)
        self.Session = scoped_session(
            sessionmaker(
                bind = self.engine,
                autocommit = False,      # We'll commit explicitly
                autoflush = False,       # Don't flush automatically
                expire_on_commit = False # Don't expire objects after commit
            )
        )

    @contextmanager
    def get_session(self): 
        """
        Context manager for database sessions.
        
        Usage:
            with db.get_session() as session:
                # Do database work
                session.add(team)
                session.commit()
        
        This pattern ensures:
        1. Sessions are properly closed (no connection leaks)
        2. Transactions are rolled back on errors
        3. Clean, readable code
        """
        session = self.Session()
        try:
            yield session
            session.commit()  #commit changes if no errors
        except Exception:
            session.rollback() # rollback to last commit if any errors
            raise              # re-raise the exception
        finally:
            session.close() #always close ths 

    def create_tables(self):
        """Create all tables defined in our models."""
        Base.metadata.create_all(self.engine)

    def drop_tables(self):
        """Drop all tables. WARNING: This deletes all data!"""
        Base.metadata.drop_all(self.engine)

    def dispose(self):
        """Properly close all database connections."""
        self.Session.remove()
        self.engine.dispose()


# Global database instance
# This is a common pattern - one database connection for the whole app
db = DatabaseConnection()
        
# Convenience function for getting sessions
def get_db_session():
    """Get a database session using the global connection."""
    return db.get_session()
        
        