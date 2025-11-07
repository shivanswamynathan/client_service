from contextlib import asynccontextmanager
from fastapi import FastAPI
from client_service.db.postgres_db import init_db, close_db
from client_service.db.mongo_db import init_db as init_mongo
import logging

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manage application lifespan: startup and shutdown events.
    
    Args:
        app: FastAPI application instance
    """
    # Startup
    logger.info("Starting application...")
    try:
        await init_db()
        logger.info("PostgreSQL Database initialized successfully")

        await init_mongo()  # Add this
        print("MongoDB initialized successfully")
        
    except Exception as e:
        logger.error(f"Failed to initialize database: {str(e)}")
        raise
    
    yield
    
    # Shutdown
    logger.info("Shutting down application...")
    try:
        await close_db()
        logger.info("Database connections closed successfully")
    except Exception as e:
        logger.error(f"Error during shutdown: {str(e)}")