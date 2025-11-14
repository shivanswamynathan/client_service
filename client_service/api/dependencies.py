from sqlalchemy.ext.asyncio import AsyncSession
from client_service.db.postgres_db import get_db


async def get_database_session() -> AsyncSession:
    """
    Dependency function to get database session.
    Acts as a wrapper around get_db for use in route handlers.
    """
    async for session in get_db():
        yield session