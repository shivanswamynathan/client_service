# ginthi_agents/client_service/alembic_migration/env.py
import os
import sys
from logging.config import fileConfig

from alembic import context
from dotenv import load_dotenv
from sqlalchemy import engine_from_config, pool

# ---------------------------------------------------------------------
# PATH SETUP
# ---------------------------------------------------------------------
# Ensure project root (ginthi_agents/) is in sys.path
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
sys.path.append(BASE_DIR)

# ---------------------------------------------------------------------
# ENVIRONMENT & DB CONFIG
# ---------------------------------------------------------------------
# Load environment variables from .env file at project root
load_dotenv(os.path.join(BASE_DIR, ".env"))

# Import SQLAlchemy Base from the service's DB models
from client_service.db.postgres_db import Base  # noqa: E402

# ---------------------------------------------------------------------
# ALEMBIC CONFIGURATION
# ---------------------------------------------------------------------
config = context.config

# Configure Alembic logging (optional but useful)
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Metadata from SQLAlchemy models for autogenerate support
target_metadata = Base.metadata

# Build database URL from .env
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")

DATABASE_URL = (
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# Inject DB URL dynamically into Alembic config
config.set_main_option("sqlalchemy.url", DATABASE_URL)

# Ensure versions folder exists
VERSIONS_DIR = os.path.join(os.path.dirname(__file__), "versions")
os.makedirs(VERSIONS_DIR, exist_ok=True)
config.set_main_option("script_location", os.path.dirname(__file__))


# ---------------------------------------------------------------------
# MIGRATION EXECUTION
# ---------------------------------------------------------------------
def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
