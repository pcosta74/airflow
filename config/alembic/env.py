import os
import sys
from logging.config import fileConfig
from pathlib import Path

from alembic import context
from sqlalchemy import engine_from_config, pool

# ---------------------------------------------------------------------
# 1. PATH CONFIGURATION FOR DYNAMIC PACKAGE DISCOVERY
# ---------------------------------------------------------------------
# Force our 'src/' repository structure onto the system path during runtime
# execution so Alembic can discover our data_infra model mapping blocks cleanly.
ALEMBIC_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = ALEMBIC_DIR.parent.parent.parent
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

# Now we can perform absolute internal package module imports safely [1]
from data_infra.integrations.shop.models import Base  # noqa: E402

# ---------------------------------------------------------------------
# 2. STANDARD ALEMBIC RUNTIME INITIALIZATION
# ---------------------------------------------------------------------
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Link your declarative class metadata footprint to the migration target [1]
target_metadata = Base.metadata


# ---------------------------------------------------------------------
# 3. MIGRATION RUN MODES
# ---------------------------------------------------------------------
def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL and not an Engine,
    though an Engine is acceptable here as well. By skipping the Engine
    creation we don't even need a DBAPI to be available.
    """
    db_url = os.environ.get("SHOP_DATABASE_URL")

    context.configure(
        url=db_url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""

    # Get the configuration dictionary
    alembic_config = context.config.get_section(context.config.config_ini_section) or {}

    #  Dynamically inject the environment variable if it exists
    db_url = os.environ.get("SHOP_DATABASE_URL")
    if db_url:
        alembic_config["sqlalchemy.url"] = db_url

    connectable = engine_from_config(
        alembic_config,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)

        with context.begin_transaction():
            context.run_migrations()


# Evaluate the active initialization entry point parameter state
if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
