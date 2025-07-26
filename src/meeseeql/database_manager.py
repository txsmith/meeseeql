import yaml
import asyncio
from typing import Dict, Any, List
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine
from sqlalchemy import create_engine, Engine, text
from sqlalchemy.engine.url import URL
from pydantic import BaseModel, model_validator
from meeseeql.password_provider import PasswordProvider, PassPasswordProvider


class ConnectionError(Exception):
    pass


class DatabaseNotFoundError(Exception):
    pass


class ConfigurationError(Exception):
    pass


class QueryError(Exception):
    pass


class DatabaseConfig(BaseModel):
    type: str
    description: str

    # Option 1: Use connection string directly
    connection_string: str | None = None

    # Option 2: Use individual fields
    host: str | None = None
    port: int | None = None
    database: str | None = None
    username: str | None = None
    password: str | None = None
    account: str | None = None  # For Snowflake

    # Custom password store key (overrides default databases/{db_name})
    password_store_key: str | None = None

    extra_params: Dict[str, str] | None = None

    @model_validator(mode="after")
    def validate_config(self):
        # Validate supported database types
        supported_types = {"postgresql", "mysql", "sqlserver", "snowflake", "sqlite"}
        if self.type not in supported_types:
            raise ConfigurationError(f"Unsupported database type: {self.type}")

        # If using connection string, no further validation needed
        if self.connection_string:
            return self

        if not self.database:
            raise ConfigurationError("database field is required")

        # For SQLite, only database field is required
        if self.type == "sqlite":
            return self

        if not all([self.host, self.database, self.username]):
            raise ConfigurationError(
                "Either connection_string or host/database/username must be provided"
            )

        return self

    @property
    def dialect(self) -> str:
        dialect_map = {
            "postgresql": "postgresql+asyncpg",
            "mysql": "mysql+aiomysql",
            "sqlserver": "mssql+pyodbc",
            "snowflake": "snowflake",
            "sqlite": "sqlite+aiosqlite",
        }
        return dialect_map[self.type]


class AppConfig(BaseModel):
    databases: Dict[str, DatabaseConfig]
    settings: Dict[str, Any]

    @model_validator(mode="after")
    def validate_config(self):
        seen_names = set()
        for db_name in self.databases.keys():
            name_lower = db_name.lower()
            if name_lower in seen_names:
                raise ConfigurationError(f"{db_name} is defined twice!")
            seen_names.add(name_lower)

        return self


class DatabaseManager:
    def __init__(
        self, config: AppConfig, password_provider: PasswordProvider | None = None
    ):
        self.config = config
        self.password_provider = password_provider or PassPasswordProvider()
        self.engines: Dict[str, AsyncEngine | Engine] = {}

    def _get_connection_url(self, label: str, db_config: DatabaseConfig):
        if db_config.connection_string:
            # Convert sync connection strings to async ones
            conn_str = db_config.connection_string
            if conn_str.startswith("sqlite://"):
                conn_str = conn_str.replace("sqlite://", "sqlite+aiosqlite://", 1)
            elif conn_str.startswith("postgresql://"):
                conn_str = conn_str.replace("postgresql://", "postgresql+asyncpg://", 1)
            elif conn_str.startswith("mysql://"):
                conn_str = conn_str.replace("mysql://", "mysql+aiomysql://", 1)
            return conn_str

        if db_config.type == "sqlite":
            if db_config.database == ":memory:":
                return f"{db_config.dialect}:///:memory:"
            else:
                return f"{db_config.dialect}:///{db_config.database}"

        password = db_config.password
        if not password and db_config.username:
            pass_key = db_config.password_store_key or f"databases/{label}"
            password = self.password_provider.get_password(pass_key)

        # Special handling for Snowflake account parameter
        query_params = db_config.extra_params.copy() if db_config.extra_params else {}
        if db_config.type == "snowflake" and db_config.account:
            query_params["account"] = db_config.account

        url = URL.create(
            drivername=db_config.dialect,
            username=db_config.username,
            password=password,
            host=db_config.host,
            port=db_config.port,
            database=db_config.database,
            query=query_params,
        )

        return url

    def _get_engine(self, db_label: str) -> AsyncEngine | Engine:
        if db_label not in self.engines:
            db_config = self.config.databases.get(db_label)
            if not db_config:
                raise DatabaseNotFoundError(
                    f"Database config entry for {db_label} not found"
                )

            engine_kwargs = {"echo": False}
            url = self._get_connection_url(db_label, db_config)

            # Only add pool_timeout for non-SQLite databases
            if not str(url).startswith("sqlite"):
                engine_kwargs["pool_timeout"] = self.config.settings.get(
                    "max_query_timeout", 30
                )

            # Snowflake doesn't have native async support, use sync engine with async wrapper
            if db_config.type == "snowflake":
                engine = create_engine(url, **engine_kwargs)
            else:
                engine = create_async_engine(url, **engine_kwargs)
            self.engines[db_label] = engine

        return self.engines.get(db_label)

    def list_database_names(self) -> List[str]:
        return list(self.config.databases.keys())

    def get_database_config(self, db_name: str) -> DatabaseConfig | None:
        return self.config.databases.get(db_name)

    def get_dialect_name(self, db_label: str) -> str:
        """Get the dialect name from the engine for the given database"""
        engine = self._get_engine(db_label)
        return engine.dialect.name.lower()

    async def execute_query(self, db_label: str, query: str):
        """Execute a query handling both sync and async connections"""
        engine = self._get_engine(db_label)

        if isinstance(engine, AsyncEngine):
            async with engine.connect() as conn:
                try:
                    result = await conn.execute(text(query))
                    return result
                except Exception as e:
                    raise QueryError(f"Error executing query: {str(e)}") from e
        else:
            loop = asyncio.get_event_loop()

            def _execute_sync():
                with engine.connect() as conn:
                    try:
                        return conn.execute(text(query))
                    except Exception as e:
                        raise QueryError(f"Error executing query: {str(e)}") from e

            result = await loop.run_in_executor(None, _execute_sync)
            return result

    def get_default_schema(self, database: str) -> str:
        """Get the default schema for a database based on its dialect"""
        dialect = self.get_dialect_name(database)
        default_schemas = {
            "postgresql": "public",
            "mysql": database,
            "sqlite": "main",
            "mssql": "dbo",
            "snowflake": "PUBLIC",
        }
        return default_schemas.get(dialect, "public")

    def reload_config(self, new_config: AppConfig, changed_db_names: set[str]):
        for db_name in changed_db_names:
            if db_name in self.engines:
                engine = self.engines[db_name]
                if engine:
                    engine.dispose()
                del self.engines[db_name]

        self.config = new_config


def load_config(config_path: str) -> AppConfig:
    with open(config_path, "r") as f:
        config_data = yaml.safe_load(f)
    return AppConfig(**config_data)
