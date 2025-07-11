from sqlalchemy import text
from meeseeql.database_manager import DatabaseManager, DatabaseConfig, AppConfig
from meeseeql.password_provider import StaticPasswordProvider, NoOpPasswordProvider


async def test_database_manager_lazy_engine_creation():
    """Test that DatabaseManager creates engines lazily"""
    config = AppConfig(
        databases={
            "test_db": DatabaseConfig(
                type="sqlite",
                description="Test DB",
                database=":memory:",
            )
        },
        settings={},
    )

    manager = DatabaseManager(config, NoOpPasswordProvider())

    # No engines should be created initially
    assert len(manager.engines) == 0

    # Engine should be created on first access
    engine = manager._get_engine("test_db")
    assert engine is not None
    assert len(manager.engines) == 1

    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))

    # Subsequent access should return cached engine
    engine2 = manager._get_engine("test_db")
    assert engine is engine2


def test_database_manager_complete_integration():
    """Integration test with complete AppConfig and multiple databases"""
    # Track password provider calls
    call_count = {}

    class TrackingPasswordProvider(StaticPasswordProvider):
        def get_password(self, pass_key: str) -> str | None:
            call_count[pass_key] = call_count.get(pass_key, 0) + 1
            return super().get_password(pass_key)

    config = AppConfig(
        databases={
            "prod_db": DatabaseConfig(
                type="postgresql",
                description="Production DB",
                host="prod.example.com",
                port=5432,
                database="prod",
                username="prod_user",
            ),
            "dev_db": DatabaseConfig(
                type="mysql",
                description="Development DB",
                host="dev.example.com",
                database="dev",
                username="dev_user",
            ),
            "cache_db": DatabaseConfig(
                type="sqlite",
                description="Cache DB",
                database=":memory:",
            ),
            "legacy_db": DatabaseConfig(
                type="postgresql",
                description="Legacy DB",
                host="legacy.example.com",
                database="legacy",
                username="legacy_user",
                password="plaintext_password",
            ),
        },
        settings={"max_query_timeout": 45},
    )

    # Provider with passwords for some databases (using default pass key format)
    password_provider = TrackingPasswordProvider(
        {
            "databases/prod_db": "prod_secret",
            "databases/dev_db": "dev_secret",
            # No password for cache_db or legacy_db
        }
    )

    manager = DatabaseManager(config, password_provider)

    # Initially no engines created and no password calls
    assert len(manager.engines) == 0
    assert len(call_count) == 0

    # List database names without creating engines
    db_names = manager.list_database_names()
    assert set(db_names) == {"prod_db", "dev_db", "cache_db", "legacy_db"}
    assert len(manager.engines) == 0  # Still no engines created

    # Get engine for prod_db - should call password provider once
    prod_engine = manager._get_engine("prod_db")
    assert prod_engine is not None
    assert len(manager.engines) == 1
    assert call_count.get("databases/prod_db") == 1

    # Get same engine again - should NOT call password provider again
    prod_engine2 = manager._get_engine("prod_db")
    assert prod_engine is prod_engine2
    assert len(manager.engines) == 1
    assert call_count.get("databases/prod_db") == 1  # No additional call

    # Get engine for dev_db - should call password provider once for dev
    dev_engine = manager._get_engine("dev_db")
    assert dev_engine is not None
    assert len(manager.engines) == 2
    assert call_count.get("databases/dev_db") == 1
    assert call_count.get("databases/prod_db") == 1  # Unchanged

    # Get engine for SQLite cache_db - should not call password provider
    cache_engine = manager._get_engine("cache_db")
    assert cache_engine is not None
    assert len(manager.engines) == 3
    assert "databases/cache_db" not in call_count

    # Get engine for legacy_db with plaintext password - should not call provider
    legacy_engine = manager._get_engine("legacy_db")
    assert legacy_engine is not None
    assert len(manager.engines) == 4
    assert call_count.get("databases/legacy_db") is None  # No call made

    # Verify final state: each password lookup happened exactly once
    assert call_count == {
        "databases/prod_db": 1,
        "databases/dev_db": 1,
    }
