from meeseeql.database_manager import DatabaseManager, DatabaseConfig, AppConfig
from meeseeql.password_provider import StaticPasswordProvider, NoOpPasswordProvider


def test_database_manager_with_pass_provider():
    """Test DatabaseManager using password provider for missing passwords"""
    config = AppConfig(
        databases={
            "test_db": DatabaseConfig(
                type="postgresql",
                description="Test DB",
                host="localhost",
                port=5432,
                database="mydb",
                username="user",
            )
        },
        settings={},
    )

    password_provider = StaticPasswordProvider(
        {"databases/test_db": "secret_from_provider"}
    )
    manager = DatabaseManager(config, password_provider)

    db_config = manager.get_database_config("test_db")
    url = manager._get_connection_url("test_db", db_config)

    assert url.password == "secret_from_provider"


def test_database_manager_password_not_found():
    """Test DatabaseManager when password provider returns None"""
    config = AppConfig(
        databases={
            "test_db": DatabaseConfig(
                type="postgresql",
                description="Test DB",
                host="localhost",
                port=5432,
                database="mydb",
                username="user",
            )
        },
        settings={},
    )

    password_provider = NoOpPasswordProvider()
    manager = DatabaseManager(config, password_provider)

    db_config = manager.get_database_config("test_db")
    url = manager._get_connection_url("test_db", db_config)

    assert url.password is None


def test_database_manager_plaintext_password():
    """Test DatabaseManager with existing plaintext password"""
    config = AppConfig(
        databases={
            "test_db": DatabaseConfig(
                type="postgresql",
                description="Test DB",
                host="localhost",
                port=5432,
                database="mydb",
                username="user",
                password="plaintext_password",
            )
        },
        settings={},
    )

    password_provider = StaticPasswordProvider(
        {"databases/test_db": "provider_password"}
    )
    manager = DatabaseManager(config, password_provider)

    db_config = manager.get_database_config("test_db")
    url = manager._get_connection_url("test_db", db_config)

    assert url.password == "plaintext_password"


def test_database_manager_connection_string():
    """Test DatabaseManager with connection string bypasses password provider"""
    config = AppConfig(
        databases={
            "test_db": DatabaseConfig(
                type="postgresql",
                description="Test DB",
                connection_string="postgresql://user:conn_pass@localhost:5432/mydb",
            )
        },
        settings={},
    )

    password_provider = StaticPasswordProvider(
        {"databases/test_db": "provider_password"}
    )
    manager = DatabaseManager(config, password_provider)

    db_config = manager.get_database_config("test_db")
    url = manager._get_connection_url("test_db", db_config)

    assert url == "postgresql+asyncpg://user:conn_pass@localhost:5432/mydb"


def test_database_manager_custom_password_store_key():
    """Test DatabaseManager with custom password_store_key"""
    config = AppConfig(
        databases={
            "test_db": DatabaseConfig(
                type="postgresql",
                description="Test DB",
                host="localhost",
                port=5432,
                database="mydb",
                username="user",
                password_store_key="custom/path/mypassword",
            )
        },
        settings={},
    )

    password_provider = StaticPasswordProvider(
        {"custom/path/mypassword": "custom_secret"}
    )
    manager = DatabaseManager(config, password_provider)

    db_config = manager.get_database_config("test_db")
    url = manager._get_connection_url("test_db", db_config)

    assert url.password == "custom_secret"


def test_database_manager_mixed_password_store_keys():
    """Test DatabaseManager with mix of default and custom password store keys"""
    call_count = {}

    class TrackingPasswordProvider(StaticPasswordProvider):
        def get_password(self, pass_key: str) -> str | None:
            call_count[pass_key] = call_count.get(pass_key, 0) + 1
            return super().get_password(pass_key)

    config = AppConfig(
        databases={
            "default_db": DatabaseConfig(
                type="postgresql",
                description="DB using default pass key",
                host="localhost",
                port=5432,
                database="defaultdb",
                username="default_user",
            ),
            "custom_db": DatabaseConfig(
                type="postgresql",
                description="DB using custom pass key",
                host="localhost",
                port=5432,
                database="customdb",
                username="custom_user",
                password_store_key="company/production/database",
            ),
        },
        settings={},
    )

    password_provider = TrackingPasswordProvider(
        {
            "databases/default_db": "default_secret",
            "company/production/database": "custom_secret",
        }
    )
    manager = DatabaseManager(config, password_provider)

    default_engine = manager._get_engine("default_db")
    assert default_engine is not None
    assert call_count.get("databases/default_db") == 1

    custom_engine = manager._get_engine("custom_db")
    assert custom_engine is not None
    assert call_count.get("company/production/database") == 1

    assert call_count == {
        "databases/default_db": 1,
        "company/production/database": 1,
    }
