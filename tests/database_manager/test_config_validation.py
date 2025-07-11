from meeseeql.database_manager import DatabaseConfig, AppConfig, ConfigurationError
from pytest import raises


def test_missing_required_fields():
    """Test that missing required fields raise ValueError at config creation time"""
    with raises(
        ConfigurationError,
        match="Either connection_string or host/database/username must be provided",
    ):
        DatabaseConfig(
            type="postgresql",
            description="Test DB",
            host="localhost",
            database="postgres",
        )


def test_unsupported_database_type():
    """Test that unsupported database types raise ValueError at config creation time"""
    with raises(ConfigurationError, match="Unsupported database type"):
        DatabaseConfig(
            type="wiggle",
            description="Test DB",
            host="localhost",
            database="mydb",
        )


def test_duplicate_database_names():
    """Test that duplicate database names (case-insensitive) raise ValueError"""
    with raises(ConfigurationError, match="TEST_DB is defined twice!"):
        AppConfig(
            databases={
                "test_db": DatabaseConfig(
                    type="sqlite",
                    description="Test DB",
                    database=":memory:",
                ),
                "TEST_DB": DatabaseConfig(
                    type="sqlite",
                    description="Another Test DB",
                    database=":memory:",
                ),
            },
            settings={},
        )
