from meeseeql.database_manager import (
    AppConfig,
    DatabaseConfig,
    DatabaseManager,
    GlobalSettings,
)


def test_get_available_tools_returns_none_when_not_configured():
    """Test that get_available_tools returns None when available_tools is not configured"""
    config = AppConfig(
        databases={
            "test_db": DatabaseConfig(
                type="sqlite", description="Test", database=":memory:"
            )
        },
        settings=GlobalSettings(),
    )
    db_manager = DatabaseManager(config)

    assert db_manager.get_available_tools() is None


def test_get_available_tools_returns_configured_tools():
    """Test that get_available_tools returns the configured tools"""
    config = AppConfig(
        databases={
            "test_db": DatabaseConfig(
                type="sqlite", description="Test", database=":memory:"
            )
        },
        settings=GlobalSettings(available_tools=["execute_query", "table_summary"]),
    )
    db_manager = DatabaseManager(config)

    tools = db_manager.get_available_tools()
    assert tools == ["execute_query", "table_summary"]


def test_get_available_tools_returns_empty_list():
    """Test that get_available_tools can return an empty list"""
    config = AppConfig(
        databases={
            "test_db": DatabaseConfig(
                type="sqlite", description="Test", database=":memory:"
            )
        },
        settings=GlobalSettings(available_tools=[]),
    )
    db_manager = DatabaseManager(config)

    tools = db_manager.get_available_tools()
    assert tools == []
