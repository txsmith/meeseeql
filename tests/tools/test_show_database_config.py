import os
import sys
import pytest
from pathlib import Path

from meeseeql.database_manager import load_config, DatabaseManager
from meeseeql.tools.show_database_config import show_database_config, DatabaseList


@pytest.fixture
def db_manager():
    """Fixture to provide database manager for tests"""
    config_path = os.path.join(os.path.dirname(__file__), "../test_config.yaml")
    config = load_config(config_path)
    return DatabaseManager(config)


def test_show_database_config_returns_all_configured_databases(db_manager):
    """Test that show_database_config returns all databases from config"""
    result = show_database_config(db_manager)

    assert isinstance(result, DatabaseList)
    assert len(result.databases) == 4

    db_names = [db.name for db in result.databases]

    expected_names = [
        "test_sqlite",
        "test_postgres",
        "chinook_sqlite",
        "chinook_sqlite_conn_str",
    ]
    assert sorted(db_names) == sorted(expected_names)


def test_show_database_config_includes_correct_database_info(db_manager):
    """Test that each database entry has correct structure and info"""
    result = show_database_config(db_manager)

    sqlite_db = next((db for db in result.databases if db.name == "test_sqlite"), None)
    assert sqlite_db is not None

    # Check structure
    # Check values
    assert sqlite_db.name == "test_sqlite"
    assert sqlite_db.type == "sqlite"
    assert sqlite_db.description == "Test SQLite database"


def test_show_database_config_with_empty_config():
    """Test show_database_config with empty database config"""
    from meeseeql.database_manager import AppConfig, GlobalSettings

    empty_config = AppConfig(databases={}, settings=GlobalSettings())
    empty_db_manager = DatabaseManager(empty_config)

    result = show_database_config(empty_db_manager)

    assert isinstance(result, DatabaseList)
    assert result.total_count == 0


def test_show_database_config_does_not_include_passwords():
    """Test that passwords are not included in the output"""
    from meeseeql.database_manager import AppConfig, DatabaseConfig, GlobalSettings

    config_with_password = AppConfig(
        databases={
            "test_db": DatabaseConfig(
                type="postgresql",
                description="Test DB with credentials",
                host="localhost",
                database="test",
                username="user",
                password="secret_password",
            )
        },
        settings=GlobalSettings(),
    )

    db_manager = DatabaseManager(config_with_password)
    result = show_database_config(db_manager)

    output_str = str(result)
    assert "secret_password" not in output_str
    assert "password:" not in output_str

    db_info = result.databases[0]
    assert not hasattr(db_info, "password")
