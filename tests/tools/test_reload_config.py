import os
import sys
import pytest
import tempfile
import yaml
from pathlib import Path

from meeseeql.database_manager import DatabaseManager, AppConfig
from meeseeql.tools.reload_config import reload_config, ConfigChange

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


@pytest.fixture
def initial_config():
    """Initial config for testing"""
    return {
        "databases": {
            "db1": {
                "type": "sqlite",
                "database": ":memory:",
                "description": "Test database 1",
            },
            "db2": {
                "type": "postgresql",
                "host": "localhost",
                "port": 5432,
                "database": "testdb",
                "username": "user",
                "password": "pass",
                "description": "Test database 2",
            },
        },
        "settings": {"max_query_timeout": 30},
    }


@pytest.fixture
def db_manager(initial_config):
    """Create a DatabaseManager with initial config"""
    config = AppConfig(**initial_config)
    return DatabaseManager(config)


@pytest.fixture
def temp_config_file():
    """Fixture to create and cleanup temporary config files"""
    temp_files = []

    def _create_config_file(config_dict):
        temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False)
        yaml.dump(config_dict, temp_file)
        temp_file.close()
        temp_files.append(temp_file.name)
        return temp_file.name

    yield _create_config_file

    for temp_file in temp_files:
        if os.path.exists(temp_file):
            os.unlink(temp_file)


def test_reload_config_no_changes(db_manager, initial_config, temp_config_file):
    """Test reload with identical config"""
    config_file = temp_config_file(initial_config)

    result = reload_config(db_manager, config_file)

    assert isinstance(result, ConfigChange)
    assert result.added == []
    assert result.removed == []
    assert result.modified == []


def test_reload_config_add_database(db_manager, initial_config, temp_config_file):
    """Test adding a new database"""
    new_config = initial_config.copy()
    new_config["databases"]["db3"] = {
        "type": "sqlite",
        "database": "/tmp/test.db",
        "description": "New test database",
    }

    config_file = temp_config_file(new_config)

    result = reload_config(db_manager, config_file)

    assert result.added == ["db3"]
    assert result.removed == []
    assert result.modified == []

    assert "db3" in db_manager.config.databases


def test_reload_config_remove_database(db_manager, initial_config, temp_config_file):
    """Test removing a database"""
    new_config = initial_config.copy()
    del new_config["databases"]["db2"]

    config_file = temp_config_file(new_config)

    result = reload_config(db_manager, config_file)

    assert result.added == []
    assert result.removed == ["db2"]
    assert result.modified == []

    assert "db2" not in db_manager.config.databases


def test_reload_config_modify_database(db_manager, initial_config, temp_config_file):
    """Test modifying an existing database"""
    new_config = initial_config.copy()
    new_config["databases"]["db1"]["description"] = "Modified description"
    new_config["databases"]["db2"]["port"] = 5433

    config_file = temp_config_file(new_config)

    result = reload_config(db_manager, config_file)

    assert result.added == []
    assert result.removed == []
    assert set(result.modified) == {"db1", "db2"}

    assert db_manager.config.databases["db1"].description == "Modified description"
    assert db_manager.config.databases["db2"].port == 5433


def test_reload_config_mixed_changes(db_manager, initial_config, temp_config_file):
    """Test combination of add, remove, and modify"""
    new_config = initial_config.copy()

    del new_config["databases"]["db1"]
    new_config["databases"]["db2"]["host"] = "newhost"
    new_config["databases"]["db3"] = {
        "type": "mysql",
        "host": "mysql.example.com",
        "database": "mydb",
        "username": "myuser",
        "password": "mypass",
        "description": "MySQL database",
    }

    config_file = temp_config_file(new_config)

    result = reload_config(db_manager, config_file)

    assert result.added == ["db3"]
    assert result.removed == ["db1"]
    assert result.modified == ["db2"]

    assert "db1" not in db_manager.config.databases
    assert "db3" in db_manager.config.databases
    assert db_manager.config.databases["db2"].host == "newhost"


def test_reload_config_closes_changed_connections(
    db_manager, initial_config, temp_config_file
):
    """Test that connections are closed for changed databases"""
    from unittest.mock import Mock

    mock_engine_1 = Mock()
    mock_engine_2 = Mock()

    db_manager.engines["db1"] = mock_engine_1
    db_manager.engines["db2"] = mock_engine_2

    new_config = initial_config.copy()
    new_config["databases"]["db1"]["description"] = "Changed"

    config_file = temp_config_file(new_config)

    result = reload_config(db_manager, config_file)

    assert result.modified == ["db1"]
    assert "db1" not in db_manager.engines
    assert "db2" in db_manager.engines
    mock_engine_1.dispose.assert_called_once()
    mock_engine_2.dispose.assert_not_called()
