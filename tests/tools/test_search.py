import pytest
from meeseeql.database_manager import (
    load_config,
    DatabaseManager,
    DatabaseConfig,
    AppConfig,
)
from meeseeql.tools.search import search


@pytest.fixture
def db_manager():
    config = load_config("tests/test_config.yaml")
    return DatabaseManager(config)


# TODO: expand on these tests
# Sqlite misses a lot of features in the INFORMATION_SCHEMA department
# So these tests miss a lot of behavior like column matches and enums
# Some day i'll hoist up a docker container with postgres and do some integration testsing

# TODO: Schema include/exclude functionality tests (requires PostgreSQL)
# These tests need a multi-schema PostgreSQL database to be properly implemented:
# - test_search_respects_include_schemas: Verify search only returns results from included schemas
# - test_search_respects_exclude_schemas: Verify search excludes results from excluded schemas
# - test_search_explicit_schema_overrides_config: Explicit schema param should ignore config filtering
# - test_search_case_sensitivity_in_schema_names: Schema filtering should handle case-sensitive schema names correctly


async def test_search_exact_match(db_manager):
    result = await search(db_manager, "chinook_sqlite", "Track")

    assert len(result.rows) > 0

    first_result = result.rows[0]
    assert first_result.object_type == "table"
    assert first_result.schema_name == "main"
    assert "Track" in first_result.user_friendly_descriptor


async def test_search_postfix_match(db_manager):
    result = await search(db_manager, "chinook_sqlite", "rack")

    assert len(result.rows) > 0

    table_results = [r for r in result.rows if r.object_type == "table"]

    assert len(table_results) >= 1
    assert table_results[0].user_friendly_descriptor == "Track"


async def test_search_prefix_match(db_manager):
    result = await search(db_manager, "chinook_sqlite", "trac")

    assert len(result.rows) > 0

    table_results = [r for r in result.rows if r.object_type == "table"]

    assert len(table_results) >= 1
    assert table_results[0].user_friendly_descriptor == "Track"


async def test_search_no_results(db_manager):
    result = await search(db_manager, "chinook_sqlite", "nonexistent")
    assert len(result.rows) == 0


async def test_search_case_insensitive(db_manager):
    result_lower = await search(db_manager, "chinook_sqlite", "customer")
    result_upper = await search(db_manager, "chinook_sqlite", "CUSTOMER")
    result_mixed = await search(db_manager, "chinook_sqlite", "Customer")

    assert len(result_lower.rows) == len(result_upper.rows) == len(result_mixed.rows)
    assert len(result_lower.rows) > 0


async def test_search_respects_allowed_tables():
    config = AppConfig(
        databases={
            "test_db": DatabaseConfig(
                type="sqlite",
                connection_string="sqlite:///tests/Chinook_Sqlite.sqlite",
                description="Test DB with table inclusion",
                allowed_tables=["Track", "Album"],
            )
        },
        settings={},
    )
    db_manager = DatabaseManager(config)

    result = await search(db_manager, "test_db", "a")

    # Should only return results from Track and Album tables, not Artist or other tables
    table_results = [r for r in result.rows if r.object_type == "table"]
    table_names = [
        (
            r.user_friendly_descriptor.split(".")[1]
            if "." in r.user_friendly_descriptor
            else r.user_friendly_descriptor
        )
        for r in table_results
    ]

    for table_name in table_names:
        assert table_name.lower() in [
            "track",
            "album",
        ], f"Found disallowed table: {table_name}"


async def test_search_respects_disallowed_tables():
    config = AppConfig(
        databases={
            "test_db": DatabaseConfig(
                type="sqlite",
                connection_string="sqlite:///tests/Chinook_Sqlite.sqlite",
                description="Test DB with table exclusion",
                disallowed_tables=["Track", "Album"],
            )
        },
        settings={},
    )
    db_manager = DatabaseManager(config)

    result = await search(db_manager, "test_db", "a")

    # Should not return Track or Album tables
    table_results = [r for r in result.rows if r.object_type == "table"]
    table_names = [
        (
            r.user_friendly_descriptor.split(".")[1]
            if "." in r.user_friendly_descriptor
            else r.user_friendly_descriptor
        )
        for r in table_results
    ]

    for table_name in table_names:
        assert table_name.lower() not in [
            "track",
            "album",
        ], f"Found excluded table: {table_name}"
