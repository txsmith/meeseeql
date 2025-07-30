import pytest
from meeseeql.database_manager import load_config, DatabaseManager
from meeseeql.tools.fuzzy_search import fuzzy_search


@pytest.fixture
def db_manager():
    config = load_config("tests/test_config.yaml")
    return DatabaseManager(config)


# TODO: expand on these tests
# Sqlite misses a lot of features in the INFORMATION_SCHEMA department
# So these tests miss a lot of behavior like column matches and enums
# Some day i'll hoist up a docker container with postgres and do some integration testsing


async def test_fuzzy_search_exact_match(db_manager):
    result = await fuzzy_search(db_manager, "chinook_sqlite", "Track")

    assert len(result.rows) > 0

    first_result = result.rows[0]
    assert first_result.object_type == "table"
    assert first_result.schema_name == "main"
    assert "Track" in first_result.user_friendly_descriptor


async def test_fuzzy_search_postfix_match(db_manager):
    result = await fuzzy_search(db_manager, "chinook_sqlite", "rack")

    assert len(result.rows) > 0

    table_results = [r for r in result.rows if r.object_type == "table"]

    assert len(table_results) >= 1
    assert table_results[0].user_friendly_descriptor == "Track"


async def test_fuzzy_search_prefix_match(db_manager):
    result = await fuzzy_search(db_manager, "chinook_sqlite", "trac")

    assert len(result.rows) > 0

    table_results = [r for r in result.rows if r.object_type == "table"]

    assert len(table_results) >= 1
    assert table_results[0].user_friendly_descriptor == "Track"


async def test_fuzzy_search_no_results(db_manager):
    result = await fuzzy_search(db_manager, "chinook_sqlite", "nonexistent")
    assert len(result.rows) == 0


async def test_fuzzy_search_case_insensitive(db_manager):
    result_lower = await fuzzy_search(db_manager, "chinook_sqlite", "customer")
    result_upper = await fuzzy_search(db_manager, "chinook_sqlite", "CUSTOMER")
    result_mixed = await fuzzy_search(db_manager, "chinook_sqlite", "Customer")

    # All should return same results
    assert len(result_lower.rows) == len(result_upper.rows) == len(result_mixed.rows)
    assert len(result_lower.rows) > 0
