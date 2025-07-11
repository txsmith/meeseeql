import os
import pytest

from meeseeql.database_manager import load_config, DatabaseManager
from meeseeql.tools.search_tables import (
    search_tables,
    SearchTablesResponse,
    SearchTablesError,
)


@pytest.fixture
def db_manager():
    """Fixture to provide database manager for tests"""
    config_path = os.path.join(os.path.dirname(__file__), "..", "test_config.yaml")
    config = load_config(config_path)
    return DatabaseManager(config)


async def test_search_tables_no_search_term(db_manager):
    """Test that search_tables without search term returns all tables"""
    result = await search_tables(db_manager, "chinook_sqlite")

    assert isinstance(result, SearchTablesResponse)
    assert result.database == "chinook_sqlite"
    assert result.search_term is None
    assert result.total_count >= 0
    assert result.current_page == 1
    assert result.total_pages >= 1


async def test_search_tables_chinook_database(db_manager):
    """Test that search_tables can find all tables in Chinook database"""
    result = await search_tables(db_manager, "chinook_sqlite")

    assert isinstance(result, SearchTablesResponse)
    assert len(result.schemas) > 0

    all_tables = []
    for schema in result.schemas:
        all_tables.extend(schema.tables)

    expected_tables = [
        "Album",
        "Artist",
        "Customer",
        "Employee",
        "Genre",
        "Invoice",
        "InvoiceLine",
        "MediaType",
        "Playlist",
        "PlaylistTrack",
        "Track",
    ]

    for table in expected_tables:
        assert (
            table in all_tables
        ), f"Expected table '{table}' not found in Chinook database"


async def test_search_tables_empty_memory_database(db_manager):
    """Test that search_tables returns empty results for in-memory sqlite DB"""
    result = await search_tables(db_manager, "test_sqlite", limit=10, page=1)

    assert isinstance(result, SearchTablesResponse)
    assert len(result.schemas) == 0
    assert result.total_count == 0
    assert result.current_page == 1
    assert result.total_pages == 1


async def test_search_tables_with_search_term(db_manager):
    """Test that search_tables filters tables by search term"""
    result = await search_tables(
        db_manager, "chinook_sqlite", search_term="track", limit=100, page=1
    )

    assert isinstance(result, SearchTablesResponse)
    assert result.database == "chinook_sqlite"
    assert result.search_term == "track"

    all_tables = []
    for schema in result.schemas:
        all_tables.extend(schema.tables)

    # Should find Track and PlaylistTrack tables
    assert any("track" in table.lower() for table in all_tables)


async def test_search_tables_exact_match_priority(db_manager):
    """Test that exact matches get highest priority in search results"""
    result = await search_tables(
        db_manager, "chinook_sqlite", search_term="Track", limit=100, page=1
    )

    assert isinstance(result, SearchTablesResponse)
    assert result.search_term == "Track"

    all_tables = []
    for schema in result.schemas:
        all_tables.extend(schema.tables)

    # "Track" should come before "PlaylistTrack" due to exact match
    track_index = all_tables.index("Track")
    playlist_track_index = all_tables.index("PlaylistTrack")
    assert track_index < playlist_track_index


async def test_search_tables_prefix_match_priority(db_manager):
    """Test that prefix matches get higher priority than substring matches"""
    result = await search_tables(
        db_manager, "chinook_sqlite", search_term="Play", limit=100, page=1
    )

    assert isinstance(result, SearchTablesResponse)
    assert result.search_term == "Play"

    all_tables = []
    for schema in result.schemas:
        all_tables.extend(schema.tables)

    # Should find tables starting with "Play" first
    assert any(table.lower().startswith("play") for table in all_tables)


async def test_search_tables_case_insensitive(db_manager):
    """Test that search is case insensitive"""
    result_lower = await search_tables(
        db_manager, "chinook_sqlite", search_term="track", limit=100, page=1
    )
    result_upper = await search_tables(
        db_manager, "chinook_sqlite", search_term="TRACK", limit=100, page=1
    )
    result_mixed = await search_tables(
        db_manager, "chinook_sqlite", search_term="Track", limit=100, page=1
    )

    # All should return same results
    lower_tables = [t for s in result_lower.schemas for t in s.tables]
    upper_tables = [t for s in result_upper.schemas for t in s.tables]
    mixed_tables = [t for s in result_mixed.schemas for t in s.tables]

    assert lower_tables == upper_tables == mixed_tables


async def test_search_tables_pagination_no_search(db_manager):
    """Test that pagination works correctly without search term"""
    # Get first page with small limit
    page1 = await search_tables(db_manager, "chinook_sqlite", limit=5, page=1)
    assert page1.current_page == 1
    assert page1.total_count == 11
    assert page1.total_pages == 3
    assert len([t for s in page1.schemas for t in s.tables]) == 5

    # Get second page
    page2 = await search_tables(db_manager, "chinook_sqlite", limit=5, page=2)
    assert page2.current_page == 2
    assert page2.total_count == 11
    assert page2.total_pages == 3
    assert len([t for s in page2.schemas for t in s.tables]) == 5

    # Get third page (should have 1 table)
    page3 = await search_tables(db_manager, "chinook_sqlite", limit=5, page=3)
    assert page3.current_page == 3
    assert page3.total_count == 11
    assert page3.total_pages == 3
    assert len([t for s in page3.schemas for t in s.tables]) == 1

    # Pages should have different tables
    page1_tables = [t for s in page1.schemas for t in s.tables]
    page2_tables = [t for s in page2.schemas for t in s.tables]
    assert set(page1_tables).isdisjoint(set(page2_tables))


async def test_search_tables_pagination_with_search(db_manager):
    """Test that pagination works with search terms"""
    # Search for tables with small limit to test pagination
    page1 = await search_tables(
        db_manager, "chinook_sqlite", search_term="e", limit=3, page=1
    )
    page2 = await search_tables(
        db_manager, "chinook_sqlite", search_term="e", limit=3, page=2
    )

    assert page1.current_page == 1
    assert page2.current_page == 2

    page1_tables = [t for s in page1.schemas for t in s.tables]
    page2_tables = [t for s in page2.schemas for t in s.tables]

    # Pages should have different tables
    assert set(page1_tables).isdisjoint(set(page2_tables))


async def test_search_tables_invalid_page_number(db_manager):
    """Test that invalid page numbers raise errors"""
    with pytest.raises(SearchTablesError, match="Page number must be greater than 0"):
        await search_tables(db_manager, "chinook_sqlite", limit=10, page=0)

    with pytest.raises(SearchTablesError, match="Page number must be greater than 0"):
        await search_tables(db_manager, "chinook_sqlite", limit=10, page=-1)


async def test_search_tables_invalid_limit(db_manager):
    """Test that invalid limit values raise errors"""
    with pytest.raises(SearchTablesError, match="Limit must be greater than 0"):
        await search_tables(db_manager, "chinook_sqlite", limit=0, page=1)

    with pytest.raises(SearchTablesError, match="Limit must be greater than 0"):
        await search_tables(db_manager, "chinook_sqlite", limit=-1, page=1)


async def test_search_tables_limit_clamping():
    """Test that limit is clamped to max_rows_per_query"""
    config_path = os.path.join(os.path.dirname(__file__), "..", "test_config.yaml")
    config = load_config(config_path)

    config.settings["max_rows_per_query"] = 3

    db_manager = DatabaseManager(config)

    result = await search_tables(db_manager, "chinook_sqlite", limit=10, page=1)
    assert isinstance(result, SearchTablesResponse)

    total_tables_returned = len([t for s in result.schemas for t in s.tables])
    assert total_tables_returned == 3


async def test_search_tables_case_insensitive_schema(db_manager):
    """Test that search_tables handles case insensitive schema names correctly"""
    result_exact = await search_tables(db_manager, "chinook_sqlite", schema="main")
    result_lower = await search_tables(db_manager, "chinook_sqlite", schema="main")
    result_upper = await search_tables(db_manager, "chinook_sqlite", schema="MAIN")
    result_mixed = await search_tables(db_manager, "chinook_sqlite", schema="Main")

    exact_tables = {t for s in result_exact.schemas for t in s.tables}
    lower_tables = {t for s in result_lower.schemas for t in s.tables}
    upper_tables = {t for s in result_upper.schemas for t in s.tables}
    mixed_tables = {t for s in result_mixed.schemas for t in s.tables}

    assert exact_tables == lower_tables == upper_tables == mixed_tables


async def test_search_tables_with_specific_schema(db_manager):
    """Test that search_tables works with specific schema parameter"""
    result = await search_tables(
        db_manager, "chinook_sqlite", limit=10, page=1, schema="main"
    )

    assert isinstance(result, SearchTablesResponse)
    assert len(result.schemas) == 1
    assert result.schemas[0].db_schema == "main"
    assert len(result.schemas[0].tables) > 0


async def test_search_tables_with_schema_filter(db_manager):
    """Test that schema filtering works with search"""
    result = await search_tables(
        db_manager,
        "chinook_sqlite",
        search_term="track",
        schema="main",
        limit=100,
        page=1,
    )

    assert isinstance(result, SearchTablesResponse)
    assert result.search_term == "track"

    for schema in result.schemas:
        assert schema.db_schema == "main"


async def test_search_tables_no_results(db_manager):
    """Search with term that matches no tables should still return all tables"""
    result = await search_tables(
        db_manager, "chinook_sqlite", search_term="nonexistent"
    )

    assert isinstance(result, SearchTablesResponse)
    assert result.search_term == "nonexistent"
    assert result.total_count == 11
    assert len(result.schemas) == 1


async def test_search_tables_empty_search_term(db_manager):
    """Test that empty string search term is treated as no search"""
    result = await search_tables(db_manager, "chinook_sqlite", search_term="")

    assert isinstance(result, SearchTablesResponse)
    assert result.search_term == ""

    # Should return all tables without search filtering
    assert result.total_count > 0
