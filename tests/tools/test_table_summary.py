import os
import pytest

from meeseeql.database_manager import (
    load_config,
    DatabaseManager,
)
from meeseeql.tools.table_summary import (
    table_summary,
    TableSummary,
    TableNotFoundError,
)


@pytest.fixture
def db_manager():
    config_path = os.path.join(os.path.dirname(__file__), "../test_config.yaml")
    config = load_config(config_path)
    return DatabaseManager(config)


async def test_table_summary_album_structure(db_manager):
    """Test that table_summary correctly describes Album table structure"""
    result = await table_summary(db_manager, "chinook_sqlite", "Album")

    assert isinstance(result, TableSummary)
    assert result.table == "main.Album"

    assert len(result.columns) == 3

    column_names = [col.name for col in result.columns]
    expected_columns = ["AlbumId", "Title", "ArtistId"]
    for expected_col in expected_columns:
        assert expected_col in column_names

    album_id_col = next(col for col in result.columns if col.name == "AlbumId")
    assert album_id_col.primary_key is True
    assert album_id_col.nullable is False

    assert len(result.foreign_keys) == 1
    fk = result.foreign_keys[0]
    assert fk.from_columns == ["ArtistId"]
    assert fk.to_table == "main.Artist"
    assert fk.to_columns == ["ArtistId"]


async def test_table_summary_artist_incoming_fks(db_manager):
    """Test that table_summary correctly finds incoming foreign keys for Artist table"""
    result = await table_summary(db_manager, "chinook_sqlite", "Artist")

    assert isinstance(result, TableSummary)
    assert result.table == "main.Artist"

    assert len(result.incoming_foreign_keys) >= 1

    album_fk = next(
        (fk for fk in result.incoming_foreign_keys if fk.from_table == "main.Album"),
        None,
    )
    assert album_fk is not None
    assert album_fk.from_columns == ["ArtistId"]
    assert album_fk.to_columns == ["ArtistId"]


async def test_table_summary_nonexistent_table(db_manager):
    """Test that table_summary handles non-existent table gracefully"""
    with pytest.raises(TableNotFoundError):
        await table_summary(db_manager, "chinook_sqlite", "NonexistentTable")


async def test_table_summary_column_types_and_nullability(db_manager):
    """Test that table_summary correctly reports column types and nullability"""
    result = await table_summary(db_manager, "chinook_sqlite", "Track")

    assert isinstance(result, TableSummary)

    track_id_col = next((col for col in result.columns if col.name == "TrackId"), None)
    name_col = next((col for col in result.columns if col.name == "Name"), None)

    assert track_id_col is not None
    assert name_col is not None

    assert track_id_col.primary_key is True
    assert track_id_col.nullable is False
    assert name_col.nullable is False


async def test_table_summary_pagination_fields(db_manager):
    """Test that table_summary returns pagination fields correctly"""
    result = await table_summary(db_manager, "chinook_sqlite", "Track", limit=5, page=1)

    assert isinstance(result, TableSummary)
    assert result.current_page == 1
    assert result.total_count > 0
    assert result.total_pages > 0
    assert len(result.columns) <= 5


async def test_table_summary_pagination_limit_validation(db_manager):
    """Test that table_summary validates pagination parameters"""
    from meeseeql.tools.table_summary import TableSummaryError

    with pytest.raises(TableSummaryError, match="Limit must be greater than 0"):
        await table_summary(db_manager, "chinook_sqlite", "Track", limit=0)

    with pytest.raises(TableSummaryError, match="Page number must be greater than 0"):
        await table_summary(db_manager, "chinook_sqlite", "Track", page=0)


async def test_table_summary_pagination_second_page(db_manager):
    """Test that table_summary pagination works for second page"""
    result_page1 = await table_summary(
        db_manager, "chinook_sqlite", "Track", limit=3, page=1
    )
    result_page2 = await table_summary(
        db_manager, "chinook_sqlite", "Track", limit=3, page=2
    )

    assert result_page1.current_page == 1
    assert result_page2.current_page == 2
    assert result_page1.total_count == result_page2.total_count
    assert result_page1.total_pages == result_page2.total_pages

    # Make sure we get different items (at least some should be different)
    page1_items = [col.name for col in result_page1.columns] + [
        fk.referred_table for fk in result_page1.foreign_keys
    ]
    page2_items = [col.name for col in result_page2.columns] + [
        fk.referred_table for fk in result_page2.foreign_keys
    ]

    # Pages should be different
    if result_page1.total_count > 3:
        assert page1_items != page2_items


async def test_table_summary_case_insensitive(db_manager):
    """Test that table_summary handles case insensitive table names correctly"""
    result_exact = await table_summary(db_manager, "chinook_sqlite", "Album")
    result_lower = await table_summary(db_manager, "chinook_sqlite", "album")
    result_upper = await table_summary(db_manager, "chinook_sqlite", "ALBUM")
    result_mixed = await table_summary(db_manager, "chinook_sqlite", "AlBuM")

    exact_cols = {col.name for col in result_exact.columns}
    lower_cols = {col.name for col in result_lower.columns}
    upper_cols = {col.name for col in result_upper.columns}
    mixed_cols = {col.name for col in result_mixed.columns}
    assert exact_cols == lower_cols == upper_cols == mixed_cols

    # Test that non-existent table still raises error
    with pytest.raises(TableNotFoundError):
        await table_summary(db_manager, "chinook_sqlite", "nonexistenttable")
