import os
import math
import pytest

from meeseeql.database_manager import (
    load_config,
    DatabaseManager,
    QueryError,
    DatabaseConfig,
    AppConfig,
)
from meeseeql.tools.execute_query import execute_query, QueryResponse
from meeseeql.sql_transformer import ReadOnlyViolationError, TableAccessError


@pytest.fixture
async def db_manager():
    """Fixture to provide database manager for tests"""
    config_path = os.path.join(os.path.dirname(__file__), "../test_config.yaml")
    config = load_config(config_path)
    return DatabaseManager(config)


async def test_execute_query_simple_select(db_manager):
    """Test that execute_query works with a simple SELECT query"""
    query = "SELECT AlbumId, Title FROM Album LIMIT 3"
    result = await execute_query(db_manager, "chinook_sqlite", query)

    assert isinstance(result, QueryResponse)
    assert len(result.columns) == 2
    assert "AlbumId" in result.columns
    assert "Title" in result.columns
    assert result.row_count == 3
    assert len(result.rows) == 3
    assert not result.truncated


async def test_execute_query_invalid_sql(db_manager):
    """Test that execute_query handles invalid SQL"""
    query = "SELECT * FROM NonexistentTable"
    with pytest.raises(QueryError):
        await execute_query(db_manager, "chinook_sqlite", query)


async def test_execute_query_join_query(db_manager):
    """Test that execute_query works with JOIN queries"""
    query = """
    SELECT a.Title, ar.Name as ArtistName
    FROM Album a
    JOIN Artist ar ON a.ArtistId = ar.ArtistId
    LIMIT 5
    """
    result = await execute_query(db_manager, "chinook_sqlite", query)

    assert isinstance(result, QueryResponse)
    assert len(result.columns) == 2
    assert "Title" in result.columns
    assert "ArtistName" in result.columns
    assert result.row_count == 5


async def test_execute_query_aggregate_functions(db_manager):
    """Test that execute_query works with aggregate functions"""
    query = "SELECT COUNT(*) as total_albums FROM Album"
    result = await execute_query(db_manager, "chinook_sqlite", query)

    assert isinstance(result, QueryResponse)
    assert len(result.columns) == 1
    assert "total_albums" in result.columns
    assert result.row_count == 1
    assert result.rows[0]["total_albums"] > 0


async def test_execute_query_truncation_behavior(db_manager):
    """Test that execute_query properly handles row limits and truncation"""
    # This test assumes Chinook has more than 100 tracks (which it does - 3503 tracks)
    query = "SELECT TrackId FROM Track"
    result = await execute_query(db_manager, "chinook_sqlite", query)

    assert isinstance(result, QueryResponse)
    # Default limit is 100, so should be truncated
    assert result.row_count == 100
    assert result.truncated
    assert result.current_page == 1
    assert result.total_pages == 1  # We only know minimum pages

    # Test with higher limit
    result = await execute_query(db_manager, "chinook_sqlite", query, limit=10)
    assert result.row_count == 10
    assert result.truncated


async def test_execute_query_empty_result_set(db_manager):
    """Test that execute_query handles queries that return no rows"""
    query = "SELECT * FROM Album WHERE AlbumId = -1"
    result = await execute_query(db_manager, "chinook_sqlite", query)

    assert isinstance(result, QueryResponse)
    assert result.row_count == 0
    assert len(result.rows) == 0
    assert not result.truncated


async def test_execute_query_data_types(db_manager):
    """Test that execute_query handles different data types correctly"""
    query = """
    SELECT
        EmployeeId,
        FirstName,
        BirthDate,
        ReportsTo
    FROM Employee
    LIMIT 1
    """
    result = await execute_query(db_manager, "chinook_sqlite", query)

    assert isinstance(result, QueryResponse)
    assert result.row_count == 1

    row = result.rows[0]
    assert isinstance(row["EmployeeId"], int)
    assert isinstance(row["FirstName"], str)
    # BirthDate and ReportsTo can be None or have values


async def test_execute_query_with_comments(db_manager):
    """Test that execute_query works with SQL comments"""
    query = """
    /* This is a comment */
    SELECT AlbumId, Title
    FROM Album
    -- Another comment
    LIMIT 2
    """
    result = await execute_query(db_manager, "chinook_sqlite", query)

    assert isinstance(result, QueryResponse)
    assert result.row_count == 2


async def test_execute_query_blocks_update_query(db_manager):
    """Test that execute_query blocks UPDATE queries with ReadOnlyViolationError"""
    query = "UPDATE Album SET Title = 'Modified' WHERE AlbumId = 1"
    with pytest.raises(ReadOnlyViolationError):
        await execute_query(db_manager, "chinook_sqlite", query)


async def test_execute_query_blocks_insert_query(db_manager):
    """Test that execute_query blocks INSERT queries with ReadOnlyViolationError"""
    query = "INSERT INTO Album (Title, ArtistId) VALUES ('New Album', 1)"
    with pytest.raises(ReadOnlyViolationError):
        await execute_query(db_manager, "chinook_sqlite", query)


async def test_execute_query_blocks_delete_query(db_manager):
    """Test that execute_query blocks DELETE queries with ReadOnlyViolationError"""
    query = "DELETE FROM Album WHERE AlbumId = 1"
    with pytest.raises(ReadOnlyViolationError):
        await execute_query(db_manager, "chinook_sqlite", query)


async def test_execute_query_accurate_count_disabled(db_manager):
    """Test that accurate_count=False uses existing estimation behavior"""
    query = "SELECT AlbumId FROM Album"
    result = await execute_query(
        db_manager, "chinook_sqlite", query, limit=10, accurate_count=False
    )

    assert isinstance(result, QueryResponse)
    assert result.total_rows is None
    assert result.row_count == 10
    assert result.truncated is True


async def test_execute_query_accurate_count_enabled(db_manager):
    """Test that accurate_count=True provides exact total count"""
    query = "SELECT AlbumId FROM Album LIMIT 50"
    result = await execute_query(
        db_manager, "chinook_sqlite", query, limit=10, accurate_count=True
    )

    assert isinstance(result, QueryResponse)
    assert result.total_rows is not None
    assert result.total_rows >= 50
    assert result.row_count == 10
    assert result.total_pages == math.ceil(result.total_rows / 10)


async def test_execute_query_accurate_count_empty_result(db_manager):
    """Test accurate_count with query that returns no results"""
    query = "SELECT * FROM Album WHERE AlbumId = -999"
    result = await execute_query(
        db_manager, "chinook_sqlite", query, accurate_count=True
    )

    assert isinstance(result, QueryResponse)
    assert result.total_rows == 0
    assert result.row_count == 0
    assert result.total_pages == 1
    assert result.truncated is False


async def test_execute_query_accurate_count_complex_query(db_manager):
    """Test accurate_count with complex query containing JOINs"""
    query = """
    SELECT a.Title, ar.Name
    FROM Album a
    JOIN Artist ar ON a.ArtistId = ar.ArtistId
    WHERE a.Title LIKE '%Rock%'
    """
    result = await execute_query(
        db_manager, "chinook_sqlite", query, limit=5, accurate_count=True
    )

    assert isinstance(result, QueryResponse)
    assert result.total_rows is not None
    assert result.total_rows >= 0
    if result.total_rows > 0:
        assert result.total_pages == math.ceil(result.total_rows / 5)
    else:
        assert result.total_pages == 1


async def test_execute_query_respects_allowed_tables():
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

    # Should work for allowed table
    result = await execute_query(db_manager, "test_db", "SELECT * FROM Track LIMIT 5")
    assert isinstance(result, QueryResponse)
    assert len(result.columns) > 0

    # Should raise error for disallowed table
    with pytest.raises(TableAccessError, match="not in the allowed list"):
        await execute_query(db_manager, "test_db", "SELECT * FROM Artist LIMIT 5")


async def test_execute_query_respects_disallowed_tables():
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

    # Should work for non-excluded table
    result = await execute_query(db_manager, "test_db", "SELECT * FROM Artist LIMIT 5")
    assert isinstance(result, QueryResponse)
    assert len(result.columns) > 0

    # Should raise error for excluded table
    with pytest.raises(TableAccessError, match="in the excluded list"):
        await execute_query(db_manager, "test_db", "SELECT * FROM Track LIMIT 5")
