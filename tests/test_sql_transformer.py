import pytest
from meeseeql.sql_transformer import (
    SqlQueryTransformer,
    InvalidSqlError,
    InvalidPaginationError,
)


class TestSqlQueryTransformerInit:
    """Test SqlQueryTransformer initialization and SQL parsing validation."""

    def test_init_with_valid_select_query(self):
        """Should successfully initialize with valid SQL and store query string."""
        query = "SELECT * FROM users"
        transformer = SqlQueryTransformer(query)
        assert transformer.query == query
        assert transformer.dialect is None

    def test_init_with_dialect(self):
        """Should initialize with specified dialect and store both query and dialect."""
        query = "SELECT * FROM users"
        dialect = "postgres"
        transformer = SqlQueryTransformer(query, dialect)
        assert transformer.query == query
        assert transformer.dialect == dialect

    def test_init_with_invalid_sql_raises_exception(self):
        """Should raise exception when given unparseable SQL query."""
        invalid_query = "SELECT * FROM WHERE"
        with pytest.raises(InvalidSqlError):
            SqlQueryTransformer(invalid_query)


class TestIsReadOnly:
    """Test read-only validation that traverses entire AST for non-SELECT operations."""

    def test_simple_select_is_read_only(self):
        """Should return True for basic SELECT query."""
        transformer = SqlQueryTransformer("SELECT * FROM users")
        assert transformer.is_read_only() is True

    def test_insert_is_not_read_only(self):
        """Should return False for INSERT statement."""
        transformer = SqlQueryTransformer("INSERT INTO users (name) VALUES ('John')")
        assert transformer.is_read_only() is False

    def test_update_is_not_read_only(self):
        """Should return False for UPDATE statement."""
        transformer = SqlQueryTransformer("UPDATE users SET name = 'John' WHERE id = 1")
        assert transformer.is_read_only() is False

    def test_delete_is_not_read_only(self):
        """Should return False for DELETE statement."""
        transformer = SqlQueryTransformer("DELETE FROM users WHERE id = 1")
        assert transformer.is_read_only() is False

    def test_create_table_is_not_read_only(self):
        """Should return False for CREATE TABLE statement."""
        transformer = SqlQueryTransformer(
            "CREATE TABLE users (id INT, name VARCHAR(50))"
        )
        assert transformer.is_read_only() is False

    def test_drop_table_is_not_read_only(self):
        """Should return False for DROP TABLE statement."""
        transformer = SqlQueryTransformer("DROP TABLE users")
        assert transformer.is_read_only() is False

    def test_alter_table_is_not_read_only(self):
        """Should return False for ALTER TABLE statement."""
        transformer = SqlQueryTransformer(
            "ALTER TABLE users ADD COLUMN email VARCHAR(100)"
        )
        assert transformer.is_read_only() is False

    def test_insert_with_select_subquery_is_not_read_only(self):
        """Should return False for INSERT statement even with SELECT subquery."""
        query = """
        INSERT INTO logs (user_id, message)
        SELECT id, 'login event'
        FROM users
        WHERE active = true
        """
        transformer = SqlQueryTransformer(query)
        assert transformer.is_read_only() is False

    def test_select_with_cte_update_is_not_read_only(self):
        """Should return False when SELECT has CTE containing UPDATE statement."""
        query = """
        WITH updated_users AS (
            UPDATE users SET last_login = NOW() WHERE active = true RETURNING id
        )
        SELECT * FROM updated_users
        """
        transformer = SqlQueryTransformer(query)
        assert transformer.is_read_only() is False

    def test_nested_select_only_is_read_only(self):
        """Should return True for complex query with only SELECT operations in subqueries."""
        query = """
        SELECT u.*, p.name as project_name
        FROM users u
        JOIN (
            SELECT id, name FROM projects
            WHERE status = 'active'
        ) p ON u.project_id = p.id
        WHERE u.id IN (
            SELECT user_id FROM user_roles WHERE role = 'admin'
        )
        """
        transformer = SqlQueryTransformer(query)
        assert transformer.is_read_only() is True

    def test_cte_with_only_selects_is_read_only(self):
        """Should return True for query with CTEs containing only SELECT statements."""
        query = """
        WITH active_projects AS (
            SELECT id, name FROM projects WHERE status = 'active'
        ),
        admin_users AS (
            SELECT user_id FROM user_roles WHERE role = 'admin'
        )
        SELECT u.*, p.name
        FROM users u
        JOIN active_projects p ON u.project_id = p.id
        WHERE u.id IN (SELECT user_id FROM admin_users)
        """
        transformer = SqlQueryTransformer(query)
        assert transformer.is_read_only() is True

    def test_case_insensitive_query_types(self):
        """Should handle case-insensitive SQL keywords for read-only validation."""
        transformer = SqlQueryTransformer("select * from users")
        assert transformer.is_read_only() is True

        transformer = SqlQueryTransformer("SELECT * FROM users")
        assert transformer.is_read_only() is True

        transformer = SqlQueryTransformer("UPDATE users SET name = 'test'")
        assert transformer.is_read_only() is False

        transformer = SqlQueryTransformer("update users set name = 'test'")
        assert transformer.is_read_only() is False


class TestAddPagination:
    """Test intelligent pagination that respects existing limits and policy enforcement."""

    def test_add_limit_to_query_without_limit(self):
        """Should add LIMIT clause to query that has no existing limit."""
        transformer = SqlQueryTransformer("SELECT * FROM users")
        result = transformer.add_pagination(limit=10).sql()
        assert "LIMIT 10" in result
        assert "users" in result

    def test_add_limit_and_offset_to_query_without_limit(self):
        """Should add both LIMIT and OFFSET clauses to query without existing pagination."""
        transformer = SqlQueryTransformer("SELECT * FROM users")
        result = transformer.add_pagination(limit=10, offset=5).sql()
        assert "LIMIT 10" in result
        assert "OFFSET 5" in result

    def test_replace_higher_existing_limit_with_lower_limit(self):
        """Should replace existing LIMIT when requested limit is more restrictive."""
        transformer = SqlQueryTransformer("SELECT * FROM users LIMIT 100")
        result = transformer.add_pagination(limit=10).sql()
        assert "LIMIT 10" in result
        assert "LIMIT 100" not in result

    def test_keep_lower_existing_limit_when_higher_requested(self):
        """Should keep existing LIMIT when it's more restrictive than requested limit."""
        transformer = SqlQueryTransformer("SELECT * FROM users LIMIT 5")
        result = transformer.add_pagination(limit=10).sql()
        assert "LIMIT 5" in result
        assert "LIMIT 10" not in result

    def test_update_offset_with_existing_limit(self):
        """Should add OFFSET while preserving existing LIMIT based on policy."""
        transformer = SqlQueryTransformer("SELECT * FROM users LIMIT 10")
        result = transformer.add_pagination(limit=20, offset=5).sql()
        assert "LIMIT 10" in result
        assert "OFFSET 5" in result

    def test_replace_existing_offset_with_new_offset(self):
        """Should replace existing OFFSET with new offset value."""
        transformer = SqlQueryTransformer("SELECT * FROM users LIMIT 10 OFFSET 20")
        result = transformer.add_pagination(limit=15, offset=5).sql()
        assert "OFFSET 5" in result
        assert "OFFSET 20" not in result

    def test_zero_offset_not_included_in_output(self):
        """Should not include OFFSET clause when offset is zero."""
        transformer = SqlQueryTransformer("SELECT * FROM users")
        result = transformer.add_pagination(limit=10, offset=0).sql()
        assert "LIMIT 10" in result
        assert "OFFSET" not in result

    def test_negative_limit_raises_exception(self):
        """Should raise exception when limit is negative."""
        transformer = SqlQueryTransformer("SELECT * FROM users")
        with pytest.raises(InvalidPaginationError):
            transformer.add_pagination(limit=-1)

    def test_negative_offset_raises_exception(self):
        """Should raise exception when offset is negative."""
        transformer = SqlQueryTransformer("SELECT * FROM users")
        with pytest.raises(InvalidPaginationError):
            transformer.add_pagination(limit=10, offset=-1)

    def test_complex_query_with_joins_and_where(self):
        """Should handle pagination on complex queries with JOINs, WHERE, and ORDER BY."""
        query = """
        SELECT u.id, u.name, p.title
        FROM users u
        JOIN projects p ON u.project_id = p.id
        WHERE u.active = true AND p.status = 'ongoing'
        ORDER BY u.name
        """
        transformer = SqlQueryTransformer(query)
        result = transformer.add_pagination(limit=25, offset=10).sql()
        assert "LIMIT 25" in result
        assert "OFFSET 10" in result
        assert "JOIN" in result
        assert "ORDER BY" in result

    def test_pagination_only_affects_top_level_query(self):
        """Should add pagination only to top-level query, preserving inner query limits."""
        query = """
        SELECT *
        FROM users u
        WHERE u.id IN (
            SELECT user_id 
            FROM user_roles 
            WHERE role = 'admin' 
            LIMIT 5
        )
        """
        transformer = SqlQueryTransformer(query)
        result = transformer.add_pagination(limit=10, offset=2).sql()

        # Should add top-level pagination
        assert "LIMIT 10" in result
        assert "OFFSET 2" in result

        # Should preserve inner query limit
        assert "LIMIT 5" in result

        # Should have both limits in the result
        limit_count = result.count("LIMIT")
        assert limit_count == 2

    def test_pagination_preserves_cte_limits(self):
        """Should add pagination only to main query, preserving CTE limits."""
        query = """
        WITH recent_users AS (
            SELECT * 
            FROM users 
            WHERE created_at > '2023-01-01'
            LIMIT 20
        ),
        top_projects AS (
            SELECT *
            FROM projects
            ORDER BY priority DESC
            LIMIT 3
        )
        SELECT u.name, p.title
        FROM recent_users u
        JOIN top_projects p ON u.project_id = p.id
        """
        transformer = SqlQueryTransformer(query)
        result = transformer.add_pagination(limit=5).sql()

        # Should add main query pagination
        assert result.count("LIMIT 5") == 1

        # Should preserve CTE limits
        assert "LIMIT 20" in result
        assert "LIMIT 3" in result

        # Should have all three limits
        limit_count = result.count("LIMIT")
        assert limit_count == 3


class TestToCountQuery:
    """Test COUNT query transformation for accurate pagination."""

    def test_simple_select_to_count(self):
        """Should wrap simple SELECT in COUNT subquery."""
        transformer = SqlQueryTransformer("SELECT * FROM users")
        result = transformer.to_count_query()
        assert "SELECT COUNT(*)" in result
        assert "FROM (SELECT * FROM users) AS count_subquery" in result

    def test_select_with_limit_strips_limit(self):
        """Should remove LIMIT/OFFSET from original query before wrapping."""
        transformer = SqlQueryTransformer("SELECT * FROM users LIMIT 10 OFFSET 5")
        result = transformer.to_count_query()
        assert "SELECT COUNT(*)" in result
        assert "LIMIT" not in result
        assert "OFFSET" not in result
        assert "FROM users) AS count_subquery" in result

    def test_complex_query_with_joins(self):
        """Should handle complex queries with JOINs by wrapping entire query."""
        query = """
        SELECT u.id, u.name, p.title
        FROM users u
        JOIN projects p ON u.project_id = p.id
        WHERE u.active = true
        ORDER BY u.name
        LIMIT 25
        """
        transformer = SqlQueryTransformer(query)
        result = transformer.to_count_query()
        assert "SELECT COUNT(*)" in result
        assert "JOIN projects" in result
        assert "WHERE u.active = TRUE" in result
        assert "ORDER BY" in result
        assert "LIMIT" not in result

    def test_query_with_cte(self):
        """Should preserve CTEs when creating count query."""
        query = """
        WITH active_users AS (
            SELECT * FROM users WHERE active = true
        )
        SELECT u.name, p.title
        FROM active_users u
        JOIN projects p ON u.project_id = p.id
        """
        transformer = SqlQueryTransformer(query)
        result = transformer.to_count_query()
        assert "SELECT COUNT(*)" in result
        assert "WITH active_users AS" in result
        assert "JOIN projects" in result

    def test_query_with_group_by(self):
        """Should handle GROUP BY queries by wrapping them."""
        query = "SELECT department, COUNT(*) FROM employees GROUP BY department"
        transformer = SqlQueryTransformer(query)
        result = transformer.to_count_query()
        assert "SELECT COUNT(*)" in result
        assert "GROUP BY department" in result
        assert ") AS count_subquery" in result
