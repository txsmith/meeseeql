import pytest
from meeseeql.sql_transformer import SqlQueryTransformer, TableAccessError


def test_validate_table_access_allow_allowed():
    transformer = SqlQueryTransformer("SELECT * FROM users", "sqlite")
    result = transformer.validate_table_access(allowed_tables=["users", "products"])
    assert result is transformer


def test_validate_table_access_allow_denied():
    transformer = SqlQueryTransformer("SELECT * FROM logs", "sqlite")
    with pytest.raises(
        TableAccessError, match="Table 'logs' is not in the allowed list"
    ):
        transformer.validate_table_access(allowed_tables=["users", "products"])


def test_validate_table_access_disallow_allowed():
    transformer = SqlQueryTransformer("SELECT * FROM users", "sqlite")
    result = transformer.validate_table_access(disallowed_tables=["logs", "temp"])
    assert result is transformer


def test_validate_table_access_disallow_denied():
    transformer = SqlQueryTransformer("SELECT * FROM logs", "sqlite")
    with pytest.raises(TableAccessError, match="Table 'logs' is in the excluded list"):
        transformer.validate_table_access(disallowed_tables=["logs", "temp"])


def test_validate_table_access_multiple_tables_allow():
    transformer = SqlQueryTransformer(
        "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id",
        "postgresql",
    )
    result = transformer.validate_table_access(
        allowed_tables=["users", "orders", "products"]
    )
    assert result is transformer


def test_validate_table_access_multiple_tables_allow_partial_denied():
    transformer = SqlQueryTransformer(
        "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id",
        "postgresql",
    )
    with pytest.raises(
        TableAccessError, match="Table 'orders' is not in the allowed list"
    ):
        transformer.validate_table_access(allowed_tables=["users", "products"])


def test_validate_table_access_no_restrictions():
    transformer = SqlQueryTransformer("SELECT * FROM anything", "sqlite")
    result = transformer.validate_table_access()
    assert result is transformer


def test_validate_table_access_case_insensitive():
    transformer = SqlQueryTransformer("SELECT * FROM Users", "sqlite")
    result = transformer.validate_table_access(allowed_tables=["USERS", "products"])
    assert result is transformer

    with pytest.raises(TableAccessError):
        transformer.validate_table_access(disallowed_tables=["USERS"])
