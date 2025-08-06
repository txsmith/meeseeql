from meeseeql.database_manager import DatabaseConfig, AppConfig, ConfigurationError
from pytest import raises


def test_missing_required_fields():
    """Test that missing required fields raise ValueError at config creation time"""
    with raises(
        ConfigurationError,
        match="Either connection_string or host/database/username must be provided",
    ):
        DatabaseConfig(
            type="postgresql",
            description="Test DB",
            host="localhost",
            database="postgres",
        )


def test_unsupported_database_type():
    """Test that unsupported database types raise ValueError at config creation time"""
    with raises(ConfigurationError, match="Unsupported database type"):
        DatabaseConfig(
            type="wiggle",
            description="Test DB",
            host="localhost",
            database="mydb",
        )


def test_duplicate_database_names():
    """Test that duplicate database names (case-insensitive) raise ValueError"""
    with raises(ConfigurationError, match="TEST_DB is defined twice!"):
        AppConfig(
            databases={
                "test_db": DatabaseConfig(
                    type="sqlite",
                    description="Test DB",
                    database=":memory:",
                ),
                "TEST_DB": DatabaseConfig(
                    type="sqlite",
                    description="Another Test DB",
                    database=":memory:",
                ),
            },
            settings={},
        )


def test_both_include_and_exclude_schemas():
    with raises(
        ConfigurationError,
        match="Cannot specify both include_schemas and exclude_schemas",
    ):
        DatabaseConfig(
            type="postgresql",
            description="Test DB",
            host="localhost",
            database="testdb",
            username="user",
            password="pass",
            include_schemas=["public", "app"],
            exclude_schemas=["temp", "log"],
        )


def test_include_schemas_only():
    config = DatabaseConfig(
        type="postgresql",
        description="Test DB",
        host="localhost",
        database="testdb",
        username="user",
        password="pass",
        include_schemas=["public", "app"],
    )
    assert config.include_schemas == ["public", "app"]
    assert config.exclude_schemas is None


def test_exclude_schemas_only():
    config = DatabaseConfig(
        type="postgresql",
        description="Test DB",
        host="localhost",
        database="testdb",
        username="user",
        password="pass",
        exclude_schemas=["temp", "log"],
    )
    assert config.exclude_schemas == ["temp", "log"]
    assert config.include_schemas is None


def test_no_schema_filtering():
    config = DatabaseConfig(
        type="postgresql",
        description="Test DB",
        host="localhost",
        database="testdb",
        username="user",
        password="pass",
    )
    assert config.include_schemas is None
    assert config.exclude_schemas is None


def test_both_allowed_and_disallowed_tables():
    with raises(
        ConfigurationError,
        match="Cannot specify both allowed_tables and disallowed_tables",
    ):
        DatabaseConfig(
            type="postgresql",
            description="Test DB",
            host="localhost",
            database="testdb",
            username="user",
            password="pass",
            allowed_tables=["users", "products"],
            disallowed_tables=["logs", "temp"],
        )


def test_allowed_tables_only():
    config = DatabaseConfig(
        type="postgresql",
        description="Test DB",
        host="localhost",
        database="testdb",
        username="user",
        password="pass",
        allowed_tables=["users", "products"],
    )
    assert config.allowed_tables == ["users", "products"]
    assert config.disallowed_tables is None


def test_disallowed_tables_only():
    config = DatabaseConfig(
        type="postgresql",
        description="Test DB",
        host="localhost",
        database="testdb",
        username="user",
        password="pass",
        disallowed_tables=["logs", "temp"],
    )
    assert config.disallowed_tables == ["logs", "temp"]
    assert config.allowed_tables is None


def test_no_table_filtering():
    config = DatabaseConfig(
        type="postgresql",
        description="Test DB",
        host="localhost",
        database="testdb",
        username="user",
        password="pass",
    )
    assert config.allowed_tables is None
    assert config.disallowed_tables is None
