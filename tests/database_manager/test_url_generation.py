from meeseeql.database_manager import DatabaseManager, DatabaseConfig, AppConfig
from meeseeql.password_provider import NoOpPasswordProvider
from sqlalchemy.engine.url import URL


def get_connection_url_for_test(
    db_config: DatabaseConfig, db_name: str = "test_db", password_provider=None
):
    """Helper function to test connection URL generation using DatabaseManager"""
    config = AppConfig(databases={db_name: db_config}, settings={})
    manager = DatabaseManager(config, password_provider or NoOpPasswordProvider())
    return manager._get_connection_url(db_name, db_config)


def test_connection_string_mode():
    """Test that connection_string mode works as before"""
    config = DatabaseConfig(
        type="postgresql",
        description="Test DB",
        connection_string="postgresql://user:pass@localhost:5432/mydb",
    )
    url = get_connection_url_for_test(config)
    assert url == "postgresql+asyncpg://user:pass@localhost:5432/mydb"


def test_individual_fields_postgresql():
    """Test PostgreSQL with individual fields"""
    config = DatabaseConfig(
        type="postgresql",
        description="Test DB",
        host="localhost",
        port=5432,
        database="mydb",
        username="user",
        password="pass",
    )
    url = get_connection_url_for_test(config)
    assert isinstance(url, URL)
    assert url.drivername == "postgresql+asyncpg"
    assert url.username == "user"
    assert url.password == "pass"
    assert url.host == "localhost"
    assert url.port == 5432
    assert url.database == "mydb"


def test_individual_fields_mysql():
    """Test MySQL with individual fields"""
    config = DatabaseConfig(
        type="mysql",
        description="Test DB",
        host="localhost",
        port=3306,
        database="mydb",
        username="user",
        password="pass",
    )
    url = get_connection_url_for_test(config)
    assert isinstance(url, URL)
    assert url.drivername == "mysql+aiomysql"
    assert url.username == "user"
    assert url.password == "pass"
    assert url.host == "localhost"
    assert url.port == 3306
    assert url.database == "mydb"


def test_individual_fields_sqlite():
    """Test SQLite with individual fields"""
    config = DatabaseConfig(
        type="sqlite", description="Test DB", database="/path/to/db.sqlite"
    )
    url = get_connection_url_for_test(config)
    assert url == "sqlite+aiosqlite:////path/to/db.sqlite"


def test_sqlite_memory():
    """Test SQLite in-memory database"""
    config = DatabaseConfig(type="sqlite", description="Test DB", database=":memory:")
    url = get_connection_url_for_test(config)
    assert url == "sqlite+aiosqlite:///:memory:"


def test_no_password():
    """Test config without password - should proceed with None password"""
    config = DatabaseConfig(
        type="postgresql",
        description="Test DB",
        host="localhost",
        port=5432,
        database="mydb",
        username="user",
        password=None,
    )
    url = get_connection_url_for_test(config)
    assert isinstance(url, URL)
    assert url.password is None
    assert url.username == "user"
    assert url.host == "localhost"


def test_no_port():
    """Test config without explicit port"""
    config = DatabaseConfig(
        type="postgresql",
        description="Test DB",
        host="localhost",
        database="mydb",
        username="user",
        password="pass",
    )
    url = get_connection_url_for_test(config)
    assert isinstance(url, URL)
    assert url.port is None
    assert url.host == "localhost"
    assert url.username == "user"
    assert url.password == "pass"


def test_extra_params():
    """Test config with extra parameters"""
    config = DatabaseConfig(
        type="postgresql",
        description="Test DB",
        host="localhost",
        database="mydb",
        username="user",
        password="pass",
        extra_params={"sslmode": "require", "connect_timeout": "10"},
    )
    url = get_connection_url_for_test(config)
    assert isinstance(url, URL)
    url_str = str(url)
    assert "sslmode=require" in url_str
    assert "connect_timeout=10" in url_str


def test_special_characters_in_password():
    """Test that special characters in password are handled properly"""
    config = DatabaseConfig(
        type="postgresql",
        description="Test DB",
        host="localhost",
        database="mydb",
        username="user",
        password="p@ss%!w$rd%",
    )
    url = get_connection_url_for_test(config)
    assert isinstance(url, URL)
    assert url.password == "p@ss%!w$rd%"


def test_snowflake_fields():
    """Test Snowflake with individual fields"""
    config = DatabaseConfig(
        type="snowflake",
        description="Test Snowflake DB",
        host="myaccount.snowflakecomputing.com",
        database="MYDB",
        username="user",
        password="pass",
        extra_params={"warehouse": "COMPUTE_WH", "schema": "PUBLIC"},
    )
    url = get_connection_url_for_test(config)
    assert isinstance(url, URL)
    url_str = str(url)
    assert "snowflake://" in url_str
    assert "warehouse=COMPUTE_WH" in url_str
    assert "schema=PUBLIC" in url_str


def test_connection_string_takes_precedence():
    """Test that connection_string takes precedence over individual fields"""
    config = DatabaseConfig(
        type="postgresql",
        description="Test DB",
        connection_string="postgresql://conn_user:conn_pass@conn_host:5432/conn_db",
        host="field_host",
        database="field_db",
        username="field_user",
        password="field_pass",
    )
    url = get_connection_url_for_test(config)
    assert url == "postgresql+asyncpg://conn_user:conn_pass@conn_host:5432/conn_db"
