from meeseeql.database_manager import DatabaseManager
from meeseeql.tools.execute_query import QueryResponse, execute_query


async def sample_table(
    db_manager: DatabaseManager,
    database: str,
    table_name: str,
    db_schema: str | None = None,
) -> QueryResponse:
    sample_size = db_manager.config.settings.get("sample_size", 10)

    if not db_schema:
        schema_value = db_manager.get_default_schema(database)
    else:
        schema_value = db_schema

    table_ref = f"{schema_value}.{table_name}"
    query = f"SELECT * FROM {table_ref}"

    return await execute_query(db_manager, database, query, limit=sample_size, page=1)
