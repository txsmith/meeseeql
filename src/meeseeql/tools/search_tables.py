# flake8: noqa: E501
from typing import List
import math
from pydantic import BaseModel
from meeseeql.database_manager import DatabaseManager
from meeseeql.tools import execute_query


class SearchTablesError(Exception):
    pass


DIALECT_QUERIES = {
    "postgresql": """
        SELECT 
            schemaname as schema_name, 
            tablename as table_name,
            CASE 
                WHEN '{search_term}' = '' THEN 0
                WHEN LOWER(tablename) = LOWER('{search_term}') THEN 100
                WHEN LOWER(tablename) LIKE LOWER('{search_term}%') THEN 90
                WHEN LOWER(tablename) LIKE LOWER('%{search_term}%') THEN 80 - (POSITION(LOWER('{search_term}') IN LOWER(tablename)) * 2)
                ELSE 0
            END as relevance_score
        FROM pg_tables
        WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
        AND (LOWER(schemaname) = LOWER('{schema_name}') OR '{schema_name}' = '')
        ORDER BY relevance_score DESC, tablename ASC
    """,
    "mysql": """
        SELECT 
            table_schema as schema_name, 
            table_name,
            CASE 
                WHEN '{search_term}' = '' THEN 0
                WHEN LOWER(table_name) = LOWER('{search_term}') THEN 100
                WHEN LOWER(table_name) LIKE LOWER('{search_term}%') THEN 90
                WHEN LOWER(table_name) LIKE LOWER('%{search_term}%') THEN 80 - (LOCATE(LOWER('{search_term}'), LOWER(table_name)) * 2)
                ELSE 0
            END as relevance_score
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'
        AND table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
        AND (LOWER(table_schema) = LOWER('{schema_name}') OR '{schema_name}' = '')
        ORDER BY relevance_score DESC, table_name ASC
    """,
    "sqlite": """
        SELECT 
            'main' as schema_name, 
            name as table_name,
            CASE 
                WHEN '{search_term}' = '' THEN 0
                WHEN LOWER(name) = LOWER('{search_term}') THEN 100
                WHEN LOWER(name) LIKE LOWER('{search_term}%') THEN 90
                WHEN LOWER(name) LIKE LOWER('%{search_term}%') THEN 80 - (INSTR(LOWER(name), LOWER('{search_term}')) * 2)
                ELSE 0
            END as relevance_score
        FROM sqlite_master
        WHERE type = 'table'
        AND name NOT LIKE 'sqlite_%'
        ORDER BY relevance_score DESC, name ASC
    """,
    "mssql": """
        SELECT 
            s.name as schema_name, 
            t.name as table_name,
            CASE 
                WHEN '{search_term}' = '' THEN 0
                WHEN LOWER(t.name) = LOWER('{search_term}') THEN 100
                WHEN LOWER(t.name) LIKE LOWER('{search_term}%') THEN 90
                WHEN LOWER(t.name) LIKE LOWER('%{search_term}%') THEN 80 - (CHARINDEX(LOWER('{search_term}'), LOWER(t.name)) * 2)
                ELSE 0
            END as relevance_score
        FROM sys.tables t
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name NOT IN ('information_schema', 'sys')
        AND (LOWER(s.name) = LOWER('{schema_name}') OR '{schema_name}' = '')
        ORDER BY relevance_score DESC, t.name ASC
    """,
    "snowflake": """
        SELECT 
            table_schema as schema_name, 
            table_name,
            CASE 
                WHEN '{search_term}' = '' THEN 0
                WHEN LOWER(table_name) = LOWER('{search_term}') THEN 100
                WHEN LOWER(table_name) LIKE LOWER('{search_term}%') THEN 90
                WHEN LOWER(table_name) LIKE LOWER('%{search_term}%') THEN 80 - (POSITION(LOWER('{search_term}'), LOWER(table_name)) * 2)
                ELSE 0
            END as relevance_score
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'
        AND table_schema NOT IN ('INFORMATION_SCHEMA')
        AND (LOWER(table_schema) = LOWER('{schema_name}') OR '{schema_name}' = '')
        ORDER BY relevance_score DESC, table_name ASC
    """,
}


class SchemaInfo(BaseModel):
    db_schema: str
    tables: List[str]

    def __str__(self) -> str:
        if len(self.tables) == 0:
            return f"Schema {self.db_schema}: (no tables)"
        elif len(self.tables) == 1:
            return f"Schema {self.db_schema}: {self.tables[0]}"
        else:
            return f"Schema {self.db_schema}: {', '.join(self.tables)}"


class SearchTablesResponse(BaseModel):
    database: str
    schemas: List[SchemaInfo]
    total_count: int
    current_page: int
    total_pages: int
    search_term: str | None = None

    def __str__(self) -> str:
        if not self.schemas:
            return f"Database '{self.database}': (no schemas found)"

        total_tables = sum(len(schema.tables) for schema in self.schemas)
        if total_tables == 0:
            return f"Database '{self.database}': (no tables found)"

        schema_lines = [str(schema) for schema in self.schemas]
        result = f"Database '{self.database}'"
        if self.search_term:
            result += f" (search: '{self.search_term}')"
        result += ":\n" + "\n".join(schema_lines)
        result += f"\n\nPage {self.current_page} of {self.total_pages} (Total: {self.total_count} tables)"
        return result


async def search_tables(
    db_manager: DatabaseManager,
    database: str,
    search_term: str | None = None,
    limit: int = 500,
    page: int = 1,
    schema: str | None = None,
) -> SearchTablesResponse:
    dialect = db_manager.get_dialect_name(database)

    if dialect not in DIALECT_QUERIES:
        raise SearchTablesError(f"Unsupported database dialect: {dialect}")

    if not schema:
        schema = ""

    query_template = DIALECT_QUERIES[dialect]
    if dialect == "sqlite":
        list_query = query_template.format(schema_name="", search_term=search_term)
    else:
        list_query = query_template.format(schema_name=schema, search_term=search_term)

    try:
        list_result = await execute_query(
            db_manager, database, list_query, limit, page, accurate_count=True
        )

        schema_tables = {}
        for row in list_result.rows:
            schema_name = row["schema_name"]
            table_name = row["table_name"]
            if schema_name not in schema_tables:
                schema_tables[schema_name] = []
            schema_tables[schema_name].append(table_name)

        schemas = [
            SchemaInfo(db_schema=schema_name, tables=tables)
            for schema_name, tables in schema_tables.items()
        ]

        return SearchTablesResponse(
            database=database,
            schemas=schemas,
            total_count=list_result.total_rows,
            current_page=page,
            total_pages=list_result.total_pages,
            search_term=search_term,
        )

    except Exception as e:
        raise SearchTablesError(
            f"Unable to search tables in database '{database}': {str(e)}"
        ) from e
