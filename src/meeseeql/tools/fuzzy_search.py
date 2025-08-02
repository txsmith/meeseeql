from typing import List
from pydantic import BaseModel
from meeseeql.database_manager import DatabaseManager
from meeseeql.sql_transformer import SqlQueryTransformer
from meeseeql.tools.sql_utils import load_sql_query


class FuzzySearchRow(BaseModel):
    object_type: str
    schema_name: str
    user_friendly_descriptor: str
    data_type: str | None
    model_config = {"extra": "forbid"}


class FuzzySearchResponse(BaseModel):
    rows: List[FuzzySearchRow]

    def __str__(self) -> str:
        if not self.rows:
            return "No results found"

        result = ""

        for row in self.rows:
            if row.data_type and row.data_type != "null":
                result += f"{row.object_type}: {row.user_friendly_descriptor} ({row.data_type}) in {row.schema_name}\n"
            else:
                result += f"{row.object_type}: {row.user_friendly_descriptor} in {row.schema_name}\n"

        return result

    def _format_value(self, value) -> str:
        if value is None:
            return "null"
        elif isinstance(value, str):
            return value
        elif isinstance(value, (int, float)):
            if isinstance(value, float) and value != int(value):
                return f"{value:.3f}".rstrip("0").rstrip(".")
            return str(value)
        else:
            return str(value)


async def fuzzy_search(
    db_manager: DatabaseManager,
    database: str,
    search_term: str,
    schema: str | None = None,
) -> FuzzySearchResponse:
    """Execute a fuzzy search across tables, columns, and enum values in a PostgreSQL database"""

    dialect = db_manager.get_dialect_name(database)

    sql_template = load_sql_query(dialect, "fuzzy")

    limit = min(250, db_manager.config.settings.get("max_rows_per_query", 250))

    schema_value = schema if schema else ""
    sql_query = sql_template.replace("{{search_term}}", search_term)
    sql_query = sql_query.replace("{{schema_filter}}", schema_value)

    transformer = SqlQueryTransformer(sql_query, dialect)
    transformer.validate_read_only()
    paginated_query = transformer.add_pagination(limit)

    result = await db_manager.execute_query(database, paginated_query)
    rows = result.fetchall()
    columns = list(result.keys())

    typed_rows = []
    for row in rows:
        row_dict = dict(zip(columns, row))
        typed_rows.append(
            FuzzySearchRow(
                object_type=row_dict["object_type"],
                schema_name=row_dict["schema_name"],
                user_friendly_descriptor=row_dict["user_friendly_descriptor"],
                data_type=row_dict["data_type"],
            )
        )

    return FuzzySearchResponse(rows=typed_rows)
