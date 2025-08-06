from typing import List
from pydantic import BaseModel
from meeseeql.database_manager import DatabaseManager
from meeseeql.sql_transformer import SqlQueryTransformer
from meeseeql.tools.sql_utils import load_sql_query


class SearchRow(BaseModel):
    object_type: str
    schema_name: str
    user_friendly_descriptor: str
    data_type: str | None
    model_config = {"extra": "forbid"}


class SearchResponse(BaseModel):
    rows: List[SearchRow]

    def __str__(self) -> str:
        if not self.rows:
            return "No results found"

        result = ""

        for row in self.rows:
            if row.object_type == "table":
                result += f"{row.object_type}: {row.user_friendly_descriptor}\n"
            elif row.data_type and row.data_type != "null":
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


def _apply_search_filters(
    transformer: SqlQueryTransformer,
    db_manager: DatabaseManager,
    database: str,
    dialect: str,
    schema: str | None,
) -> None:
    """Apply schema and table filters to the search query"""
    if schema:
        transformer.add_where_condition(f"LOWER(schema_name) = LOWER('{schema}')")
    else:
        schema_filter_type = db_manager.get_schema_filter_type(database)
        filtered_schemas = db_manager.get_filtered_schemas(database)

        if schema_filter_type and filtered_schemas:
            if schema_filter_type == "include":
                schema_list = "', '".join(s.lower() for s in filtered_schemas)
                transformer.add_where_condition(
                    f"LOWER(schema_name) IN ('{schema_list}')"
                )
            elif schema_filter_type == "exclude":
                schema_list = "', '".join(s.lower() for s in filtered_schemas)
                transformer.add_where_condition(
                    f"LOWER(schema_name) NOT IN ('{schema_list}')"
                )

    table_filter_type = db_manager.get_table_filter_type(database)
    filtered_tables = db_manager.get_filtered_tables(database)

    if table_filter_type and filtered_tables:
        table_column = "name" if dialect == "sqlite" else "object_name"

        if table_filter_type == "allow":
            table_list = "', '".join(t.lower() for t in filtered_tables)
            transformer.add_where_condition(
                f"LOWER({table_column}) IN ('{table_list}') OR object_type != 'table'"
            )
        elif table_filter_type == "deny":
            table_list = "', '".join(t.lower() for t in filtered_tables)
            transformer.add_where_condition(
                f"LOWER({table_column}) NOT IN ('{table_list}') OR object_type != 'table'"
            )


async def search(
    db_manager: DatabaseManager,
    database: str,
    search_term: str,
    schema: str | None = None,
) -> SearchResponse:
    dialect = db_manager.get_dialect_name(database)

    sql_template = load_sql_query(dialect, "search")

    limit = min(250, db_manager.config.settings.max_rows_per_query)

    sql_query = sql_template.replace("{{search_term}}", search_term)

    transformer = SqlQueryTransformer(sql_query, dialect)

    _apply_search_filters(transformer, db_manager, database, dialect, schema)

    paginated_query = transformer.add_pagination(limit).validate_read_only().sql()

    result = await db_manager.execute_query(database, paginated_query)
    rows = result.fetchall()
    columns = list(result.keys())

    typed_rows = []
    for row in rows:
        row_dict = dict(zip(columns, row))
        typed_rows.append(
            SearchRow(
                object_type=row_dict["object_type"],
                schema_name=row_dict["schema_name"],
                user_friendly_descriptor=row_dict["user_friendly_descriptor"],
                data_type=row_dict["data_type"],
            )
        )

    return SearchResponse(rows=typed_rows)
