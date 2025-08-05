import math
from typing import Dict, Any, List
from pydantic import BaseModel
from meeseeql.database_manager import DatabaseManager
from meeseeql.sql_transformer import SqlQueryTransformer


class QueryResponse(BaseModel):
    columns: List[str]
    rows: List[Dict[str, Any]]
    row_count: int
    current_page: int
    total_pages: int
    truncated: bool
    total_rows: int | None = None

    def __str__(self) -> str:
        if not self.rows:
            return "Query returned 0 rows"

        col_widths = {}
        for col in self.columns:
            col_widths[col] = len(str(col))

        for row in self.rows:
            for col in self.columns:
                value = row.get(col)
                formatted_value = self._format_value(value)
                col_widths[col] = max(col_widths[col], len(formatted_value))

        header = "  ".join(str(col).ljust(col_widths[col]) for col in self.columns)
        result = f"{header}\n"

        for row in self.rows:
            row_parts = []
            for col in self.columns:
                value = row.get(col)
                formatted_value = self._format_value(value)
                row_parts.append(formatted_value.ljust(col_widths[col]))
            result += "  ".join(row_parts) + "\n"

        if self.total_rows is not None:
            result += f"\nPage {self.current_page} of {self.total_pages} (showing {self.row_count} of {self.total_rows} rows)"
        else:
            if self.truncated:
                result += f"\nPage {self.current_page} (showing {self.row_count} rows, more may exist)"
            else:
                result += f"\nPage {self.current_page} of {self.total_pages} (showing {self.row_count} rows)"

        return result

    def _format_value(self, value: Any) -> str:
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


async def execute_query(
    db_manager: DatabaseManager,
    database: str,
    query: str,
    limit: int = 100,
    page: int = 1,
    accurate_count: bool = False,
) -> QueryResponse:
    """Execute a SELECT query on the specified database with pagination"""

    if limit < 1:
        raise ValueError("Limit must be greater than 0")

    if page < 1:
        raise ValueError("Page number must be greater than 0")

    max_rows = db_manager.config.settings.get("max_rows_per_query", 1000)
    if limit > max_rows:
        limit = max_rows

    dialect = db_manager.get_dialect_name(database)
    transformer = SqlQueryTransformer(query.strip(), dialect)
    transformer.validate_read_only()

    total_rows = None
    if accurate_count:
        count_query = transformer.to_count_query()
        count_result = await db_manager.execute_query(database, count_query)
        count_row = count_result.fetchone()
        total_rows = count_row[0] if count_row else 0

    offset = (page - 1) * limit
    paginated_query = transformer.add_pagination(limit=limit, offset=offset).sql()

    result = await db_manager.execute_query(database, paginated_query)
    rows = result.fetchall()
    columns = list(result.keys())

    data = []
    for row in rows:
        data.append(dict(zip(columns, row)))

    row_count = len(data)

    if total_rows is not None:
        # Accurate count: calculate exact pagination
        total_pages = math.ceil(total_rows / limit) if total_rows > 0 else 1
        truncated = (page * limit) < total_rows
    else:
        # Estimation mode: we don't know the exact total
        truncated = row_count == limit
        total_pages = page

    return QueryResponse(
        columns=columns,
        rows=data,
        row_count=row_count,
        current_page=page,
        total_pages=total_pages,
        truncated=truncated,
        total_rows=total_rows,
    )
