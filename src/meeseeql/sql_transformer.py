import sqlglot
from sqlglot import expressions as exp


class InvalidSqlError(Exception):
    pass


class ReadOnlyViolationError(Exception):
    pass


class InvalidPaginationError(Exception):
    pass


class SqlQueryTransformer:
    def __init__(self, query: str, dialect: str | None = None):
        self.query = query
        self.dialect = self._map_dialect_to_sqlglot(dialect) if dialect else None
        try:
            self.ast = sqlglot.parse_one(query, dialect=self.dialect)
        except Exception as e:
            raise InvalidSqlError(f"Invalid SQL query: {e}") from e

    def _map_dialect_to_sqlglot(self, dialect: str) -> str:
        dialect_mapping = {
            "postgres": "postgres",
            "postgresql": "postgres",
            "mysql": "mysql",
            "sqlite": "sqlite",
            "mssql": "tsql",
            "snowflake": "snowflake",
        }
        return dialect_mapping.get(dialect, dialect)

    def is_read_only(self) -> bool:
        return (
            next(
                self.ast.find_all(
                    exp.Insert,
                    exp.Update,
                    exp.Delete,
                    exp.Create,
                    exp.Drop,
                    exp.Alter,
                    exp.TruncateTable,
                ),
                None,
            )
            is None
        )

    def add_pagination(self, limit: int, offset: int = 0) -> str:
        if limit < 0:
            raise InvalidPaginationError("Limit must be non-negative")
        if offset < 0:
            raise InvalidPaginationError("Offset must be non-negative")

        if not isinstance(self.ast, exp.Select):
            return self.ast.sql(dialect=self.dialect)

        ast_copy = self.ast.copy()

        # Check for LIMIT/OFFSET on the top-level query
        existing_limit = ast_copy.args.get("limit")
        existing_offset = ast_copy.args.get("offset")

        if existing_limit:
            existing_limit_value = int(existing_limit.expression.this)
            if limit < existing_limit_value:
                ast_copy = ast_copy.limit(limit, copy=False)
        else:
            ast_copy = ast_copy.limit(limit, copy=False)

        if existing_offset or offset > 0:
            ast_copy = ast_copy.offset(offset, copy=False)

        return ast_copy.sql(dialect=self.dialect)

    def to_count_query(self) -> str:
        if not isinstance(self.ast, exp.Select):
            return "SELECT 0"  # can't count on non-SELECT queries

        ast_copy = self.ast.copy()

        if isinstance(ast_copy, exp.Select):
            if ast_copy.args.get("limit"):
                ast_copy.args.pop("limit")
            if ast_copy.args.get("offset"):
                ast_copy.args.pop("offset")

        count_subquery = ast_copy.subquery(alias="count_subquery")
        count_query = exp.select(exp.func("COUNT", exp.Star())).from_(count_subquery)

        return count_query.sql(dialect=self.dialect)

    def validate_read_only(self) -> None:
        """Validate that query is read-only, raising exception if not."""
        if not self.is_read_only():
            raise ReadOnlyViolationError("Query contains non-SELECT operations")
