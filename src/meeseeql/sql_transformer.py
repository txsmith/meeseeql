import sqlglot
from sqlglot import expressions as exp
from typing_extensions import Self


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

    def add_pagination(self, limit: int, offset: int = 0) -> Self:
        if limit < 0:
            raise InvalidPaginationError("Limit must be non-negative")
        if offset < 0:
            raise InvalidPaginationError("Offset must be non-negative")

        if not isinstance(self.ast, exp.Select):
            return self

        existing_limit = self.ast.args.get("limit")
        existing_offset = self.ast.args.get("offset")

        if existing_limit:
            existing_limit_value = int(existing_limit.expression.this)
            if limit < existing_limit_value:
                self.ast = self.ast.limit(limit, copy=False)
        else:
            self.ast = self.ast.limit(limit, copy=False)

        if existing_offset or offset > 0:
            self.ast = self.ast.offset(offset, copy=False)

        return self

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

    def validate_read_only(self) -> Self:
        if not self.is_read_only():
            raise ReadOnlyViolationError("Query contains non-SELECT operations")
        return self

    def add_where_condition(self, condition: str) -> Self:
        if not isinstance(self.ast, exp.Select):
            return self

        try:
            condition_ast = sqlglot.parse_one(
                f"SELECT * FROM t WHERE {condition}", dialect=self.dialect
            )
            new_condition = condition_ast.find(exp.Where).this

            existing_where = self.ast.find(exp.Where)
            if existing_where:
                combined_condition = exp.And(
                    this=existing_where.this, expression=new_condition
                )
                existing_where.set("this", combined_condition)
            else:
                self.ast = self.ast.where(new_condition, copy=False)
            return self
        except Exception as e:
            raise InvalidSqlError(f"Failed to add WHERE condition: {e}") from e

    def sql(self) -> str:
        return self.ast.sql(dialect=self.dialect)
