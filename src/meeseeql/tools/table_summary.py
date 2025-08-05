# flake8: noqa: E501 # don't warn on line too long
from typing import Any, List
import math
from pydantic import BaseModel
from meeseeql.database_manager import DatabaseManager
from meeseeql.sql_transformer import SqlQueryTransformer
from meeseeql.tools.execute_query import execute_query
from meeseeql.tools.sql_utils import load_sql_query


class TableNotFoundError(Exception):
    pass


class TableSummaryError(Exception):
    pass


class ColumnInfo(BaseModel):
    name: str
    type: str
    nullable: bool
    default: Any = None
    primary_key: bool = False
    enum_values: str | None = None


class ForeignKey(BaseModel):
    from_table: str
    from_columns: List[str]
    to_table: str
    to_columns: List[str]
    constraint_name: str | None = None


class TableSummary(BaseModel):
    table: str
    columns: List[ColumnInfo]
    sample_rows: List[List[Any]]
    foreign_keys: List[ForeignKey]
    incoming_foreign_keys: List[ForeignKey]
    total_count: int
    current_page: int
    total_pages: int

    def __str__(self) -> str:
        result = f'Table "{self.table}"\n'

        if self.columns:
            result += "\nCOLUMNS:\n"
            for col in self.columns:
                nullable_str = "not null" if not col.nullable else "nullable"
                parts = [col.type, nullable_str]
                if col.primary_key:
                    parts.insert(-1, "PRIMARY KEY")
                if col.default is not None:
                    parts.append(f"default: {col.default}")
                if col.enum_values:
                    parts.append(f"values: {col.enum_values}")
                result += f"  {col.name}: {', '.join(parts)}\n"

        if self.sample_rows:
            result += "\nSAMPLE ROWS:\n"
            if self.columns:
                column_names = [col.name for col in self.columns]
                result += f"  {' | '.join(column_names)}\n"
                result += f"  {'-' * (len(' | '.join(column_names)))}\n"
                for row in self.sample_rows:
                    row_str = " | ".join(
                        (
                            # Truncate column values to 50 chars to prevent massive overflow
                            str(val)[:50] + ("..." if len(str(val)) > 50 else "")
                            if val is not None
                            else "NULL"
                        )
                        for val in row
                    )
                    result += f"  {row_str}\n"

        if self.foreign_keys:
            result += "\nFOREIGN KEY CONSTRAINTS:\n"
            for fk in self.foreign_keys:
                source_cols = ", ".join(fk.from_columns)
                target_cols = ", ".join(fk.to_columns)
                result += f"  {source_cols} → {fk.to_table}({target_cols})\n"

        if self.incoming_foreign_keys:
            result += "\nREFERENCED BY:\n"
            for fk in self.incoming_foreign_keys:
                source_cols = ", ".join(fk.from_columns)
                target_cols = ", ".join(fk.to_columns)
                result += f"  {fk.from_table}.{source_cols} → {target_cols}\n"

        result += f"\nPage {self.current_page} of {self.total_pages} (Total: {self.total_count} items)"
        return result


async def _get_enum_values(
    db_manager: DatabaseManager,
    database: str,
    table_name: str,
    db_schema: str,
) -> dict[str, str]:
    try:
        dialect = db_manager.get_dialect_name(database)
        enum_query_template = load_sql_query(dialect, "enum_values")
        enum_query = enum_query_template.replace("{{table_name}}", table_name).replace(
            "{{schema_name}}", db_schema
        )

        transformer = SqlQueryTransformer(enum_query, dialect)
        transformer.validate_read_only()

        result = await db_manager.execute_query(database, enum_query)
        rows = result.fetchall()
        return {row[0]: row[1] for row in rows if row[0] and row[1]}
    except Exception:
        return {}


async def _get_primary_keys(
    db_manager: DatabaseManager,
    database: str,
    table_name: str,
    db_schema: str,
) -> set:
    try:
        dialect = db_manager.get_dialect_name(database)
        pk_query_template = load_sql_query(dialect, "primary_key")
        pk_query = pk_query_template.replace("{{table_name}}", table_name).replace(
            "{{schema_name}}", db_schema
        )

        transformer = SqlQueryTransformer(pk_query, dialect)
        transformer.validate_read_only()

        result = await db_manager.execute_query(database, pk_query)
        rows = result.fetchall()
        return {row[0] for row in rows}
    except Exception as e:
        raise TableSummaryError(
            f"Failed to get primary keys for table '{table_name}' in database '{database}': {str(e)}"
        ) from e


async def _get_columns(
    db_manager: DatabaseManager,
    database: str,
    table_name: str,
    db_schema: str,
    primary_keys: set,
    enum_values: dict[str, str],
    limit: int,
    offset: int,
) -> List[ColumnInfo]:
    try:
        dialect = db_manager.get_dialect_name(database)
        base_query_template = load_sql_query(dialect, "columns")
        base_query = base_query_template.replace("{{table_name}}", table_name).replace(
            "{{schema_name}}", db_schema
        )

        transformer = SqlQueryTransformer(base_query, dialect)
        transformer.validate_read_only()

        paginated_query = transformer.add_pagination(limit, offset).sql()

        result = await db_manager.execute_query(database, paginated_query)
        rows = result.fetchall()

        columns = []
        for row in rows:
            column_name = row[0]
            data_type = row[1]
            is_nullable = row[2]
            column_default = row[3]

            nullable = is_nullable == "YES" if is_nullable else True
            is_primary_key = column_name in primary_keys

            columns.append(
                ColumnInfo(
                    name=column_name,
                    type=data_type,
                    nullable=nullable,
                    default=column_default,
                    primary_key=is_primary_key,
                    enum_values=enum_values.get(column_name),
                )
            )
        return columns
    except Exception as e:
        raise TableSummaryError(
            f"Failed to get columns for table '{table_name}' in database '{database}': {str(e)}"
        ) from e


async def _get_foreign_keys(
    db_manager: DatabaseManager,
    database: str,
    db_schema: str,
    limit: int,
    offset: int,
    table_name: str | None = None,
) -> List[ForeignKey]:
    try:
        dialect = db_manager.get_dialect_name(database)
        base_query_template = load_sql_query(dialect, "foreign_key")
        base_query = base_query_template.replace("{{table_name}}", table_name).replace(
            "{{schema_name}}", db_schema
        )

        transformer = SqlQueryTransformer(base_query, dialect)
        transformer.validate_read_only()

        paginated_query = transformer.add_pagination(limit, offset).sql()

        result = await db_manager.execute_query(database, paginated_query)
        rows = result.fetchall()

        # There may be multiple rows for the same constraint, in case it reference multiple columns
        # This loop deduplicates them first in a dict
        fk_groups = {}
        for row in rows:
            source_schema = row[0]
            source_table = row[1]
            source_column = row[2]
            dest_schema = row[3]
            dest_table = row[4]
            dest_column = row[5]
            constraint_name = row[6]

            if constraint_name not in fk_groups:
                fk_groups[constraint_name] = {
                    "from_table": (
                        f"{source_schema}.{source_table}"
                        if source_schema
                        else source_table
                    ),
                    "from_columns": [],
                    "to_table": (
                        f"{dest_schema}.{dest_table}" if dest_schema else dest_table
                    ),
                    "to_columns": [],
                }

            if source_column:
                fk_groups[constraint_name]["from_columns"].append(source_column)
            if dest_column:
                fk_groups[constraint_name]["to_columns"].append(dest_column)

        # Generate the final list of FKs
        fks = []
        for constraint_name, fk_data in fk_groups.items():
            from_cols = (
                fk_data["from_columns"]
                if fk_data["from_columns"]
                else ["(column mapping not available)"]
            )
            to_cols = (
                fk_data["to_columns"]
                if fk_data["to_columns"]
                else ["(column mapping not available)"]
            )

            fks.append(
                ForeignKey(
                    from_table=fk_data["from_table"],
                    from_columns=from_cols,
                    to_table=fk_data["to_table"],
                    to_columns=to_cols,
                    constraint_name=constraint_name,
                )
            )
        return fks
    except Exception as e:
        raise TableSummaryError(
            f"Failed to get foreign keys for table '{table_name}' in database '{database}': {str(e)}"
        ) from e


async def _get_counts(
    db_manager: DatabaseManager,
    database: str,
    table_name: str,
    db_schema: str,
) -> tuple[int, int, int]:
    """Get counts for columns, outgoing FKs, and incoming FKs"""
    try:
        dialect = db_manager.get_dialect_name(database)

        column_base_query_template = load_sql_query(dialect, "columns")
        column_base_query = column_base_query_template.replace(
            "{{table_name}}", table_name
        ).replace("{{schema_name}}", db_schema)

        fk_base_query_template = load_sql_query(dialect, "foreign_key")
        fk_base_query = fk_base_query_template.replace(
            "{{table_name}}", table_name
        ).replace("{{schema_name}}", db_schema)

        dialect = db_manager.get_dialect_name(database)

        column_transformer = SqlQueryTransformer(column_base_query, dialect)
        column_transformer.validate_read_only()

        fk_transformer = SqlQueryTransformer(fk_base_query, dialect)
        fk_transformer.validate_read_only()

        column_count_query = column_transformer.to_count_query()
        fk_count_query = fk_transformer.to_count_query()

        column_result = await db_manager.execute_query(database, column_count_query)
        fk_result = await db_manager.execute_query(database, fk_count_query)

        column_count = column_result.scalar()
        fk_count = fk_result.scalar()

        return column_count, fk_count, 0
    except Exception as e:
        raise TableSummaryError(
            f"Failed to get counts for table '{table_name}' in database '{database}': {str(e)}"
        ) from e


async def _check_table_exists(
    db_manager: DatabaseManager,
    database: str,
    table_name: str,
    db_schema: str,
) -> bool:
    """Check if a table exists using a simple query"""
    try:
        dialect = db_manager.get_dialect_name(database)
        query_template = load_sql_query(dialect, "table_exists")
        query = query_template.replace("{{table_name}}", table_name).replace(
            "{{schema_name}}", db_schema
        )

        transformer = SqlQueryTransformer(query, dialect)
        transformer.validate_read_only()

        result = await db_manager.execute_query(database, query)
        rows = result.fetchall()
        return len(rows) > 0
    except Exception as e:
        raise TableSummaryError(
            f"Failed to check if table '{table_name}' exists in database '{database}': {str(e)}"
        ) from e


async def table_summary(
    db_manager: DatabaseManager,
    database: str,
    table_name: str,
    db_schema: str | None = None,
    limit: int = 250,
    page: int = 1,
) -> TableSummary:
    if limit < 1:
        raise TableSummaryError("Limit must be greater than 0")

    if page < 1:
        raise TableSummaryError("Page number must be greater than 0")

    max_rows = db_manager.config.settings.get("max_rows_per_query", 1000)
    if limit > max_rows:
        limit = max_rows

    dialect = db_manager.get_dialect_name(database)

    if not db_schema:
        schema_value = db_manager.get_default_schema(database)
    else:
        schema_value = db_schema

    table_exists = await _check_table_exists(
        db_manager, database, table_name, schema_value
    )
    if not table_exists:
        raise TableNotFoundError(
            f"Table '{table_name}' not found in database '{database}'"
        )

    column_count, outgoing_fk_count, incoming_fk_count = await _get_counts(
        db_manager, database, table_name, schema_value
    )

    total_count = column_count + outgoing_fk_count + incoming_fk_count
    total_pages = math.ceil(total_count / limit) if total_count > 0 else 1

    offset = (page - 1) * limit

    columns = []
    outgoing_foreign_keys = []
    incoming_foreign_keys = []

    primary_keys = await _get_primary_keys(
        db_manager, database, table_name, schema_value
    )

    enum_values = await _get_enum_values(db_manager, database, table_name, schema_value)

    sample_response = await execute_query(
        db_manager,
        database,
        f"SELECT * FROM {schema_value}.{table_name}",
        limit=5,
        page=1,
    )
    sample_rows = [list(row.values()) for row in sample_response.rows]

    remaining_limit = limit
    current_offset = offset

    if current_offset < column_count and remaining_limit > 0:
        num_columns_to_fetch = min(remaining_limit, column_count - current_offset)
        columns = await _get_columns(
            db_manager,
            database,
            table_name,
            schema_value,
            primary_keys,
            enum_values,
            num_columns_to_fetch,
            current_offset,
        )
        remaining_limit -= len(columns)
        current_offset = 0
    else:
        current_offset -= column_count

    if current_offset < outgoing_fk_count and remaining_limit > 0:
        fks_to_fetch = min(remaining_limit, outgoing_fk_count - current_offset)
        all_fks = await _get_foreign_keys(
            db_manager,
            database,
            schema_value,
            fks_to_fetch,
            current_offset,
            table_name,
        )

        # Separate into outgoing and incoming FKs
        outgoing_foreign_keys = []
        incoming_foreign_keys = []
        for fk in all_fks:
            # Extract table name from schema-qualified name (e.g., "main.Album" -> "Album")
            # and compare case-insensitively with the user's input table name
            from_table_name = fk.from_table.split(".")[-1]
            if from_table_name.lower() == table_name.lower():
                outgoing_foreign_keys.append(fk)
            else:
                incoming_foreign_keys.append(fk)

    table_ref = f"{schema_value}.{table_name}"
    return TableSummary(
        table=table_ref,
        columns=columns,
        sample_rows=sample_rows,
        foreign_keys=outgoing_foreign_keys,
        incoming_foreign_keys=incoming_foreign_keys,
        total_count=total_count,
        current_page=page,
        total_pages=total_pages,
    )
