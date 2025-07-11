# flake8: noqa: E501 # don't warn on line too long
from typing import Any, List
import math
from pydantic import BaseModel
from meeseeql.database_manager import DatabaseManager
from meeseeql.sql_transformer import SqlQueryTransformer


class TableNotFoundError(Exception):
    pass


class DescribeTableError(Exception):
    pass


class ColumnInfo(BaseModel):
    name: str
    type: str
    nullable: bool
    default: Any = None
    primary_key: bool = False


class ForeignKey(BaseModel):
    from_table: str
    from_columns: List[str]
    to_table: str
    to_columns: List[str]
    constraint_name: str | None = None


class TableDescription(BaseModel):
    table: str
    columns: List[ColumnInfo]
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
                result += f"  {col.name}: {', '.join(parts)}\n"

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


DIALECT_QUERIES = {
    "postgresql": {
        "table_exists": """
            SELECT table_name FROM information_schema.tables
            WHERE LOWER(table_name) = LOWER('{table_name}')
            AND table_schema NOT IN ('information_schema', 'pg_catalog')
            AND LOWER(table_schema) = LOWER('{schema_name}')
            LIMIT 1
        """,
        "columns": """
            SELECT
                c.column_name,
                c.data_type,
                c.is_nullable,
                c.column_default
            FROM information_schema.columns c
            WHERE c.table_schema NOT IN ('information_schema', 'pg_catalog')
            AND LOWER(c.table_name) = LOWER('{table_name}')
            AND LOWER(c.table_schema) = LOWER('{schema_name}')
            ORDER BY c.ordinal_position
        """,
        "foreign_key": """
            SELECT
                n.nspname as source_schema_name,
                t.relname as source_table_name,
                a.attname as source_column_name,
                fn.nspname as dest_schema_name,
                ft.relname as dest_table_name,
                fa.attname as dest_column_name,
                c.conname as constraint_name
            FROM pg_constraint c
            JOIN pg_class t ON c.conrelid = t.oid
            JOIN pg_namespace n ON t.relnamespace = n.oid
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(c.conkey)
            JOIN pg_class ft ON c.confrelid = ft.oid
            JOIN pg_namespace fn ON ft.relnamespace = fn.oid
            JOIN pg_attribute fa ON fa.attrelid = ft.oid AND fa.attnum = ANY(c.confkey)
            WHERE c.contype = 'f'
            AND (
                (LOWER(t.relname) = LOWER('{table_name}') AND LOWER(n.nspname) = LOWER('{schema_name}'))
                OR
                (LOWER(ft.relname) = LOWER('{table_name}') AND LOWER(fn.nspname) = LOWER('{schema_name}'))
            )
            ORDER BY
                CASE WHEN LOWER(t.relname) = LOWER('{table_name}') THEN 0 ELSE 1 END,
                c.conname
        """,
        "primary_key": """
            SELECT c.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
            JOIN information_schema.columns c ON kcu.column_name = c.column_name
                AND kcu.table_name = c.table_name
            WHERE tc.constraint_type = 'PRIMARY KEY'
            AND LOWER(tc.table_name) = LOWER('{table_name}')
            AND LOWER(tc.table_schema) = LOWER('{schema_name}')
            ORDER BY kcu.ordinal_position
        """,
    },
    "mysql": {
        "table_exists": """
            SELECT table_name FROM information_schema.tables
            WHERE LOWER(table_name) = LOWER('{table_name}')
            AND table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
            AND LOWER(table_schema) = LOWER('{schema_name}')
            LIMIT 1
        """,
        "columns": """
            SELECT
                c.column_name,
                c.data_type,
                c.is_nullable,
                c.column_default
            FROM information_schema.columns c
            WHERE c.table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
            AND LOWER(c.table_name) = LOWER('{table_name}')
            AND LOWER(c.table_schema) = LOWER('{schema_name}')
            ORDER BY c.ordinal_position
        """,
        "foreign_key": """
            SELECT
                kcu.table_schema as source_schema_name,
                kcu.table_name as source_table_name,
                kcu.column_name as source_column_name,
                kcu.referenced_table_schema as dest_schema_name,
                kcu.referenced_table_name as dest_table_name,
                kcu.referenced_column_name as dest_column_name,
                kcu.constraint_name
            FROM information_schema.key_column_usage kcu
            WHERE kcu.referenced_table_name IS NOT NULL
            AND kcu.table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
            AND (
                (LOWER(kcu.table_name) = LOWER('{table_name}') AND LOWER(kcu.table_schema) = LOWER('{schema_name}'))
                OR
                (LOWER(kcu.referenced_table_name) = LOWER('{table_name}') AND LOWER(kcu.referenced_table_schema) = LOWER('{schema_name}'))
            )
            ORDER BY
                CASE WHEN LOWER(kcu.table_name) = LOWER('{table_name}') THEN 0 ELSE 1 END,
                kcu.constraint_name
        """,
        "primary_key": """
            SELECT c.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
            WHERE tc.constraint_type = 'PRIMARY KEY'
            AND LOWER(tc.table_name) = LOWER('{table_name}')
            AND LOWER(tc.table_schema) = LOWER('{schema_name}')
            ORDER BY kcu.ordinal_position
        """,
    },
    "sqlite": {
        "table_exists": """
            SELECT name FROM sqlite_master
            WHERE type='table' AND LOWER(name) = LOWER('{table_name}')
            LIMIT 1
        """,
        "columns": """
            SELECT
                p.name as column_name,
                p.type as data_type,
                CASE WHEN p."notnull" = 0 THEN 'YES' ELSE 'NO' END as is_nullable,
                p.dflt_value as column_default
            FROM pragma_table_info('{table_name}') p
            ORDER BY p.cid
        """,
        "foreign_key": """
            SELECT
                'main' as source_schema_name,
                m.name as source_table_name,
                fk."from" as source_column_name,
                'main' as dest_schema_name,
                fk."table" as dest_table_name,
                fk."to" as dest_column_name,
                'fk_' || m.name || '_' || fk.id as constraint_name
            FROM sqlite_master m
            JOIN pragma_foreign_key_list(m.name) fk
            WHERE m.type = 'table' AND m.name NOT LIKE 'sqlite_%'
            AND (LOWER(m.name) = LOWER('{table_name}') OR LOWER(fk."table") = LOWER('{table_name}'))
            ORDER BY
                CASE WHEN LOWER(m.name) = LOWER('{table_name}') THEN 0 ELSE 1 END,
                constraint_name
        """,
        "primary_key": """
            SELECT
                p.name as column_name
            FROM pragma_table_info('{table_name}') p
            WHERE p.pk = 1
            ORDER BY p.pk
        """,
    },
    "mssql": {
        "table_exists": """
            SELECT t.name FROM sys.tables t
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE LOWER(t.name) = LOWER('{table_name}')
            AND s.name NOT IN ('information_schema', 'sys')
            AND LOWER(s.name) = LOWER('{schema_name}')
        """,
        "columns": """
            SELECT
                c.name as column_name,
                tp.name as data_type,
                CASE WHEN c.is_nullable = 1 THEN 'YES' ELSE 'NO' END as is_nullable,
                dc.definition as column_default
            FROM sys.tables t
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            INNER JOIN sys.columns c ON t.object_id = c.object_id
            INNER JOIN sys.types tp ON c.user_type_id = tp.user_type_id
            LEFT JOIN sys.default_constraints dc ON c.default_object_id = dc.object_id
            WHERE s.name NOT IN ('information_schema', 'sys')
            AND LOWER(t.name) = LOWER('{table_name}')
            AND LOWER(s.name) = LOWER('{schema_name}')
            ORDER BY c.column_id
        """,
        "foreign_key": """
            SELECT
                s.name as source_schema_name,
                t.name as source_table_name,
                c.name as source_column_name,
                rs.name as dest_schema_name,
                rt.name as dest_table_name,
                rc.name as dest_column_name,
                fk.name as constraint_name
            FROM sys.foreign_keys fk
            JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
            JOIN sys.tables t ON fk.parent_object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            JOIN sys.columns c ON fkc.parent_object_id = c.object_id AND fkc.parent_column_id = c.column_id
            JOIN sys.tables rt ON fk.referenced_object_id = rt.object_id
            JOIN sys.schemas rs ON rt.schema_id = rs.schema_id
            JOIN sys.columns rc ON fkc.referenced_object_id = rc.object_id AND fkc.referenced_column_id = rc.column_id
            WHERE s.name NOT IN ('information_schema', 'sys')
            AND (
                (LOWER(t.name) = LOWER('{table_name}') AND LOWER(s.name) = LOWER('{schema_name}'))
                OR
                (LOWER(rt.name) = LOWER('{table_name}') AND LOWER(rs.name) = LOWER('{schema_name}'))
            )
            ORDER BY
                CASE WHEN LOWER(t.name) = LOWER('{table_name}') THEN 0 ELSE 1 END,
                fk.name
        """,
        "primary_key": """
            SELECT
                c.name as column_name
            FROM sys.key_constraints kc
            JOIN sys.index_columns ic ON kc.parent_object_id = ic.object_id AND kc.unique_index_id = ic.index_id
            JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
            JOIN sys.tables t ON kc.parent_object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE kc.type = 'PK'
            AND LOWER(t.name) = LOWER('{table_name}')
            AND LOWER(s.name) = LOWER('{schema_name}')
            ORDER BY ic.key_ordinal
        """,
    },
    "snowflake": {
        "table_exists": """
            SELECT table_name FROM information_schema.tables
            WHERE LOWER(table_name) = LOWER('{table_name}')
            AND table_schema NOT IN ('INFORMATION_SCHEMA')
            AND LOWER(table_schema) = LOWER('{schema_name}')
            LIMIT 1
        """,
        "columns": """
            SELECT
                c.column_name,
                c.data_type,
                c.is_nullable,
                c.column_default
            FROM information_schema.columns c
            WHERE c.table_schema NOT IN ('INFORMATION_SCHEMA')
            AND LOWER(c.table_name) = LOWER('{table_name}')
            AND LOWER(c.table_schema) = LOWER('{schema_name}')
            ORDER BY c.ordinal_position
        """,
        "foreign_key": """
            SELECT
                fk_tco.table_schema as source_schema_name,
                fk_tco.table_name as source_table_name,
                NULL as source_column_name,
                pk_tco.table_schema as dest_schema_name,
                pk_tco.table_name as dest_table_name,
                NULL as dest_column_name,
                fk_tco.constraint_name
            FROM information_schema.referential_constraints rco
            JOIN information_schema.table_constraints fk_tco
                ON fk_tco.constraint_name = rco.constraint_name
                AND fk_tco.constraint_schema = rco.constraint_schema
            JOIN information_schema.table_constraints pk_tco
                ON pk_tco.constraint_name = rco.unique_constraint_name
                AND pk_tco.constraint_schema = rco.unique_constraint_schema
            WHERE (
                (LOWER(fk_tco.table_name) = LOWER('{table_name}') AND LOWER(fk_tco.table_schema) = LOWER('{schema_name}'))
                OR
                (LOWER(pk_tco.table_name) = LOWER('{table_name}') AND LOWER(pk_tco.table_schema) = LOWER('{schema_name}'))
            )
            ORDER BY
                CASE WHEN LOWER(fk_tco.table_name) = LOWER('{table_name}') THEN 0 ELSE 1 END,
                fk_tco.constraint_name
        """,
        "primary_key": """
            SELECT 'snowflake does not have PKs' as column_name LIMIT 0
        """,
    },
}


async def _get_primary_keys(
    db_manager: DatabaseManager,
    database: str,
    table_name: str,
    db_schema: str,
) -> set:
    """Get primary key columns for a table"""
    try:
        dialect = db_manager.get_dialect_name(database)
        queries = DIALECT_QUERIES[dialect]
        pk_query = queries["primary_key"].format(
            table_name=table_name, schema_name=db_schema
        )
        result = await db_manager.execute_query(database, pk_query)
        rows = result.fetchall()
        return {row[0] for row in rows}
    except Exception as e:
        raise DescribeTableError(
            f"Failed to get primary keys for table '{table_name}' in database '{database}': {str(e)}"
        ) from e


async def _get_columns(
    db_manager: DatabaseManager,
    database: str,
    table_name: str,
    db_schema: str,
    primary_keys: set,
    limit: int,
    offset: int,
) -> List[ColumnInfo]:
    """Get column information for a table"""
    try:
        dialect = db_manager.get_dialect_name(database)
        queries = DIALECT_QUERIES[dialect]
        base_query = queries["columns"].format(
            table_name=table_name,
            schema_name=db_schema,
        )

        dialect = db_manager.get_dialect_name(database)
        transformer = SqlQueryTransformer(base_query, dialect)

        paginated_query = transformer.add_pagination(limit, offset)

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
                )
            )
        return columns
    except Exception as e:
        raise DescribeTableError(
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
    """Get foreign key information (outgoing or incoming)"""
    try:
        dialect = db_manager.get_dialect_name(database)
        queries = DIALECT_QUERIES[dialect]

        base_query = queries["foreign_key"].format(
            table_name=table_name,
            schema_name=db_schema,
        )

        dialect = db_manager.get_dialect_name(database)
        transformer = SqlQueryTransformer(base_query, dialect)

        paginated_query = transformer.add_pagination(limit, offset)

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
        raise DescribeTableError(
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
        queries = DIALECT_QUERIES[dialect]

        column_base_query = queries["columns"].format(
            table_name=table_name, schema_name=db_schema
        )
        fk_base_query = queries["foreign_key"].format(
            table_name=table_name, schema_name=db_schema
        )

        dialect = db_manager.get_dialect_name(database)

        column_transformer = SqlQueryTransformer(column_base_query, dialect)
        fk_transformer = SqlQueryTransformer(fk_base_query, dialect)

        column_count_query = column_transformer.to_count_query()
        fk_count_query = fk_transformer.to_count_query()

        column_result = await db_manager.execute_query(database, column_count_query)
        fk_result = await db_manager.execute_query(database, fk_count_query)

        column_count = column_result.scalar()
        fk_count = fk_result.scalar()

        return column_count, fk_count, 0
    except Exception as e:
        raise DescribeTableError(
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
        queries = DIALECT_QUERIES[dialect]
        query = queries["table_exists"].format(
            table_name=table_name, schema_name=db_schema
        )
        result = await db_manager.execute_query(database, query)
        rows = result.fetchall()
        return len(rows) > 0
    except Exception as e:
        raise DescribeTableError(
            f"Failed to check if table '{table_name}' exists in database '{database}': {str(e)}"
        ) from e


async def describe_table(
    db_manager: DatabaseManager,
    database: str,
    table_name: str,
    db_schema: str | None = None,
    limit: int = 250,
    page: int = 1,
) -> TableDescription:
    """Get table structure including columns and foreign keys with pagination"""

    if limit < 1:
        raise DescribeTableError("Limit must be greater than 0")

    if page < 1:
        raise DescribeTableError("Page number must be greater than 0")

    max_rows = db_manager.config.settings.get("max_rows_per_query", 1000)
    if limit > max_rows:
        limit = max_rows

    dialect = db_manager.get_dialect_name(database)

    if dialect not in DIALECT_QUERIES:
        raise DescribeTableError(f"Unsupported database dialect: {dialect}")

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
    return TableDescription(
        table=table_ref,
        columns=columns,
        foreign_keys=outgoing_foreign_keys,
        incoming_foreign_keys=incoming_foreign_keys,
        total_count=total_count,
        current_page=page,
        total_pages=total_pages,
    )
