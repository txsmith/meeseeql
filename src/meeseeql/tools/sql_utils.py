import os


def load_sql_query(dialect: str, query_name: str) -> str:
    """Load SQL query from file"""
    sql_file_path = os.path.join(
        os.path.dirname(__file__), "sql", dialect, f"{query_name}.sql"
    )

    if not os.path.exists(sql_file_path):
        raise ValueError(
            f"Dialect '{dialect}' is not supported for {query_name} queries"
        )

    with open(sql_file_path, "r") as f:
        return f.read()
