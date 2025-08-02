# meeseeql
[![Tests](https://github.com/txsmith/meeseeql/actions/workflows/test.yml/badge.svg)](https://github.com/txsmith/meeseeql/actions/workflows/test.yml)
![PyPI - Version](https://img.shields.io/pypi/v/meeseeql)

A FastMCP server for exploring multiple databases with support for SELECT queries, table sampling, and structure inspection.

## Features

- **Multiple Database Support**: Configure multiple databases with connection strings or individual fields
- **Safe Query Execution**: Only SELECT queries allowed for read-only exploration
- **Table Sampling**: Sample rows from tables with configurable limits
- **Schema Inspection**: View table structure, columns, and foreign key relationships


> [!WARNING]
> **Use at your own risk**: This tool attempts to restrict database operations to read-only queries (SELECT statements), but it does not guarantee that all destructive operations are prevented. Database functions, stored procedures, or certain SELECT statements with side effects could potentially modify database state. Always use read-only database connections and users with minimal privileges when connecting to production databases. 


## Available Tools

- **list_databases** - Lists all configured databases with their types and descriptions
- **table_summary** - Gets table structure including columns, data types, and foreign keys
- **execute_query** - Executes SELECT queries on databases with pagination support
- **test_connection** - Tests database connection to verify configuration


## Supported Databases

- PostgreSQL (via psycopg2)
- MySQL (via PyMySQL)
- SQLite (local files and in-memory)
- SQL Server (via pyodbc)
- Snowflake (data warehouse)


## Installation

From PyPI:
```bash
uvx meeseeql
```

From GitHub:
```bash
uvx --from git+https://github.com/txsmith/meeseeql.git meeseeql
```

For development:
```bash
git clone https://github.com/txsmith/meeseeql.git
cd meeseeql
uv sync --dev
```

## Configuration

Create a `config.yaml` file with your database connections. The config file is searched in this order:

1. `--config /path/to/config.yaml` (command line flag)
2. `MEESEEQL_CONFIG` environment variable
3. `./config.yaml` (current working directory)
4. `~/.config/meeseeql/config.yaml` (recommended for users)
5. `~/meeseeql.yaml` (simple alternative)

See `config_example.yaml` for more examples of both formats.

3. Run the server:
```bash
fastmcp dev main.py
```


## Installing as MCP Server

### Claude Desktop
```bash
claude mcp add --scope user sql-explorer uvx meeseeql
```

### Cursor
Add to your MCP settings in Cursor:

1. Open Cursor Settings → Features → Model Context Protocol
2. Add a new server configuration:

```json
{
  "meeseeql": {
    "command": "uvx", 
    "args": ["meeseeql"]
  }
}
```
## Security considerations
Configure your databases with read-only users to prevent destructive operations. The server does not restrict query types at the application level. Also make sure to keep your config.yaml private as this will likely contain sensitive information.

> [!TIP]
> **Password Management**: Instead of storing plaintext passwords in your config file, you can use the Unix [`pass`](https://www.passwordstore.org/) password manager. Simply omit the `password` field from your database configuration and meeseeql will automatically attempt to retrieve the password using `pass databases/{database_name}`. See config_example.yaml for configuration options.

## Development

### Running Tests

```bash
uv run pytest
```

### Code Formatting

```bash
uv run black .
uv run flake8
```

