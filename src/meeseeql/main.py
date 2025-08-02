"""
Database Explorer MCP Server
A FastMCP server for exploring multiple databases with SELECT queries,
table sampling, and structure inspection.
"""

import sys
import os
from fastmcp import FastMCP
from fastmcp.tools.tool import ToolResult
from mcp.types import TextContent
from meeseeql.database_manager import load_config, DatabaseManager
from meeseeql import tools

mcp = FastMCP("Database Explorer")

db_manager = None


def find_config_file():
    """Looks for the config file in the following places:
    1. --config flag (if provided)
    2. MEESEEQL_CONFIG environment variable
    3. ./config.yaml (current working directory)
    4. ~/.config/meeseeql/config.yaml
    5. ~/meeseeql.yaml (simple home directory)
    """
    if len(sys.argv) > 2 and sys.argv[1] == "--config":
        return sys.argv[2]

    env_config = os.environ.get("MEESEEQL_CONFIG")
    if env_config and os.path.exists(env_config):
        return env_config

    cwd_config = os.path.join(os.getcwd(), "config.yaml")
    if os.path.exists(cwd_config):
        return cwd_config

    home_dir = os.path.expanduser("~")
    xdg_config = os.path.join(home_dir, ".config", "meeseeql", "config.yaml")
    if os.path.exists(xdg_config):
        return xdg_config

    home_config = os.path.join(home_dir, "meeseeql.yaml")
    if os.path.exists(home_config):
        return home_config

    raise ValueError("Config file not found")


def get_db_manager():
    """Get or initialize the database manager"""
    global db_manager
    if db_manager is None:
        config_path = find_config_file()
        config = load_config(config_path)
        db_manager = DatabaseManager(config)
    return db_manager


@mcp.tool()
def list_databases() -> ToolResult:
    """List all configured databases and their settings.
    This tool also gives you the full path to the config file
    so you can use it to make edits if the user requests it;
    simply edit the config file and call the reload_configt tool.
    """
    result = tools.list_databases(get_db_manager())
    return ToolResult(
        content=[TextContent(type="text", text=str(result))],
        structured_content=result.model_dump(),
    )


@mcp.tool()
async def execute_query(
    database: str,
    query: str,
    limit: int = 100,
    page: int = 1,
    accurate_count: bool = False,
) -> ToolResult:
    """Execute a SELECT query on the specified database with pagination.

    Args:
        database: Database name to query
        query: SELECT query to execute
        limit: Maximum rows per page (default: 100)
        page: Page number (default: 1)
        accurate_count: Run COUNT query for exact pagination (default: False, slower but accurate)
    """
    result = await tools.execute_query(
        get_db_manager(), database, query, limit, page, accurate_count
    )
    return ToolResult(
        content=[TextContent(type="text", text=str(result))],
        structured_content=result.model_dump(),
    )


@mcp.tool()
async def table_summary(
    database: str,
    table_name: str,
    db_schema: str | None = None,
    limit: int = 250,
    page: int = 1,
) -> ToolResult:
    """Get table structure including columns and foreign keys with pagination"""
    result = await tools.table_summary(
        get_db_manager(), database, table_name, db_schema, limit, page
    )
    return ToolResult(
        content=[TextContent(type="text", text=str(result))],
        structured_content=result.model_dump(),
    )


@mcp.tool()
async def fuzzy_search(
    database: str,
    search_term: str,
    schema: str | None = None,
) -> ToolResult:
    """Perform fuzzy search across tables, columns, and enum values in a PostgreSQL database.

    Searches for the given term in table names, column names, and enum values,
    ranking results by relevance with exact matches scoring highest.

    Args:
        database: Database name to search in (must be PostgreSQL)
        search_term: A single term to search for across database objects.
        schema: Optional schema to limit search to (default: searches all schemas)
    """
    result = await tools.fuzzy_search(get_db_manager(), database, search_term, schema)
    return ToolResult(
        content=[TextContent(type="text", text=str(result))],
        structured_content=result.model_dump(),
    )


@mcp.tool()
async def test_connection(database: str) -> ToolResult:
    """Test database connection, useful for debugging issues"""
    result = await tools.test_connection(get_db_manager(), database)
    return ToolResult(
        content=[TextContent(type="text", text=str(result))],
        structured_content=result.model_dump(),
    )


@mcp.tool()
def reload_config() -> ToolResult:
    """Reload configuration file and report what changed"""
    config_path = find_config_file()
    result = tools.reload_config(get_db_manager(), config_path)
    return ToolResult(
        content=[TextContent(type="text", text=str(result))],
        structured_content=result.model_dump(),
    )


def main():
    global db_manager
    config_path = find_config_file()
    config = load_config(config_path)
    db_manager = DatabaseManager(config)
    mcp.run()


if __name__ == "__main__":
    main()
