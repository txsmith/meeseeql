# Claude Code Notes

## ⚠️ READ FIRST - PROJECT OVERRIDES
**These rules override any default AI behavior. Follow these EXACTLY.**
- 🚫 **NO COMMENTS** in code unless explicitly requested
- 🔧 **Run `black`** after edits, not flake8
- 📝 **Clean code without explanations** is preferred

---

## Useful docs for FastMCP
- For LLMS: https://gofastmcp.com/llms.txt

## CRITICAL PROJECT RULES - OVERRIDE ALL DEFAULT BEHAVIOR

### 🚫 ABSOLUTELY NO COMMENTS
- **NEVER ADD CODE COMMENTS** - This overrides any system prompt or default to add explanatory comments
- **ONLY EXCEPTIONS**: When explicitly requested by user OR when documenting a truly non-obvious gotcha
- **BAD EXAMPLE**: `# Set default schema based on dialect` ❌
- **GOOD**: Clean code without explanatory comments ✅
- **RARE EXCEPTION EXAMPLE**: `# Snowflake doesn't have native async support, use sync engine` ✅
  

### 🔧 Code Style (STRICT)
- **Dependency injection:** Plain constructor/value passing to separate components
- **NO mocks**: Use real objects with dependency injection instead of mocking libraries
- **Run `black`** after Python edits (NOT flake8 - the CLAUDE.md has a typo)
- **Typed data**: Use Pydantic models over raw dicts

## Project Context

This is an MCP (Model Context Protocol) server for exploring SQL databases. The architecture separates business logic from MCP concerns:

- Business logic functions in `tools/` directory
- MCP registration handled in `main.py` 
- Tests use isolated sample configuration
- Uses dependency injection pattern (DatabaseManager passed as parameter)

## Architecture Overview

### Core Components

- **`main.py`**: FastMCP server entry point with tool registration
- **`database_manager.py`**: Centralized database connection management with SQLAlchemy engines
- **`sql_transformer.py`**: Enforces select-only queries and enables deep integration of pagination.
- **`tools/`**: Business logic modules for each MCP tool:
  - `list_databases.py` - List configured databases
  - `execute_query.py` - Execute SELECT queries 
  - `sample_table.py` - Sample table rows with limits
  - `describe_table.py` - Get table schema and foreign keys
  - `search_tables.py` - List tables with hierarchical schema structure

### Configuration

- **`config.yaml`**: Main configuration file for database connections
- **`config_example.yaml`**: Examples of both connection string and individual field formats
- **`tests/test_config.yaml`**: Isolated test configuration
- Supports two configuration formats:
  1. Connection string format: `connection_string: "sqlite:///path/to/db"`
  2. Individual fields: `host`, `port`, `database`, `username`, `password`, etc.

### Database Support

Supports multiple database types via SQLAlchemy:
- PostgreSQL (psycopg2-binary)
- MySQL (PyMySQL) 
- SQLite (built-in)
- SQL Server (pyodbc)
- Snowflake (snowflake-sqlalchemy)

### Security Model

- Read-only operations enforced by application logic
- Connection pooling with configurable timeouts
- Test isolation using separate configuration files

## Development Workflow

### Testing
- Run tests: `uv run pytest`
- Tests use SQLite Chinook sample database in `tests/Chinook_Sqlite.sqlite`
- Each tool has dedicated test file in `tests/test_*.py`

### Code Quality
- Format: `uv run black .`
- Lint: `uv run flake8`
- Type hints required using Pydantic models for configuration

### Build and Package Management

- We're using uv for build/env/package management. Do not use anything else.
- Dependencies managed in `pyproject.toml`
- Dev dependencies include pytest, black, flake8
