from pydantic import BaseModel
from meeseeql.database_manager import DatabaseManager


class ConnectionTestResponse(BaseModel):
    database: str
    message: str

    def __str__(self) -> str:
        return f"Database '{self.database}': {self.message}"


async def test_connection(
    db_manager: DatabaseManager, database: str
) -> ConnectionTestResponse:
    """Test database connection"""

    await db_manager.execute_query(database, "SELECT 1")
    return ConnectionTestResponse(database=database, message="Connection successful")
