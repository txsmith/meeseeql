from typing import List
from pydantic import BaseModel
from meeseeql.database_manager import DatabaseManager


class DatabaseInfo(BaseModel):
    name: str
    description: str
    type: str
    host: str | None
    port: int | None
    username: str | None
    database: str | None


class DatabaseList(BaseModel):
    databases: List[DatabaseInfo]
    total_count: int

    def __str__(self) -> str:
        if not self.databases:
            return "No databases configured"

        result = ""

        for db in self.databases:
            description = db.description
            if len(description) > 20:
                description = description[:17] + ".."

            description = description.ljust(20)

            if db.type == "sqlite":
                connection_info = db.database or db.name
            else:
                connection_info = f"{db.username}@{db.host}:{db.port}"

            result += f"{description} {connection_info}\n"

        return result.rstrip()


def list_databases(db_manager: DatabaseManager) -> DatabaseList:
    """List all configured databases"""
    databases = []
    for db_name, db_config in db_manager.config.databases.items():
        databases.append(
            DatabaseInfo(
                name=db_name,
                description=db_config.description,
                type=db_config.type,
                host=db_config.host,
                port=db_config.port,
                username=db_config.username,
                database=db_config.database,
            )
        )

    return DatabaseList(databases=databases, total_count=len(databases))
