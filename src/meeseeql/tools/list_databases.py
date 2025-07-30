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
    config_path: str | None = None

    def __str__(self) -> str:
        if not self.databases:
            return "No databases configured"

        result = ""
        if self.config_path:
            result += f"# Config file: {self.config_path}\n"

        result += "databases:\n"

        for db in self.databases:
            result += f"  {db.name}:\n"
            result += f"    type: {db.type}\n"
            result += f'    description: "{db.description}"\n'

            if db.host:
                result += f"    host: {db.host}\n"
            if db.port:
                result += f"    port: {db.port}\n"
            if db.database:
                result += f"    database: {db.database}\n"
            if db.username:
                result += f"    username: {db.username}\n"

            result += "\n"

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

    return DatabaseList(
        databases=databases,
        total_count=len(databases),
        config_path=db_manager.config.config_path,
    )
