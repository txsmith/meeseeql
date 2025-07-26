from typing import List
from pydantic import BaseModel
from meeseeql.database_manager import DatabaseManager, load_config


class ConfigChange(BaseModel):
    added: List[str]
    removed: List[str]
    modified: List[str]

    def __str__(self) -> str:
        result = []

        if self.added:
            result.append(f"Added: {', '.join(self.added)}")

        if self.removed:
            result.append(f"Removed: {', '.join(self.removed)}")

        if self.modified:
            result.append(f"Modified: {', '.join(self.modified)}")

        if not result:
            return "No changes detected"

        return "\n".join(result)


def reload_config(db_manager: DatabaseManager, config_path: str) -> ConfigChange:
    """Reload configuration and return details of what changed"""
    new_config = load_config(config_path)

    old_databases = set(db_manager.config.databases.keys())
    new_databases = set(new_config.databases.keys())

    added = list(new_databases - old_databases)
    removed = list(old_databases - new_databases)

    modified = []

    common_databases = old_databases & new_databases
    for db_name in common_databases:
        old_db_config = db_manager.config.databases[db_name]
        new_db_config = new_config.databases[db_name]

        if old_db_config != new_db_config:
            modified.append(db_name)

    changed_db_names = set(added + removed + modified)

    db_manager.reload_config(new_config, changed_db_names)

    return ConfigChange(
        added=sorted(added),
        removed=sorted(removed),
        modified=sorted(modified),
    )
