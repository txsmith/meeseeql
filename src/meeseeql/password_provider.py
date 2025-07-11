import subprocess
from abc import ABC, abstractmethod


class PasswordProvider(ABC):
    """Abstract base class for password providers"""

    @abstractmethod
    def get_password(self, pass_key: str) -> str | None:
        """Get password for database connection using pass key. Returns None if not found."""
        pass


class PassPasswordProvider(PasswordProvider):
    """Password provider using Unix 'pass' password manager"""

    def get_password(self, pass_key: str) -> str | None:
        result = subprocess.run(["pass", pass_key], capture_output=True, text=True)

        if result.returncode == 1:
            # Not found - return None to allow proceeding without password
            return None
        elif result.returncode != 0:
            # Other error (timeout, etc.)
            raise ValueError(f"Failed to get password from pass: {pass_key}")

        return result.stdout.strip()


class NoOpPasswordProvider(PasswordProvider):
    """Password provider that always returns None (for testing)"""

    def get_password(self, pass_key: str) -> str | None:
        return None


class StaticPasswordProvider(PasswordProvider):
    """Password provider with predefined passwords (for testing)"""

    def __init__(self, passwords: dict[str, str]):
        self.passwords = passwords

    def get_password(self, pass_key: str) -> str | None:
        return self.passwords.get(pass_key)
