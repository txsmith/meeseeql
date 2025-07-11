import pytest
from unittest.mock import patch

from meeseeql.password_provider import PassPasswordProvider


def test_pass_password_provider_success():
    """Test successful password retrieval from pass"""
    with patch("subprocess.run") as mock_run:
        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = "secret_password\n"

        provider = PassPasswordProvider()
        password = provider.get_password("test_db")
        assert password == "secret_password"
        mock_run.assert_called_once_with(
            ["pass", "test_db"], capture_output=True, text=True
        )


def test_pass_password_provider_custom_prefix():
    """Test pass provider with custom prefix"""
    with patch("subprocess.run") as mock_run:
        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = "secret_password\n"

        provider = PassPasswordProvider()
        password = provider.get_password("test_db")
        assert password == "secret_password"
        mock_run.assert_called_once_with(
            ["pass", "test_db"], capture_output=True, text=True
        )


def test_pass_password_provider_not_found():
    """Test pass command when entry not found (exit code 1)"""
    with patch("subprocess.run") as mock_run:
        mock_run.return_value.returncode = 1
        mock_run.return_value.stdout = ""

        provider = PassPasswordProvider()
        password = provider.get_password("test_db")
        assert password is None


def test_pass_password_provider_failure():
    """Test pass command failure (exit code > 1)"""
    with patch("subprocess.run") as mock_run:
        mock_run.return_value.returncode = 2
        mock_run.return_value.stdout = ""

        provider = PassPasswordProvider()
        with pytest.raises(
            ValueError, match="Failed to get password from pass: test_db"
        ):
            provider.get_password("test_db")
