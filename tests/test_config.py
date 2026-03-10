"""Tests for configuration loading and CLI path handling."""

from click.testing import CliRunner

from slurm_watchdog.__main__ import main
from slurm_watchdog.config import load_config


def test_load_config_creates_explicit_path(tmp_path):
    """Loading with create_if_missing should create the requested file."""
    config_path = tmp_path / "configs" / "custom.toml"

    config = load_config(config_path, create_if_missing=True)

    assert config_path.exists()
    assert config.database.path == "~/.local/share/slurm-watchdog/watchdog.db"


def test_config_init_uses_explicit_path(tmp_path):
    """The config init CLI should honor --config."""
    config_path = tmp_path / "configs" / "watchdog.toml"
    runner = CliRunner()

    result = runner.invoke(main, ["--config", str(config_path), "config", "init"])

    assert result.exit_code == 0
    assert config_path.exists()
    assert f"Created configuration file: {config_path}" in result.output


def test_config_show_reports_explicit_path(tmp_path):
    """The config show CLI should report the selected config path."""
    config_path = tmp_path / "configs" / "watchdog.toml"
    load_config(config_path, create_if_missing=True)
    runner = CliRunner()

    result = runner.invoke(main, ["--config", str(config_path), "config", "show"])

    assert result.exit_code == 0
    assert f"Configuration file: {config_path}" in result.output


def test_config_validate_reports_missing_explicit_path(tmp_path):
    """Validation errors should mention the requested path, not the default one."""
    config_path = tmp_path / "missing.toml"
    runner = CliRunner()

    result = runner.invoke(main, ["--config", str(config_path), "config", "validate"])

    assert result.exit_code == 1
    assert f"Configuration file not found: {config_path}" in result.output
