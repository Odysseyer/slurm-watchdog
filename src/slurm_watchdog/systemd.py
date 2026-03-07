"""Systemd user service management for Slurm Watchdog."""

import os
import subprocess
from pathlib import Path
from typing import Optional

# Default paths
SERVICE_NAME = "slurm-watchdog"
SERVICE_DIR = Path("~/.config/systemd/user").expanduser()
SERVICE_FILE = SERVICE_DIR / f"{SERVICE_NAME}.service"


SYSTEMD_SERVICE_TEMPLATE = """[Unit]
Description=Slurm Job Watchdog
Documentation=https://github.com/yourname/slurm-watchdog
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart={exec_path}
Restart=on-failure
RestartSec=60

# Environment file (optional)
EnvironmentFile=-{env_file}

# Resource limits
MemoryMax=100M
CPUQuota=5%

[Install]
WantedBy=default.target
"""


class SystemdError(Exception):
    """Error managing systemd service."""

    pass


def get_service_dir() -> Path:
    """Get the systemd user service directory."""
    return SERVICE_DIR


def get_service_file() -> Path:
    """Get the systemd service file path."""
    return SERVICE_FILE


def get_executable_path() -> str:
    """Get the path to the slurm-watchdog executable."""
    # Try to find the executable in PATH
    result = subprocess.run(
        ["which", "slurm-watchdog"],
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        return result.stdout.strip()

    # Fall back to ~/.local/bin
    home = Path.home()
    local_bin = home / ".local" / "bin" / "slurm-watchdog"
    if local_bin.exists():
        return str(local_bin)

    # Default assumption
    return str(local_bin)


def get_env_file_path() -> str:
    """Get the environment file path."""
    config_dir = Path("~/.config/slurm-watchdog").expanduser()
    return str(config_dir / "env")


def generate_service_content() -> str:
    """Generate the systemd service file content.

    Returns:
        Service file content.
    """
    exec_path = get_executable_path()
    env_file = get_env_file_path()

    return SYSTEMD_SERVICE_TEMPLATE.format(
        exec_path=exec_path,
        env_file=env_file,
    )


def ensure_service_dir() -> None:
    """Ensure the systemd service directory exists."""
    SERVICE_DIR.mkdir(parents=True, exist_ok=True)


def install_service(overwrite: bool = False) -> Path:
    """Install the systemd user service.

    Args:
        overwrite: If True, overwrite existing service file.

    Returns:
        Path to the installed service file.

    Raises:
        SystemdError: If service file exists and overwrite is False.
    """
    ensure_service_dir()

    if SERVICE_FILE.exists() and not overwrite:
        raise SystemdError(
            f"Service file already exists at {SERVICE_FILE}. "
            "Use overwrite=True to replace it."
        )

    content = generate_service_content()
    SERVICE_FILE.write_text(content)

    return SERVICE_FILE


def uninstall_service() -> bool:
    """Remove the systemd user service file.

    Returns:
        True if file was removed, False if it didn't exist.
    """
    if SERVICE_FILE.exists():
        SERVICE_FILE.unlink()
        return True
    return False


def run_systemctl_command(args: list[str], check: bool = True) -> subprocess.CompletedProcess:
    """Run a systemctl --user command.

    Args:
        args: Command arguments.
        check: If True, raise on failure.

    Returns:
        CompletedProcess result.

    Raises:
        SystemdError: If command fails.
    """
    cmd = ["systemctl", "--user"] + args

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=check,
        )
        return result
    except subprocess.CalledProcessError as e:
        raise SystemdError(f"systemctl command failed: {e.stderr}") from e


def daemon_reload() -> None:
    """Reload systemd user daemon."""
    run_systemctl_command(["daemon-reload"])


def is_service_installed() -> bool:
    """Check if the service file is installed."""
    return SERVICE_FILE.exists()


def is_service_enabled() -> bool:
    """Check if the service is enabled."""
    try:
        result = run_systemctl_command(["is-enabled", SERVICE_NAME], check=False)
        return result.returncode == 0
    except SystemdError:
        return False


def is_service_active() -> bool:
    """Check if the service is currently running."""
    try:
        result = run_systemctl_command(["is-active", SERVICE_NAME], check=False)
        return result.returncode == 0
    except SystemdError:
        return False


def get_service_status() -> str:
    """Get the service status string."""
    try:
        result = run_systemctl_command(["status", SERVICE_NAME], check=False)
        return result.stdout
    except SystemdError as e:
        return f"Error getting status: {e}"


def enable_service() -> None:
    """Enable the service to start at login."""
    run_systemctl_command(["enable", SERVICE_NAME])


def disable_service() -> None:
    """Disable the service from starting at login."""
    run_systemctl_command(["disable", SERVICE_NAME])


def start_service() -> None:
    """Start the service."""
    run_systemctl_command(["start", SERVICE_NAME])


def stop_service() -> None:
    """Stop the service."""
    run_systemctl_command(["stop", SERVICE_NAME])


def restart_service() -> None:
    """Restart the service."""
    run_systemctl_command(["restart", SERVICE_NAME])


def get_journal_logs(lines: int = 50, follow: bool = False) -> str:
    """Get journal logs for the service.

    Args:
        lines: Number of lines to show.
        follow: If True, follow logs (blocking).

    Returns:
        Log output (empty string if follow=True).
    """
    cmd = ["journalctl", "--user", "-u", SERVICE_NAME, "-n", str(lines)]
    if follow:
        cmd.append("-f")

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return result.stdout
    except subprocess.CalledProcessError as e:
        return f"Error reading logs: {e}"


def check_linger_enabled() -> bool:
    """Check if user lingering is enabled (services run after logout).

    Returns:
        True if linger is enabled.
    """
    # Check if linger is enabled by looking at /var/lib/systemd/linger/$USER
    linger_file = Path(f"/var/lib/systemd/linger/{os.environ.get('USER', '')}")
    return linger_file.exists()


def enable_linger() -> str:
    """Provide instructions for enabling lingering.

    Returns:
        Instructions string.
    """
    if check_linger_enabled():
        return "Lingering is already enabled."

    return (
        "To enable lingering (allow services to run after logout), run:\n"
        "  loginctl enable-linger\n"
        "This requires no sudo on most systems."
    )


def get_service_info() -> dict:
    """Get comprehensive service information.

    Returns:
        Dictionary with service status information.
    """
    return {
        "service_file": str(SERVICE_FILE),
        "installed": is_service_installed(),
        "enabled": is_service_enabled(),
        "active": is_service_active(),
        "linger_enabled": check_linger_enabled(),
    }


def full_install() -> str:
    """Perform a full installation of the service.

    Returns:
        Status message.
    """
    messages = []

    # Install service file
    try:
        install_service(overwrite=True)
        messages.append("✓ Service file installed")
    except SystemdError as e:
        messages.append(f"✗ Failed to install service file: {e}")
        return "\n".join(messages)

    # Reload daemon
    try:
        daemon_reload()
        messages.append("✓ Systemd daemon reloaded")
    except SystemdError as e:
        messages.append(f"✗ Failed to reload daemon: {e}")

    # Enable service
    try:
        enable_service()
        messages.append("✓ Service enabled")
    except SystemdError as e:
        messages.append(f"✗ Failed to enable service: {e}")

    # Start service
    try:
        start_service()
        messages.append("✓ Service started")
    except SystemdError as e:
        messages.append(f"✗ Failed to start service: {e}")

    # Check linger
    if not check_linger_enabled():
        messages.append("")
        messages.append("⚠ Lingering is not enabled.")
        messages.append("  To run services after logout, run:")
        messages.append("    loginctl enable-linger")

    return "\n".join(messages)
