"""CLI entry point for Slurm Watchdog."""

import signal
import sys
import time

import click

from slurm_watchdog import __version__
from slurm_watchdog.config import (
    create_default_config,
    get_config_template,
    load_config,
    resolve_config_path,
    validate_config,
)
from slurm_watchdog.database import Database
from slurm_watchdog.notifier import Notifier
from slurm_watchdog.systemd import (
    disable_service,
    enable_service,
    full_install,
    get_journal_logs,
    get_service_info,
    get_service_status,
    is_service_active,
    is_service_enabled,
    is_service_installed,
    restart_service,
    start_service,
    stop_service,
    uninstall_service,
)
from slurm_watchdog.watcher import JobWatcher


def _get_selected_config_path(ctx: click.Context) -> str:
    """Resolve the config path selected on the CLI."""
    return str(resolve_config_path(ctx.obj.get("config_path")))


@click.group()
@click.version_option(version=__version__)
@click.option(
    "--config",
    "-c",
    "config_path",
    type=click.Path(),
    default=None,
    help="Path to configuration file",
)
@click.pass_context
def main(ctx: click.Context, config_path: str | None) -> None:
    """Slurm Watchdog - Monitor Slurm jobs and send notifications."""
    ctx.ensure_object(dict)
    ctx.obj["config_path"] = config_path


@main.command()
@click.pass_context
def install(ctx: click.Context) -> None:
    """Install and enable the systemd user service."""
    click.echo("Installing Slurm Watchdog service...")
    result = full_install()
    click.echo(result)


@main.command()
@click.pass_context
def start(ctx: click.Context) -> None:
    """Start the watchdog service."""
    if not is_service_installed():
        click.echo("Service not installed. Run 'slurm-watchdog install' first.")
        sys.exit(1)

    try:
        start_service()
        click.echo("Service started.")
    except Exception as e:
        click.echo(f"Failed to start service: {e}")
        sys.exit(1)


@main.command()
@click.pass_context
def stop(ctx: click.Context) -> None:
    """Stop the watchdog service."""
    try:
        stop_service()
        click.echo("Service stopped.")
    except Exception as e:
        click.echo(f"Failed to stop service: {e}")
        sys.exit(1)


@main.command()
@click.pass_context
def restart(ctx: click.Context) -> None:
    """Restart the watchdog service."""
    try:
        restart_service()
        click.echo("Service restarted.")
    except Exception as e:
        click.echo(f"Failed to restart service: {e}")
        sys.exit(1)


@main.command()
@click.pass_context
def status(ctx: click.Context) -> None:
    """Show service status."""
    info = get_service_info()

    click.echo("Slurm Watchdog Status")
    click.echo("=" * 40)
    click.echo(f"Service file: {info['service_file']}")
    click.echo(f"Installed: {'Yes' if info['installed'] else 'No'}")
    click.echo(f"Enabled: {'Yes' if info['enabled'] else 'No'}")
    click.echo(f"Running: {'Yes' if info['active'] else 'No'}")
    click.echo(f"Linger enabled: {'Yes' if info['linger_enabled'] else 'No'}")
    click.echo()

    if info["installed"]:
        click.echo("Systemd status:")
        click.echo("-" * 40)
        click.echo(get_service_status())


@main.command()
@click.option("-f", "--follow", is_flag=True, help="Follow log output")
@click.option("-n", "--lines", default=50, help="Number of lines to show")
@click.pass_context
def logs(ctx: click.Context, follow: bool, lines: int) -> None:
    """View service logs."""
    output = get_journal_logs(lines=lines, follow=follow)
    click.echo(output)


@main.command()
@click.pass_context
def scan(ctx: click.Context) -> None:
    """Run a single scan of Slurm jobs."""
    config_path = _get_selected_config_path(ctx)
    config = load_config(config_path, create_if_missing=True)

    with Database(config.database.path) as db:
        watcher = JobWatcher(config, db)
        updated_jobs, new_events = watcher.scan()

        click.echo("Scanned Slurm queue")
        click.echo(f"Updated {len(updated_jobs)} jobs")
        click.echo(f"Created {len(new_events)} events")

        if new_events:
            click.echo("\nNew events:")
            for event in new_events:
                job = db.get_job(event.job_id)
                job_name = job.name if job else event.job_id
                click.echo(f"  {event.event_type.value}: {job_name} ({event.job_id})")


@main.command()
@click.pass_context
def run(ctx: click.Context) -> None:
    """Run the watchdog daemon (foreground)."""
    config_path = _get_selected_config_path(ctx)
    config = load_config(config_path, create_if_missing=True)

    # Validate configuration
    warnings = validate_config(config)
    for warning in warnings:
        click.echo(f"Warning: {warning}")

    click.echo(f"Starting Slurm Watchdog v{__version__}")
    click.echo(f"Monitoring user: {config.watchdog.user}")
    click.echo(f"Database: {config.database.path}")
    click.echo(
        "Poll interval: "
        f"{config.watchdog.poll_interval_running}s (active) / "
        f"{config.watchdog.poll_interval_idle}s (idle)"
    )
    click.echo(f"Notifications: {len(config.notify.urls)} endpoint(s) configured")

    if not config.notify.urls:
        click.echo("Warning: No notification URLs configured!")

    # Setup signal handlers for graceful shutdown
    shutdown_requested = False

    def signal_handler(signum, frame):
        nonlocal shutdown_requested
        shutdown_requested = True
        click.echo("\nShutdown requested...")

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    with Database(config.database.path) as db:
        watcher = JobWatcher(config, db)
        notifier = Notifier(config, db)

        click.echo("Watchdog started. Press Ctrl+C to stop.")

        while not shutdown_requested:
            try:
                # Scan for job updates
                updated_jobs, new_events = watcher.scan()

                if updated_jobs:
                    click.echo(
                        f"[{time.strftime('%H:%M:%S')}] "
                        f"Updated {len(updated_jobs)} jobs, {len(new_events)} events"
                    )

                # Process pending notifications
                if notifier.has_urls_configured():
                    success, failed = notifier.process_pending_events()
                    if success or failed:
                        click.echo(
                            f"[{time.strftime('%H:%M:%S')}] "
                            f"Notifications: {success} sent, {failed} failed"
                        )

                    # Retry failed notifications
                    retry_success, retry_failed = notifier.retry_failed_events()
                    if retry_success or retry_failed:
                        click.echo(
                            f"[{time.strftime('%H:%M:%S')}] "
                            f"Retries: {retry_success} sent, {retry_failed} failed"
                        )

                # Get adaptive poll interval
                interval = watcher.get_poll_interval()

                # Sleep with interrupt check
                for _ in range(interval):
                    if shutdown_requested:
                        break
                    time.sleep(1)

            except Exception as e:
                click.echo(f"Error: {e}")
                time.sleep(60)  # Wait before retrying

        click.echo("Watchdog stopped.")


@main.command()
@click.argument("message", default="Test notification from Slurm Watchdog")
@click.pass_context
def test_notify(ctx: click.Context, message: str) -> None:
    """Send a test notification."""
    config_path = _get_selected_config_path(ctx)
    config = load_config(config_path, create_if_missing=True)

    if not config.notify.urls:
        click.echo("No notification URLs configured!")
        click.echo(f"Edit {config_path} to add notification services.")
        sys.exit(1)

    click.echo(f"Sending test notification to {len(config.notify.urls)} endpoint(s)...")

    with Database(config.database.path) as db:
        notifier = Notifier(config, db)
        success = notifier.test_notify(message)

        if success:
            click.echo("✓ Test notification sent successfully!")
        else:
            click.echo("✗ Test notification failed!")
            sys.exit(1)


# Config commands group
@main.group()
def config_cmd() -> None:
    """Configuration management commands."""
    pass


@config_cmd.command("show")
@click.pass_context
def config_show(ctx: click.Context) -> None:
    """Show current configuration."""
    config_path = _get_selected_config_path(ctx)
    config = load_config(config_path, create_if_missing=True)

    click.echo(f"Configuration file: {config_path}")
    click.echo()
    click.echo("[watchdog]")
    click.echo(f"  poll_interval_running = {config.watchdog.poll_interval_running}")
    click.echo(f"  poll_interval_idle = {config.watchdog.poll_interval_idle}")
    click.echo(f"  disappeared_grace_seconds = {config.watchdog.disappeared_grace_seconds}")
    click.echo(f"  user = {config.watchdog.user}")
    if config.watchdog.job_name_filter:
        click.echo(f"  job_name_filter = {config.watchdog.job_name_filter}")
    if config.watchdog.partition_filter:
        click.echo(f"  partition_filter = {config.watchdog.partition_filter}")

    click.echo()
    click.echo("[database]")
    click.echo(f"  path = {config.database.path}")

    click.echo()
    click.echo("[notify]")
    click.echo(f"  urls = {len(config.notify.urls)} configured")
    click.echo(f"  on_job_started = {config.notify.on_job_started}")
    click.echo(f"  on_job_completed = {config.notify.on_job_completed}")
    click.echo(f"  on_job_failed = {config.notify.on_job_failed}")
    click.echo(f"  on_job_cancelled = {config.notify.on_job_cancelled}")
    click.echo(f"  on_job_timeout = {config.notify.on_job_timeout}")
    click.echo(f"  on_job_boot_fail = {config.notify.on_job_boot_fail}")
    click.echo(f"  on_job_out_of_memory = {config.notify.on_job_out_of_memory}")
    click.echo(f"  on_job_lost = {config.notify.on_job_lost}")

    click.echo()
    click.echo("[output_analysis]")
    click.echo(f"  enabled = {config.output_analysis.enabled}")
    click.echo(f"  tail_lines = {config.output_analysis.tail_lines}")


@config_cmd.command("init")
@click.pass_context
def config_init(ctx: click.Context) -> None:
    """Initialize configuration file with defaults."""
    config_path = create_default_config(_get_selected_config_path(ctx))
    click.echo(f"Created configuration file: {config_path}")
    click.echo("Edit this file to configure notification URLs and other settings.")


@config_cmd.command("template")
@click.pass_context
def config_template(ctx: click.Context) -> None:
    """Print the configuration template."""
    click.echo(get_config_template())


@config_cmd.command("validate")
@click.pass_context
def config_validate(ctx: click.Context) -> None:
    """Validate current configuration."""
    config_path = _get_selected_config_path(ctx)

    try:
        config = load_config(config_path, create_if_missing=False)
    except FileNotFoundError:
        click.echo(f"Configuration file not found: {config_path}")
        click.echo("Run 'slurm-watchdog config init' to create it.")
        sys.exit(1)
    except ValueError as e:
        click.echo(f"Configuration error: {e}")
        sys.exit(1)

    warnings = validate_config(config)

    if warnings:
        click.echo("Configuration warnings:")
        for warning in warnings:
            click.echo(f"  - {warning}")
    else:
        click.echo("Configuration is valid!")


# Register config command group
main.add_command(config_cmd, name="config")


# Additional commands for service management
@main.command()
@click.pass_context
def enable(ctx: click.Context) -> None:
    """Enable the service to start at login."""
    try:
        enable_service()
        click.echo("Service enabled.")
    except Exception as e:
        click.echo(f"Failed to enable service: {e}")
        sys.exit(1)


@main.command()
@click.pass_context
def disable(ctx: click.Context) -> None:
    """Disable the service from starting at login."""
    try:
        disable_service()
        click.echo("Service disabled.")
    except Exception as e:
        click.echo(f"Failed to disable service: {e}")
        sys.exit(1)


@main.command()
@click.pass_context
def uninstall(ctx: click.Context) -> None:
    """Uninstall the systemd service."""
    if is_service_active():
        click.echo("Stopping service...")
        stop_service()

    if is_service_enabled():
        click.echo("Disabling service...")
        disable_service()

    if is_service_installed():
        click.echo("Removing service file...")
        uninstall_service()

    click.echo("Service uninstalled.")


if __name__ == "__main__":
    main()
