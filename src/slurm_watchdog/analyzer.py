"""Output file analysis for Slurm job outputs."""

import os
import re
from pathlib import Path
from typing import Optional

from slurm_watchdog.config import Config
from slurm_watchdog.models import OutputAnalysis


class OutputAnalyzer:
    """Analyzes Slurm job output files for convergence and errors."""

    def __init__(self, config: Config):
        """Initialize analyzer.

        Args:
            config: Configuration.
        """
        self.config = config
        self.analysis_config = config.output_analysis

    def analyze(self, output_path: str) -> OutputAnalysis:
        """Analyze a job output file.

        Args:
            output_path: Path to output file.

        Returns:
            OutputAnalysis with results.
        """
        path = Path(output_path).expanduser()

        if not path.exists():
            return OutputAnalysis(
                converged=False,
                has_errors=False,
                error_lines=[f"Output file not found: {output_path}"],
            )

        try:
            with open(path, "r", errors="replace") as f:
                # Read last N lines
                lines = self._tail_file(f, self.analysis_config.tail_lines)
        except Exception as e:
            return OutputAnalysis(
                converged=False,
                has_errors=True,
                error_lines=[f"Failed to read output file: {e}"],
            )

        # Analyze for convergence
        convergence_lines = self._find_patterns(
            lines, self.analysis_config.convergence_patterns
        )

        # Analyze for errors
        error_lines = self._find_patterns(lines, self.analysis_config.error_patterns)

        return OutputAnalysis(
            converged=len(convergence_lines) > 0,
            has_errors=len(error_lines) > 0,
            convergence_lines=convergence_lines,
            error_lines=error_lines,
            tail_lines=lines[-10:],  # Keep last 10 lines for context
        )

    def _tail_file(self, file, n: int) -> list[str]:
        """Read the last n lines from a file.

        Args:
            file: Open file object.
            n: Number of lines to read.

        Returns:
            List of lines.
        """
        # For small files, just read all
        try:
            file.seek(0, os.SEEK_END)
            size = file.tell()
            if size < 100000:  # < 100KB, read all
                file.seek(0)
                return file.readlines()[-n:]
        except OSError:
            pass

        # For larger files, use a more efficient approach
        # Read in blocks from the end
        file.seek(0, os.SEEK_END)
        end_pos = file.tell()

        lines = []
        pos = end_pos
        block_size = 4096

        while pos > 0 and len(lines) < n:
            read_size = min(block_size, pos)
            pos -= read_size
            file.seek(pos)
            chunk = file.read(read_size)
            lines = chunk.splitlines() + lines

        return lines[-n:]

    def _find_patterns(self, lines: list[str], patterns: list[str]) -> list[str]:
        """Find lines matching any of the patterns.

        Args:
            lines: Lines to search.
            patterns: Patterns to match (case-insensitive substring match).

        Returns:
            Matching lines (stripped).
        """
        matches = []
        for line in lines:
            line_stripped = line.strip()
            if not line_stripped:
                continue

            for pattern in patterns:
                if pattern.lower() in line_stripped.lower():
                    matches.append(line_stripped)
                    break

        return matches

    def analyze_for_summary(self, output_path: str, max_lines: int = 20) -> str:
        """Get a brief summary of the output file.

        Args:
            output_path: Path to output file.
            max_lines: Maximum lines to include.

        Returns:
            Summary string.
        """
        analysis = self.analyze(output_path)

        parts = []

        if analysis.converged:
            parts.append("✅ Converged")
        else:
            parts.append("⏳ Not converged" if not analysis.has_errors else "❌ Failed")

        if analysis.has_errors:
            parts.append(f"({len(analysis.error_lines)} errors)")

        return " ".join(parts)

    def get_detailed_report(
        self,
        output_path: str,
        include_tail: bool = True,
    ) -> str:
        """Get a detailed analysis report.

        Args:
            output_path: Path to output file.
            include_tail: Include tail lines in report.

        Returns:
            Detailed report string.
        """
        analysis = self.analyze(output_path)
        lines = []

        lines.append(f"Output File: {output_path}")
        lines.append(f"Status: {analysis.get_summary()}")
        lines.append("")

        if analysis.convergence_lines:
            lines.append("Convergence indicators found:")
            for cl in analysis.convergence_lines[:5]:
                lines.append(f"  • {cl}")
            if len(analysis.convergence_lines) > 5:
                lines.append(f"  ... and {len(analysis.convergence_lines) - 5} more")
            lines.append("")

        if analysis.error_lines:
            lines.append("Errors detected:")
            for el in analysis.error_lines[:10]:
                lines.append(f"  ! {el}")
            if len(analysis.error_lines) > 10:
                lines.append(f"  ... and {len(analysis.error_lines) - 10} more")
            lines.append("")

        if include_tail and analysis.tail_lines:
            lines.append("Last lines:")
            for tl in analysis.tail_lines:
                lines.append(f"  {tl}")

        return "\n".join(lines)


def find_output_file(job_id: str, working_dir: Optional[str] = None) -> Optional[str]:
    """Try to find the output file for a job.

    Slurm output files are typically named:
    - slurm-{job_id}.out
    - slurm-{job_id}.err
    - {job_name}.out
    - Custom pattern from sbatch --output

    Args:
        job_id: Job ID.
        working_dir: Working directory to search.

    Returns:
        Path to output file if found, None otherwise.
    """
    if working_dir is None:
        working_dir = os.getcwd()

    working_dir = Path(working_dir).expanduser()

    # Common output file patterns
    patterns = [
        f"slurm-{job_id}.out",
        f"slurm-{job_id}.err",
        f"slurm-{job_id}*.out",
        f"slurm_{job_id}.out",
    ]

    for pattern in patterns:
        matches = list(working_dir.glob(pattern))
        if matches:
            return str(matches[0])

    return None
