from __future__ import annotations

from importlib.metadata import version

from typer.testing import CliRunner

from huldra import __version__
from huldra.cli import app


def test_runtime_version_matches_distribution_metadata() -> None:
    assert __version__ == version("huldra-arxiv")


def test_cli_help_and_version() -> None:
    runner = CliRunner()
    help_result = runner.invoke(app, ["--help"])
    version_result = runner.invoke(app, ["version"])
    assert help_result.exit_code == 0
    assert "local arXiv metadata broker" in help_result.output
    assert version_result.exit_code == 0
    assert f"huldra {__version__}" in version_result.output
