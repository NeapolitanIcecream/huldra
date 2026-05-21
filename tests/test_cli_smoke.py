from __future__ import annotations

from typer.testing import CliRunner

from huldra.cli import app


def test_cli_help_and_version() -> None:
    runner = CliRunner()
    help_result = runner.invoke(app, ["--help"])
    version_result = runner.invoke(app, ["version"])
    assert help_result.exit_code == 0
    assert "local arXiv metadata broker" in help_result.output
    assert version_result.exit_code == 0
    assert "huldra 0.1.0" in version_result.output
