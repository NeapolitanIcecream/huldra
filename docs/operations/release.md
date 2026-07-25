# Release

Use this checklist to prepare a Huldra package release from a clean `main`
checkout. Tagged distributions are published by GitHub Actions through PyPI
Trusted Publishing; maintainers do not upload with a long-lived API token.

## Update Release Metadata

1. Set the same version in `pyproject.toml` and `src/huldra/__init__.py`.
2. Keep `[project].name` as `huldra-arxiv` unless the PyPI distribution is
   intentionally renamed. The import package and CLI command remain `huldra`.
3. Add a dated entry to `CHANGELOG.md`.
4. Confirm the version has not already been tagged:

```bash
git tag --list 'v0.1.0'
```

## Validate

Run the local release gates:

```bash
uv run ruff check .
uv run pyright
uv run pytest
uv build
uv run --with twine twine check dist/*
```

Install the wheel in a temporary environment and check the CLI entry point:

```bash
tmpdir=$(mktemp -d)
uv venv "$tmpdir/venv" --python 3.13
uv pip install --python "$tmpdir/venv/bin/python" dist/*.whl
"$tmpdir/venv/bin/huldra" --help
rm -rf "$tmpdir"
```

## One-Time Publisher Setup

The PyPI project owner must configure the publisher before the first automated
release:

1. In the `huldra-arxiv` PyPI project, open **Publishing** and add a GitHub
   Trusted Publisher.
2. Set owner `NeapolitanIcecream`, repository `huldra`, workflow
   `publish.yml`, and environment `pypi`.
3. In the GitHub repository, create the `pypi` environment. Add a required
   reviewer if releases should pause for human approval.

If the PyPI project does not yet exist, create the same configuration as a
pending publisher from the account's publishing settings.

## Tag And Publish

Create an annotated tag after validation passes:

```bash
git tag -a v0.4.2 -m "Release v0.4.2"
git push origin main v0.4.2
```

Create and publish the matching GitHub Release. The `Publish to PyPI` workflow
then verifies that the tag, package version, and commit match; builds and checks
the distributions; and publishes them from the protected `pypi` environment.

The manual workflow is only for retrying an existing tag. Select that tag as
the workflow ref and enter the same version without the `v` prefix.
