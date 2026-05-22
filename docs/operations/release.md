# Release

Use this checklist to prepare a Huldra package release from a clean `main`
checkout.

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
rm -rf dist
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

## Tag And Publish

Create an annotated tag after validation passes:

```bash
git tag -a v0.1.0 -m "Release v0.1.0"
git push origin main v0.1.0
```

Publish the built artifacts only after the tag is pushed:

```bash
uv publish
```
