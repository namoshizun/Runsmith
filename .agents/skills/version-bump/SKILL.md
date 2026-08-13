---
name: version-bump
description: Bump the Runsmith package version, sync runsmith/__init__.py, then run tests, docs, and lint. Use when the user asks to bump, release, or set the version and supplies a version number.
---

# Version bump

Input: a version number only (e.g. `1.2.3`). Do not invent a version. Do not write changelog entries.

## Check

`CHANGELOG.md` must already contain a heading for that version:

```markdown
### 1.2.3 (YYYY-MM-DD)
```

Match a line that starts with `### {version}` (date in parentheses is optional).

If missing, **stop**. Tell the user to add the `CHANGELOG.md` section first. Do not bump.

## Actions

Run in this order. Stop on the first failure.

1. Bump package metadata and lockfile:

   ```bash
   uv version {version}
   ```

2. Set `__version__` in `runsmith/__init__.py` to the same string.

3. Validate:

   ```bash
   uv run --group test pytest
   uv run --group docs sphinx-build -E -b html docs/source docs/_build/html
   uv run --group dev ruff check
   uv run --group dev pyright --level=error
   ```

Do not commit unless the user asks.
