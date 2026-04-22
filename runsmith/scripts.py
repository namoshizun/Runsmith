from __future__ import annotations

import subprocess


def build_docs() -> None:
    command = ["sphinx-build", "-E", "-b", "html", "docs/source", "docs/_build/html"]
    raise SystemExit(subprocess.call(command))
