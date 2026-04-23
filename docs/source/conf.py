from __future__ import annotations

import re
import sys
from dataclasses import MISSING, fields
from datetime import datetime
from pathlib import Path
from typing import Any, get_type_hints

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from runsmith import __version__

project = "Runsmith"
author = "Di Lu"
copyright = f"{datetime.now().year}, {author}"
version = __version__
release = __version__

extensions = [
    "myst_parser",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "sphinx.ext.intersphinx",
    "sphinx_design",
    "sphinxcontrib.mermaid",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

autosummary_generate = True
autodoc_typehints = "description"
autodoc_member_order = "bysource"

myst_enable_extensions = ["colon_fence", "substitution"]
myst_substitutions = {"version": __version__}

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
}

html_theme = "sphinx_book_theme"
html_title = f"Runsmith {__version__}"
html_logo = "../logo.svg"
html_theme_options = {
    "repository_url": "https://github.com/namoshizun/Runsmith",
    "use_repository_button": True,
    "show_toc_level": 2,
}
html_static_path = ["_static"]


def _write_changelog(app) -> None:
    project_root = Path(__file__).resolve().parents[2]
    source_path = project_root / "CHANGELOG.md"
    output_path = Path(__file__).resolve().parent / "generated" / "changelog.md"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if source_path.exists():
        changelog = source_path.read_text(encoding="utf-8").strip()
        if changelog.startswith("## Changelog"):
            changelog = changelog.removeprefix("## Changelog").lstrip()
        changelog = re.sub(
            r"^(#{2,6})\s+",
            lambda m: f"{'#' * (len(m.group(1)) - 1)} ",
            changelog,
            flags=re.MULTILINE,
        )
        content = f"# Changelog\n\n{changelog}\n"
    else:
        content = "# Changelog\n\n- `CHANGELOG.md` was not found in the project root.\n"

    output_path.write_text(content, encoding="utf-8")


def _generate_api_docs(app) -> None:
    from sphinx.ext.apidoc import main as apidoc_main

    project_root = Path(__file__).resolve().parents[2]
    package_dir = project_root / "runsmith"
    output_dir = Path(__file__).resolve().parent / "api" / "generated"
    output_dir.mkdir(parents=True, exist_ok=True)

    apidoc_main(
        [
            "--force",
            "--module-first",
            "--separate",
            "--output-dir",
            str(output_dir),
            str(package_dir),
        ]
    )


def _format_type_name(annotation: Any) -> str:
    try:
        return annotation.__name__
    except AttributeError:
        return str(annotation).replace("typing.", "")


def _write_settings_docs(app) -> None:
    from runsmith.settings import RunsmithSettings

    output_path = Path(__file__).resolve().parent / "generated" / "settings.md"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    settings_fields = fields(RunsmithSettings)
    hints = get_type_hints(RunsmithSettings)

    grouped: dict[str, list[Any]] = {}
    for setting in settings_fields:
        group = str(setting.metadata.get("group", "Other"))
        grouped.setdefault(group, []).append(setting)

    lines: list[str] = [
        "# Settings",
        "",
        "Runsmith settings are loaded from environment variables prefixed with `RUNSMITH_`.",
        "",
    ]

    for group_name, group_fields in grouped.items():
        lines.append(f"## {group_name}")
        lines.append("")
        for setting in group_fields:
            default_value = (
                f"`{setting.default!r}`" if setting.default is not MISSING else "`<required>`"
            )
            doc = str(setting.metadata.get("doc", "No description provided."))
            type_name = _format_type_name(hints.get(setting.name, Any))

            lines.extend(
                [
                    f"`{setting.name}`",
                    f"- **Type**: `{type_name}`",
                    f"- **Default**: {default_value}",
                    f"- **What it controls**: {doc}",
                    "",
                ]
            )

    output_path.write_text("\n".join(lines), encoding="utf-8")


def setup(app) -> None:
    app.connect("builder-inited", _generate_api_docs)
    app.connect("builder-inited", _write_changelog)
    app.connect("builder-inited", _write_settings_docs)
