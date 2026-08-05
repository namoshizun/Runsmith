## Code Style

- Prefer iteration and modularization over code duplication. Implementation must be elegant, intuitive and Pythonic.
- Follow the "let it crash" principle: avoid excessive error handling and edge-case checks, especially for experimental solutions or features. Do not obscure the main intent with defensive boilerplate.
- All method parameters **must** be typed, all variables **should** be typed wherever sensible.
- Adopt Python 3.10+ typing styles. Must use native collection types (e.g., list, dict) instead of importing them from the typing module (e.g., from typing import List).
- **Important**: 
  1. Fix problems at their root cause, not their symptoms.
  2. If a bug reveals a deeper design flaw or incomplete design, propose fixing the design instead.


## Tooling
- Use loguru instead of the builtin logging module
- Write all Python tests as `pytest` style functions, not `unittest` classes.
- Build docs with: `uv run --group docs sphinx-build -E -b html docs/source docs/_build/html`