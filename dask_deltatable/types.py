from __future__ import annotations

from typing import Any, Union

Filter = tuple[str, str, Any]
Filters = Union[list[Filter], list[list[Filter]], None]
