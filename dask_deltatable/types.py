from __future__ import annotations

from typing import Any, List, Tuple, Union

Filter = Tuple[str, str, Any]
Filters = Union[List[Filter], List[List[Filter]], None]
