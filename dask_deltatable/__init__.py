from __future__ import annotations

__all__ = [
    "read_deltalake",
    "to_deltalake",
]

from .core import read_deltalake as read_deltalake
from .write import to_deltalake as to_deltalake
