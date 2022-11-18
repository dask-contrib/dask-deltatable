from typing import Dict, List, Optional

import dask.dataframe as dd
from _typeshed import Incomplete

class DeltaTableWrapper:
    path: str
    version: int
    columns: List[str]
    datetime: str
    storage_options: Dict[str, any]
    dt: Incomplete
    schema: Incomplete
    def __init__(
        self,
        path: str,
        version: int,
        columns: List[str],
        datetime: Optional[str] = ...,
        storage_options: Dict[str, str] = ...,
    ) -> None: ...
    def read_delta_dataset(self, f: str, **kwargs: Dict[any, any]): ...
    def history(self, limit: Optional[int] = ..., **kwargs) -> dd.core.DataFrame: ...
    def vacuum(self, retention_hours: int = ..., dry_run: bool = ...) -> None: ...
    def get_pq_files(self) -> List[str]: ...
    def read_delta_table(self, **kwargs) -> dd.core.DataFrame: ...

def read_delta_table(
    path: str = ...,
    catalog: Optional[str] = ...,
    database_name: str = ...,
    table_name: str = ...,
    version: int = ...,
    columns: List[str] = ...,
    storage_options: Dict[str, str] = ...,
    datetime: str = ...,
    **kwargs
): ...
def read_delta_history(
    path: str, limit: Optional[int] = ..., storage_options: Dict[str, str] = ...
) -> dd.core.DataFrame: ...
def vacuum(
    path: str,
    retention_hours: int = ...,
    dry_run: bool = ...,
    storage_options: Dict[str, str] = ...,
) -> None: ...
