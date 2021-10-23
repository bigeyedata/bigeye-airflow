from dataclasses import dataclass
from typing import Dict


@dataclass
class TableIndexKey:
    schema_name: str
    table_name: str
