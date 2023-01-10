from typing import Any, Sequence

import pandas as pd
import tabulize
from tinytim.rows import row_dicts_to_data
from sqlalchemy.engine import Engine

Record = dict[str, Any]


class SqlDataFrame(pd.DataFrame):
    _metadata = ['sqltable', 'primary_keys', 'name', 'engine']

    def __init__(self, name: str, engine: Engine, *args, **kwargs):
        self.sqltable = tabulize.SqlTable(name, engine)
        data = row_dicts_to_data(self.sqltable.old_records)
        super().__init__(data, *args, **kwargs)

    @property
    def _constructor(self):
        return SqlDataFrame

    @property
    def _constructor_sliced(self):
        return SqlDataFrame

    @property
    def primary_keys(self) -> list[str]:
        return self.sqltable.primary_keys

    @primary_keys.setter
    def primary_keys(self, column_names: Sequence[str]) -> None:
        self.sqltable.primary_keys = list(column_names)

    def record_changes(self) -> dict[str, list[Record]]:
        return self.sqltable.record_changes(self)

    def insert_record(self, record: Record) -> None:
        self.append(record, ignore_index=True)

    def insert_records(self, records: Sequence[Record]) -> None:
        for record in records:
            self.insert_record(record)

    def pull(self):
        self.sqltable.pull()

    def push(self) -> None:
        self.sqltable.push(self)
        self.pull()


    