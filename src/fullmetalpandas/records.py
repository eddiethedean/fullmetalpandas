from typing import Any, Dict, Generator, Iterable, List
import pandas as pd

Record = Dict[str, Any]


def insert_record(
    df: pd.DataFrame,
    record: Record
) -> None:
    df.loc[len(df.index)] = record # type: ignore


def insert_records(
    df: pd.DataFrame,
    records: Iterable[Record]
) -> None:
    for record in records:
        insert_record(df, record)


def iterrecords(df) -> Generator[dict, None, None]:
    for i, (_, series) in enumerate(df.iterrows()):
        yield series.to_dict()


def records(df) -> List[Record]:
    return list(iterrecords(df))


def df_to_records(df: pd.DataFrame) -> List[Record]:
    return df.to_dict('records') # type: ignore
    