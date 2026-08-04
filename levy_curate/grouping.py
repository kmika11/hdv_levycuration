"""Strategies for partitioning a flat file list into datasets.

Three strategies cover every batch this project has deposited so far:

by_column          dataset_title is already a column in the table
by_filename_regex  extract a key from the filename (e.g. the year in A99_12624.jpg)
by_chunk           arbitrary fixed-size buckets (the 500-file illustration datasets)

Each returns a pandas Series of dataset titles aligned to the input frame.
"""

from __future__ import annotations

import re

import numpy as np
import pandas as pd

from .config import GroupingSpec


def convert_two_digit_year(value, pivot: int = 49):
    """Expand a two-digit year. <= pivot is 20xx, otherwise 19xx.

    A99 -> 1999, A12 -> 2012. The pivot is configurable because the correct
    cutoff depends on the collection's date range.
    """
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return np.nan
    try:
        year = int(value)
    except (TypeError, ValueError):
        return np.nan
    return 2000 + year if year <= pivot else 1900 + year


def by_column(df: pd.DataFrame, spec: GroupingSpec) -> pd.Series:
    """Use an existing column as the dataset title."""
    col = spec.column
    if not col:
        raise ValueError("grouping.strategy 'by_column' requires grouping.column")
    if col not in df.columns:
        raise KeyError(
            f"grouping.column {col!r} not found. Available columns: {list(df.columns)}"
        )
    if df[col].isna().any():
        n = int(df[col].isna().sum())
        raise ValueError(f"grouping.column {col!r} has {n} empty value(s)")
    return df[col].astype(str).map(lambda k: spec.title_template.format(key=k))


def by_filename_regex(df: pd.DataFrame, spec: GroupingSpec) -> pd.Series:
    """Derive the dataset title from a capture group in the filename.

    If the captured value is a two-digit number it is expanded to a four-digit
    year, which is how the photograph batches are organised.
    """
    if not spec.pattern:
        raise ValueError(
            "grouping.strategy 'by_filename_regex' requires grouping.pattern"
        )
    rx = re.compile(spec.pattern)

    def key_for(name: str):
        m = rx.search(str(name))
        if not m:
            return np.nan
        key = m.group(1) if m.groups() else m.group(0)
        if re.fullmatch(r"\d{2}", key):
            key = convert_two_digit_year(key, spec.two_digit_year_pivot)
        return key

    keys = df["filename"].map(key_for)

    if keys.isna().any():
        bad = df.loc[keys.isna(), "filename"].head(5).tolist()
        raise ValueError(
            f"grouping.pattern {spec.pattern!r} did not match "
            f"{int(keys.isna().sum())} filename(s), e.g. {bad}"
        )

    return keys.map(lambda k: spec.title_template.format(key=k))


def by_chunk(df: pd.DataFrame, spec: GroupingSpec) -> pd.Series:
    """Split into fixed-size buckets, numbered from 1, in current row order.

    Sort the frame before calling if bucket membership needs to be stable
    across runs -- otherwise a reordered input silently reshuffles datasets.
    """
    size = int(spec.size)
    if size < 1:
        raise ValueError(f"grouping.size must be >= 1, got {size}")
    numbers = (np.arange(len(df)) // size) + 1
    return pd.Series(
        [spec.title_template.format(key=n) for n in numbers], index=df.index
    )


STRATEGIES = {
    "by_column": by_column,
    "by_filename_regex": by_filename_regex,
    "by_chunk": by_chunk,
}


def apply(df: pd.DataFrame, spec: GroupingSpec) -> pd.Series:
    """Dispatch to the configured strategy and return dataset titles."""
    fn = STRATEGIES.get(spec.strategy)
    if fn is None:
        raise ValueError(
            f"Unknown grouping strategy {spec.strategy!r}. "
            f"Choose one of: {', '.join(sorted(STRATEGIES))}"
        )
    return fn(df, spec)
