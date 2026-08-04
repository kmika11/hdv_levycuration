"""The inventory: one row per file to deposit, validated before any network call.

The canonical schema is four columns:

    filename             as it exists on disk, including extension
    file_description     per-file description (the formatted OCHRE link)
    dataset_title        the grouping key
    dataset_description  dataset abstract, identical across a dataset's rows

Per-batch column names are mapped to these via `column_map` in the config, so a
new spreadsheet layout is a config change rather than a code change.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from .config import BatchConfig

CANONICAL = ["filename", "file_description", "dataset_title", "dataset_description"]
OPTIONAL = ["metadata_link", "prod_date"]


@dataclass
class ValidationReport:
    """Outcome of validating an inventory. Falsy when problems were found."""

    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    missing_on_disk: pd.DataFrame = field(default_factory=pd.DataFrame)
    missing_in_table: pd.DataFrame = field(default_factory=pd.DataFrame)
    n_rows: int = 0
    n_datasets: int = 0

    @property
    def ok(self) -> bool:
        return not self.errors

    def __bool__(self) -> bool:
        return self.ok

    def report(self) -> str:
        lines = [f"{self.n_rows} files across {self.n_datasets} dataset(s)"]
        for e in self.errors:
            lines.append(f"  ERROR    {e}")
        for w in self.warnings:
            lines.append(f"  warning  {w}")
        lines.append("  OK -- inventory is valid" if self.ok else "  NOT READY TO DEPOSIT")
        return "\n".join(lines)


def normalize(df: pd.DataFrame, column_map: dict[str, str] | None = None) -> pd.DataFrame:
    """Rename columns to the canonical schema and strip whitespace from headers.

    Trailing spaces in headers ("IDs ") have caused silent lookup failures in
    this project before, so they are stripped unconditionally.
    """
    df = df.copy()
    df.columns = [str(c).strip().lstrip("﻿") for c in df.columns]
    if column_map:
        df = df.rename(columns={k.strip(): v for k, v in column_map.items()})
    return df


def load(cfg: BatchConfig, path: str | Path | None = None) -> pd.DataFrame:
    """Read the inventory CSV and normalize its columns."""
    path = Path(path) if path else cfg.inventory
    if not path.exists():
        raise FileNotFoundError(
            f"Inventory not found: {path}\n"
            f"Run the scaffolder first, or set `inventory` in the batch config."
        )
    df = pd.read_csv(path, index_col=None, low_memory=False)
    return normalize(df, cfg.column_map)


def validate(df: pd.DataFrame, cfg: BatchConfig, check_disk: bool = True) -> ValidationReport:
    """Check an inventory for every problem we can catch without the network.

    This is the step that would have prevented most of what is recorded in
    errors.md -- bad descriptions and filename drift both surface here.
    """
    rep = ValidationReport(n_rows=len(df))

    missing_cols = [c for c in CANONICAL if c not in df.columns]
    if missing_cols:
        rep.errors.append(
            f"missing required column(s): {missing_cols}; present: {list(df.columns)}"
        )
        return rep  # nothing else is meaningful without the schema

    if df.empty:
        rep.errors.append("inventory is empty")
        return rep

    rep.n_datasets = df["dataset_title"].nunique()

    # -- required values --------------------------------------------------
    for col in CANONICAL:
        blank = df[col].isna() | (df[col].astype(str).str.strip() == "")
        if blank.any():
            rep.errors.append(f"{col}: {int(blank.sum())} empty value(s)")

    # -- one description per dataset --------------------------------------
    per_dataset = df.groupby("dataset_title")["dataset_description"].nunique()
    inconsistent = per_dataset[per_dataset > 1]
    if len(inconsistent):
        rep.errors.append(
            f"dataset_description differs within dataset(s): {list(inconsistent.index)[:5]}"
        )

    # -- duplicate filenames ----------------------------------------------
    dupes = df["filename"][df["filename"].duplicated(keep=False)]
    if len(dupes):
        rep.errors.append(
            f"{dupes.nunique()} duplicate filename(s), e.g. {sorted(set(dupes))[:3]}"
        )

    # -- description sanity (the Bean Validation class of failure) ---------
    too_long = df["file_description"].astype(str).str.len() > 1000
    if too_long.any():
        rep.errors.append(
            f"file_description: {int(too_long.sum())} value(s) over 1000 chars; "
            f"Dataverse rejects these at file-registration time"
        )
    embedded = df["file_description"].astype(str).str.contains(r"[\r\n]", regex=True)
    if embedded.any():
        rep.errors.append(
            f"file_description: {int(embedded.sum())} value(s) contain newlines"
        )

    # -- extension case ----------------------------------------------------
    exts = df["filename"].astype(str).str.rsplit(".", n=1).str[-1]
    mixed = exts[exts != exts.str.lower()]
    if len(mixed):
        rep.warnings.append(
            f"{len(mixed)} filename(s) have uppercase extensions, e.g. "
            f"{df.loc[mixed.index, 'filename'].head(3).tolist()}"
        )

    # -- reconcile against disk -------------------------------------------
    if check_disk:
        rep.missing_on_disk, rep.missing_in_table = reconcile(
            df, cfg.files_dir, cfg.scaffold.file_extensions
        )
        if len(rep.missing_on_disk):
            rep.errors.append(
                f"{len(rep.missing_on_disk)} row(s) have no matching file in "
                f"{cfg.files_dir}"
            )
        if len(rep.missing_in_table):
            rep.warnings.append(
                f"{len(rep.missing_in_table)} file(s) on disk are absent from the "
                f"inventory and will not be deposited"
            )

    return rep


def reconcile(
    df: pd.DataFrame,
    files_dir: str | Path,
    extensions: list[str] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Compare inventory rows against files on disk.

    Parameters
    ----------
    extensions : list of str, optional
        Restrict the disk side to these extensions, so unrelated files sitting
        in the directory (READMEs, sidecar .xml) are not reported as omissions.

    Return
    ------
    (missing_on_disk, missing_in_table)
        Rows whose file is absent, and a frame of filenames present on disk but
        not listed. This is the generalized form of check_files.zsh.
    """
    files_dir = Path(files_dir)
    if not files_dir.exists():
        raise FileNotFoundError(f"files_dir does not exist: {files_dir}")

    exts = {e.lower().lstrip(".") for e in (extensions or [])}
    on_disk = {
        p.name
        for p in files_dir.iterdir()
        if p.is_file()
        and not p.name.startswith(".")
        and (not exts or p.suffix.lower().lstrip(".") in exts)
    }
    listed = set(df["filename"].astype(str))

    missing_on_disk = df[~df["filename"].astype(str).isin(on_disk)].copy()
    missing_in_table = pd.DataFrame(sorted(on_disk - listed), columns=["filename"])
    return missing_on_disk, missing_in_table


def split_by_dataset(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Partition a validated inventory into {dataset_title: rows}."""
    return {title: group.copy() for title, group in df.groupby("dataset_title", sort=True)}
