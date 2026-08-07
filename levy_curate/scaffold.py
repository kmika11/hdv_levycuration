"""Build a complete metadata table from the files on disk plus a partial CSV.

The input you supply by hand is minimal: a CSV with a filename and a raw OCHRE
link per row. Everything else -- HTML-formatted descriptions, dataset titles,
dataset abstracts -- is derived here.

The enumeration starts from the *files on disk*, not from the partial CSV, so a
file you forgot to list still appears in the output (flagged, with an empty
description) rather than silently vanishing from the deposit.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from . import grouping
from .config import BatchConfig
from .inventory import CANONICAL, normalize


@dataclass
class ScaffoldResult:
    """What the scaffolder produced, and what needs a human's attention."""

    table: pd.DataFrame
    output_path: Path | None = None
    n_files: int = 0
    without_links: list[str] = field(default_factory=list)
    listed_but_absent: list[str] = field(default_factory=list)

    @property
    def complete(self) -> bool:
        return not self.without_links

    def report(self) -> str:
        lines = [
            f"scaffolded {len(self.table)} row(s) from {self.n_files} file(s) on disk"
        ]
        if self.table is not None and "dataset_title" in self.table:
            counts = self.table["dataset_title"].value_counts().sort_index()
            for title, n in counts.items():
                lines.append(f"    {title:<50} {n:>6} files")
        if self.without_links:
            lines.append(
                f"  ACTION NEEDED  {len(self.without_links)} file(s) have no OCHRE "
                f"link, e.g. {self.without_links[:3]}"
            )
        if self.listed_but_absent:
            lines.append(
                f"  warning  {len(self.listed_but_absent)} row(s) in the partial CSV "
                f"have no file on disk, e.g. {self.listed_but_absent[:3]}"
            )
        if self.output_path:
            lines.append(f"  wrote {self.output_path}")
        return "\n".join(lines)


def list_files(files_dir: Path, extensions: list[str] | None = None) -> list[str]:
    """Enumerate depositable files, sorted, ignoring dotfiles.

    Sorting matters: by_chunk grouping assigns buckets in row order, so a stable
    order keeps dataset membership reproducible across runs.
    """
    files_dir = Path(files_dir)
    if not files_dir.exists():
        raise FileNotFoundError(f"files_dir does not exist: {files_dir}")

    exts = {e.lower().lstrip(".") for e in (extensions or [])}
    names = [
        p.name
        for p in files_dir.iterdir()
        if p.is_file()
        and not p.name.startswith(".")
        and (not exts or p.suffix.lower().lstrip(".") in exts)
    ]
    return sorted(names)


def expand_url(value: str, template: str) -> str:
    """Expand a bare identifier into a full URL.

    A value that is already a URL is returned unchanged, so a column holding a
    mix of OCHRE UUIDs and full links resolves correctly either way.
    """
    value = str(value).strip()
    if value.lower().startswith(("http://", "https://")):
        return value
    return template.format(value=value)


def format_description(raw: str, cfg: BatchConfig) -> str:
    """Turn one or more raw links or identifiers into the HTML description format.

    Reproduces both variants already present in the collection:
        one link   ->  OCHRE link: <a href="URL">URL</a>
        many links ->  OCHRE link(s): <a ...>; <a ...>
    """
    spec = cfg.scaffold
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return ""

    urls = [
        expand_url(u, spec.link_url_template)
        for u in str(raw).split(spec.link_separator)
        if u.strip()
    ]
    if not urls:
        return ""

    if len(urls) == 1:
        return spec.description_template.format(url=urls[0])

    items = "; ".join(spec.link_item_template.format(url=u) for u in urls)
    return spec.multi_description_template.format(links=items)


def build(
    cfg: BatchConfig,
    partial: str | Path | None = None,
    write: bool = True,
) -> ScaffoldResult:
    """Assemble a complete metadata table for a batch.

    Parameters
    ----------
    cfg : BatchConfig
    partial : path, optional
        CSV holding at minimum a filename column and a raw link column.
        Defaults to `scaffold.partial` in the config. If absent entirely, the
        table is built from filenames alone and descriptions are left empty.
    write : bool
        Write the result to `scaffold.output`.

    Return
    ------
    ScaffoldResult
    """
    spec = cfg.scaffold

    names = list_files(cfg.files_dir, spec.file_extensions)
    table = pd.DataFrame({"filename": names})
    result = ScaffoldResult(table=table, n_files=len(names))

    # -- merge in whatever the partial CSV supplies -----------------------
    partial_path = Path(partial) if partial else cfg.scaffold_partial
    part = None
    if partial_path and Path(partial_path).exists():
        try:
            part = normalize(pd.read_csv(partial_path, low_memory=False), cfg.column_map)
        except pd.errors.EmptyDataError:
            part = None  # an empty hand-started CSV is a valid starting point

    if part is not None:
        if "filename" not in part.columns:
            raise KeyError(
                f"{Path(partial_path).name} has no 'filename' column after applying "
                f"column_map; found {list(part.columns)}"
            )
        if spec.link_column not in part.columns:
            raise KeyError(
                f"{Path(partial_path).name} has no {spec.link_column!r} column after "
                f"applying column_map; found {list(part.columns)}. Set "
                f"scaffold.link_column or add a column_map entry."
            )

        part["filename"] = part["filename"].astype(str).str.strip()
        # Collapse duplicate rows for one file into a multi-link description.
        collapsed = (
            part.groupby("filename")[spec.link_column]
            .apply(lambda s: spec.link_separator.join(
                str(v).strip() for v in s.dropna() if str(v).strip()
            ))
            .reset_index()
        )

        result.listed_but_absent = sorted(set(collapsed["filename"]) - set(names))
        table = table.merge(collapsed, on="filename", how="left")
    else:
        table[spec.link_column] = pd.NA

    # -- derived columns ---------------------------------------------------
    table["file_description"] = table[spec.link_column].map(
        lambda raw: format_description(raw, cfg)
    )
    result.without_links = table.loc[
        table["file_description"] == "", "filename"
    ].tolist()

    table["dataset_title"] = grouping.apply(table, cfg.grouping)

    template = spec.dataset_description_template
    if template:
        table["dataset_description"] = table["dataset_title"].map(
            lambda t: template.format(key=_key_of(t, cfg), title=t)
        )
    else:
        table["dataset_description"] = ""

    ordered = CANONICAL + [c for c in table.columns if c not in CANONICAL]
    table = table[ordered]

    result.table = table

    if write:
        out = cfg.scaffold_output
        out.parent.mkdir(parents=True, exist_ok=True)
        table.to_csv(out, index=False)
        result.output_path = out

    return result


def _key_of(title: str, cfg: BatchConfig) -> str:
    """Recover the grouping key from a formatted dataset title.

    Lets dataset_description_template reference {key} (e.g. the year) even when
    the title wraps it, as in "2019 Photographs".
    """
    tmpl = cfg.grouping.title_template
    if tmpl == "{key}":
        return title
    prefix, _, suffix = tmpl.partition("{key}")
    out = title
    if prefix and out.startswith(prefix):
        out = out[len(prefix):]
    if suffix and out.endswith(suffix):
        out = out[: -len(suffix)]
    return out
