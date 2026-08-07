"""Batch configuration and credential loading.

Credentials come from .env (never from the config file, which is committed).
Everything else comes from a per-batch YAML file.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from dotenv import find_dotenv, load_dotenv

VALID_TARGETS = ("demo", "harvard")


@dataclass(frozen=True)
class Credentials:
    """A Dataverse installation URL and API token."""

    target: str
    url: str
    token: str

    def __repr__(self) -> str:
        # Redacted so a token can never surface in a notebook traceback or log.
        return (
            f"Credentials(target={self.target!r}, url={self.url!r}, "
            f"token=<redacted len={len(self.token)}>)"
        )


def load_credentials(target: str | None = None) -> Credentials:
    """Read Dataverse credentials from the environment.

    Parameters
    ----------
    target : str, optional
        "demo" or "harvard". Defaults to $DATAVERSE_TARGET, which itself
        defaults to "demo" so an untested run cannot reach production.

    Return
    ------
    Credentials
    """
    load_dotenv(find_dotenv(), override=True)
    target = target or os.environ.get("DATAVERSE_TARGET", "demo")

    if target not in VALID_TARGETS:
        raise ValueError(
            f"DATAVERSE_TARGET is {target!r}; expected one of {VALID_TARGETS}"
        )

    prefix = "DEMO_" if target == "demo" else ""
    url_var, token_var = f"{prefix}DATAVERSE_URL", f"{prefix}DATAVERSE_API_TOKEN"

    missing = [v for v in (url_var, token_var) if not os.environ.get(v)]
    if missing:
        raise KeyError(
            f"Missing in .env: {', '.join(missing)}. "
            f"Copy .env.example to .env and fill it in."
        )

    return Credentials(
        target=target,
        url=os.environ[url_var].rstrip("/"),
        token=os.environ[token_var],
    )


@dataclass
class GroupingSpec:
    """How files are partitioned into datasets. See grouping.py."""

    strategy: str
    column: str | None = None
    pattern: str | None = None
    two_digit_year_pivot: int = 49
    size: int = 500
    title_template: str = "{key}"


@dataclass
class ScaffoldSpec:
    """How a complete metadata table is built from a partial one."""

    partial: str | None = None
    link_column: str = "metadata_link"
    link_separator: str = ";"
    # Expands a bare identifier into a full URL, e.g.
    #   "https://pi.lib.uchicago.edu/1001/org/ochre/{value}"
    # Values that are already URLs are passed through untouched, so a column
    # mixing UUIDs and full links resolves correctly either way.
    link_url_template: str = "{value}"
    description_template: str = 'OCHRE link: <a href="{url}">{url}</a>'
    multi_description_template: str = 'OCHRE link(s): {links}'
    link_item_template: str = '<a href="{url}">{url}</a>'
    dataset_description_template: str = ""
    file_extensions: list[str] = field(default_factory=list)
    output: str = "metadata_complete.csv"


@dataclass
class BatchConfig:
    """Everything needed to scaffold, validate, and deposit one batch."""

    name: str
    collection: str
    files_dir: Path
    inventory: Path
    grouping: GroupingSpec
    scaffold: ScaffoldSpec
    constants: dict[str, Any]
    column_map: dict[str, str] = field(default_factory=dict)
    license: str = "CC BY-NC-ND 4.0"
    insecure_ssl: bool = False
    n_parallel: int = 2
    sleep_between_datasets: int = 300
    manifest: Path = Path("manifests/run.json")
    root: Path = Path(".")
    target: str | None = None

    # -- construction -------------------------------------------------

    @classmethod
    def from_yaml(cls, path: str | Path) -> "BatchConfig":
        """Load a batch config. Paths inside it resolve relative to the repo root."""
        path = Path(path).expanduser().resolve()
        with open(path) as fh:
            raw = yaml.safe_load(fh) or {}

        root = Path(raw.get("root", path.parent.parent)).expanduser().resolve()

        def _p(value: str | None, default: str | None = None) -> Path:
            v = value if value is not None else default
            p = Path(v).expanduser()
            return p if p.is_absolute() else (root / p)

        required = ("name", "collection", "files_dir")
        missing = [k for k in required if not raw.get(k)]
        if missing:
            raise ValueError(f"{path.name} is missing required key(s): {missing}")

        grouping = GroupingSpec(**(raw.get("grouping") or {"strategy": "by_column"}))
        scaffold = ScaffoldSpec(**(raw.get("scaffold") or {}))

        return cls(
            name=raw["name"],
            collection=raw["collection"],
            files_dir=_p(raw["files_dir"]),
            inventory=_p(raw.get("inventory"), scaffold.output),
            grouping=grouping,
            scaffold=scaffold,
            constants=raw.get("constants") or {},
            column_map=raw.get("column_map") or {},
            license=raw.get("license", "CC BY-NC-ND 4.0"),
            insecure_ssl=bool(raw.get("insecure_ssl", False)),
            n_parallel=int(raw.get("n_parallel", 2)),
            sleep_between_datasets=int(raw.get("sleep_between_datasets", 300)),
            manifest=_p(raw.get("manifest"), "manifests/run.json"),
            root=root,
            target=raw.get("target"),
        )

    # -- derived ------------------------------------------------------

    @property
    def credentials(self) -> Credentials:
        """Credentials for this batch's target installation."""
        return load_credentials(self.target)

    @property
    def scaffold_partial(self) -> Path | None:
        """Absolute path to the partial metadata CSV, if one is configured."""
        if not self.scaffold.partial:
            return None
        p = Path(self.scaffold.partial).expanduser()
        return p if p.is_absolute() else (self.root / p)

    @property
    def scaffold_output(self) -> Path:
        """Absolute path the scaffolder writes its complete metadata table to."""
        p = Path(self.scaffold.output).expanduser()
        return p if p.is_absolute() else (self.root / p)

    def summary(self) -> str:
        """A short, token-free description for printing before a run."""
        creds = self.credentials
        return (
            f"batch:      {self.name}\n"
            f"target:     {creds.target} -> {creds.url}\n"
            f"collection: {self.collection}\n"
            f"files_dir:  {self.files_dir}\n"
            f"inventory:  {self.inventory}\n"
            f"grouping:   {self.grouping.strategy}\n"
            f"license:    {self.license}"
        )
