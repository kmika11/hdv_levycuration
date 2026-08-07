"""Turn a validated inventory into Dataverse datasets and upload them.

This is the layer that talks to easyDataverse. Everything above it -- scaffold,
inventory, grouping -- is pure pandas and testable without a network.
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from .config import BatchConfig
from .inventory import split_by_dataset
from .manifest import Manifest


def disable_ssl_verification() -> None:
    """Patch httpx and requests to skip certificate verification.

    A workaround for HUIT's bot-traffic rate limiter, effective only on the
    Harvard VPN. It weakens transport security process-wide, so it is opt-in via
    `insecure_ssl: true` in the batch config and announces itself loudly.
    """
    import httpx
    import requests

    print("WARNING: SSL certificate verification is DISABLED for this process.")

    _get = httpx.get

    class InsecureClient(httpx.Client):
        def __init__(self, *a, **kw):
            kw["verify"] = False
            super().__init__(*a, **kw)

    class InsecureAsyncClient(httpx.AsyncClient):
        def __init__(self, *a, **kw):
            kw["verify"] = False
            super().__init__(*a, **kw)

    httpx.Client = InsecureClient
    httpx.AsyncClient = InsecureAsyncClient
    httpx.get = lambda *a, **kw: _get(*a, **{**kw, "verify": False})

    _request = requests.Session.request
    requests.Session.request = lambda self, m, u, **kw: _request(
        self, m, u, **{**kw, "verify": False}
    )


def build_dataset_metadata(rows: pd.DataFrame, cfg: BatchConfig) -> dict[str, Any]:
    """Assemble the metadata dict for one dataset.

    Dataset-level values come from the inventory (title, description); everything
    constant across the collection comes from the config's `constants` block.
    """
    if rows.empty:
        raise ValueError("cannot build metadata from an empty dataset inventory")

    c = cfg.constants
    first = rows.index[0]

    meta: dict[str, Any] = {
        "title": rows.at[first, "dataset_title"],
        "description": [{"dsDescriptionValue": rows.at[first, "dataset_description"]}],
        "author": [
            {
                "authorName": a["name"],
                "authorAffiliation": a.get("affiliation", ""),
                # An author may carry a persistent identifier. ORCID is assumed
                # when a bare `orcid:` key is given; `identifier_scheme` allows
                # ROR, ISNI, VIAF and the rest of Dataverse's vocabulary.
                "authorIdentifier": a.get("orcid") or a.get("identifier"),
                "authorIdentifierScheme": (
                    a.get("identifier_scheme", "ORCID") if (a.get("orcid") or a.get("identifier"))
                    else None
                ),
            }
            for a in c.get("authors", [])
        ],
        "contact": [
            {
                "datasetContactName": c.get("contact", {}).get("name", ""),
                "datasetContactEmail": c.get("contact", {}).get("email", ""),
            }
        ],
        "subject": [c.get("subject")] if c.get("subject") else [],
        "keywords": [
            {"keywordValue": k.strip()}
            for k in str(c.get("keywords", "")).split(";")
            if k.strip()
        ],
        "language": [c["language"]] if c.get("language") else [],
        "depositor": c.get("depositor"),
        "funding": c.get("funding"),
        "geographicCoverage": c.get("geo", {}).get("coverage"),
        "geographicUnit": c.get("geo", {}).get("unit"),
        "license": cfg.license,
    }

    if "prod_date" in rows.columns and pd.notna(rows.at[first, "prod_date"]):
        meta["productionDate"] = str(rows.at[first, "prod_date"])

    return meta


def populate_dataset(ds, meta: dict[str, Any]):
    """Copy a metadata dict onto an easyDataverse Dataset model."""
    ds.citation.title = meta["title"]

    for a in meta.get("author", []):
        kwargs = {"name": a["authorName"], "affiliation": a["authorAffiliation"]}
        if a.get("authorIdentifier"):
            kwargs["identifier"] = a["authorIdentifier"]
            kwargs["identifier_scheme"] = a.get("authorIdentifierScheme", "ORCID")
        ds.citation.add_author(**kwargs)

    for d in meta.get("description", []):
        ds.citation.add_ds_description(value=d["dsDescriptionValue"])

    for ct in meta.get("contact", []):
        ds.citation.add_dataset_contact(
            name=ct["datasetContactName"], email=ct["datasetContactEmail"]
        )

    if meta.get("subject"):
        ds.citation.subject = meta["subject"]

    for kw in meta.get("keywords", []):
        ds.citation.add_keyword(value=kw["keywordValue"])

    if meta.get("depositor"):
        ds.citation.depositor = meta["depositor"]
    if meta.get("language"):
        ds.citation.language = meta["language"]
    if meta.get("funding"):
        ds.citation.add_grant_number(agency=meta["funding"])
    if meta.get("productionDate"):
        ds.citation.production_date = str(meta["productionDate"])

    # Installations disagree on the geospatial field names: Harvard exposes
    # `unit` / `add_coverage`, demo.dataverse.org exposes `geographic_unit` /
    # `add_geographic_coverage`. Resolve at runtime rather than assuming either.
    if meta.get("geographicUnit"):
        _set_geo_unit(ds, meta["geographicUnit"])
    if meta.get("geographicCoverage"):
        _add_geo_coverage(ds, meta["geographicCoverage"])

    return ds


def _set_geo_unit(ds, value: str) -> None:
    """Set the geographic unit under whichever name this installation uses."""
    for attr in ("geographic_unit", "unit"):
        if not hasattr(ds.geospatial, attr):
            continue
        current = getattr(ds.geospatial, attr)
        if isinstance(current, list):
            current.append(value)
        else:
            setattr(ds.geospatial, attr, value)
        return
    raise AttributeError(
        "no geographic unit field on this installation's geospatial block; "
        f"available: {sorted(ds.geospatial.model_fields)}"
    )


def _add_geo_coverage(ds, value: str) -> None:
    """Add free-text geographic coverage under whichever method name exists."""
    for method in ("add_geographic_coverage", "add_coverage"):
        fn = getattr(ds.geospatial, method, None)
        if fn is not None:
            fn(other_geographic_coverage=value)
            return
    raise AttributeError(
        "no geographic coverage method on this installation's geospatial block"
    )


def attach_files(ds, rows: pd.DataFrame, files_dir: Path):
    """Add every file in a dataset's inventory, with its description."""
    for _, row in rows.iterrows():
        ds.add_file(
            local_path=os.path.join(str(files_dir), row["filename"]),
            description=row["file_description"],
        )
    return ds


def plan(df: pd.DataFrame, cfg: BatchConfig) -> str:
    """Describe what a deposit would do. No network calls, nothing created."""
    groups = split_by_dataset(df)
    creds = cfg.credentials
    lines = [
        "DRY RUN -- nothing will be created",
        f"target:     {creds.target} -> {creds.url}",
        f"collection: {cfg.collection}",
        f"license:    {cfg.license}",
        f"datasets:   {len(groups)}   files: {len(df)}",
        "",
    ]
    width = max((len(t) for t in groups), default=10)
    for title, rows in groups.items():
        lines.append(f"  {title:<{width}}  {len(rows):>6} files")
    return "\n".join(lines)


def deposit(
    df: pd.DataFrame,
    cfg: BatchConfig,
    dataverse=None,
    dry_run: bool = False,
    on_progress: Callable[[str], None] = print,
) -> Manifest:
    """Create and upload every dataset in the inventory, recording progress.

    Datasets already marked uploaded in the manifest are skipped, so re-running
    after a failure resumes rather than duplicating.
    """
    from easyDataverse import Dataverse

    if dry_run:
        on_progress(plan(df, cfg))
        return Manifest(cfg.manifest, cfg.name)

    if cfg.insecure_ssl:
        disable_ssl_verification()

    creds = cfg.credentials
    dataverse = dataverse or Dataverse(server_url=creds.url, api_token=creds.token)

    groups = split_by_dataset(df)
    man = Manifest(cfg.manifest, cfg.name)
    man.data["target"] = f"{creds.target}:{creds.url}"
    man.save()

    todo = man.pending(list(groups))
    skipped = len(groups) - len(todo)
    if skipped:
        on_progress(f"resuming: {skipped} dataset(s) already uploaded, {len(todo)} to go")

    for i, title in enumerate(todo):
        rows = groups[title]
        man.start(title, len(rows))
        on_progress(f"[{i + 1}/{len(todo)}] {title}: {len(rows)} files")

        try:
            ds = dataverse.create_dataset()
            populate_dataset(ds, build_dataset_metadata(rows, cfg))
            attach_files(ds, rows, cfg.files_dir)

            if len(ds.files) != len(rows):
                raise RuntimeError(
                    f"attached {len(ds.files)} files but inventory has {len(rows)}"
                )

            ds.license = dataverse.licenses[cfg.license]
            pid = ds.upload(dataverse_name=cfg.collection, n_parallel=cfg.n_parallel)
            man.succeeded(title, pid, len(rows))
            on_progress(f"    uploaded -> {pid}")

        except Exception as exc:  # recorded, then re-raised after the loop context
            man.failed(title, exc)
            on_progress(f"    FAILED: {type(exc).__name__}: {exc}")
            raise

        if i < len(todo) - 1 and cfg.sleep_between_datasets:
            on_progress(f"    waiting {cfg.sleep_between_datasets}s (rate limiter)")
            time.sleep(cfg.sleep_between_datasets)

    return man
