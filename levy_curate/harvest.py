"""Post-deposit operations: harvest file DOIs, re-detect file types.

Generalizes the notebook's troubleshooting section and file_dois.zsh so the same
code works for any dataset on any installation.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import requests

from .config import BatchConfig, Credentials, load_credentials


def _headers(creds: Credentials) -> dict[str, str]:
    return {"X-Dataverse-key": creds.token}


def dataset_files(pid: str, creds: Credentials, version: str = ":draft") -> list[dict]:
    """Fetch the file records for one dataset.

    Published datasets default to "latestVersion"; drafts need ":draft"
    explicitly -- a distinction that has cost time on this project before.
    """
    url = f"{creds.url}/api/datasets/:persistentId/versions/{version}/files"
    resp = requests.get(url, headers=_headers(creds), params={"persistentId": pid})
    resp.raise_for_status()
    payload = resp.json()
    if payload.get("status") != "OK":
        raise RuntimeError(f"{pid}: {payload.get('message', payload)}")
    return payload["data"]


def file_dois(
    pids: dict[str, str] | list[str],
    creds: Credentials | None = None,
    version: str = ":draft",
    out: str | Path | None = None,
) -> pd.DataFrame:
    """Harvest filename -> file DOI for one or many datasets.

    Parameters
    ----------
    pids : dict or list
        {dataset_title: pid} (e.g. Manifest.pids()) or a bare list of pids.

    Return
    ------
    DataFrame with filename, file_doi, dataset_name, dataset_doi,
    direct_download_link -- matching summer2026/file_dois.csv.
    """
    creds = creds or load_credentials()
    if isinstance(pids, (list, tuple, set)):
        pids = {p: p for p in pids}

    rows = []
    for name, pid in pids.items():
        for rec in dataset_files(pid, creds, version):
            data = rec.get("dataFile", {})
            doi = data.get("persistentId", "")
            rows.append(
                {
                    "filename": data.get("filename"),
                    "file_doi": doi,
                    "dataset_name": name,
                    "dataset_doi": pid,
                    "direct_download_link": (
                        f"{creds.url}/api/access/datafile/:persistentId?persistentId={doi}"
                        if doi
                        else ""
                    ),
                }
            )

    df = pd.DataFrame(rows)
    if out:
        Path(out).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out, index=False)
    return df


def redetect(pid: str, creds: Credentials | None = None, dry_run: bool = True) -> pd.DataFrame:
    """Ask Dataverse to re-identify the MIME type of every file in a dataset.

    Files ingested with the wrong type (a recurring problem with TIFFs and PDFs)
    are fixed by this rather than by re-uploading.
    """
    creds = creds or load_credentials()
    records = dataset_files(pid, creds)

    rows = []
    for rec in records:
        data = rec.get("dataFile", {})
        fid, name = data.get("id"), data.get("filename")
        before = data.get("contentType")

        if dry_run:
            rows.append({"file_id": fid, "filename": name, "before": before, "after": None})
            continue

        resp = requests.post(
            f"{creds.url}/api/files/{fid}/redetect",
            headers=_headers(creds),
            params={"dryRun": "false"},
        )
        after = None
        if resp.ok:
            after = resp.json().get("data", {}).get("contentType")
        rows.append(
            {"file_id": fid, "filename": name, "before": before, "after": after}
        )

    df = pd.DataFrame(rows)
    if not dry_run and not df.empty:
        changed = df[df["before"] != df["after"]]
        print(f"redetect: {len(changed)} of {len(df)} file(s) changed type")
    return df


def harvest_batch(cfg: BatchConfig, version: str = ":draft") -> pd.DataFrame:
    """Harvest DOIs for every dataset recorded in this batch's manifest."""
    from .manifest import Manifest

    man = Manifest(cfg.manifest, cfg.name)
    pids = man.pids()
    if not pids:
        raise RuntimeError(f"no uploaded datasets recorded in {cfg.manifest}")
    out = cfg.root / f"{cfg.name}_file_dois.csv"
    return file_dois(pids, cfg.credentials, version=version, out=out)
