"""Tests for the pure-pandas layers: scaffold, grouping, inventory, manifest.

Nothing here touches the network. Deposit is exercised via plan() and a fake
Dataverse client so the orchestration logic is covered without an installation.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
import yaml

from levy_curate import grouping, inventory, scaffold
from levy_curate.config import BatchConfig, GroupingSpec
from levy_curate.manifest import Manifest

OCHRE = "https://pi.lib.uchicago.edu/1001/org/ochre"


# ---------------------------------------------------------------- fixtures


def make_batch(tmp_path: Path, *, filenames, partial_rows=None, **overrides) -> BatchConfig:
    """Build a config plus a fake files_dir on disk."""
    files_dir = tmp_path / "files"
    files_dir.mkdir()
    for name in filenames:
        (files_dir / name).write_bytes(b"x")

    cfg_dir = tmp_path / "configs"
    cfg_dir.mkdir()

    raw = {
        "name": "testbatch",
        "collection": "testcollection",
        "files_dir": "files",
        "inventory": "metadata.csv",
        "scaffold": {
            "link_column": "metadata_link",
            "output": "metadata.csv",
            "dataset_description_template": "Files from {key}.",
        },
        "grouping": {"strategy": "by_chunk", "size": 100, "title_template": "Dataset {key}"},
        "constants": {
            "authors": [{"name": "Master, Daniel M.", "affiliation": "Wheaton College"}],
            "contact": {"name": "Master, Daniel M.", "email": "d@example.edu"},
            "subject": "Arts and Humanities",
            "keywords": "Archaeology;Ashkelon",
            "language": "English",
            "depositor": "Mika, Katherine",
            "funding": "The Leon Levy Foundation",
            "geo": {"coverage": "Ashkelon", "unit": "City"},
        },
        "root": str(tmp_path),
    }
    for k, v in overrides.items():
        if isinstance(v, dict) and isinstance(raw.get(k), dict):
            raw[k].update(v)
        else:
            raw[k] = v

    if partial_rows is not None:
        pd.DataFrame(partial_rows).to_csv(tmp_path / "partial.csv", index=False)
        raw["scaffold"]["partial"] = "partial.csv"

    path = cfg_dir / "batch.yaml"
    path.write_text(yaml.safe_dump(raw))
    return BatchConfig.from_yaml(path)


@pytest.fixture(autouse=True)
def _fake_credentials(monkeypatch):
    monkeypatch.setenv("DATAVERSE_TARGET", "demo")
    monkeypatch.setenv("DEMO_DATAVERSE_URL", "https://demo.dataverse.org")
    monkeypatch.setenv("DEMO_DATAVERSE_API_TOKEN", "0" * 36)


# ---------------------------------------------------------------- scaffold


def test_scaffold_formats_single_link(tmp_path):
    cfg = make_batch(
        tmp_path,
        filenames=["a.tif"],
        partial_rows=[{"filename": "a.tif", "metadata_link": f"{OCHRE}/abc"}],
    )
    result = scaffold.build(cfg)
    desc = result.table.loc[0, "file_description"]
    assert desc == f'OCHRE link: <a href="{OCHRE}/abc">{OCHRE}/abc</a>'
    assert result.complete


def test_scaffold_collapses_repeated_filenames_into_multi_link(tmp_path):
    """Two rows for one file become one row with the 'OCHRE link(s)' format."""
    cfg = make_batch(
        tmp_path,
        filenames=["a.tif"],
        partial_rows=[
            {"filename": "a.tif", "metadata_link": f"{OCHRE}/one"},
            {"filename": "a.tif", "metadata_link": f"{OCHRE}/two"},
        ],
    )
    result = scaffold.build(cfg)
    assert len(result.table) == 1
    desc = result.table.loc[0, "file_description"]
    assert desc.startswith("OCHRE link(s): ")
    assert desc.count("<a href=") == 2
    assert "; " in desc


def test_scaffold_starts_from_disk_not_the_csv(tmp_path):
    """A file on disk missing from the partial CSV still gets a row, flagged."""
    cfg = make_batch(
        tmp_path,
        filenames=["a.tif", "b.tif"],
        partial_rows=[{"filename": "a.tif", "metadata_link": f"{OCHRE}/abc"}],
    )
    result = scaffold.build(cfg)
    assert set(result.table["filename"]) == {"a.tif", "b.tif"}
    assert result.without_links == ["b.tif"]
    assert not result.complete


def test_scaffold_reports_rows_with_no_file(tmp_path):
    cfg = make_batch(
        tmp_path,
        filenames=["a.tif"],
        partial_rows=[
            {"filename": "a.tif", "metadata_link": f"{OCHRE}/abc"},
            {"filename": "ghost.tif", "metadata_link": f"{OCHRE}/xyz"},
        ],
    )
    result = scaffold.build(cfg)
    assert result.listed_but_absent == ["ghost.tif"]


def test_scaffold_filters_by_extension(tmp_path):
    cfg = make_batch(
        tmp_path,
        filenames=["a.tif", "notes.txt", "b.TIF"],
        partial_rows=[],
        scaffold={"file_extensions": ["tif"]},
    )
    result = scaffold.build(cfg)
    assert sorted(result.table["filename"]) == ["a.tif", "b.TIF"]


def test_scaffold_applies_column_map(tmp_path):
    cfg = make_batch(
        tmp_path,
        filenames=["a.tif"],
        partial_rows=[{"filename": "a.tif", "ochre_metadata": f"{OCHRE}/abc"}],
        column_map={"ochre_metadata": "metadata_link"},
    )
    result = scaffold.build(cfg)
    assert "<a href=" in result.table.loc[0, "file_description"]


def test_scaffold_writes_canonical_columns_first(tmp_path):
    cfg = make_batch(
        tmp_path,
        filenames=["a.tif"],
        partial_rows=[{"filename": "a.tif", "metadata_link": f"{OCHRE}/abc"}],
    )
    result = scaffold.build(cfg)
    assert list(result.table.columns)[:4] == inventory.CANONICAL
    assert result.output_path.exists()


def test_scaffold_expands_bare_uuids_into_urls(tmp_path):
    """The GeoTIFF batch supplies OCHRE UUIDs, not links."""
    uuid = "5e664bee-2311-4ce1-a997-ae0e31188c81"
    cfg = make_batch(
        tmp_path,
        filenames=["a.tif"],
        partial_rows=[{"filename": "a.tif", "metadata_link": uuid}],
        scaffold={"link_url_template": f"{OCHRE}/{{value}}"},
    )
    result = scaffold.build(cfg)
    assert result.table.loc[0, "file_description"] == (
        f'OCHRE link: <a href="{OCHRE}/{uuid}">{OCHRE}/{uuid}</a>'
    )


def test_scaffold_leaves_full_urls_untouched(tmp_path):
    """A column mixing UUIDs and URLs must resolve correctly either way."""
    cfg = make_batch(
        tmp_path,
        filenames=["a.tif", "b.tif"],
        partial_rows=[
            {"filename": "a.tif", "metadata_link": "abc-123"},
            {"filename": "b.tif", "metadata_link": f"{OCHRE}/already-a-url"},
        ],
        scaffold={"link_url_template": f"{OCHRE}/{{value}}"},
    )
    table = scaffold.build(cfg).table.set_index("filename")
    assert f'href="{OCHRE}/abc-123"' in table.loc["a.tif", "file_description"]
    assert f'href="{OCHRE}/already-a-url"' in table.loc["b.tif", "file_description"]
    assert table.loc["b.tif", "file_description"].count(OCHRE) == 2  # not double-prefixed


def test_expand_url_is_case_insensitive_about_scheme():
    from levy_curate.scaffold import expand_url

    assert expand_url("HTTPS://example.org/x", "pre/{value}") == "HTTPS://example.org/x"
    assert expand_url("bare", "pre/{value}") == "pre/bare"


# ---------------------------------------------------------------- grouping


def test_by_filename_regex_expands_two_digit_years():
    df = pd.DataFrame({"filename": ["A99_1.jpg", "A12_2.jpg"]})
    spec = GroupingSpec(strategy="by_filename_regex", pattern=r"^[A-Za-z](\d{2})_",
                        title_template="{key} Photographs")
    assert list(grouping.apply(df, spec)) == ["1999 Photographs", "2012 Photographs"]


def test_by_filename_regex_reports_unmatched_filenames():
    df = pd.DataFrame({"filename": ["A99_1.jpg", "no-year.jpg"]})
    spec = GroupingSpec(strategy="by_filename_regex", pattern=r"^[A-Za-z](\d{2})_")
    with pytest.raises(ValueError, match="did not match"):
        grouping.apply(df, spec)


def test_by_chunk_buckets_in_order():
    df = pd.DataFrame({"filename": [f"f{i}.tif" for i in range(5)]})
    spec = GroupingSpec(strategy="by_chunk", size=2, title_template="Dataset {key}")
    assert list(grouping.apply(df, spec)) == [
        "Dataset 1", "Dataset 1", "Dataset 2", "Dataset 2", "Dataset 3",
    ]


def test_by_column_rejects_missing_column():
    df = pd.DataFrame({"filename": ["a.tif"]})
    spec = GroupingSpec(strategy="by_column", column="dataset")
    with pytest.raises(KeyError):
        grouping.apply(df, spec)


def test_unknown_strategy_is_rejected():
    df = pd.DataFrame({"filename": ["a.tif"]})
    with pytest.raises(ValueError, match="Unknown grouping strategy"):
        grouping.apply(df, GroupingSpec(strategy="by_vibes"))


# --------------------------------------------------------------- inventory


def _valid_frame():
    return pd.DataFrame(
        {
            "filename": ["a.tif", "b.tif"],
            "file_description": ["d1", "d2"],
            "dataset_title": ["D1", "D1"],
            "dataset_description": ["desc", "desc"],
        }
    )


def test_validate_accepts_a_good_inventory(tmp_path):
    cfg = make_batch(tmp_path, filenames=["a.tif", "b.tif"])
    rep = inventory.validate(_valid_frame(), cfg)
    assert rep.ok, rep.report()
    assert rep.n_rows == 2 and rep.n_datasets == 1


def test_validate_catches_missing_files_on_disk(tmp_path):
    cfg = make_batch(tmp_path, filenames=["a.tif"])
    rep = inventory.validate(_valid_frame(), cfg)
    assert not rep.ok
    assert list(rep.missing_on_disk["filename"]) == ["b.tif"]


def test_validate_catches_inconsistent_dataset_description(tmp_path):
    cfg = make_batch(tmp_path, filenames=["a.tif", "b.tif"])
    df = _valid_frame()
    df.loc[1, "dataset_description"] = "different"
    rep = inventory.validate(df, cfg)
    assert any("dataset_description differs" in e for e in rep.errors)


def test_validate_catches_duplicate_filenames(tmp_path):
    cfg = make_batch(tmp_path, filenames=["a.tif"])
    df = _valid_frame()
    df.loc[1, "filename"] = "a.tif"
    rep = inventory.validate(df, cfg)
    assert any("duplicate filename" in e for e in rep.errors)


def test_validate_catches_overlong_and_multiline_descriptions(tmp_path):
    cfg = make_batch(tmp_path, filenames=["a.tif", "b.tif"])
    df = _valid_frame()
    df.loc[0, "file_description"] = "x" * 1001
    df.loc[1, "file_description"] = "has\nnewline"
    rep = inventory.validate(df, cfg)
    assert any("over 1000 chars" in e for e in rep.errors)
    assert any("newlines" in e for e in rep.errors)


def test_validate_reports_missing_columns_without_crashing(tmp_path):
    cfg = make_batch(tmp_path, filenames=["a.tif"])
    rep = inventory.validate(pd.DataFrame({"filename": ["a.tif"]}), cfg)
    assert not rep.ok
    assert "missing required column" in rep.errors[0]


def test_normalize_strips_header_whitespace_and_bom():
    df = pd.DataFrame({"﻿filename": ["a"], "IDs ": ["1"]})
    out = inventory.normalize(df, {})
    assert list(out.columns) == ["filename", "IDs"]


def test_reconcile_finds_files_absent_from_the_table(tmp_path):
    cfg = make_batch(tmp_path, filenames=["a.tif", "b.tif", "extra.tif"])
    missing_disk, missing_table = inventory.reconcile(_valid_frame(), cfg.files_dir)
    assert missing_disk.empty
    assert list(missing_table["filename"]) == ["extra.tif"]


# ---------------------------------------------------------------- manifest


def test_manifest_records_and_resumes(tmp_path):
    man = Manifest(tmp_path / "run.json", "testbatch")
    man.succeeded("D1", "doi:10.5072/ABC", 10)
    man.failed("D2", RuntimeError("boom"))

    assert man.pending(["D1", "D2", "D3"]) == ["D2", "D3"]
    assert man.pids() == {"D1": "doi:10.5072/ABC"}

    reloaded = Manifest(tmp_path / "run.json", "testbatch")
    assert reloaded.is_done("D1")
    assert reloaded.status_of("D2") == "failed"


def test_manifest_survives_partial_run(tmp_path):
    path = tmp_path / "run.json"
    man = Manifest(path, "b")
    man.start("D1", 5)
    assert json.loads(path.read_text())["datasets"]["D1"]["status"] == "in_progress"


def test_manifest_path_is_scoped_to_target(tmp_path):
    """A demo run and a production run must never share a manifest."""
    cfg = make_batch(tmp_path, filenames=["a.tif"], manifest="manifests/geotiffs.json")
    assert cfg.manifest_path().name == "geotiffs.demo.json"

    cfg.target = "harvard"  # what `--target harvard` sets
    assert cfg.manifest_path().name == "geotiffs.harvard.json"


def test_demo_run_cannot_mark_a_harvard_deposit_as_done(tmp_path):
    """The regression this guards: production silently skipping every dataset."""
    cfg = make_batch(tmp_path, filenames=["a.tif"], manifest="manifests/b.json")

    demo = Manifest(cfg.manifest_path(), cfg.name)
    demo.succeeded("D1", "doi:10.5072/DEMO", 1)

    cfg.target = "harvard"
    harvard = Manifest(cfg.manifest_path(), cfg.name)
    assert harvard.pending(["D1"]) == ["D1"], "production must still deposit D1"
    assert harvard.pids() == {}


def test_dotenv_wins_over_shell_environment(tmp_path, monkeypatch):
    """.env is authoritative, so a stale `export` cannot redirect a run.

    The dangerous direction is a leftover shell variable silently sending a
    deposit to production; making the file win removes that possibility.
    """
    from levy_curate.config import load_credentials

    monkeypatch.setenv("DATAVERSE_TARGET", "harvard")
    assert load_credentials().target == "demo"          # .env value, not the export
    assert load_credentials("harvard").target == "harvard"  # explicit arg still wins


# ----------------------------------------------------------------- deposit


def test_build_dataset_metadata_maps_constants(tmp_path):
    from levy_curate.deposit import build_dataset_metadata

    cfg = make_batch(tmp_path, filenames=["a.tif", "b.tif"])
    meta = build_dataset_metadata(_valid_frame(), cfg)

    assert meta["title"] == "D1"
    assert meta["author"][0]["authorName"] == "Master, Daniel M."
    assert meta["keywords"] == [{"keywordValue": "Archaeology"}, {"keywordValue": "Ashkelon"}]
    assert meta["geographicCoverage"] == "Ashkelon"
    assert meta["license"] == "CC BY-NC-ND 4.0"


def test_author_orcid_flows_into_metadata(tmp_path):
    from levy_curate.deposit import build_dataset_metadata

    cfg = make_batch(
        tmp_path,
        filenames=["a.tif", "b.tif"],
        constants={
            "authors": [
                {
                    "name": "Pierce, George",
                    "affiliation": "Brigham Young University",
                    "orcid": "0000-0002-8332-8495",
                }
            ]
        },
    )
    author = build_dataset_metadata(_valid_frame(), cfg)["author"][0]
    assert author["authorIdentifier"] == "0000-0002-8332-8495"
    assert author["authorIdentifierScheme"] == "ORCID"


def test_author_without_orcid_carries_no_identifier(tmp_path):
    from levy_curate.deposit import build_dataset_metadata

    cfg = make_batch(tmp_path, filenames=["a.tif", "b.tif"])
    author = build_dataset_metadata(_valid_frame(), cfg)["author"][0]
    assert author["authorIdentifier"] is None
    assert author["authorIdentifierScheme"] is None


def test_author_identifier_scheme_can_be_overridden(tmp_path):
    from levy_curate.deposit import build_dataset_metadata

    cfg = make_batch(
        tmp_path,
        filenames=["a.tif", "b.tif"],
        constants={
            "authors": [
                {"name": "X", "identifier": "https://ror.org/abc", "identifier_scheme": "ROR"}
            ]
        },
    )
    author = build_dataset_metadata(_valid_frame(), cfg)["author"][0]
    assert author["authorIdentifierScheme"] == "ROR"


class _FakeDataset:
    """Stands in for an easyDataverse Dataset that has been uploaded."""

    def __init__(self, pid):
        self.p_id = pid


def test_verify_upload_confirms_files_that_actually_landed(tmp_path, monkeypatch):
    """The errors.md case: a 500 raised after every file registered."""
    from levy_curate import deposit as dep

    monkeypatch.setattr(
        "levy_curate.harvest.dataset_files", lambda pid, creds, version=":draft": [{}] * 129
    )
    cfg = make_batch(tmp_path, filenames=["a.tif"])
    assert dep.verify_upload(_FakeDataset("doi:10.5072/X"), 129, cfg.credentials) == "doi:10.5072/X"


def test_verify_upload_rejects_a_partial_upload(tmp_path, monkeypatch):
    from levy_curate import deposit as dep

    monkeypatch.setattr(
        "levy_curate.harvest.dataset_files", lambda pid, creds, version=":draft": [{}] * 40
    )
    cfg = make_batch(tmp_path, filenames=["a.tif"])
    assert dep.verify_upload(_FakeDataset("doi:10.5072/X"), 129, cfg.credentials) is None


def test_verify_upload_is_conservative_when_it_cannot_check(tmp_path, monkeypatch):
    """No PID, or an unreachable API, must never be read as success."""
    from levy_curate import deposit as dep

    cfg = make_batch(tmp_path, filenames=["a.tif"])
    assert dep.verify_upload(_FakeDataset(None), 129, cfg.credentials) is None

    def boom(pid, creds, version=":draft"):
        raise ConnectionError("unreachable")

    monkeypatch.setattr("levy_curate.harvest.dataset_files", boom)
    assert dep.verify_upload(_FakeDataset("doi:10.5072/X"), 129, cfg.credentials) is None


def test_plan_is_read_only_and_lists_datasets(tmp_path):
    from levy_curate.deposit import plan

    cfg = make_batch(tmp_path, filenames=["a.tif", "b.tif"])
    text = plan(_valid_frame(), cfg)
    assert "DRY RUN" in text and "D1" in text and "2 files" in text


def test_credentials_never_repr_the_token(tmp_path):
    cfg = make_batch(tmp_path, filenames=["a.tif"])
    assert "0000" not in repr(cfg.credentials)
    assert "redacted" in repr(cfg.credentials)


# -------------------------------------------------------------- end to end


def test_scaffold_then_validate_round_trip(tmp_path):
    """The output of scaffold must pass validate with no hand editing."""
    names = [f"A19_{i}.tif" for i in range(5)]
    cfg = make_batch(
        tmp_path,
        filenames=names,
        partial_rows=[{"filename": n, "metadata_link": f"{OCHRE}/{n}"} for n in names],
        grouping={"strategy": "by_filename_regex", "pattern": r"^[A-Za-z](\d{2})_",
                  "title_template": "{key} Georeferenced Imagery"},
    )
    result = scaffold.build(cfg)
    assert result.complete

    df = inventory.load(cfg)
    rep = inventory.validate(df, cfg)
    assert rep.ok, rep.report()
    assert rep.n_datasets == 1
    assert df["dataset_title"].iloc[0] == "2019 Georeferenced Imagery"
    assert df["dataset_description"].iloc[0] == "Files from 2019."
