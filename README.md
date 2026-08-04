# Leon Levy Expedition to Ashkelon — Dataverse Curation

A config-driven pipeline for depositing file collections into
[Harvard Dataverse](https://dataverse.harvard.edu), built for the Leon Levy Expedition to
Ashkelon archive (photographs, illustrations, fieldbooks, pottery registries and readings,
material culture registry, field reports, videos, georeferenced imagery).

You supply the files and a CSV with one filename and one OCHRE link per row. The pipeline
builds the complete metadata table, validates it against the files on disk, creates the
datasets, uploads them with a resumable manifest, and harvests file DOIs afterward.

**Design principle:** what varies per batch lives in a YAML config; what varies per
installation lives in `.env`; the code stays fixed. A new collection should be a new config
file, not a new notebook.

---

## Quick start

```bash
conda env create -f environment.yml
conda activate curation

cp .env.example .env && chmod 600 .env    # then paste your API tokens

python -m levy_curate scaffold configs/geotiffs.yaml   # build metadata table
python -m levy_curate validate configs/geotiffs.yaml   # check before any network call
python -m levy_curate plan     configs/geotiffs.yaml   # dry run
python -m levy_curate deposit  configs/geotiffs.yaml   # create + upload
python -m levy_curate harvest  configs/geotiffs.yaml   # collect file DOIs
```

Or drive the same steps from [run_batch.ipynb](run_batch.ipynb), which keeps the QA and
troubleshooting cells alongside.

`DATAVERSE_TARGET` in `.env` selects the installation and **defaults to `demo`**, so an
untested run cannot write to production. Depositing to Harvard from the CLI additionally
requires typing the batch name to confirm.

---

## Commands

| Command | What it does |
| --- | --- |
| `scaffold` | Enumerate files on disk, merge the partial CSV, derive descriptions / titles / abstracts, write the complete metadata table. Exit 1 if any file still lacks a link. |
| `validate` | Every check possible without the network. Writes `<batch>_missing_files.csv` and `<batch>_missing_metadata.csv`. |
| `reconcile` | Alias for `validate` — the generalized form of `check_files.zsh`. |
| `plan` | Dry run: target, collection, datasets and file counts. Creates nothing. |
| `deposit` | Create and upload datasets, recording a resumable manifest. Refuses an invalid inventory. |
| `harvest` | Collect `filename → file DOI` for every dataset in the manifest. |
| `status` | Print the config summary and the manifest. |

---

## How scaffolding works

The only file you prepare by hand:

```csv
filename,metadata_link
plan_A19_001.tif,https://pi.lib.uchicago.edu/1001/org/ochre/aaa
plan_A19_002.tif,https://pi.lib.uchicago.edu/1001/org/ochre/bbb
plan_A19_003.tif,https://pi.lib.uchicago.edu/1001/org/ochre/ccc
plan_A19_003.tif,https://pi.lib.uchicago.edu/1001/org/ochre/ddd
```

`scaffold` produces the full canonical table from it:

| Column | Derived how |
| --- | --- |
| `filename` | Enumerated from `files_dir`, filtered by `file_extensions` |
| `file_description` | Raw link(s) wrapped in the project's HTML format |
| `dataset_title` | The configured grouping strategy |
| `dataset_description` | `dataset_description_template`, with `{key}` and `{title}` |

Three behaviours worth knowing:

- **Enumeration starts from disk, not from your CSV.** A file you forgot to list still gets a
  row — flagged with an empty description — rather than silently vanishing from the deposit.
  This is the failure mode behind every `missing_files` report in this repo's history.
- **Repeated filenames collapse into one row.** Two rows for `plan_A19_003.tif` become a
  single row using the `OCHRE link(s): <a…>; <a…>` format.
- **Rows naming a file that isn't on disk are reported**, not silently carried forward.

Column names vary across your source spreadsheets (`ochre_metadata`, `metadata`, `dataset`,
`IDs ` with a trailing space). Map them in the config rather than editing spreadsheets:

```yaml
column_map:
  ochre_metadata: metadata_link
  dataset: dataset_title
```

Header whitespace and BOM markers are stripped unconditionally — both have caused silent
lookup failures here before.

### Grouping strategies

| Strategy | Use | Config |
| --- | --- | --- |
| `by_column` | `dataset_title` already present | `column: dataset` |
| `by_filename_regex` | Key encoded in the filename | `pattern: '^[A-Za-z](\d{2})_'` |
| `by_chunk` | Fixed-size buckets | `size: 500` |

A two-digit capture group is expanded to a four-digit year (`A99_` → 1999, `A12_` → 2012)
using a configurable pivot, matching how the photograph batches were organised.

---

## The metadata contract

One row per file. Dataset-level values repeat across a dataset's rows.

| Column | Meaning |
| --- | --- |
| `filename` | As on disk, including extension |
| `file_description` | Per-file description (the formatted OCHRE link) |
| `dataset_title` | The grouping key |
| `dataset_description` | Dataset abstract, identical within a dataset |
| `prod_date` | *(optional)* Production date |

Collection-constant fields — authors, contact, subject, keywords, language, funding,
depositor, geographic coverage — live in the config's `constants` block.

### What validation catches

Everything below is caught **before** the first network call:

- Missing or misnamed required columns
- Empty values in any required column
- Duplicate filenames
- `dataset_description` differing within a single dataset
- `file_description` over 1000 chars or containing newlines — the
  `Bean Validation constraints were violated` failure recorded in [errors.md](errors.md),
  which otherwise surfaces only *after* files have uploaded
- Rows whose file is absent from disk; files on disk absent from the table
- Uppercase file extensions (warning)

---

## Package layout

```
levy_curate/
  config.py      Batch YAML + credentials from .env (token redacted in repr)
  scaffold.py    files on disk + partial CSV -> complete metadata table
  inventory.py   canonical schema, column mapping, validation, reconciliation
  grouping.py    by_column | by_filename_regex | by_chunk
  deposit.py     dataset metadata, easyDataverse upload, dry-run planning
  manifest.py    append-only run log; resume support
  harvest.py     file DOIs, MIME re-detection
  cli.py         command line entry point
configs/         one YAML per batch
tests/           26 tests, no network required
run_batch.ipynb  thin driver + QA surface
```

Everything above `deposit.py` is pure pandas and tested without a network.

```bash
python -m pytest tests/ -q
```

---

## Resuming a failed deposit

The manifest is written *during* the loop, not after. If a batch dies partway, re-run
`deposit` — datasets already marked uploaded are skipped:

```
resuming: 4 dataset(s) already uploaded, 2 to go
```

`manifest.pids()` returns `{dataset_title: pid}`, the successor to `upload_pids.json`.

---

## Known failure modes

- **Bean Validation error on file registration.** Raised *after* files upload, so a naive
  retry can double-upload. Usually a malformed `file_description` — now caught by `validate`.
- **HUIT rate limiting.** Mitigations: Harvard VPN, low `n_parallel`, `sleep_between_datasets`
  (default 300s). `insecure_ssl: true` disables TLS verification process-wide as a last
  resort; it announces itself loudly and is off by default.
- **Wrong MIME types**, common with TIFFs and PDFs. Fix with `harvest.redetect(...)` rather
  than re-uploading. Always dry-run first.
- **Filename ↔ metadata drift.** Historically the largest source of missing files; now caught
  at `validate` time instead of at upload time.

---

## A data-quality note

Auditing the existing batches turned up **five** different OCHRE link formats across roughly
9,000 already-deposited files:

| Count | Prefix |
| --- | --- |
| 7,466 | `OCHRE link: ` |
| 686 | `OCHRE link(s): ` |
| 428 | `OCHRE Link: ` |
| 405 | *(none — videos have no prefix)* |
| 37 | `OCHRE Link:` |

An artifact of building these tables by hand. The scaffolder emits one consistent format, so
this stops accumulating; the already-deposited descriptions would need a separate metadata
update pass to normalize. The `metadata_pottery_all.csv` and `recent_photos.csv` formats are
reproduced exactly by the scaffolder (verified against all 7,466 rows), so the templates in
the config match established practice.

Also noted: `dataset_description` for the material culture registry reads
`"Matieral culture registry files…"` — a typo carried into the deposited metadata.

---

## Batch working directories

| Path | Batch |
| --- | --- |
| [illustrations/](illustrations/) | Illustrations, in buckets of 500 |
| [remaining_photos/](remaining_photos/) | Photo cleanup: missing images, bad filenames, bad links |
| [summer2026/](summer2026/) | Fieldbooks, field reports, pottery, material culture, videos, 2019+ photos |
| [legacy_data_and_scripts/](legacy_data_and_scripts/) | Pre-2025 notebooks and templates. Reference only |

Deposit files themselves live in Box / OneDrive and are gitignored. Metadata CSVs and
`upload_pids.json` are tracked as provenance.

The pre-rewrite state is tagged **`v1.0-legacy`** if you need the original notebooks:

```bash
git show v1.0-legacy:levy_upload_script.ipynb > old_script.ipynb
```

---

## Credentials

Never in code, never in the config. `.env` only, gitignored, `chmod 600`. See
[.env.example](.env.example).

All API tokens previously committed to this repository have been revoked. `Credentials`
redacts its token in `repr()` so it cannot surface in a notebook traceback.

---

## Legacy scripts

[052025LeonLevy_curationScript.ipynb](052025LeonLevy_curationScript.ipynb) is the original
end-to-end notebook, kept for reference and for troubleshooting cells not yet ported.
[curate.py](curate.py) is superseded by `levy_curate/deposit.py`.
[upload.py](upload.py) remains as a minimal single-dataset uploader.
