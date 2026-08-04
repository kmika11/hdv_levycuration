# Leon Levy Expedition to Ashkelon — Dataverse Curation

Working scripts, metadata tables, and notes for depositing the Leon Levy Expedition to
Ashkelon archive (photographs, illustrations, fieldbooks, pottery registries and readings,
material culture registry, field reports, videos) into
[Harvard Dataverse](https://dataverse.harvard.edu).

Files are deposited in batches. Each batch is described by a **file-level metadata table**
(one row per file), grouped into datasets, deposited with
[easyDataverse](https://github.com/gdcc/easyDataverse) / [dvuploader](https://github.com/gdcc/python-dvuploader),
QA'd by hand in the Dataverse UI, then published.

> **Status:** this is a working curation repository, not a packaged tool. The pipeline lives
> mostly in one long notebook with hard-coded paths and per-batch edits. See
> [Toward a reusable pipeline](#toward-a-reusable-pipeline) for where this is headed.

---

## ⚠️ Before anything else: credentials

A live-looking Harvard Dataverse API token is currently **hard-coded in plaintext** in at
least these places:

- [upload.py:54](upload.py#L54)
- [052025LeonLevy_curationScript.ipynb](052025LeonLevy_curationScript.ipynb) (global variables cell, and again in the file-DOI section)
- [052025LeonLevy_curationScript.md](052025LeonLevy_curationScript.md) (the exported copy of the same)

**Action items:**

1. Rotate that token in Dataverse (account menu → API Token → Recreate).
2. Replace every occurrence with `os.environ["DATAVERSE_API_TOKEN"]`.
3. Add a `.gitignore` covering `.env`, `.DS_Store`, `__pycache__/`, `.ipynb_checkpoints/`,
   and the bulk data directories — and confirm nothing with a token has ever been committed
   (`git log -S 'X-Dataverse-key'`).

[file_dois.zsh](file_dois.zsh) already does the right thing: placeholder values you fill in
from your shell environment.

---

## Repository layout

### Pipeline code

| Path | What it is |
| --- | --- |
| [052025LeonLevy_curationScript.ipynb](052025LeonLevy_curationScript.ipynb) | **The main pipeline.** Latest end-to-end batch workflow: setup → metadata assembly → dataset creation → deposit → QA/troubleshooting → file DOIs. |
| [052025LeonLevy_curationScript.md](052025LeonLevy_curationScript.md) | Markdown export of the notebook (readable/greppable without Jupyter; regenerate with `jupyter nbconvert --to markdown`). |
| [curate.py](curate.py) | Reusable function module: `create_dataset_metadata`, `create_datafile_metadata`, `create_dataset`, `add_files_to_dataset`. The notebook currently re-defines these inline rather than importing them — they have drifted. |
| [curate_py.txt](curate_py.txt) | Byte-identical copy of `curate.py`. Safe to delete. |
| [upload.py](upload.py) | One-off script: bulk-upload files to a single existing dataset PID via dvuploader, no metadata creation. |
| [upload_simple.py](upload_simple.py) | Stub (one line). |
| [file_dois.zsh](file_dois.zsh) | curl + jq: dump `filename,persistentId,description` for one dataset to CSV. |
| [illustrations/check_files.zsh](illustrations/check_files.zsh) | Reconciles a metadata CSV against a directory of files; writes `missing_in_directory.csv` and `missing_in_csv.csv`. |
| [Untitled.ipynb](Untitled.ipynb) | Scratch notebook. |

### Batch working directories

| Path | Batch |
| --- | --- |
| [illustrations/](illustrations/) | Illustrations subcollection. Datasets are arbitrary buckets of 500 files ("Dataset 55"…"Dataset 60"). Includes the metadata master, missing-file and missing-metadata reports, and the corrected `missing_illustrations_FIXED.csv` used as the inventory. |
| [remaining_photos/](remaining_photos/) | Photo cleanup batch — files that failed earlier passes for three reasons: missing image, bad filename, missing/bad OCHRE link. Each has its own CSV; they get concatenated and de-duplicated on `filename`. |
| [summer2026/](summer2026/) | Summer 2026 batch: fieldbooks, field reports, pottery registry & readings, material culture registry, videos, 2019+ photos. Source `.xlsx` from Box plus derived `metadata_*.csv`. [summer2026/upload_pids.json](summer2026/upload_pids.json) maps dataset title → assigned DOI. |
| [legacy_data_and_scripts/](legacy_data_and_scripts/) | Everything from the 2012–2017 era of this project: the original notebooks (`messy_2012_script.ipynb`, `Publishing.ipynb`, `Troubleshooting.ipynb`), JSON metadata templates, and year-range masterlists. Reference only. |

### Outputs and reports (repo root)

| File | Contents |
| --- | --- |
| `files_to_add.csv` | Assembled inventory for a deposit run (`filename`, `file_description`, `dataset_title`, `dataset_description`, `status`). |
| `file_metadata.csv` | Output of `file_dois.zsh` for one dataset. |
| `illustrations_dois.csv`, `illustrations_dois_v2.csv` | Harvested `filename → file DOI` for the illustrations datasets. |
| `photos_dois_files.csv`, `photos_dois_datasets.csv` | Same for photographs, at file and dataset level. |
| `summer2026/file_dois.csv` | Richer export: `filename, file_doi, dataset_name, dataset_doi, direct_download_link`. |
| `missing_files.csv`, `missing_metadata.csv` | Reconciliation reports: rows with no file on disk, files on disk with no row. |
| `errors.md` | Captured tracebacks from failed deposits (see [Known failure modes](#known-failure-modes)). |
| `outline_2026.txt` | Batch checklist for Summer 2026 — source spreadsheet ↔ Box folder ↔ status, plus dataset DOIs. |

---

## The metadata contract

Everything flows from a **file-level table**: one row per file to deposit. Dataset-level
values are repeated on every row belonging to that dataset; grouping on `dataset_title`
splits the table into datasets.

Required columns:

| Column | Meaning |
| --- | --- |
| `filename` | Filename as it exists on disk, including extension (lowercase the extension before deposit). |
| `file_description` | Per-file description. In practice: the OCHRE permalink wrapped in an `<a href=…>` tag. |
| `dataset_title` | Dataset this file belongs to — the grouping key. |
| `dataset_description` | Dataset abstract; identical across all rows in a dataset. |
| `prod_date` | *(optional)* Production date, used as `productionDate`. `curate.py` requires it; the notebook has it commented out. |

Fields constant across the whole collection are set once at the top of the notebook rather
than in the table: authors + affiliations, contact name/email, subject, keywords, language,
funding agency, depositor, geographic coverage/unit, license.

Column naming is **not** consistent across batches yet — you will see `ochre_metadata`,
`metadata`, `dataset`, `Dataset`, `IDs `(with a trailing space) in the raw CSVs, renamed to
the canonical names partway through the notebook. Normalizing this is job one for the
rewrite.

---

## Environment

Developed against a conda env named `curation` on Python 3.12.

```bash
conda create -n curation python=3.12
conda activate curation
pip install pandas numpy easyDataverse pyDataverse dvuploader httpx requests rich openpyxl jupyter
```

`jq` is needed for `file_dois.zsh` (`brew install jq`).

There is no `requirements.txt` / `environment.yml` yet — adding one is on the roadmap.

---

## Running a batch

### 0. Setup

Set globals at the top of the notebook: `source_path`, `g_dataverse_inventory_file`
(the metadata CSV), `datafiles_path` (directory holding the actual files),
`dv_installation_url`, `dv_collection`, and the API token (→ move to env var).

**The SSL patch cell.** The first cell monkey-patches `httpx.Client`,
`httpx.AsyncClient`, `httpx.get`, and `requests.Session.request` to pass `verify=False`.
This exists to get around HUIT's bot-traffic rate limiter and **only works while connected
to the Harvard VPN**. It disables certificate verification process-wide — it is a workaround,
not a practice to carry forward. Any rewrite should make this an explicit, documented,
opt-in flag.

Note that `dv_installation_url` sometimes points at the staging host
(`https://dvn-cloud-app-2.lib.harvard.edu`) and sometimes at production
(`https://dataverse.harvard.edu`). Check which one you're on before depositing.

### 1. Assemble metadata

Read the batch CSV(s), concatenate, drop duplicates on `filename`, derive the grouping key,
rename columns to the canonical names, and wrap OCHRE links in `<a href>`.

Photo batches derive `dataset_title` from the year encoded in the filename prefix
(`A99_12624.jpg` → `1999`) via `convert_year` — a two-digit year ≤ 49 is 20xx, otherwise
19xx. Illustration batches instead chunk into arbitrary buckets of 500 files.

Then reconcile the table against the files on disk (`check_files.zsh`, or the in-notebook
equivalent) so you know before uploading which rows have no file and which files have no row.

### 2. Build dataset + datafile metadata, create datasets

Split the inventory on `dataset_title` into `g_dataset_inventories`, build a metadata dict
per dataset, instantiate an easyDataverse `Dataset` for each, and attach files with
descriptions. Verify the total attached file count matches the inventory row count before
depositing.

### 3. Deposit

Loop over datasets calling `dataset.upload(dataverse_name=dv_collection, n_parallel=2..3)`,
**sleeping ~5 minutes between datasets** to stay under the rate limiter. Record the returned
PIDs (this is what `summer2026/upload_pids.json` is).

The loop is written as `list(dv_dataset_info.items())[N:]` so you can resume from dataset N
after a failure — reset the slice to `[0:]` for a fresh run.

Enable file DOIs on the collection first if the deposited files need their own DOIs.

### 4. QA, then publish

Manually review drafts in the Dataverse UI. Common post-deposit fixes handled in the
notebook's troubleshooting section:

- **Re-detect file types** — `POST /api/files/{id}/redetect` for files ingested with the wrong MIME type.
- **Harvest file DOIs** — `GET /api/datasets/:persistentId/versions/:draft/files` and pull `dataFile.persistentId` into a CSV (also available as `file_dois.zsh`).
- **Add stragglers** to an existing dataset by PID once missing files/links are resolved.

Then publish.

---

## Known failure modes

- **`Failed to register files: … Bean Validation constraints were violated … prePersist for
  class FileMetadata`** (see [errors.md](errors.md)). Raised at the *registration* step after
  files have already uploaded, so a retry may double-upload. Usually traceable to a bad field
  in file metadata — most often an over-long or malformed `description`. Check the
  description strings for the batch, and check for embedded commas/newlines that survived CSV
  round-tripping.
- **Rate limiting / connection resets** from HUIT. Mitigations in use: VPN, `verify=False`
  patch, low `n_parallel`, 5-minute sleeps between datasets.
- **Partial deposits.** If a batch dies mid-loop, the dataset exists as a draft with some
  files. Resume via the list slice, and reconcile against the dataset's actual file list
  before re-uploading.
- **Filename ↔ metadata drift.** The single largest source of missing files. Filenames get
  corrected on disk without the CSV being updated (or vice versa) — hence the recurring
  `missing_files` / `missing_metadata` report pairs.

---

## Toward a reusable pipeline

The current workflow is coupled to this collection in ways that block reuse: hard-coded
absolute paths, a metadata schema that changes shape per batch, dataset-level fields frozen
as notebook globals, credentials in source, and the logic living in a notebook that is edited
in place for each run (so there's no record of what was actually run for a given batch).

A sketch of the target design:

1. **Config, not code.** One `config.yaml` per batch: installation URL, collection, inventory
   path, files path, and the collection-constant metadata (authors, contact, subject,
   keywords, funding, license, geo). Credentials from the environment only.
2. **A declared inventory schema.** Canonical column names with a validation step that runs
   *before* any network call — checks required columns, non-empty values, one
   `dataset_description` per `dataset_title`, and that every `filename` resolves on disk.
   Per-batch column renaming becomes a declared mapping in the config, not notebook edits.
3. **Pluggable grouping strategies.** `by_column`, `by_filename_regex` (the year extractor),
   `by_chunk(n)` (the 500-file buckets) — the three strategies already in use here, named and
   swappable.
4. **`curate.py` as the single source of truth.** Import it from the notebook instead of
   re-pasting the functions; drop `curate_py.txt`; add tests for the metadata builders using
   small fixture CSVs (no network needed).
5. **Idempotent, resumable deposits.** A run manifest (batch id, dataset title → PID, files
   uploaded, timestamp, status) written incrementally, so a resume is "diff manifest against
   the dataset's actual file list", not "edit a list slice". `upload_pids.json` is the
   ancestor of this.
6. **Reconciliation as a first-class command.** Generalize `check_files.zsh` into a Python
   step that emits the missing-files/missing-metadata pair for any inventory + directory.
7. **A thin CLI.** `validate`, `plan` (dry-run: datasets to create and their file counts),
   `deposit`, `harvest-dois`, `reconcile` — with the notebook reduced to exploration and QA.
8. **Separate data from code.** Bulk CSVs, `.xlsx`, and image directories move out of the
   repo (or behind `.gitignore` + a documented external location); the repo holds code,
   config, and small fixtures.

Nothing above changes what gets deposited — it changes how much of a batch is retyped versus
declared.
