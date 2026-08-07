# Changelog

## v2.0.0 — Config-driven curation pipeline

Replaces the per-batch notebook workflow with an importable, testable package.
Batch specifics live in a YAML config, credentials in `.env`, and the code stays
fixed. A new collection is a new config file, not a new notebook.

Validated end to end: 129 GeoTIFFs deposited to
[demo](https://demo.dataverse.org) and to Harvard Dataverse
(`doi:10.7910/DVN/FPZLY4`) using this pipeline.

### Added

- **`levy_curate` package** — `config`, `scaffold`, `inventory`, `grouping`,
  `deposit`, `manifest`, `harvest`, `cli`.
- **Metadata scaffolding.** Supply the files plus a CSV of filenames and OCHRE
  identifiers; the complete metadata table is generated. Bare UUIDs expand to
  full persistent links, repeated filenames collapse into the multi-link
  description format, and dataset titles and abstracts are derived. Replaces
  building these tables by hand.
- **Pre-flight validation.** Required columns, empty values, duplicate
  filenames, inconsistent dataset descriptions, over-long or multi-line
  descriptions, and reconciliation against the files on disk — all before the
  first network call.
- **Three grouping strategies** — `by_column`, `by_filename_regex` (with
  two-digit year expansion), `by_chunk`.
- **Resumable manifests**, scoped per target installation. A re-run skips
  datasets already uploaded rather than duplicating them.
- **CLI** — `scaffold`, `validate`, `reconcile`, `plan`, `deposit`, `harvest`,
  `status`.
- **`configs/template.yaml`** — a commented, project-agnostic starting point,
  kept generic by a test.
- **48 tests**, no network required.
- **`environment.yml`** and **`.env.example`**.

### Changed

- **Credentials come from `.env`.** Every API token previously in source has
  been revoked. `Credentials` redacts its token in `repr()`.
- **Deposits default to the demo installation.** Reaching production takes an
  explicit `--target harvard` and a typed confirmation.
- **Machine settings moved to `.env`** — `insecure_ssl`, `n_parallel`,
  `sleep_between_datasets` describe a machine, not a batch, so committed configs
  stay portable.
- **Repository reorganized.** Pre-2025 work moved to
  `legacy_data_and_scripts/`; `.gitignore` excludes roughly 51 GB of deposit
  files and generated payloads.

### Fixed

- **Author ORCIDs** are carried through to Dataverse; previously they were
  silently dropped.
- **Geospatial field names resolve at runtime.** Harvard exposes
  `unit`/`add_coverage`, demo exposes `geographic_unit`/`add_geographic_coverage`;
  assuming either one crashed against the other.
- **SSL patch signature.** `requests.api.post` passes `method`/`url` by keyword,
  so a positional patch broke every deposit with `insecure_ssl` enabled.
- **False failure reports.** Dataverse can return a 500 or 504 at file
  registration *after* the files have landed. The pipeline now polls the API to
  see what actually exists before recording a failure, so a retry cannot
  double-upload. Ambiguous cases still fail loudly.

### Known issues

- The Harvard-only code path (SSL patch, rate-limit handling) cannot be
  exercised by the test suite; all three production bugs above surfaced there.
- Descriptions already deposited use five inconsistent OCHRE link formats, an
  artifact of hand-built tables. New deposits are consistent; normalizing the
  existing ~9,000 would need a separate metadata-update pass.
- The package name is project-specific. Something neutral would suit a
  general-purpose release better.

### Upgrading

The pre-rewrite workflow is preserved at tag **`v1.0-legacy`**:

```bash
git show v1.0-legacy:levy_upload_script.ipynb > old_script.ipynb
```
