# A) Overall architecture (to be replicated for the others)

* **Process layer**: `run/datacite_process.py`

  * Orchestrates input discovery, chunking, two-pass processing (subjects first, then objects + citations), and I/O (CSV output, cache file, lock handling).
* **Processing layer**: `datacite/datacite_processing.py` (`DataciteProcessing`)

  * Translates DataCite records into CSV rows, extracts/normalizes identifiers (DOI/ORCID/ISSN/ISBN), formats authors/editors, and queries auxiliary sources (publisher mapping, DOI -> ORCID index).
* **ID managers**: `oc_idmanager/*`

  * `ORCIDManager`, `DOIManager`, etc., encapsulate normalization and validity checks, backed by storage managers (SQLite / Redis / in-memory).
* **Storage**:

  * **Persistent** (SQLite or Redis) plus **temporary in-memory** storage per run; temporary results are flushed into persistent storage at safe points.

# B) Current ORCID usage (generic)

* ORCID values are read from DataCite metadata (`creators[*].nameIdentifiers`, `contributors[*].nameIdentifiers`) and turned into canonical `orcid:XXXX-XXXX-XXXX-XXXX`.
* Validity is determined by consulting local caches/stores first, then (optionally) using the DOI -> ORCID index, and finally (only if allowed) the ORCID API via `ORCIDManager`.
* The DOI -> ORCID index is also used to **enrich** author/editor strings when an ORCID is missing, based on the record’s DOI.

# C) What was changed (ORCID - API not active parameter + bug fix ra / br redis)

## 1) `datacite/datacite_processing.py` (class `DataciteProcessing`)

* **Configurable ORCID API usage**

  * `__init__` now accepts `use_orcid_api: bool` and passes it to both

    * `self.orcid_m = ORCIDManager(use_api_service=use_orcid_api, ...)`
    * `self.tmp_orcid_m = ORCIDManager(use_api_service=use_orcid_api, ...)`
  * Effect: when `use_orcid_api=False`, `ORCIDManager` does **not** call the remote ORCID API; only caches, storage, and (if provided) the DOI -> ORCID index are used.

* **ORCID extraction/validation path hardened**

  * `find_datacite_orcid(all_author_ids, doi=None)`:

    * Normalizes each candidate ORCID.
    * Checks temporary and persistent stores (via `validated_as(...)`) **before** any further work.
    * If a DOI is provided, can use the DOI -> ORCID index (`orcid_finder(doi)`) to confirm the candidate without contacting the API. When confirmed, the temporary store is marked valid.
    * Otherwise falls back to `to_validated_id_list(...)`, which:

      * Consults Redis-derived validity snapshots gathered for the current chunk.
      * Uses the corresponding `ORCIDManager.is_valid(...)` in a way that respects `use_orcid_api` (no network if disabled).

* **Support methods to minimize revalidation**

  * `update_redis_values(...)`, `to_validated_id_list(...)`, `memory_to_storage(...)`, `extract_all_ids(...)`, `get_reids_validity_list(...)`, and `validated_as(...)` were integrated to:

    * Pull known-valid IDs from Redis (when available),
    * Stage validity in a fast, temporary store for the current file/chunk,
    * Flush staged validity to persistent storage once a chunk is finished.

> Outcome: when the ORCID API is disabled, no remote lookup is attempted, and ORCIDs only appear if already known valid via local storage, Redis, or the DOI -> ORCID index.

## 2) `run/datacite_process.py`

* **CLI toggle for ORCID API**

  * Added `--no-orcid-api` flag; mapped to `use_orcid_api = not no_orcid_api`.
  * Passed `use_orcid_api` to every `DataciteProcessing` instance (both the first and second pass).

* **Cache lock guard**

  * After processing, the code now **removes** the cache lock file `cache.json.lock` (or the fallback lock) to avoid stale locks:

    * If `cache` is provided: delete `cache` and `cache + ".lock"` when done.
    * If `cache` is not provided: delete fallback `./cache.json` and `./cache.json.lock`.

* **Minor correctness**

  * The helper that collects IDs from Redis ensures responsible-agent IDs (ORCIDs) are fetched from the RA Redis and not the BR Redis.

## 3) `test/datacite_process_test.py`

* **New tests to freeze the behavior when ORCID API is off**

  * `test_preprocess_orcid_api_disabled_no_index`: with API disabled and **no** DOI -> ORCID index, **no** `[orcid:...]` must appear in `_subject.csv`.
  * `test_preprocess_orcid_api_disabled_no_leak`: with API disabled and the **sample** DOI -> ORCID index that does **not** cover the sample DOIs, again **no** `[orcid:...]` must appear in `_subject.csv`.
* These tests ensure there is no “leak” of ORCIDs coming from remote validation when the API is explicitly switched off.

# D) How this fits together (process -> processing -> id managers)

* The **process layer** (`preprocess`) orchestrates the two-pass run and supplies policy knobs: storage back-end, Redis on/off, and now “ORCID API on/off.”
* The **processing layer** (DataciteProcessing) implements the domain logic for DataCite:

  * Uses the **ID managers** exclusively for normalization and validity checks,
  * Defers to local caches, Redis and the DOI -> ORCID index before considering any remote validation,
  * Obeys the `use_orcid_api` policy so that external calls can be centrally enabled/disabled.
* The **ID managers** remain the single point of truth for identifier normalization/validation. They are configured once and then reused by the processing logic without duplicating remote access code.

# E) Practical implications and reuse

* When `--no-orcid-api` is set:

  * ORCID resolution is **purely offline**: previously validated entries, Redis, and (optionally) DOI -> ORCID index.
  * If neither the stores nor the index can confirm an ORCID, it is omitted from author/editor strings.
* The same pattern can be replicated in other ingestion pipelines (e.g., Crossref):

  * Thread the `use_orcid_api` toggle down into their processing classes,
  * Ensure their ORCID resolution uses the same cache/index-first approach,
  * Reuse the tests that assert “no ORCID leakage” when the API is disabled.

This separation of concerns keeps the **policy** (whether network is allowed) at the process boundary, while the **mechanism** (how ORCIDs are found/validated) remains in the processing + ID-manager layers and can be shared across sources.

## sample call [--no-orcid-api] -> ORCID API CALL NOT ACTIVE

`python -m oc_ds_converter.run.datacite_process \
  -dc test/datacite_process/sample_dc \
  -out test/datacite_process/output_dir \
  -p test/datacite_processing/publishers.csv \
  -o test/datacite_processing/iod \
  -ca test/datacite_process/cache.json \
  -sp test/datacite_process/anydb.db \
  -t \
  --no-orcid-api`
