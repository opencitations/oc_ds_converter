# A) Overall architecture (replicated across DataCite & Crossref)

* **Process layer**

  * `run/datacite_process.py`
  * `run/crossref_process.py`
    Handles input discovery, chunking, two-pass processing (first subjects/citing entities, then objects/cited entities and citations), and manages I/O operations such as CSV output, cache management, and lock handling.

* **Processing layer**

  * `datacite/datacite_processing.py` (`DataciteProcessing`)
  * `crossref/crossref_processing.py` (`CrossrefProcessing`)
    Translates source metadata into structured CSV rows, extracts and validates identifiers (DOI, ORCID, ISSN, ISBN where applicable), formats authors and editors, and interacts with auxiliary data such as publisher mappings and the DOI→ORCID index.

* **ID managers** (`oc_idmanager/*`)
  Components such as `ORCIDManager`, `DOIManager`, `ISSNManager`, and `ISBNManager` encapsulate normalization and validation logic, using backends like SQLite, Redis, or in-memory storage.

* **Storage**
  Combines **persistent storage** (SQLite or Redis) with **temporary in-memory** storage for each processing run or chunk.
  Temporary results are merged into persistent storage once processing completes safely.

---

# B) Current ORCID usage (generic)

* ORCID identifiers are extracted from source metadata (`creators[*].nameIdentifiers` and `contributors[*].nameIdentifiers` for DataCite; `author[*].ORCID` and `editor[*].ORCID` for Crossref) and normalized into canonical form:
  `orcid:XXXX-XXXX-XXXX-XXXX`.

* Validation proceeds through a defined chain:
  local caches and persistent stores → DOI→ORCID index → (only if enabled) the remote ORCID API.

* The DOI→ORCID index may also enrich author/editor fields when an ORCID is not directly provided but can be inferred via the DOI.

---

# C) Changes introduced (ORCID API toggle, RA/BR Redis fix, unified Crossref behavior)

## 1) `datacite/datacite_processing.py` (`DataciteProcessing`)

* **Configurable ORCID API usage**

  * Added parameter `use_orcid_api: bool = True` in `__init__`.
  * Propagated to both:

    * `self.orcid_m = ORCIDManager(use_api_service=use_orcid_api, ...)`
    * `self.tmp_orcid_m = ORCIDManager(use_api_service=use_orcid_api, ...)`
  * When `use_orcid_api=False`, all remote ORCID API calls are disabled. Only local caches, Redis, and the DOI→ORCID index are consulted.

* **Deterministic ORCID validation**

  * `find_datacite_orcid(...)` implements the ordered validation chain detailed in Section D.
  * ORCIDs confirmed through the index or Redis are immediately marked valid in temporary storage.

* **Supporting methods**

  * `update_redis_values(...)`, `to_validated_id_list(...)`, `get_reids_validity_list(...)`, `memory_to_storage(...)`, and `validated_as(...)`
    provide consistent handling of ID validity across local and temporary storage and Redis.

---

## 2) `crossref/crossref_processing.py` (`CrossrefProcessing`)

* **Unified with DataCite**

  * Introduces the same `use_orcid_api` toggle and validation chain.
  * The ORCID validation procedure now exactly mirrors the one in `DataciteProcessing`.

* **Deterministic ORCID validation**

  * `find_crossref_orcid(...)` performs:

    1. Validation in temporary and persistent storage.
    2. Lookup in DOI→ORCID index (if DOI is available).
    3. Check in RA Redis.
    4. Optional network validation if API is enabled.

* **RA/BR Redis correction**

  * ORCIDs (responsible agents) are always verified using **RA Redis**.
  * Bibliographic resource identifiers (e.g., DOIs) are verified using **BR Redis**.

* **Result**

  * Crossref and DataCite now share a unified ORCID validation and caching mechanism.

---

## 3) `run/datacite_process.py` and `run/crossref_process.py`

* **CLI toggle for ORCID API**

  * Added `--no-orcid-api` option, mapping to `use_orcid_api = not no_orcid_api`.
  * The parameter is passed to all processing instances for both passes.

* **Cache lock management**

  * After completion, the process removes any cache lock files (`cache.json.lock` or fallback) to prevent stale locks.

---

## 4) `test/datacite_process_test.py` and `test/crossref_processing_test.py`

* Added test coverage ensuring that:

  * No ORCID values appear when the API is disabled and the index is empty.
  * ORCIDs appear only when confirmed via storage, Redis, or the DOI→ORCID index.
  * Crossref follows the same logic as DataCite.

---

# D) Unified ORCID validation mechanism

### Step 1 — Normalization

Each candidate ORCID is normalized to canonical form: `orcid:XXXX-XXXX-XXXX-XXXX`.

### Step 2 — Local cache and persistent storage

`validated_as(...)` checks:

1. Temporary storage (`tmp_orcid_m`)
2. Persistent storage (`orcid_m`)

   * If valid, the ORCID is accepted immediately.
   * If explicitly marked invalid, it is skipped.

### Step 3 — DOI→ORCID index

If a DOI is provided, `orcid_finder(doi)` is queried.
If the normalized ORCID appears in the index:

* It is accepted.
* It is marked valid in temporary storage.

### Step 4 — Redis snapshot

If the ORCID is listed in the RA Redis snapshot (`_redis_values_ra`):

* It is accepted.
* It is marked valid in temporary storage.

### Step 5 — Validation fallback (only if API is enabled)

If no confirmation was found:

* `to_validated_id_list(...)` is called, which:

  1. Checks Redis again for completeness.
  2. Calls `is_valid(...)` in the appropriate ID manager.

     * With `use_api_service=True`: may perform a remote validation.
     * With `use_api_service=False`: no network access is attempted.

If the check succeeds, the ORCID is accepted and stored as valid; otherwise, it is skipped.

### Step 6 — Final outcome

If none of the checks confirm validity, the method returns an empty string `""`, meaning no ORCID is added.

---

# E) Behavioral summary

| Mode    | Local Cache | DOI→ORCID Index | Redis RA | API/Network | ORCID Returned                     |
| ------- | ----------- | --------------- | -------- | ----------- | ---------------------------------- |
| API OFF | Yes         | Yes             | Yes      | No          | Only if found in cache/index/Redis |
| API ON  | Yes         | Yes             | Yes      | Yes         | Yes, full validation allowed       |

When the API is disabled, validation is strictly offline (cache, Redis, index).
When enabled, full validation including remote checks is performed.

---

# F) Component interaction (process → processing → ID managers)

* The **process layer** defines configuration and policy (e.g., API on/off, storage backend, Redis usage) and orchestrates the two-pass pipeline.
* The **processing layer** implements source-specific logic (DataCite, Crossref) and uses ID managers for normalization and validation, following process-layer policy.
* The **ID managers** (`DOIManager`, `ORCIDManager`, etc.) provide consistent normalization and validation across all ingestion modules.

---

# G) Truth table (expected behavior)

| Scenario                        | API | In local cache | In DOI→ORCID index | In Redis RA | ORCID appears           |
| ------------------------------- | --- | -------------- | ------------------ | ----------- | ----------------------- |
| None present                    | OFF | No             | No                 | No          | No                      |
| Present only in DOI→ORCID index | OFF | No             | Yes                | –           | Yes                     |
| Present only in Redis           | OFF | No             | No                 | Yes         | Yes                     |
| Present only in cache           | OFF | Yes            | –                  | –           | Yes                     |
| Not found anywhere              | ON  | No             | No                 | No          | Depends on `is_valid()` |
| Found in Redis or cache         | ON  | Yes/No         | –                  | Yes/No      | Yes                     |

---

# H) Example executions

**DataCite**

```bash
python -m oc_ds_converter.run.datacite_process \
  -dc test/datacite_process/sample_dc \
  -out test/datacite_process/output_dir \
  -p test/datacite_processing/publishers.csv \
  -o test/datacite_processing/iod \
  -ca test/datacite_process/cache.json \
  -sp test/datacite_process/anydb.db \
  -t \
  --no-orcid-api
```

**Crossref**

```bash
python -m oc_ds_converter.run.crossref_process \
  -cr test/crossref_process/sample_cr \
  -out test/crossref_process/output_dir \
  -ca test/crossref_process/cache.json \
  --no-orcid-api
```

After execution, both processes remove any remaining cache lock files (`cache.json.lock` or fallback) to ensure clean termination.
