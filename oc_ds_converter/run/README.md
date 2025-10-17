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
  `ORCIDManager`, `DOIManager`, `ISSNManager`, and `ISBNManager` encapsulate normalization and validation logic, using backends like SQLite, Redis, or in-memory storage.

* **Storage**
  Combines **persistent storage** (SQLite or Redis) with **temporary in-memory** storage per run/chunk. Temporary results are merged into persistent storage at safe flush points.

---

# B) Current ORCID usage (generic)

* ORCID identifiers are extracted from source metadata (`creators[*].nameIdentifiers` and `contributors[*].nameIdentifiers` for DataCite; `author[*].ORCID` and `editor[*].ORCID` for Crossref) and normalized to:
  `orcid:XXXX-XXXX-XXXX-XXXX`.

* Validation follows a fixed chain:
  local caches and persistent stores → DOI→ORCID index → (only if enabled) the ORCID API.

* The DOI→ORCID index can enrich author/editor strings when an ORCID is not explicitly provided but can be inferred from the DOI.

---

# C) Changes introduced (ORCID API toggle, RA/BR Redis fix, full Crossref alignment)

## 1) `datacite/datacite_processing.py` (`DataciteProcessing`)

* **Configurable ORCID API usage**

  * New parameter `use_orcid_api: bool = True` in `__init__`.
  * Propagated to `ORCIDManager` instances (`self.orcid_m`, `self.tmp_orcid_m`).
  * With `use_orcid_api=False`, no remote calls are made; only local storage, Redis, and the DOI→ORCID index are used.

* **Deterministic ORCID validation**

  * `find_datacite_orcid(...)` implements the ordered validation chain in Section D.
  * When confirmed via index/Redis, ORCIDs are immediately marked valid in the temporary storage.

* **Support methods**

  * `update_redis_values(...)`, `to_validated_id_list(...)`, `get_reids_validity_list(...)`, `memory_to_storage(...)`, `validated_as(...)` provide consistent handling of validity across caches and Redis.

## 2) `crossref/crossref_processing.py` (`CrossrefProcessing`)

* **Unified with DataCite**

  * Adds `use_orcid_api` and mirrors the same validation chain and storage behavior.

* **Deterministic ORCID validation**

  * `find_crossref_orcid(...)` checks, in order: temporary/persistent storage → DOI→ORCID index → RA Redis → optional API.

* **RA/BR Redis correction**

  * ORCIDs (responsible agents) are checked against **RA** Redis.
  * Bibliographic resources (DOIs) are checked against **BR** Redis.

* **Bug fix**

  * In the Redis pre-scan, the RA accumulator now correctly extends with `ent_all_ra` (no self-extension).

## 3) `run/datacite_process.py` and `run/crossref_process.py`

* **CLI toggle for ORCID API**

  * New flag `--no-orcid-api`, mapped to `use_orcid_api = not no_orcid_api`, passed to processing classes in both passes.

* **Cache lock management**

  * On completion, any cache file and its `.lock` are removed (or the fallback `cache.json` and `cache.json.lock`), preventing stale locks between runs.

## 4) Tests

* `test/datacite_process_test.py` and `test/crossref_processing_test.py` now include cases that:

  * Assert no ORCID leakage when the API is disabled and the DOI→ORCID index is missing.
  * Assert ORCID appearance only when confirmed via storage, Redis, or index.
  * Verify Crossref uses the same offline/online behavior as DataCite.

---

# D) Unified ORCID validation mechanism

1. **Normalize** to `orcid:XXXX-XXXX-XXXX-XXXX`.
2. **Local lookups** via `validated_as(...)`:

   * Temporary storage → persistent storage.
   * If valid → accept; if explicitly invalid → skip.
3. **DOI→ORCID index** (when DOI available):

   * If present → accept and mark valid in temporary storage.
4. **Redis snapshot (RA)**:

   * If present → accept and mark valid in temporary storage.
5. **Fallback (only if API enabled)**:

   * `to_validated_id_list(...)` rechecks Redis and, if allowed, calls `is_valid(...)` on the manager.
6. **Result**:

   * If none confirm validity → return `""` (no ORCID attached).

---

# E) Behavioral summary

| Mode    | Local Cache | DOI→ORCID Index | Redis RA | API/Network | ORCID Returned                     |
| ------- | ----------- | --------------- | -------- | ----------- | ---------------------------------- |
| API OFF | Yes         | Yes             | Yes      | No          | Only if found in cache/index/Redis |
| API ON  | Yes         | Yes             | Yes      | Yes         | Full validation allowed            |

---

# F) Component interaction (process → processing → ID managers)

* **Process layer** sets policy (API on/off, storage backend, Redis usage) and orchestrates the two-pass pipeline.
* **Processing layer** applies source-specific mapping while calling ID managers for normalization/validation in accordance with policy.
* **ID managers** provide consistent normalization/validation across ingestion modules.

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
  -cf test/crossref_processing/tar_gz_cited_test/3.json.tar.gz \
  -out test/crossref_processing/output_dir \
  -ca test/crossref_processing/cache.json \
  --no-orcid-api
```

Both processes clean up the cache file and any `.lock` to ensure a clean state for subsequent runs.
