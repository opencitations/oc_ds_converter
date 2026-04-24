<!--
SPDX-License-Identifier: CC0-1.0
-->

# [2.0.0](https://github.com/opencitations/oc_ds_converter/compare/v1.1.0...v2.0.0) (2026-04-02)


* refactor!(jalc): remove publisher prefix mapping ([a64328e](https://github.com/opencitations/oc_ds_converter/commit/a64328e8a644852bec8474db1d34e106a48d631a))
* refactor(crossref)!: auto-generate publishers file from Crossref API ([dd6496a](https://github.com/opencitations/oc_ds_converter/commit/dd6496aae829ca48f01d308e12f4e31ed63f19eb))
* refactor(crossref)!: replace tqdm with Rich for progress display ([8d07567](https://github.com/opencitations/oc_ds_converter/commit/8d07567be3be3ba759d8b0c6c7d3d85a04faeeb5))
* refactor(storage)!: make Redis the only storage backend ([4df3775](https://github.com/opencitations/oc_ds_converter/commit/4df3775c1b197cf79858bd06998f6f2f15a93103))


### Bug Fixes

* **cache:** handle empty cache file in init_cache ([db92656](https://github.com/opencitations/oc_ds_converter/commit/db92656e7cfb8a314fd03ec797698c25a7f17b8c))
* **ci:** track .coveragerc so CI can find it ([c69e63c](https://github.com/opencitations/oc_ds_converter/commit/c69e63ce8dccef0aea3ebf8fb6a1e24ab67ee5a9))
* **ci:** use Python 3.12 for coverage badge generation ([13354eb](https://github.com/opencitations/oc_ds_converter/commit/13354eb5da48952501dbe4b62ce9404462541a49))
* clean up PROCESS-DB after preprocessing completes ([1119b72](https://github.com/opencitations/oc_ds_converter/commit/1119b72b1801e2f9dc09b3c56067a0dc3a959e83))
* **crossref:** skip citing entities without DOI references ([f2f16b8](https://github.com/opencitations/oc_ds_converter/commit/f2f16b889059ec1fb7ce867b0c2c51b04d356901))
* **datacite:** resolve test failures after PR [#12](https://github.com/opencitations/oc_ds_converter/issues/12) merge ([a9328ee](https://github.com/opencitations/oc_ds_converter/commit/a9328ee88450aa68e9fd6041b2e6d6599ba7d9b7))
* **doi:** only attempt DOI repair when API service is enabled ([9093d88](https://github.com/opencitations/oc_ds_converter/commit/9093d880610a62d85f79ad23eeaa1a167f62e057))
* **jalc:** use lock for atomic counter increments in multiprocessing ([84efa34](https://github.com/opencitations/oc_ds_converter/commit/84efa34aa9cf3e98a27817f381dd4999089ad532))
* **progress:** exclude cached items from time remaining estimates ([96bebb4](https://github.com/opencitations/oc_ds_converter/commit/96bebb438be1068f54bef2ec81d0ca16d4075eed))
* **progress:** use EMA for time remaining estimates ([3b5fec2](https://github.com/opencitations/oc_ds_converter/commit/3b5fec23b2aa8e56b9fa850e5ab74066d068629c))
* resolve type errors and linting issues across process modules ([7a687b6](https://github.com/opencitations/oc_ds_converter/commit/7a687b682f8b7bb868ae738c38848630ac8f69a4))
* restore tqdm dependency for process modules ([2429343](https://github.com/opencitations/oc_ds_converter/commit/2429343bb72828b62e88c57cc9f0cbf893b44ea8))
* **test:** switch coverage runner from unittest to pytest ([fa2cc44](https://github.com/opencitations/oc_ds_converter/commit/fa2cc446d7da893104efef02b8c5dd4f1fd02414))
* **types:** correct type annotations across processing and storage modules ([e5d22c6](https://github.com/opencitations/oc_ds_converter/commit/e5d22c6031d5c3571009971237f3449208849f84))


### Features

* **crossref:** add Redis publishers storage and age-based regeneration ([c01039b](https://github.com/opencitations/oc_ds_converter/commit/c01039bd8552e5ac05a438e4fb08b9aee906b735))
* **crossref:** store DOI-ORCID index in Redis for multiprocessing ([d4c2ed4](https://github.com/opencitations/oc_ds_converter/commit/d4c2ed4d1082806c12f1b6a570f0bd8bf3d4e6de))
* **jalc:** extract ORCID from researcher_id_list in creator metadata ([b03e9e0](https://github.com/opencitations/oc_ds_converter/commit/b03e9e0e05d963b94b001a908d2460b9c2b0c8e6))
* **jalc:** track progress per JSON file in multiprocessing mode ([0709722](https://github.com/opencitations/oc_ds_converter/commit/0709722572f685310be7763a728e798e09ccefd7))
* **orcid-index:** parallelize CSV loading to Redis with ProcessPoolExecutor ([6f8fa00](https://github.com/opencitations/oc_ds_converter/commit/6f8fa00e86706d786a12be66f2b3a7876b44fbb2))
* **storage:** restore SqliteStorageManager and InMemoryStorageManager ([91f3ca7](https://github.com/opencitations/oc_ds_converter/commit/91f3ca71180b0d5cba1df39d9db80ce06f11dcef))


### Performance Improvements

* **crossref:** only invoke BeautifulSoup when the text actually contains angle brackets ([3ee7afc](https://github.com/opencitations/oc_ds_converter/commit/3ee7afc9eae3a9b2775041951c01b2b18c3d84d9))
* **crossref:** prefetch DOI-ORCID index ([99e4f57](https://github.com/opencitations/oc_ds_converter/commit/99e4f57685cea838e3d88cc9d1cfa804af86a785))
* **crossref:** remove broken O(n²) ORCID fallback in get_agents_strings_list ([413fff2](https://github.com/opencitations/oc_ds_converter/commit/413fff27f3d3b64a6ab66abf1efc5976425dfde7))


### BREAKING CHANGES

* JalcProcessing no longer accepts publishers_filepath
or use_redis_publishers parameters. The -p/--publishers CLI argument
has been removed from jalc_process.py.
* CLI arguments --storage_path and --redis_storage_manager removed.
* The verbose parameter is removed from preprocess()
and the -v/--verbose CLI flag no longer exists. Progress is now
always displayed.
* The -p/--publishers CLI argument has been removed.
The publishers file is now generated automatically.
