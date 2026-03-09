import json
import unittest
from os import makedirs
from os.path import exists, join

from oc_ds_converter.oc_idmanager.doi import DOIManager

class DOIIdentifierManagerTest(unittest.TestCase):
    """This class aim at testing identifiers manager."""

    def setUp(self):
        if not exists("tmp"):
            makedirs("tmp")

        self.test_dir = join("test", "data")
        self.test_json_path = join(self.test_dir, "glob.json")
        with open(self.test_json_path, encoding="utf-8") as fp:
            self.data = json.load(fp)

        self.valid_doi_1 = "10.1108/jd-12-2013-0166"
        self.valid_doi_2 = "10.1130/2015.2513(00)"
        self.invalid_doi_1 = "10.1108/12-2013-0166"
        self.invalid_doi_2 = "10.1371"

    def test_exists(self):
        with self.subTest(msg="get_extra_info=True, allow_extra_api=None"):
            doi_manager = DOIManager()
            output = doi_manager.exists('10.1007/s11192-022-04367-w', get_extra_info=True, allow_extra_api=None)
            expected_output = (True, {'id': '10.1007/s11192-022-04367-w', 'valid': True, 'ra': 'unknown'})
            self.assertEqual(output, expected_output)
        with self.subTest(msg="get_extra_info=False, allow_extra_api=None"):
            doi_manager = DOIManager()
            output = doi_manager.exists('10.1007/s11192-022-04367-w', get_extra_info=False, allow_extra_api=None)
            expected_output = True
            self.assertEqual(output, expected_output)
        with self.subTest(msg="get_extra_info=False, allow_extra_api='crossref'"):
            doi_manager = DOIManager()
            output = doi_manager.exists('10.1007/s11192-022-04367-w', get_extra_info=False, allow_extra_api='crossref')
            expected_output = True
            self.assertEqual(output, expected_output)

    def test_doi_normalise(self):
        dm = DOIManager()
        self.assertEqual(
            self.valid_doi_1,
            dm.normalise(self.valid_doi_1.upper().replace("10.", "doi: 10. ")),
        )
        self.assertEqual(
            self.valid_doi_1,
            dm.normalise(self.valid_doi_1.upper().replace("10.", "doi:10.")),
        )
        self.assertEqual(
            self.valid_doi_1,
            dm.normalise(
                self.valid_doi_1.upper().replace("10.", "https://doi.org/10.")
            ),
        )

    def test_doi_is_valid(self):
        dm_nofile = DOIManager()
        self.assertTrue(dm_nofile.is_valid(self.valid_doi_1))
        self.assertTrue(dm_nofile.is_valid(self.valid_doi_2))
        self.assertFalse(dm_nofile.is_valid(self.invalid_doi_1))
        self.assertFalse(dm_nofile.is_valid(self.invalid_doi_2))

        dm_file = DOIManager(testing=True, use_api_service=False)
        # Pre-seed storage with data from glob.json
        for key, value in self.data.items():
            if key.startswith("doi:"):
                dm_file.storage_manager.set_value(key, value.get("valid", False))
        self.assertTrue(dm_file.normalise(self.valid_doi_1, include_prefix=True) in self.data)
        self.assertTrue(dm_file.normalise(self.invalid_doi_1, include_prefix=True) in self.data)
        self.assertTrue(dm_file.is_valid(self.valid_doi_1))
        self.assertFalse(dm_file.is_valid(self.invalid_doi_1))


    def test_doi_default(self):
        am_nofile = DOIManager(testing=True)
        # Uses RedisStorageManager with testing=True (fakeredis)
        # uses API
        self.assertTrue(am_nofile.is_valid(self.valid_doi_1))
        self.assertTrue(am_nofile.is_valid(self.valid_doi_2))
        self.assertFalse(am_nofile.is_valid(self.invalid_doi_2))
        self.assertFalse(am_nofile.is_valid(self.invalid_doi_1))
        validated_ids = [self.valid_doi_1, self.valid_doi_2, self.invalid_doi_1, self.invalid_doi_2]
        # check that all the validated ids are stored in redis
        all_ids_stored = am_nofile.storage_manager.get_all_keys()
        self.assertTrue(all(am_nofile.normalise(x, include_prefix=True) in all_ids_stored for x in validated_ids))
        am_nofile.storage_manager.delete_storage()
        # check that the storage was correctly deleted
        self.assertEqual(am_nofile.storage_manager.get_all_keys(), set())

    def test_doi_memory_file_noapi(self):
        # Uses pre-seeded data (without updating it)
        # Uses RedisStorageManager storage manager
        # does not use API (so a syntactically correct id is considered to be valid)
        am_file = DOIManager(testing=True, use_api_service=False)
        # Pre-seed storage with data from glob.json
        for key, value in self.data.items():
            if key.startswith("doi:"):
                am_file.storage_manager.set_value(key, value.get("valid", False))
        norm_valid = am_file.normalise(self.valid_doi_1, include_prefix=True)
        norm_invalid = am_file.normalise(self.invalid_doi_1.strip().lower(), include_prefix=True)
        norm_fake = am_file.normalise("10.1109/5.771073FAKE_ID", include_prefix=True)
        assert norm_valid is not None
        assert norm_invalid is not None
        assert norm_fake is not None
        self.assertTrue(norm_valid in self.data)
        self.assertTrue(norm_invalid in self.data)
        self.assertFalse(am_file.is_valid(self.invalid_doi_1))
        self.assertTrue(am_file.is_valid(norm_fake))

    def test_doi_memory_file_api(self):
        # Uses support file (without updating it)
        # Uses RedisStorageManager storage manager
        # uses API (so a syntactically correct id which is not valid is considered to be invalid)
        am_file = DOIManager(testing=True, use_api_service=True)
        self.assertFalse(am_file.is_valid(self.invalid_doi_1))

    def test_doi_memory_nofile_noapi(self):
        # Does not use support file
        # Uses RedisStorageManager storage manager
        # Does not API (so a syntactically correct id which is not valid is considered to be valid)
        am_nofile_noapi = DOIManager(testing=True, use_api_service=False)
        self.assertTrue(am_nofile_noapi.is_valid(self.valid_doi_1))
        self.assertTrue(am_nofile_noapi.is_valid(self.invalid_doi_1))
        am_nofile_noapi.storage_manager.delete_storage()



    def test_doi_sqlite_nofile_api(self):
        # No pre-existing data
        # storage manager : RedisStorageManager
        # uses API
        sql_am_nofile = DOIManager(testing=True)
        self.assertTrue(sql_am_nofile.is_valid(self.valid_doi_1))
        self.assertTrue(sql_am_nofile.is_valid(self.valid_doi_2))
        self.assertFalse(sql_am_nofile.is_valid(self.invalid_doi_1))
        self.assertFalse(sql_am_nofile.is_valid(self.invalid_doi_2))
        # check that the redis storage contains all the validated ids
        validated_ids = [self.valid_doi_1, self.valid_doi_2, self.invalid_doi_1, self.invalid_doi_2]
        all_ids_stored = sql_am_nofile.storage_manager.get_all_keys()
        normalized_ids = [sql_am_nofile.normalise(x, include_prefix=True) for x in validated_ids]
        self.assertTrue(all(nid in all_ids_stored for nid in normalized_ids if nid is not None))
        sql_am_nofile.storage_manager.delete_storage()
        # check that the storage was correctly deleted
        self.assertEqual(sql_am_nofile.storage_manager.get_all_keys(), set())

    def test_doi_sqlite_file_api(self):
        # Uses pre-existing data in Redis
        # Uses RedisStorageManager storage manager
        # tests validation behavior with pre-seeded data
        to_insert = [self.invalid_doi_1, self.valid_doi_1]
        sql_file = DOIManager(testing=True, use_api_service=True)
        for doi_id in to_insert:
            norm_id = sql_file.normalise(doi_id, include_prefix=True)
            assert norm_id is not None
            is_valid = sql_file.is_valid(norm_id)
            sql_file.storage_manager.set_value(norm_id, is_valid)

        sql_no_api = DOIManager(testing=True, use_api_service=False)
        # Copy values from the first manager to the second for testing
        for doi_id in to_insert:
            norm_id = sql_no_api.normalise(doi_id, include_prefix=True)
            value = sql_file.storage_manager.get_value(norm_id)
            if value is not None:
                sql_no_api.storage_manager.set_value(norm_id, value)
        all_db_keys = sql_no_api.storage_manager.get_all_keys()
        normalized_ids = [sql_no_api.normalise(x, include_prefix=True) for x in to_insert]
        self.assertTrue(all(nid in all_db_keys for nid in normalized_ids if nid is not None))
        self.assertTrue(sql_no_api.is_valid(self.valid_doi_1))
        self.assertFalse(sql_no_api.is_valid(self.invalid_doi_1))
        norm_fake = sql_no_api.normalise("10.1109/5.771073FAKE_ID", include_prefix=True)
        assert norm_fake is not None
        self.assertTrue(sql_no_api.is_valid(norm_fake))
        sql_no_api.storage_manager.delete_storage()

    def test_doi_sqlite_nofile_noapi(self):
        # Does not use support file
        # Uses RedisStorageManager storage manager
        # Does not use API (so a syntactically correct id which is not valid is considered to be valid)
        am_nofile_noapi = DOIManager(testing=True, use_api_service=False)
        self.assertTrue(am_nofile_noapi.is_valid(self.valid_doi_1))
        self.assertTrue(am_nofile_noapi.is_valid(self.invalid_doi_1))
        am_nofile_noapi.storage_manager.delete_storage()

    def test_attempt_repair_removes_backslash(self):
        dm = DOIManager(use_api_service=True)
        repaired = dm.attempt_repair("10.1108/jd-12-2013-0166\\")
        self.assertEqual(repaired, "10.1108/jd-12-2013-0166")

    def test_attempt_repair_removes_double_underscore(self):
        dm = DOIManager(use_api_service=True)
        repaired = dm.attempt_repair("10.1108/jd__12-2013-0166")
        self.assertIsNone(repaired)

    def test_attempt_repair_removes_double_dot(self):
        dm = DOIManager(use_api_service=True)
        repaired = dm.attempt_repair("10..1108/jd-12-2013-0166")
        self.assertEqual(repaired, "10.1108/jd-12-2013-0166")

    def test_attempt_repair_removes_html_tags(self):
        dm = DOIManager(use_api_service=True)
        repaired = dm.attempt_repair("10.1108/jd-12-2013-0166<tag>content</tag>")
        self.assertEqual(repaired, "10.1108/jd-12-2013-0166")

    def test_attempt_repair_removes_self_closing_tags(self):
        dm = DOIManager(use_api_service=True)
        repaired = dm.attempt_repair("10.1108/jd-12-2013-0166<br/>")
        self.assertEqual(repaired, "10.1108/jd-12-2013-0166")

    def test_attempt_repair_no_change_returns_none(self):
        dm = DOIManager(use_api_service=True)
        repaired = dm.attempt_repair("10.1108/jd-12-2013-0166")
        self.assertIsNone(repaired)

    def test_attempt_repair_api_disabled_returns_none(self):
        dm = DOIManager(use_api_service=False)
        repaired = dm.attempt_repair("10.1108/jd-12-2013-0166\\")
        self.assertIsNone(repaired)

    def test_is_valid_repairs_malformed_doi(self):
        dm = DOIManager(use_api_service=True)
        malformed_doi = "10.1108/jd-12-2013-0166\\"
        self.assertTrue(dm.is_valid(malformed_doi))

    def test_is_valid_repairs_malformed_doi_with_extra_info(self):
        dm = DOIManager(use_api_service=True)
        malformed_doi = "10.1108/jd-12-2013-0166\\"
        result = dm.is_valid(malformed_doi, get_extra_info=True)
        assert isinstance(result, tuple)
        self.assertTrue(result[0])
        self.assertEqual(result[1]["id"], "10.1108/jd-12-2013-0166")

    def test_is_valid_no_repair_when_api_disabled(self):
        dm = DOIManager(use_api_service=False)
        malformed_doi = "10.1108/jd-12-2013-0166\\"
        self.assertTrue(dm.is_valid(malformed_doi))

    def test_is_valid_with_extra_info_valid_doi(self):
        dm = DOIManager(use_api_service=True)
        result = dm.is_valid(self.valid_doi_1, get_extra_info=True)
        assert isinstance(result, tuple)
        self.assertTrue(result[0])
        self.assertEqual(result[1]["id"], self.valid_doi_1)

    def test_normalise_removes_dx_doi_prefix(self):
        dm = DOIManager()
        doi_with_prefix = "http://dx.doi.org/10.1108/jd-12-2013-0166"
        self.assertEqual(dm.normalise(doi_with_prefix), "10.1108/jd-12-2013-0166")

    def test_normalise_removes_suffix_pmid(self):
        dm = DOIManager()
        doi_with_suffix = "10.1108/jd-12-2013-0166.PMID:12345"
        self.assertEqual(dm.normalise(doi_with_suffix), "10.1108/jd-12-2013-0166")

    def test_normalise_invalid_string_returns_none(self):
        dm = DOIManager()
        self.assertIsNone(dm.normalise("not a doi"))

    def test_base_normalise_invalid_string_returns_none(self):
        dm = DOIManager()
        self.assertIsNone(dm.base_normalise("not a doi"))

    def test_is_valid_normalise_returns_none(self):
        dm = DOIManager()
        self.assertFalse(dm.is_valid("not a doi"))

    def test_syntax_ok_without_prefix(self):
        dm = DOIManager()
        self.assertTrue(dm.syntax_ok("10.1108/jd-12-2013-0166"))

    def test_normalise_removes_embedded_url_prefix(self):
        dm = DOIManager()
        doi_with_embedded_url = "10.1108http://dx.doi.org/jd-12-2013-0166"
        self.assertEqual(dm.normalise(doi_with_embedded_url), "10.1108")
