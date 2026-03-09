import json
import unittest
from os import makedirs
from os.path import exists, join

from oc_ds_converter.oc_idmanager.pmcid import PMCIDManager


class pmcIdentifierManagerTest(unittest.TestCase):
    """This class aim at testing identifiers manager."""

    def setUp(self):
        if not exists("tmp"):
            makedirs("tmp")

        self.test_dir = join("test", "data")
        self.test_json_path = join(self.test_dir, "glob.json")
        with open(self.test_json_path, encoding="utf-8") as fp:
            self.data = json.load(fp)

        self.valid_pmc_1 = "PMC8384044"
        self.valid_pmc_2 = "PMC6716460"
        self.invalid_pmc_1 = "0128564"
        self.invalid_pmc_2 = "PMC6716"
        self.invalid_pmc_3 = "PMC10000716468"
        self.invalid_pmc_4 = "PMC100007468"

    def test_exists(self):
        with self.subTest(msg="get_extra_info=True, allow_extra_api=None"):
            pmc_manager = PMCIDManager()
            output = pmc_manager.exists('PMC8384044', get_extra_info=True, allow_extra_api=None)
            expected_output = (True, {'id': 'PMC8384044', 'valid': True})
            self.assertEqual(output, expected_output)
        with self.subTest(msg="get_extra_info=False, allow_extra_api=None"):
            pmc_manager = PMCIDManager()
            output = pmc_manager.exists('PMC6716460', get_extra_info=False, allow_extra_api=None)
            expected_output = True
            self.assertEqual(output, expected_output)


    def test_pmcid_normalise(self):
        pcm = PMCIDManager()
        self.assertEqual(
            pcm.normalise(self.valid_pmc_1),
            pcm.normalise(' ' + self.valid_pmc_1),
        )
        self.assertEqual(
            pcm.normalise(self.valid_pmc_2),
            pcm.normalise("https://www.ncbi.nlm.nih.gov/pmc/articles/" + self.valid_pmc_2),
        )

    def test_pmcid_is_valid(self):
        pcm = PMCIDManager()
        self.assertTrue(pcm.is_valid(self.valid_pmc_1))
        self.assertTrue(pcm.is_valid(self.valid_pmc_2))
        self.assertFalse(pcm.is_valid(self.invalid_pmc_1))
        self.assertFalse(pcm.is_valid(self.invalid_pmc_2))

    def test_pmc_is_valid(self):
        dm_nofile = PMCIDManager()
        self.assertTrue(dm_nofile.is_valid(self.valid_pmc_1))
        self.assertTrue(dm_nofile.is_valid(self.valid_pmc_2))
        self.assertFalse(dm_nofile.is_valid(self.invalid_pmc_1))
        self.assertFalse(dm_nofile.is_valid(self.invalid_pmc_2))

        dm_file = PMCIDManager(testing=True, use_api_service=False)
        # Pre-seed storage with data from glob.json
        for key, value in self.data.items():
            if key.startswith("pmcid:"):
                dm_file.storage_manager.set_value(key, value.get("valid", False))
        self.assertTrue(dm_file.normalise(self.valid_pmc_1, include_prefix=True) in self.data)
        self.assertTrue(dm_file.normalise(self.invalid_pmc_4, include_prefix=True) in self.data)
        self.assertTrue(dm_file.is_valid(self.valid_pmc_1))
        self.assertFalse(dm_file.is_valid(self.invalid_pmc_4))


    def test_pmc_default(self):
        am_nofile = PMCIDManager(testing=True)
        # Uses RedisStorageManager with testing=True (fakeredis)
        # uses API
        self.assertTrue(am_nofile.is_valid(self.valid_pmc_1))
        self.assertTrue(am_nofile.is_valid(self.valid_pmc_2))
        self.assertFalse(am_nofile.is_valid(self.invalid_pmc_2))
        self.assertFalse(am_nofile.is_valid(self.invalid_pmc_1))
        validated_ids = [self.valid_pmc_1, self.valid_pmc_2, self.invalid_pmc_1, self.invalid_pmc_2]
        validated = [am_nofile.normalise(x, include_prefix=True) for x in validated_ids if am_nofile.normalise(x, include_prefix=True)]
        # check that all the validated ids are stored in redis
        all_ids_stored = am_nofile.storage_manager.get_all_keys()
        self.assertTrue(all(x in all_ids_stored for x in validated))
        am_nofile.storage_manager.delete_storage()
        # check that the storage was correctly deleted
        self.assertEqual(am_nofile.storage_manager.get_all_keys(), set())

    def test_pmc_memory_file_noapi(self):
        # Uses support file (without updating it)
        # Uses RedisStorageManager storage manager
        # does not use API (so a syntactically correct id is considered to be valid)
        am_file = PMCIDManager(testing=True, use_api_service=False)
        self.assertTrue(am_file.normalise(self.valid_pmc_1, include_prefix=True) in self.data)
        self.assertTrue(am_file.normalise(self.invalid_pmc_4, include_prefix=True) in self.data)
        self.assertFalse(am_file.is_valid(self.invalid_pmc_1)) # is stored in support file as invalid
        self.assertTrue(am_file.is_valid(am_file.normalise(self.invalid_pmc_3, include_prefix=True))) # is not stored in support file as invalid, does not exist but has correct syntax

    def test_pmc_memory_file_api(self):
        # Uses support file (without updating it)
        # Uses RedisStorageManager storage manager
        # uses API (so a syntactically correct id which is not valid is considered to be invalid)
        am_file = PMCIDManager(testing=True, use_api_service=True)
        self.assertFalse(am_file.is_valid(self.invalid_pmc_1))

    def test_pmc_memory_nofile_noapi(self):
        # Does not use support file
        # Uses RedisStorageManager storage manager
        # Does not use API (so a syntactically correct id which is not valid is considered to be valid)
        am_nofile_noapi = PMCIDManager(testing=True, use_api_service=False)
        self.assertTrue(am_nofile_noapi.is_valid(self.valid_pmc_1))
        self.assertTrue(am_nofile_noapi.is_valid(self.invalid_pmc_3))
        am_nofile_noapi.storage_manager.delete_storage()



    def test_pmc_sqlite_nofile_api(self):
        # No pre-existing data
        # storage manager : RedisStorageManager
        # uses API
        sql_am_nofile = PMCIDManager(testing=True)
        self.assertTrue(sql_am_nofile.is_valid(self.valid_pmc_1))
        self.assertTrue(sql_am_nofile.is_valid(self.valid_pmc_2))
        self.assertFalse(sql_am_nofile.is_valid(self.invalid_pmc_1))
        self.assertFalse(sql_am_nofile.is_valid(self.invalid_pmc_2))
        # check that the redis storage contains all the validated ids
        validated_ids = [self.valid_pmc_1, self.valid_pmc_2, self.invalid_pmc_1, self.invalid_pmc_2]
        all_ids_stored = sql_am_nofile.storage_manager.get_all_keys()
        validated = [sql_am_nofile.normalise(x, include_prefix=True) for x in validated_ids if sql_am_nofile.normalise(x, include_prefix=True)]
        self.assertTrue(all(x in all_ids_stored for x in validated))
        sql_am_nofile.storage_manager.delete_storage()
        # check that the storage was correctly deleted
        self.assertEqual(sql_am_nofile.storage_manager.get_all_keys(), set())

    def test_pmc_sqlite_file_api(self):
        # Uses pre-existing data in Redis
        # Uses RedisStorageManager storage manager
        # tests validation behavior with pre-seeded data
        # Note: invalid_pmc_4 has valid PMC format but doesn't exist
        to_insert = [self.invalid_pmc_4, self.valid_pmc_1]
        sql_file = PMCIDManager(testing=True, use_api_service=True)
        for pmcid in to_insert:
            norm_id = sql_file.normalise(pmcid, include_prefix=True)
            if norm_id:
                is_valid = sql_file.is_valid(norm_id)
                sql_file.storage_manager.set_value(norm_id, is_valid)

        sql_no_api = PMCIDManager(testing=True, use_api_service=False)
        # Copy values from the first manager to the second for testing
        for pmcid in to_insert:
            norm_id = sql_no_api.normalise(pmcid, include_prefix=True)
            if norm_id:
                value = sql_file.storage_manager.get_value(norm_id)
                if value is not None:
                    sql_no_api.storage_manager.set_value(norm_id, value)
        all_db_keys = sql_no_api.storage_manager.get_all_keys()
        # check that all the normalised ids in the list were correctly inserted
        normalized_ids = [sql_no_api.normalise(x, include_prefix=True) for x in to_insert]
        self.assertTrue(all(nid in all_db_keys for nid in normalized_ids if nid))
        self.assertTrue(sql_no_api.is_valid(self.valid_pmc_1))  # is stored as valid
        self.assertFalse(sql_no_api.is_valid(self.invalid_pmc_4))  # is stored as invalid
        self.assertTrue(sql_no_api.is_valid(sql_no_api.normalise(self.invalid_pmc_3, include_prefix=True)))  # not stored, has correct syntax
        sql_no_api.storage_manager.delete_storage()

    def test_pmc_sqlite_nofile_noapi(self):
        # Does not use support file
        # Uses RedisStorageManager storage manager
        # Does not use API (so a syntactically correct id which is not valid is considered to be valid)
        am_nofile_noapi = PMCIDManager(testing=True, use_api_service=False)
        self.assertTrue(am_nofile_noapi.is_valid(self.valid_pmc_1))
        self.assertTrue(am_nofile_noapi.is_valid(self.invalid_pmc_3))
        am_nofile_noapi.storage_manager.delete_storage()
