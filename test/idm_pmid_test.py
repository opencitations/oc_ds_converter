import json
import unittest
from os import makedirs
from os.path import exists, join

from oc_ds_converter.oc_idmanager.pmid import PMIDManager


class pmidIdentifierManagerTest(unittest.TestCase):
    """This class aim at testing identifiers manager."""

    def setUp(self):
        if not exists("tmp"):
            makedirs("tmp")

        self.test_dir = join("test", "data")
        self.test_json_path = join(self.test_dir, "glob.json")
        with open(self.test_json_path, encoding="utf-8") as fp:
            self.data = json.load(fp)

        self.valid_pmid_1 = "2942070"
        self.valid_pmid_2 = "1509982"
        self.invalid_pmid_1 = "0067308798798"
        self.invalid_pmid_2 = "pmid:174777777777"
        self.invalid_pmid_3 = "pmid:174777777779"


    def test_pmid_normalise(self):
        pm = PMIDManager()
        self.assertEqual(
            self.valid_pmid_1, pm.normalise(self.valid_pmid_1.replace("", "pmid:"))
        )
        self.assertEqual(
            self.valid_pmid_1, pm.normalise(self.valid_pmid_1.replace("", " "))
        )
        self.assertEqual(
            self.valid_pmid_1,
            pm.normalise("https://pubmed.ncbi.nlm.nih.gov/" + self.valid_pmid_1),
        )
        self.assertEqual(self.valid_pmid_2, pm.normalise("000" + self.valid_pmid_2))

    def test_pmid_is_valid(self):
        pm_nofile = PMIDManager()
        self.assertTrue(pm_nofile.is_valid(self.valid_pmid_1))
        self.assertTrue(pm_nofile.is_valid(self.valid_pmid_2))
        self.assertFalse(pm_nofile.is_valid(self.invalid_pmid_1))
        self.assertFalse(pm_nofile.is_valid(self.invalid_pmid_2))

        pm_file = PMIDManager(use_api_service=False, testing=True)
        # Pre-seed storage with data from glob.json
        for key, value in self.data.items():
            if key.startswith("pmid:"):
                pm_file.storage_manager.set_value(key, value.get("valid", False))
        self.assertTrue(pm_file.normalise(self.valid_pmid_1, include_prefix=True) in self.data)
        self.assertTrue(pm_file.normalise(self.invalid_pmid_1, include_prefix=True) in self.data)
        self.assertTrue(pm_file.is_valid(self.valid_pmid_1))
        self.assertFalse(pm_file.is_valid(self.invalid_pmid_1))

        pm_nofile_noapi = PMIDManager(testing=True, use_api_service=False)
        self.assertTrue(pm_nofile_noapi.is_valid(self.valid_pmid_1))
        self.assertTrue(pm_nofile_noapi.is_valid(self.invalid_pmid_3))

    def test_exists(self):
        with self.subTest(msg="get_extra_info=True, allow_extra_api=None"):
            pmid_manager = PMIDManager()
            output = pmid_manager.exists('pmid:8384044', get_extra_info=True, allow_extra_api=None)
            expected_output = (True, {'valid': True, 'title': 'Brevetoxin depresses synaptic transmission in guinea pig hippocampal slices.', 'author': ['Adler, M', 'Sheridan, R E', 'Apland, J P'], 'pub_date': '1993', 'venue': 'Brain research bulletin [issn:0361-9230]', 'volume': '31', 'issue': '1-2', 'page': '201-7', 'type': ['journal article'], 'publisher': [], 'editor': [], 'doi': '10.1016/0361-9230(93)90026-8', 'id': 'pmid:8384044'})
            self.assertEqual(expected_output[0], output[0])
            self.assertCountEqual({k:v for k,v in expected_output[1].items() if k!= "author"}, {k:v for k,v in output[1].items() if k!= "author"})
            self.assertCountEqual(expected_output[1]["author"], output[1]["author"])

        with self.subTest(msg="get_extra_info=False, allow_extra_api=None"):
            pmid_manager = PMIDManager()
            output = pmid_manager.exists('pmid6716460', get_extra_info=False, allow_extra_api=None)
            expected_output = True
            self.assertEqual(output, expected_output)


    def test_pmidid_normalise(self):
        pcm = PMIDManager()
        self.assertEqual(
            pcm.normalise(self.valid_pmid_1),
            pcm.normalise(' ' + self.valid_pmid_1),
        )
        self.assertEqual(
            pcm.normalise(self.valid_pmid_2),
            pcm.normalise("https://www.ncbi.nlm.nih.gov/pmid/articles/" + self.valid_pmid_2),
        )

        dm_file = PMIDManager(testing=True, use_api_service=False)
        # Pre-seed storage with data from glob.json
        for key, value in self.data.items():
            if key.startswith("pmid:"):
                dm_file.storage_manager.set_value(key, value.get("valid", False))
        self.assertTrue(dm_file.normalise(self.valid_pmid_1, include_prefix=True) in self.data)
        self.assertTrue(dm_file.normalise(self.invalid_pmid_2, include_prefix=True) in self.data)
        self.assertTrue(dm_file.is_valid(self.valid_pmid_1))
        self.assertFalse(dm_file.is_valid(self.invalid_pmid_2))


    def test_pmid_default(self):
        am_nofile = PMIDManager(testing=True)
        # Uses RedisStorageManager with testing=True (fakeredis)
        # uses API
        self.assertTrue(am_nofile.is_valid(self.valid_pmid_1))
        self.assertTrue(am_nofile.is_valid(self.valid_pmid_2))
        self.assertFalse(am_nofile.is_valid(self.invalid_pmid_2))
        self.assertFalse(am_nofile.is_valid(self.invalid_pmid_1))
        validated_ids = [self.valid_pmid_1, self.valid_pmid_2, self.invalid_pmid_1, self.invalid_pmid_2]
        validated = [am_nofile.normalise(x, include_prefix=True) for x in validated_ids if am_nofile.normalise(x, include_prefix=True)]
        # check that all the validated ids are stored in redis
        all_ids_stored = am_nofile.storage_manager.get_all_keys()
        self.assertTrue(all(x in all_ids_stored for x in validated))
        am_nofile.storage_manager.delete_storage()
        # check that the storage was correctly deleted
        self.assertEqual(am_nofile.storage_manager.get_all_keys(), set())

    def test_pmid_memory_file_noapi(self):
        # Uses pre-seeded data (without updating it)
        # Uses RedisStorageManager storage manager
        # does not use API (so a syntactically correct id is considered to be valid)
        am_file = PMIDManager(testing=True, use_api_service=False)
        # Pre-seed storage with data from glob.json
        for key, value in self.data.items():
            if key.startswith("pmid:"):
                am_file.storage_manager.set_value(key, value.get("valid", False))
        self.assertTrue(am_file.normalise(self.valid_pmid_1, include_prefix=True) in self.data)
        self.assertTrue(am_file.normalise(self.invalid_pmid_2, include_prefix=True) in self.data)
        self.assertFalse(am_file.is_valid(self.invalid_pmid_2))  # is stored as invalid
        self.assertTrue(am_file.is_valid(am_file.normalise(self.invalid_pmid_3, include_prefix=True)))  # not stored as invalid, has correct syntax

    def test_pmid_memory_file_api(self):
        # Uses support file (without updating it)
        # Uses RedisStorageManager storage manager
        # uses API (so a syntactically correct id which is not valid is considered to be invalid)
        am_file = PMIDManager(testing=True, use_api_service=True)
        self.assertFalse(am_file.is_valid(self.invalid_pmid_1))

    def test_pmid_memory_nofile_noapi(self):
        # Does not use support file
        # Uses RedisStorageManager storage manager
        # Does not use API (so a syntactically correct id which is not valid is considered to be valid)
        am_nofile_noapi = PMIDManager(testing=True, use_api_service=False)
        self.assertTrue(am_nofile_noapi.is_valid(self.valid_pmid_1))
        self.assertTrue(am_nofile_noapi.is_valid(self.invalid_pmid_1))
        am_nofile_noapi.storage_manager.delete_storage()



    def test_pmid_sqlite_nofile_api(self):
        # No pre-existing data
        # storage manager : RedisStorageManager
        # uses API
        sql_am_nofile = PMIDManager(testing=True)
        self.assertTrue(sql_am_nofile.is_valid(self.valid_pmid_1))
        self.assertTrue(sql_am_nofile.is_valid(self.valid_pmid_2))
        self.assertFalse(sql_am_nofile.is_valid(self.invalid_pmid_1))
        self.assertFalse(sql_am_nofile.is_valid(self.invalid_pmid_2))
        # check that the redis storage contains all the validated ids
        validated_ids = [self.valid_pmid_1, self.valid_pmid_2, self.invalid_pmid_1, self.invalid_pmid_2]
        all_ids_stored = sql_am_nofile.storage_manager.get_all_keys()
        validated = [sql_am_nofile.normalise(x, include_prefix=True) for x in validated_ids if sql_am_nofile.normalise(x, include_prefix=True)]
        self.assertTrue(all(x in all_ids_stored for x in validated))
        sql_am_nofile.storage_manager.delete_storage()
        # check that the storage was correctly deleted
        self.assertEqual(sql_am_nofile.storage_manager.get_all_keys(), set())

    def test_pmid_sqlite_file_api(self):
        # Uses pre-existing data in Redis
        # Uses RedisStorageManager storage manager
        # tests validation behavior with pre-seeded data
        to_insert = [self.invalid_pmid_1, self.valid_pmid_1]
        sql_file = PMIDManager(testing=True, use_api_service=True)
        for pmid in to_insert:
            norm_id = sql_file.normalise(pmid, include_prefix=True)
            is_valid = sql_file.is_valid(norm_id)
            sql_file.storage_manager.set_value(norm_id, is_valid)

        sql_no_api = PMIDManager(testing=True, use_api_service=False)
        # Copy values from the first manager to the second for testing
        for pmid in to_insert:
            norm_id = sql_no_api.normalise(pmid, include_prefix=True)
            value = sql_file.storage_manager.get_value(norm_id)
            if value is not None:
                sql_no_api.storage_manager.set_value(norm_id, value)
        all_db_keys = sql_no_api.storage_manager.get_all_keys()
        # check that all the normalised ids in the list were correctly inserted
        self.assertTrue(all(sql_no_api.normalise(x, include_prefix=True) in all_db_keys for x in to_insert))
        self.assertTrue(sql_no_api.is_valid(self.valid_pmid_1))  # is stored as valid
        self.assertFalse(sql_no_api.is_valid(self.invalid_pmid_1))  # is stored as invalid
        self.assertTrue(sql_no_api.is_valid(sql_no_api.normalise(self.invalid_pmid_2, include_prefix=True)))  # not stored, has correct syntax
        sql_no_api.storage_manager.delete_storage()

    def test_pmid_sqlite_nofile_noapi(self):
        # Does not use support file
        # Uses RedisStorageManager storage manager
        # Does not use API (so a syntactically correct id which is not valid is considered to be valid)
        am_nofile_noapi = PMIDManager(testing=True, use_api_service=False)
        self.assertTrue(am_nofile_noapi.is_valid(self.valid_pmid_1))
        self.assertTrue(am_nofile_noapi.is_valid(self.invalid_pmid_2))
        am_nofile_noapi.storage_manager.delete_storage()
