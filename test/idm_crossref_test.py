import json
import unittest
from os import makedirs
from os.path import exists, join

import xmltodict
from oc_ds_converter.oc_idmanager import *
from oc_ds_converter.oc_idmanager.base import IdentifierManager
from requests import ReadTimeout, get
from requests.exceptions import ConnectionError
from oc_ds_converter.oc_idmanager.oc_data_storage.storage_manager import StorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.in_memory_manager import InMemoryStorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.sqlite_manager import SqliteStorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.redis_manager import RedisStorageManager

class CrossrefIdentifierManagerTest(unittest.TestCase):
    """This class aim at testing identifiers manager."""

    def setUp(self):
        if not exists("tmp"):
            makedirs("tmp")

        self.test_dir = join("test","data")
        self.test_json_path = join(self.test_dir, "glob.json")
        with open(self.test_json_path, encoding="utf-8") as fp:
            self.data = json.load(fp)

        self.valid_crmid1 = "297"
        self.valid_crmid2 = "4443"
        self.invalid_crmid1 = "342427"
        self.invalid_crmid2 = "0123"

    def test_crossref_is_valid(self):
        crmngr_nofile = CrossrefManager()
        self.assertTrue(crmngr_nofile.is_valid(self.valid_crmid1))
        self.assertTrue(crmngr_nofile.is_valid(self.valid_crmid2))
        self.assertFalse(crmngr_nofile.is_valid(self.invalid_crmid1))
        self.assertFalse(crmngr_nofile.is_valid(self.invalid_crmid2))

        crmngr_file = CrossrefManager(use_api_service=False, testing=True)
        # Pre-seed storage with data from glob.json
        for key, value in self.data.items():
            if key.startswith("crossref:"):
                crmngr_file.storage_manager.set_value(key, value.get("valid", False))
        self.assertTrue(crmngr_file.normalise(self.valid_crmid1, include_prefix=True) in self.data)
        self.assertTrue(crmngr_file.normalise(self.invalid_crmid1, include_prefix=True) in self.data)
        self.assertTrue(crmngr_file.is_valid(self.valid_crmid1))
        self.assertFalse(crmngr_file.is_valid(self.invalid_crmid1))

        crmngr_nofile_noapi = CrossrefManager(testing=True, use_api_service=False)
        self.assertTrue(crmngr_nofile_noapi.is_valid(self.valid_crmid1))
        self.assertTrue(crmngr_nofile_noapi.is_valid(self.valid_crmid2))

    def test_exists(self):
        with self.subTest(msg="get_extra_info=True, allow_extra_api=None"):
            crmngr = CrossrefManager()
            output = crmngr.exists(self.valid_crmid1, get_extra_info=True, allow_extra_api=None)
            expected_output = (True, {'valid': True})
            self.assertEqual(expected_output[0], output[0])
            # self.assertCountEqual({k:v for k,v in expected_output[1].items() if k!= "author"}, {k:v for k,v in output[1].items() if k!= "author"})
            # self.assertCountEqual(expected_output[1]["author"], output[1]["author"])

        with self.subTest(msg="get_extra_info=False, allow_extra_api=None"):
            crmngr = CrossrefManager()
            output = crmngr.exists(self.valid_crmid2, get_extra_info=False, allow_extra_api=None)
            expected_output = True
            self.assertEqual(output, expected_output)


    def test_openalex_normalise(self):
        crmngr = CrossrefManager()

        self.assertEqual(
            self.valid_crmid1, crmngr.normalise("crossref:" + self.valid_crmid1)
        )
        self.assertEqual(
            self.valid_crmid1, crmngr.normalise(self.valid_crmid1.replace("", " "))
        )
        self.assertEqual(
            self.valid_crmid1,
            crmngr.normalise("https://api.crossref.org/members/" + self.valid_crmid1),
        )
        self.assertEqual(
            crmngr.normalise(self.valid_crmid1),
            crmngr.normalise(' ' + self.valid_crmid1),
        )
        self.assertEqual(
            crmngr.normalise(self.valid_crmid2),
            crmngr.normalise("https://api.crossref.org/members/" + self.valid_crmid2),
        )

        dm_file = CrossrefManager(testing=True, use_api_service=False)
        # Pre-seed storage with data from glob.json
        for key, value in self.data.items():
            if key.startswith("crossref:"):
                dm_file.storage_manager.set_value(key, value.get("valid", False))
        self.assertTrue(dm_file.normalise(self.valid_crmid1, include_prefix=True) in self.data)
        self.assertTrue(dm_file.normalise(self.invalid_crmid2, include_prefix=True) in self.data)
        self.assertTrue(dm_file.is_valid(self.valid_crmid1))
        self.assertFalse(dm_file.is_valid(self.invalid_crmid2))

    def test_crossref_default(self):
        mngr = CrossrefManager(testing=True)
        # Uses RedisStorageManager with testing=True (fakeredis)
        # uses API
        self.assertTrue(mngr.is_valid(self.valid_crmid1))
        self.assertTrue(mngr.is_valid(self.valid_crmid2))
        self.assertFalse(mngr.is_valid(self.invalid_crmid2))
        self.assertFalse(mngr.is_valid(self.invalid_crmid1))
        validated_ids = [self.valid_crmid1, self.valid_crmid2, self.invalid_crmid1, self.invalid_crmid2]
        validated = [mngr.normalise(x, include_prefix=True) for x in validated_ids if mngr.normalise(x, include_prefix=True)]
        # check that all the validated ids are stored in redis
        all_ids_stored = mngr.storage_manager.get_all_keys()
        self.assertTrue(all(x in all_ids_stored for x in validated))
        mngr.storage_manager.delete_storage()
        # check that the storage was correctly deleted
        self.assertEqual(mngr.storage_manager.get_all_keys(), set())

    #### IN MEMORY STORAGE MANAGER

    def test_crossref_memory_file_noapi(self):
        # Uses pre-seeded data (without updating it)
        # Uses RedisStorageManager storage manager
        # does not use API (so a syntactically correct id is considered to be valid)
        am_file = CrossrefManager(testing=True, use_api_service=False)
        # Pre-seed storage with data from glob.json
        for key, value in self.data.items():
            if key.startswith("crossref:"):
                am_file.storage_manager.set_value(key, value.get("valid", False))
        self.assertTrue(am_file.normalise(self.valid_crmid1, include_prefix=True) in self.data)
        self.assertTrue(am_file.normalise(self.invalid_crmid2, include_prefix=True) in self.data)
        self.assertFalse(am_file.is_valid(self.invalid_crmid2))  # is stored as invalid

    def test_crossref_memory_file_api(self):
        # Uses support file (without updating it)
        # Uses RedisStorageManager storage manager
        # uses API (so a syntactically correct id which is not valid is considered to be invalid)
        am_file = CrossrefManager(testing=True, use_api_service=True)
        self.assertFalse(am_file.is_valid(self.invalid_crmid1))

    def test_crossref_memory_nofile_noapi(self):
        # Does not use support file
        # Uses RedisStorageManager storage manager
        # Does not use API (so a syntactically correct id which is not valid is considered to be valid)
        am_nofile_noapi = CrossrefManager(testing=True, use_api_service=False)
        self.assertTrue(am_nofile_noapi.is_valid(self.valid_crmid1))
        self.assertTrue(am_nofile_noapi.is_valid(self.invalid_crmid1))
        am_nofile_noapi.storage_manager.delete_storage()

    #### SQLITE STORAGE MANAGER

    def test_crossref_sqlite_nofile_api(self):
        # No pre-existing data
        # storage manager : RedisStorageManager
        # uses API
        sql_am_nofile = CrossrefManager(testing=True)
        self.assertTrue(sql_am_nofile.is_valid(self.valid_crmid1))
        self.assertTrue(sql_am_nofile.is_valid(self.valid_crmid2))
        self.assertFalse(sql_am_nofile.is_valid(self.invalid_crmid1))
        self.assertFalse(sql_am_nofile.is_valid(self.invalid_crmid2))
        # check that the redis storage contains all the validated ids
        validated_ids = [self.valid_crmid1, self.valid_crmid2, self.invalid_crmid1, self.invalid_crmid2]
        all_ids_stored = sql_am_nofile.storage_manager.get_all_keys()
        validated = [sql_am_nofile.normalise(x, include_prefix=True) for x in validated_ids if sql_am_nofile.normalise(x, include_prefix=True)]
        self.assertTrue(all(x in all_ids_stored for x in validated))
        sql_am_nofile.storage_manager.delete_storage()
        # check that the storage was correctly deleted
        self.assertEqual(sql_am_nofile.storage_manager.get_all_keys(), set())

    def test_crossref_sqlite_file_api(self):
        # Uses pre-existing data in Redis
        # Uses RedisStorageManager storage manager
        # tests validation behavior with pre-seeded data
        to_insert = [self.invalid_crmid1, self.valid_crmid1]
        sql_file = CrossrefManager(testing=True, use_api_service=True)
        for crmid in to_insert:
            norm_id = sql_file.normalise(crmid, include_prefix=True)
            is_valid = sql_file.is_valid(norm_id)
            sql_file.storage_manager.set_value(norm_id, is_valid)

        sql_no_api = CrossrefManager(testing=True, use_api_service=False)
        # Copy values from the first manager to the second for testing
        for crmid in to_insert:
            norm_id = sql_no_api.normalise(crmid, include_prefix=True)
            value = sql_file.storage_manager.get_value(norm_id)
            if value is not None:
                sql_no_api.storage_manager.set_value(norm_id, value)
        all_db_keys = sql_no_api.storage_manager.get_all_keys()
        # check that all the normalised ids in the list were correctly inserted
        self.assertTrue(all(sql_no_api.normalise(x, include_prefix=True) in all_db_keys for x in to_insert))
        self.assertTrue(sql_no_api.is_valid(self.valid_crmid1))  # is stored as valid
        self.assertFalse(sql_no_api.is_valid(self.invalid_crmid1))  # is stored as invalid
        self.assertTrue(sql_no_api.is_valid(sql_no_api.normalise(self.invalid_crmid2, include_prefix=True)))  # not stored, has correct syntax
        sql_no_api.storage_manager.delete_storage()

    def test_crossref_sqlite_nofile_noapi(self):
        # Does not use support file
        # Uses RedisStorageManager storage manager
        # Does not use API (so a syntactically correct id which is not valid is considered to be valid)
        am_nofile_noapi = CrossrefManager(testing=True, use_api_service=False)
        self.assertTrue(am_nofile_noapi.is_valid(self.valid_crmid1))
        self.assertTrue(am_nofile_noapi.is_valid(self.invalid_crmid2))
        am_nofile_noapi.storage_manager.delete_storage()

    #### REDIS STORAGE MANAGER

    def test_crossref_redis_nofile_api(self):
        # No support files (it generates it)
        # storage manager : RedisStorageManager
        # uses API
        redis_cm_nofile = CrossrefManager(storage_manager=RedisStorageManager())
        self.assertTrue(redis_cm_nofile.is_valid(self.valid_crmid1))
        self.assertTrue(redis_cm_nofile.is_valid(self.valid_crmid2))
        self.assertFalse(redis_cm_nofile.is_valid(self.invalid_crmid1))
        self.assertFalse(redis_cm_nofile.is_valid(self.invalid_crmid2))
        # check that the redis db was correctly filled and that it contains all the validated ids
        validated_ids = {self.valid_crmid1, self.valid_crmid2, self.invalid_crmid1, self.invalid_crmid2}
        validated_ids = {redis_cm_nofile.normalise(x, include_prefix=True) for x in validated_ids}
        all_ids_stored = redis_cm_nofile.storage_manager.get_all_keys()
        # check that all the validated ids are stored in the json file
        self.assertEqual(validated_ids, all_ids_stored)
        redis_cm_nofile.storage_manager.delete_storage()
        # check that the support file was correctly deleted
        self.assertEqual(redis_cm_nofile.storage_manager.get_all_keys(), set())

    def test_crossref_redis_file_api(self):
        # Uses data in redis db
        # Uses RedisStorageManager
        # does not use API (so a syntactically correct id is considered to be valid)
        # fills db
        to_insert = [self.invalid_crmid1, self.valid_crmid1]
        storage_manager = RedisStorageManager(testing=True)
        redis_file = CrossrefManager(storage_manager=storage_manager, use_api_service=True)
        for id in to_insert:
            norm_id = redis_file.normalise(id, include_prefix=True)
            is_valid = redis_file.is_valid(norm_id)
            # insert_tup = (norm_id, is_valid)
            redis_file.storage_manager.set_value(norm_id, is_valid)

        redis_no_api = CrossrefManager(storage_manager=storage_manager, use_api_service=False)
        all_db_keys = redis_no_api.storage_manager.get_all_keys()
        # check that all the normalised ids in the list were correctly inserted in the db
        self.assertTrue(all(redis_no_api.normalise(x, include_prefix=True) in all_db_keys for x in to_insert))
        self.assertTrue(redis_no_api.is_valid(self.valid_crmid1))  # is stored in support file as valid
        self.assertFalse(redis_no_api.is_valid(self.invalid_crmid1))  # is stored in support file as invalid
        self.assertTrue(redis_no_api.is_valid(self.invalid_crmid2))  # is not stored in support file as invalid, does not exist but has correct syntax
        redis_no_api.storage_manager.delete_storage()

    def test_crossref_redis_nofile_noapi(self):
        # Does not use support file
        # Uses RedisStorageManager storage manager
        # Does not use API (so a syntactically correct id which is not valid is considered to be valid)
        cr_nofile_noapi = CrossrefManager(storage_manager=SqliteStorageManager(), use_api_service=False)
        self.assertTrue(cr_nofile_noapi.is_valid(self.valid_crmid1))
        self.assertTrue(cr_nofile_noapi.is_valid(self.invalid_crmid2))
        cr_nofile_noapi.storage_manager.delete_storage()
