
import json
import sqlite3
import os.path
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

class WikidataIdentifierManagerTest(unittest.TestCase):
    """This class aim at testing identifiers manager."""

    def setUp(self):
        if not exists("tmp"):
            makedirs("tmp")

        self.test_dir = os.path.join("test","data")
        self.test_json_path = join(self.test_dir, "glob.json")
        with open(self.test_json_path, encoding="utf-8") as fp:
            self.data = json.load(fp)

        self.valid_wikidata_1 = "Q34433"
        self.valid_wikidata_2 = "Q24698708"
        self.valid_wikidata_3 = "Q15767074"
        self.invalid_wikidata_1 = "Q34433Q345"
        self.invalid_wikidata_2 = "Q24698722" #valid format but not existing
        self.invalid_wikidata_3 = "Q12"  # not existing yet

    def test_wikidata_normalise(self):
        wdm = WikidataManager()
        self.assertTrue(
            self.valid_wikidata_1,
            wdm.normalise(self.valid_wikidata_1.replace("Q", "https://www.wikidata.org/wiki/Q"))
        )
        self.assertTrue(
            self.valid_wikidata_2,
            wdm.normalise(self.valid_wikidata_2)
        )
        self.assertTrue(
            self.valid_wikidata_2,
            wdm.normalise(self.valid_wikidata_2.replace("Q", "wikidata: Q"))
        )
        self.assertTrue(
            self.valid_wikidata_3,
            wdm.normalise((self.valid_wikidata_3.replace("Q", "Q ")))
        )

    def test_wikidata_is_valid(self):
        wdm = WikidataManager()
        self.assertTrue(wdm.is_valid(self.valid_wikidata_1))
        self.assertTrue(wdm.is_valid(self.valid_wikidata_2))
        self.assertTrue(wdm.is_valid(self.valid_wikidata_3))
        self.assertFalse(wdm.is_valid(self.invalid_wikidata_1))
        self.assertFalse(wdm.is_valid(self.invalid_wikidata_3))

        wdm_file = WikidataManager(storage_manager=InMemoryStorageManager(self.test_json_path))
        self.assertTrue(wdm_file.normalise(self.valid_wikidata_1, include_prefix=True) in self.data)
        self.assertTrue(wdm_file.normalise(self.valid_wikidata_2, include_prefix=True) in self.data)
        self.assertTrue(wdm_file.normalise(self.invalid_wikidata_3, include_prefix=True) in self.data)
        self.assertTrue(wdm_file.is_valid((wdm_file.normalise(self.valid_wikidata_1, include_prefix=True))))
        self.assertTrue(wdm_file.is_valid((wdm_file.normalise(self.valid_wikidata_2, include_prefix=True))))
        self.assertFalse(wdm_file.is_valid((wdm_file.normalise(self.invalid_wikidata_3, include_prefix=True))))

        wdm_nofile_noapi = WikidataManager(storage_manager=InMemoryStorageManager(self.test_json_path), use_api_service=False)
        self.assertTrue(wdm_nofile_noapi.is_valid(self.valid_wikidata_1))
        self.assertTrue(wdm_nofile_noapi.is_valid(self.valid_wikidata_2))

    def test_wikidata_exists(self):
        with self.subTest(msg="get_extra_info=True, allow_extra_api=None"):
            wikidata_manager = WikidataManager()
            output = wikidata_manager.exists(self.valid_wikidata_1, get_extra_info=True, allow_extra_api=None)
            expected_output = (True, {'valid': True})
            self.assertEqual(output, expected_output)
        with self.subTest(msg="get_extra_info=False, allow_extra_api=None"):
            wikidata_manager = WikidataManager()
            output = wikidata_manager.exists(self.valid_wikidata_1, get_extra_info=False, allow_extra_api=None)
            expected_output = True
            self.assertEqual(output, expected_output)
        with self.subTest(msg="get_extra_info=False, allow_extra_api='None'"):
            wikidata_manager = WikidataManager()
            output = wikidata_manager.exists(self.valid_wikidata_2, get_extra_info=False, allow_extra_api=None)
            expected_output = True
            self.assertEqual(output, expected_output)
        with self.subTest(msg="get_extra_info=False, allow_extra_api='None'"):
            wikidata_manager = WikidataManager()
            output = wikidata_manager.exists(self.invalid_wikidata_1, get_extra_info=False, allow_extra_api=None)
            expected_output = False
            self.assertEqual(output, expected_output)
        with self.subTest(msg="get_extra_info=True, allow_extra_api='None'"):
            wikidata_manager = WikidataManager()
            output = wikidata_manager.exists(self.invalid_wikidata_1, get_extra_info=True, allow_extra_api=None)
            expected_output = (False, {'valid': False})
            self.assertEqual(output, expected_output)


    def test_wikidata_default(self):
        wm_nofile = WikidataManager()
        # No support files (it generates it)
        # Default storage manager : in Memory + generates file on method call (not automatically)
        # uses API
        self.assertTrue(wm_nofile.is_valid(self.valid_wikidata_1))
        self.assertTrue(wm_nofile.is_valid(self.valid_wikidata_2))
        self.assertFalse(wm_nofile.is_valid(self.invalid_wikidata_3))
        self.assertFalse(wm_nofile.is_valid(self.invalid_wikidata_1))
        wm_nofile.storage_manager.store_file()
        validated_ids = [self.valid_wikidata_1, self.valid_wikidata_2, self.invalid_wikidata_1, self.invalid_wikidata_3]
        # check that the support file was correctly created
        self.assertTrue(os.path.exists("storage/id_value.json"))
        lj = open("storage/id_value.json")
        load_dict = json.load(lj)
        lj.close()
        # check that all the validated ids are stored in the json file
        self.assertTrue(all(wm_nofile.normalise(x, include_prefix=True) in load_dict for x in validated_ids))
        wm_nofile.storage_manager.delete_storage()
        # check that the support file was correctly deleted
        self.assertFalse(os.path.exists("storage/id_value.json"))

    ##### IN-MEMORY STORAGE MANAGER

    def test_wikidata_memory_file_noapi(self):
        # Uses support file (without updating it)
        # Uses InMemoryStorageManager storage manager
        # does not use API (so a syntactically correct id is considered to be valid)
        wm_file = WikidataManager(storage_manager=InMemoryStorageManager(self.test_json_path), use_api_service=False)
        self.assertTrue(wm_file.normalise(self.valid_wikidata_1, include_prefix=True) in self.data)
        self.assertTrue(wm_file.normalise(self.valid_wikidata_2, include_prefix=True) in self.data)
        self.assertFalse(wm_file.is_valid(self.invalid_wikidata_3)) # is stored in support file as invalid
        self.assertTrue(wm_file.is_valid(wm_file.normalise(self.invalid_wikidata_2, include_prefix=True))) # is not stored in support file as invalid, does not exist but has correct syntax

    def test_wikidata_memory_file_api(self):
        # Uses support file (without updating it)
        # Uses InMemoryStorageManager storage manager
        # uses API (so a syntactically correct id which is not valid is considered to be invalid)
        wm_file = WikidataManager(storage_manager=InMemoryStorageManager(self.test_json_path), use_api_service=True)
        self.assertFalse(wm_file.is_valid(self.invalid_wikidata_2))

    def test_wikidata_memory_nofile_noapi(self):
        # Does not use support file
        # Uses InMemoryStorageManager storage manager
        # Does not use API (so a syntactically correct id which is not valid is considered to be valid)
        wm_nofile_noapi = WikidataManager(storage_manager=InMemoryStorageManager(), use_api_service=False)
        self.assertTrue(wm_nofile_noapi.is_valid(self.valid_wikidata_1))
        self.assertTrue(wm_nofile_noapi.is_valid(self.invalid_wikidata_2))
        wm_nofile_noapi.storage_manager.delete_storage()

    ##### SQLITE STORAGE MANAGER

    def test_wikidata_sqlite_nofile_api(self):
        # No support files (it generates it)
        # storage manager : SqliteStorageManager
        # uses API
        sql_wm_nofile = WikidataManager(storage_manager=SqliteStorageManager())
        self.assertTrue(sql_wm_nofile.is_valid(self.valid_wikidata_1))
        self.assertTrue(sql_wm_nofile.is_valid(self.valid_wikidata_2))
        self.assertFalse(sql_wm_nofile.is_valid(self.invalid_wikidata_2))
        self.assertFalse(sql_wm_nofile.is_valid(self.invalid_wikidata_3))
        # check that the support db was correctly created and that it contains all the validated ids
        self.assertTrue(os.path.exists("storage/id_valid_dict.db"))
        validated_ids = [self.valid_wikidata_1, self.valid_wikidata_2, self.invalid_wikidata_2, self.invalid_wikidata_3]
        all_ids_stored = sql_wm_nofile.storage_manager.get_all_keys()
        # check that all the validated ids are stored in the json file
        self.assertTrue(all(sql_wm_nofile.normalise(x, include_prefix=True) in all_ids_stored for x in validated_ids))

        sql_wm_nofile.storage_manager.delete_storage()
        # check that the support file was correctly deleted
        self.assertFalse(os.path.exists("storage/id_valid_dict.db"))

    def test_wikidata_sqlite_file_api(self):
        # Uses support file
        # Uses SqliteStorageManager storage manager
        # does not use API (so a syntactically correct id is considered to be valid)
        # db creation
        test_sqlite_db = os.path.join(self.test_dir, "database.db")
        if os.path.exists(test_sqlite_db):
            os.remove(test_sqlite_db)
        #con = sqlite3.connect(test_sqlite_db)
        #cur = con.cursor()
        to_insert = [self.invalid_wikidata_3, self.valid_wikidata_1]
        sql_file = WikidataManager(storage_manager=SqliteStorageManager(test_sqlite_db), use_api_service=True)
        for id in to_insert:
            norm_id = sql_file.normalise(id, include_prefix=True)
            is_valid = 1 if sql_file.is_valid(norm_id) else 0
            insert_tup = (norm_id, is_valid)
            sql_file.storage_manager.cur.execute( f"INSERT OR REPLACE INTO info VALUES (?,?)", insert_tup )
            sql_file.storage_manager.con.commit()
        sql_file.storage_manager.con.close()

        sql_no_api = WikidataManager(storage_manager=SqliteStorageManager(test_sqlite_db), use_api_service=False)
        all_db_keys = sql_no_api.storage_manager.get_all_keys()
        #check that all the normalised ind in the list were correctly inserted in the db
        self.assertTrue(all(sql_no_api.normalise(x,include_prefix=True) in all_db_keys for x in to_insert))
        self.assertTrue(sql_no_api.is_valid(self.valid_wikidata_1)) # is stored in support file as valid
        self.assertFalse(sql_no_api.is_valid(self.invalid_wikidata_3)) # is stored in support file as invalid
        self.assertTrue(sql_no_api.is_valid(sql_no_api.normalise(self.invalid_wikidata_2, include_prefix=True))) # is not stored in support file as invalid, does not exist but has correct syntax
        sql_no_api.storage_manager.delete_storage()

    def test_wikidata_sqlite_nofile_noapi(self):
        # Does not use support file
        # Uses SqliteStorageManager storage manager
        # Does not use API (so a syntactically correct id which is not valid is considered to be valid)
        wm_nofile_noapi = WikidataManager(storage_manager=SqliteStorageManager(), use_api_service=False)
        self.assertTrue(wm_nofile_noapi.is_valid(self.valid_wikidata_1))
        self.assertTrue(wm_nofile_noapi.is_valid(self.invalid_wikidata_2))
        wm_nofile_noapi.storage_manager.delete_storage()

    ##### REDIS STORAGE MANAGER

    def test_wikidata_redis_nofile_api(self):
        # No available data in redis db
        # Storage manager : RedisStorageManager
        # uses API
        wm_nofile = WikidataManager(storage_manager=RedisStorageManager(testing=True))
        self.assertTrue(wm_nofile.is_valid(self.valid_wikidata_1))
        self.assertTrue(wm_nofile.is_valid(self.valid_wikidata_2))

        self.assertFalse(wm_nofile.is_valid(self.invalid_wikidata_2))
        self.assertFalse(wm_nofile.is_valid(self.invalid_wikidata_3))
        # check that the redis db was correctly filled and that it contains all the validated ids

        validated_ids = {self.valid_wikidata_1, self.valid_wikidata_2, self.invalid_wikidata_2, self.invalid_wikidata_3}
        validated_ids = {wm_nofile.normalise(x, include_prefix=True) for x in validated_ids}
        all_ids_stored = wm_nofile.storage_manager.get_all_keys()
        # check that all the validated ids are stored in the json file
        self.assertEqual(validated_ids, all_ids_stored)
        wm_nofile.storage_manager.delete_storage()
        # check that the support file was correctly deleted
        self.assertEqual(wm_nofile.storage_manager.get_all_keys(), set())

    def test_wikidata_redis_file_api(self):
        # Uses data in redis db
        # Uses RedisStorageManager
        # fills db

        # use API to save validity values
        to_insert = [self.invalid_wikidata_3, self.valid_wikidata_3, self.valid_wikidata_1]
        storage_manager = RedisStorageManager(testing=True)
        redis_file = WikidataManager(storage_manager=storage_manager, use_api_service=True)
        for id in to_insert:
            norm_id = redis_file.normalise(id, include_prefix=True)
            is_valid = redis_file.is_valid(norm_id)
            # insert_tup = (norm_id, is_valid)
            redis_file.storage_manager.set_value(norm_id, is_valid)

        # does not use API, retrieve values from DB
        redis_no_api = WikidataManager(storage_manager=storage_manager, use_api_service=False)
        all_db_keys = redis_no_api.storage_manager.get_all_keys()
        # check that all the normalised ids in the list were correctly inserted in the db
        self.assertTrue(all(redis_no_api.normalise(x, include_prefix=True) in all_db_keys for x in to_insert))
        self.assertTrue(redis_no_api.is_valid(self.valid_wikidata_1))  # is stored in support file as valid
        self.assertTrue(redis_no_api.is_valid(self.valid_wikidata_3))  # is stored in support file as valid
        self.assertFalse(redis_no_api.is_valid(self.invalid_wikidata_3))  # is stored in support file as invalid
        self.assertTrue(redis_no_api.is_valid(
            self.invalid_wikidata_2))  # is not stored in support file as invalid, does not exist but has correct syntax
        redis_no_api.storage_manager.delete_storage()

    def test_wikidata_redis_nofile_noapi(self):
        # No data in redis db
        # Uses RedisStorageManager
        # Does not use API (so a syntactically correct id which is not valid is considered to be valid)
        wm_nofile_noapi = WikidataManager(storage_manager=RedisStorageManager(testing=True), use_api_service=False)
        self.assertTrue(wm_nofile_noapi.is_valid(self.valid_wikidata_2))
        self.assertTrue(wm_nofile_noapi.is_valid(self.invalid_wikidata_2))

        wm_nofile_noapi.storage_manager.delete_storage()