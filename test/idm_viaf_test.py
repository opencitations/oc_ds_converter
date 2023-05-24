from oc_ds_converter.oc_idmanager.viaf import ViafManager
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

class viafIdentifierManagerTest(unittest.TestCase):
    """This class aim at testing identifiers manager."""

    def setUp(self):
        if not exists("tmp"):
            makedirs("tmp")

        self.test_dir = join("test", "data")
        self.test_json_path = join(self.test_dir, "glob.json")
        with open(self.test_json_path, encoding="utf-8") as fp:
            self.data = json.load(fp)

        self.valid_viaf_1 = "5604148947771454950004"
        self.valid_viaf_2 = "234145033"
        self.valid_viaf_3 = "56752857"
        self.invalid_viaf_1 = "012517637138"
        self.invalid_viaf_2 = "5604148947771454953333"
        self.valid_viaf_3 = "56752857abc"
        self.invalid_viaf_4 = "5604148999953333"


    def test_exists(self):
        with self.subTest(msg="get_extra_info=True, allow_extra_api=None"):
            viaf_manager = ViafManager()
            output = viaf_manager.exists(self.valid_viaf_3, get_extra_info=True, allow_extra_api=None)
            expected_output = (True, {'id': viaf_manager.normalise(self.valid_viaf_3), 'valid': True})
            self.assertEqual(output, expected_output)
        with self.subTest(msg="get_extra_info=False, allow_extra_api=None"):
            viaf_manager = ViafManager()
            output = viaf_manager.exists(self.valid_viaf_3, get_extra_info=False, allow_extra_api=None)
            expected_output = True
            self.assertEqual(output, expected_output)
        with self.subTest(msg="get_extra_info=False, allow_extra_api='None'"):
            viaf_manager = ViafManager()
            output = viaf_manager.exists(self.valid_viaf_2, get_extra_info=False, allow_extra_api=None)
            expected_output = True
            self.assertEqual(output, expected_output)


    def test_viaf_normalise(self):
        vm = ViafManager()
        self.assertEqual(
            vm.normalise(self.valid_viaf_1),
            vm.normalise(' ' + self.valid_viaf_1),
        )
        self.assertEqual(
            vm.normalise(self.valid_viaf_1),
            vm.normalise('abc' + self.valid_viaf_1),
        )
        self.assertEqual(
            vm.normalise(self.valid_viaf_1),
            vm.normalise('\n' + self.valid_viaf_1),
        )

    def test_viaf_is_valid(self):
        vm = ViafManager()
        self.assertTrue(vm.is_valid(self.valid_viaf_1))
        self.assertTrue(vm.is_valid(self.valid_viaf_2))
        self.assertFalse(vm.is_valid(self.invalid_viaf_1))


        dm_file = ViafManager(storage_manager=InMemoryStorageManager(self.test_json_path), use_api_service=False)
        self.assertTrue(dm_file.normalise(self.valid_viaf_1, include_prefix=True) in self.data)
        self.assertTrue(dm_file.normalise(self.invalid_viaf_2, include_prefix=True) in self.data)
        self.assertTrue(dm_file.is_valid(self.valid_viaf_1))
        self.assertFalse(dm_file.is_valid(self.invalid_viaf_2))


    def test_viaf_default(self):
        am_nofile = ViafManager()
        # No support files (it generates it)
        # Default storage manager : in Memory + generates file on method call (not automatically)
        # uses API
        self.assertTrue(am_nofile.is_valid(self.valid_viaf_1))
        self.assertTrue(am_nofile.is_valid(self.valid_viaf_2))
        self.assertFalse(am_nofile.is_valid(self.invalid_viaf_2))
        self.assertFalse(am_nofile.is_valid(self.invalid_viaf_1))
        am_nofile.storage_manager.store_file()
        validated_ids = [self.valid_viaf_1, self.valid_viaf_2, self.invalid_viaf_1, self.invalid_viaf_2]
        # check that the support file was correctly created
        self.assertTrue(os.path.exists("storage/id_value.json"))
        lj = open("storage/id_value.json")
        load_dict = json.load(lj)
        lj.close()
        # check that all the validated ids are stored in the json file
        self.assertTrue(all(am_nofile.normalise(x, include_prefix=True) in load_dict for x in validated_ids))
        am_nofile.storage_manager.delete_storage()
        # check that the support file was correctly deleted
        self.assertFalse(os.path.exists("storage/id_value.json"))

    def test_viaf_memory_file_noapi(self):
        # Uses support file (without updating it)
        # Uses InMemoryStorageManager storage manager
        # does not use API (so a syntactically correct id is considered to be valid)
        am_file = ViafManager(storage_manager=InMemoryStorageManager(self.test_json_path), use_api_service=False)
        self.assertTrue(am_file.normalise(self.valid_viaf_1, include_prefix=True) in self.data)
        self.assertTrue(am_file.normalise(self.invalid_viaf_2, include_prefix=True) in self.data)
        self.assertFalse(am_file.is_valid(self.invalid_viaf_2)) # is stored in support file as invalid
        self.assertTrue(am_file.is_valid(am_file.normalise(self.invalid_viaf_4, include_prefix=True))) # is not stored in support file as invalid, does not exist but has correct syntax

    def test_viaf_memory_file_api(self):
        # Uses support file (without updating it)
        # Uses InMemoryStorageManager storage manager
        # uses API (so a syntactically correct id which is not valid is considered to be invalid)
        am_file = ViafManager(storage_manager=InMemoryStorageManager(self.test_json_path), use_api_service=True)
        self.assertFalse(am_file.is_valid(self.invalid_viaf_1))

    def test_viaf_memory_nofile_noapi(self):
        # Does not use support file
        # Uses InMemoryStorageManager storage manager
        # Does not API (so a syntactically correct id which is not valid is considered to be valid)
        am_nofile_noapi = ViafManager(storage_manager=InMemoryStorageManager(), use_api_service=False)
        self.assertTrue(am_nofile_noapi.is_valid(self.valid_viaf_1))
        self.assertTrue(am_nofile_noapi.is_valid(self.invalid_viaf_2))
        am_nofile_noapi.storage_manager.delete_storage()



    def test_viaf_sqlite_nofile_api(self):
        # No support files (it generates it)
        # storage manager : SqliteStorageManager
        # uses API
        sql_am_nofile = ViafManager(storage_manager=SqliteStorageManager())
        self.assertTrue(sql_am_nofile.is_valid(self.valid_viaf_1))
        self.assertTrue(sql_am_nofile.is_valid(self.valid_viaf_2))
        self.assertFalse(sql_am_nofile.is_valid(self.invalid_viaf_1))
        self.assertFalse(sql_am_nofile.is_valid(self.invalid_viaf_2))
        # check that the support db was correctly created and that it contains all the validated ids
        self.assertTrue(os.path.exists("storage/id_valid_dict.db"))
        validated_ids = [self.valid_viaf_1, self.valid_viaf_2, self.invalid_viaf_1, self.invalid_viaf_2]
        all_ids_stored = sql_am_nofile.storage_manager.get_all_keys()
        # check that all the validated ids are stored in the json file
        self.assertTrue(all(sql_am_nofile.normalise(x, include_prefix=True) in all_ids_stored for x in validated_ids))
        sql_am_nofile.storage_manager.delete_storage()
        # check that the support file was correctly deleted
        self.assertFalse(os.path.exists("storage/id_valid_dict.db"))

    def test_viaf_sqlite_file_api(self):
        # Uses support file
        # Uses SqliteStorageManager storage manager
        # does not use API (so a syntactically correct id is considered to be valid)
        # db creation
        test_sqlite_db = os.path.join(self.test_dir, "database.db")
        if os.path.exists(test_sqlite_db):
            os.remove(test_sqlite_db)
        con = sqlite3.connect(test_sqlite_db)
        cur = con.cursor()
        to_insert = [self.invalid_viaf_1, self.valid_viaf_1]
        sql_file = ViafManager(storage_manager=SqliteStorageManager(test_sqlite_db), use_api_service=True)
        for id in to_insert:
            norm_id = sql_file.normalise(id, include_prefix=True)
            is_valid = 1 if sql_file.is_valid(norm_id) else 0
            insert_tup = (norm_id, is_valid)
            cur.execute(f"INSERT OR REPLACE INTO info VALUES (?,?)", insert_tup)
            con.commit()
        sql_no_api = ViafManager(storage_manager=SqliteStorageManager(test_sqlite_db), use_api_service=False)
        all_db_keys = sql_no_api.storage_manager.get_all_keys()
        #check that all the normalised ind in the list were correctly inserted in the db
        self.assertTrue(all(sql_no_api.normalise(x,include_prefix=True) in all_db_keys for x in to_insert))
        self.assertTrue(sql_no_api.is_valid(self.valid_viaf_1)) # is stored in support file as valid
        self.assertFalse(sql_no_api.is_valid(self.invalid_viaf_1)) # is stored in support file as invalid
        self.assertTrue(sql_no_api.is_valid(sql_no_api.normalise("10.1109/5.771073FAKE_ID", include_prefix=True))) # is not stored in support file as invalid, does not exist but has correct syntax
        sql_no_api.storage_manager.delete_storage()

    def test_viaf_sqlite_nofile_noapi(self):
        # Does not use support file
        # Uses SqliteStorageManager storage manager
        # Does not use API (so a syntactically correct id which is not valid is considered to be valid)
        am_nofile_noapi = ViafManager(storage_manager=SqliteStorageManager(), use_api_service=False)
        self.assertTrue(am_nofile_noapi.is_valid(self.valid_viaf_1))
        self.assertTrue(am_nofile_noapi.is_valid(self.invalid_viaf_2))
        am_nofile_noapi.storage_manager.delete_storage()
