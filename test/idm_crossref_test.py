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

class CrossrefIdentifierManagerTest(unittest.TestCase):
    """This class aim at testing identifiers manager."""

    def setUp(self):
        if not exists("tmp"):
            makedirs("tmp")

        self.test_dir = join("test", "data")
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

        crmngr_file = CrossrefManager(use_api_service=False, storage_manager=InMemoryStorageManager(self.test_json_path))
        self.assertTrue(crmngr_file.normalise(self.valid_crmid1, include_prefix=True) in self.data)
        self.assertTrue(crmngr_file.normalise(self.invalid_crmid1, include_prefix=True) in self.data)
        self.assertTrue(crmngr_file.is_valid(self.valid_crmid1))
        self.assertFalse(crmngr_file.is_valid(self.invalid_crmid1))

        crmngr_nofile_noapi = CrossrefManager(storage_manager=InMemoryStorageManager(self.test_json_path), use_api_service=False)
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

        dm_file = CrossrefManager(storage_manager=InMemoryStorageManager(self.test_json_path), use_api_service=False)
        self.assertTrue(dm_file.normalise(self.valid_crmid1, include_prefix=True) in self.data)
        self.assertTrue(dm_file.normalise(self.invalid_crmid2, include_prefix=True) in self.data)
        self.assertTrue(dm_file.is_valid(self.valid_crmid1))
        self.assertFalse(dm_file.is_valid(self.invalid_crmid2))

    def test_crossref_default(self):
        mngr = CrossrefManager()
        # No support files (it generates it)
        # Default storage manager : in Memory + generates file on method call (not automatically)
        # uses API
        self.assertTrue(mngr.is_valid(self.valid_crmid1))
        self.assertTrue(mngr.is_valid(self.valid_crmid2))
        self.assertFalse(mngr.is_valid(self.invalid_crmid2))
        self.assertFalse(mngr.is_valid(self.invalid_crmid1))
        mngr.storage_manager.store_file()
        validated_ids = [self.valid_crmid1, self.valid_crmid2, self.invalid_crmid1, self.invalid_crmid2]
        validated = [mngr.normalise(x, include_prefix=True) for x in validated_ids if mngr.normalise(x, include_prefix=True)]
        # check that the support file was correctly created
        self.assertTrue(os.path.exists("storage/id_value.json"))
        lj = open("storage/id_value.json")
        load_dict = json.load(lj)
        lj.close()
        stored = [mngr.normalise(x, include_prefix=True) for x in load_dict if mngr.normalise(x, include_prefix=True)]

        # check that all the validated ids are stored in the json file
        self.assertTrue(all(x in stored for x in validated))
        mngr.storage_manager.delete_storage()
        # check that the support file was correctly deleted
        self.assertFalse(os.path.exists("storage/id_value.json"))

    def test_crossref_memory_file_noapi(self):
        # Uses support file (without updating it)
        # Uses InMemoryStorageManager storage manager
        # does not use API (so a syntactically correct id is considered to be valid)
        am_file = CrossrefManager(storage_manager=InMemoryStorageManager(self.test_json_path), use_api_service=False)
        self.assertTrue(am_file.normalise(self.valid_crmid1, include_prefix=True) in self.data)
        self.assertTrue(am_file.normalise(self.invalid_crmid2, include_prefix=True) in self.data)
        self.assertFalse(am_file.is_valid(self.invalid_crmid2))  # is stored in support file as invalid
        # self.assertTrue(am_file.is_valid(am_file.normalise(self.invalid_wid, include_prefix=True))) # is not stored in support file as invalid, does not exist but has correct syntax

    def test_crossref_memory_file_api(self):
        # Uses support file (without updating it)
        # Uses InMemoryStorageManager storage manager
        # uses API (so a syntactically correct id which is not valid is considered to be invalid)
        am_file = CrossrefManager(storage_manager=InMemoryStorageManager(self.test_json_path), use_api_service=True)
        self.assertFalse(am_file.is_valid(self.invalid_crmid1))

    def test_crossref_memory_nofile_noapi(self):
        # Does not use support file
        # Uses InMemoryStorageManager storage manager
        # Does not use API (so a syntactically correct id which is not valid is considered to be valid)
        am_nofile_noapi = CrossrefManager(storage_manager=InMemoryStorageManager(), use_api_service=False)
        self.assertTrue(am_nofile_noapi.is_valid(self.valid_crmid1))
        self.assertTrue(am_nofile_noapi.is_valid(self.invalid_crmid1))
        am_nofile_noapi.storage_manager.delete_storage()

    def test_crossref_sqlite_nofile_api(self):
        # No support files (it generates it)
        # storage manager : SqliteStorageManager
        # uses API
        sql_am_nofile = CrossrefManager(storage_manager=SqliteStorageManager())
        self.assertTrue(sql_am_nofile.is_valid(self.valid_crmid1))
        self.assertTrue(sql_am_nofile.is_valid(self.valid_crmid2))
        self.assertFalse(sql_am_nofile.is_valid(self.invalid_crmid1))
        self.assertFalse(sql_am_nofile.is_valid(self.invalid_crmid2))
        # check that the support db was correctly created and that it contains all the validated ids
        self.assertTrue(os.path.exists("storage/id_valid_dict.db"))
        validated_ids = [self.valid_crmid1, self.valid_crmid2, self.invalid_crmid1, self.invalid_crmid2]
        all_ids_stored = sql_am_nofile.storage_manager.get_all_keys()
        # check that all the validated ids are stored in the json file
        stored = [x for x in all_ids_stored]
        validated = [sql_am_nofile.normalise(x, include_prefix=True) for x in validated_ids if sql_am_nofile.normalise(x, include_prefix=True)]
        self.assertTrue(all(x in stored for x in validated))
        sql_am_nofile.storage_manager.delete_storage()
        # check that the support file was correctly deleted
        self.assertFalse(os.path.exists("storage/id_valid_dict.db"))

    def test_crossref_sqlite_file_api(self):
        # Uses support file
        # Uses SqliteStorageManager storage manager
        # does not use API (so a syntactically correct id is considered to be valid)
        # db creation
        test_sqlite_db = os.path.join(self.test_dir, "database.db")
        if os.path.exists(test_sqlite_db):
            os.remove(test_sqlite_db)
        #con = sqlite3.connect(test_sqlite_db)
        #cur = con.cursor()
        to_insert = [self.invalid_crmid1, self.valid_crmid1]
        sql_file = CrossrefManager(storage_manager=SqliteStorageManager(test_sqlite_db), use_api_service=True)
        for id in to_insert:
            norm_id = sql_file.normalise(id, include_prefix=True)
            is_valid = 1 if sql_file.is_valid(norm_id) else 0
            insert_tup = (norm_id, is_valid)
            sql_file.storage_manager.cur.execute(f"INSERT OR REPLACE INTO info VALUES (?,?)", insert_tup)
            sql_file.storage_manager.con.commit()
        sql_file.storage_manager.con.close()

        sql_no_api = CrossrefManager(storage_manager=SqliteStorageManager(test_sqlite_db), use_api_service=False)
        all_db_keys = sql_no_api.storage_manager.get_all_keys()
        #check that all the normalised ind in the list were correctly inserted in the db
        self.assertTrue(all(sql_no_api.normalise(x, include_prefix=True) in all_db_keys for x in to_insert))
        self.assertTrue(sql_no_api.is_valid(self.valid_crmid1))  # is stored in support file as valid
        self.assertFalse(sql_no_api.is_valid(self.invalid_crmid1))  # is stored in support file as invalid
        self.assertTrue(sql_no_api.is_valid(sql_no_api.normalise(self.invalid_crmid2, include_prefix=True))) # is not stored in support file as invalid, does not exist but has correct syntax
        sql_no_api.storage_manager.delete_storage()

    def test_crossref_sqlite_nofile_noapi(self):
        # Does not use support file
        # Uses SqliteStorageManager storage manager
        # Does not use API (so a syntactically correct id which is not valid is considered to be valid)
        am_nofile_noapi = CrossrefManager(storage_manager=SqliteStorageManager(), use_api_service=False)
        self.assertTrue(am_nofile_noapi.is_valid(self.valid_crmid1))
        self.assertTrue(am_nofile_noapi.is_valid(self.invalid_crmid2))
        am_nofile_noapi.storage_manager.delete_storage()
