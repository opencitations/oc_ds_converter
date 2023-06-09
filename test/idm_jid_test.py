import json
import sqlite3
import os.path
import unittest
from os import makedirs
from os.path import exists, join

from oc_ds_converter.oc_idmanager import *
from oc_ds_converter.oc_idmanager.jid import JIDManager
from oc_ds_converter.oc_idmanager.oc_data_storage.sqlite_manager import SqliteStorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.in_memory_manager import InMemoryStorageManager


class JidIdentifierManagerTest(unittest.TestCase):
    """This class aim at testing jid identifiers manager."""

    def setUp(self):
        if not exists("tmp"):
            makedirs("tmp")

        self.test_dir = join("test", "data")
        self.test_json_path = join(self.test_dir, "glob.json")
        with open(self.test_json_path, encoding="utf-8") as fp:
            self.data = json.load(fp)

        self.valid_jid_1 = "otoljpn1970"
        self.valid_jid_2 = "jscej1944b"
        self.valid_jid_3 = "japeoj"  # SYS_ERR_009
        self.invalid_jid_1 = "hjmee"
        self.invalid_jid_2 = "gee1973e" # saved in glob as invalid

    def test_jid_normalise(self):
        jm = JIDManager()
        self.assertEqual(
            self.valid_jid_1, jm.normalise(self.valid_jid_1.replace("", " "))
        )
        self.assertEqual(
            self.valid_jid_2, jm.normalise("jid:" + self.valid_jid_2)
        )

    def test_jid_syntax_ok(self):
        jm = JIDManager()
        self.assertTrue(jm.syntax_ok(self.valid_jid_1))
        self.assertTrue(jm.syntax_ok(self.invalid_jid_1))
        self.assertFalse(jm.syntax_ok('1950' + self.valid_jid_1))

    def test_jid_is_valid(self):
        jm = JIDManager()
        self.assertTrue(jm.is_valid(self.valid_jid_1))
        self.assertTrue(jm.is_valid(self.valid_jid_2))
        self.assertFalse(jm.is_valid(self.invalid_jid_1))
        self.assertFalse(jm.is_valid(self.invalid_jid_2))

        jm_file = JIDManager(storage_manager=InMemoryStorageManager(self.test_json_path))
        self.assertTrue(jm_file.normalise(self.valid_jid_1, include_prefix=True) in self.data)
        self.assertTrue(jm_file.normalise(self.valid_jid_2, include_prefix=True) in self.data)
        self.assertTrue(jm_file.normalise(self.valid_jid_3, include_prefix=True) in self.data)
        self.assertTrue(jm_file.is_valid(jm_file.normalise(self.valid_jid_1, include_prefix=True)))
        self.assertTrue(jm_file.is_valid(jm_file.normalise(self.valid_jid_2, include_prefix=True)))
        self.assertTrue(jm_file.is_valid(jm_file.normalise(self.valid_jid_3, include_prefix=True)))

        jm_nofile_noapi = JIDManager(storage_manager=InMemoryStorageManager(self.test_json_path), use_api_service=False)
        self.assertTrue(jm_nofile_noapi.is_valid(self.valid_jid_1))
        self.assertTrue(jm_nofile_noapi.is_valid(self.invalid_jid_1))

    def test_jid_exists(self):
        with self.subTest(msg="get_extra_info = True, allow_extra_api=None"):
            jm = JIDManager()
            output = jm.exists(self.valid_jid_1, get_extra_info=True, allow_extra_api=None)
            expected_output = (True, {"valid": True})
            self.assertEqual(output, expected_output)
        with self.subTest(msg="get_extra_info = True, allow_extra_api=None"):
            jm = JIDManager()
            output = jm.exists(self.valid_jid_3, get_extra_info=True, allow_extra_api=None)
            expected_output = (True, {"valid": True})
            self.assertEqual(output, expected_output)
        with self.subTest(msg="get_extra_info = True, allow_extra_api=None"):
            jm = JIDManager()
            output = jm.exists(self.invalid_jid_1, get_extra_info=True, allow_extra_api=None)
            expected_output = (False, {"valid": False})
            self.assertEqual(output, expected_output)

    def test_jid_default(self):
        jm_nofile = JIDManager()
        # No support files (it generates it)
        # Default storage manager : in Memory + generates file on method call (not automatically)
        # uses API
        self.assertTrue(jm_nofile.is_valid(self.valid_jid_1))
        self.assertTrue(jm_nofile.is_valid(self.valid_jid_2))
        self.assertTrue(jm_nofile.is_valid(self.valid_jid_3))
        self.assertFalse(jm_nofile.is_valid(self.invalid_jid_1))
        self.assertFalse(jm_nofile.is_valid(self.invalid_jid_2))
        jm_nofile.storage_manager.store_file()
        validated_ids = [self.valid_jid_1, self.valid_jid_2, self.valid_jid_3, self.invalid_jid_1, self.invalid_jid_2]
        # check that the support file was correctly created
        self.assertTrue(os.path.exists("storage/id_value.json"))
        lj = open("storage/id_value.json")
        load_dict = json.load(lj)
        lj.close()
        # check that all the validated ids are stored in the json file
        self.assertTrue(all(jm_nofile.normalise(x, include_prefix=True) in load_dict for x in validated_ids))
        jm_nofile.storage_manager.delete_storage()
        # check that the support file was correctly deleted
        self.assertFalse(os.path.exists("storage/id_value.json"))

    def test_jid_memory_file_noapi(self):
        # Uses support file (without updating it)
        # Uses InMemoryStorageManager storage manager
        # does not use API (so a syntactically correct id is considered to be valid)
        jm_file = JIDManager(storage_manager=InMemoryStorageManager(self.test_json_path), use_api_service=False)
        self.assertTrue(jm_file.normalise(self.valid_jid_1.replace("", " "), include_prefix=True) in self.data)
        self.assertTrue(jm_file.normalise("jid:" + self.valid_jid_2, include_prefix=True) in self.data)
        self.assertTrue(jm_file.is_valid(self.valid_jid_1))
        self.assertFalse(jm_file.is_valid(self.invalid_jid_2)) # is stored in support file as invalid
        self.assertTrue(jm_file.is_valid("jid:pjab1978")) # is not stored in support file as invalid, does not exist but has correct syntax

    def test_jid_memory_file_api(self):
        # Uses support file (without updating it)
        # Uses InMemoryStorageManager storage manager
        # uses API (so a syntactically correct id which is not valid is considered to be invalid)
        jm_file = JIDManager(storage_manager=InMemoryStorageManager(self.test_json_path), use_api_service=True)
        self.assertFalse(jm_file.is_valid("jid:pjab1978"))

    def test_jid_memory_nofile_noapi(self):
        # Does not use support file
        # Uses InMemoryStorageManager storage manager
        # Does not use API (so a syntactically correct id which is not valid is considered to be valid)
        jm_nofile_noapi = JIDManager(storage_manager=InMemoryStorageManager(), use_api_service=False)
        self.assertTrue(jm_nofile_noapi.is_valid(self.valid_jid_1))
        self.assertTrue(jm_nofile_noapi.is_valid(self.invalid_jid_1))
        jm_nofile_noapi.storage_manager.delete_storage()

    def test_jid_sqlite_nofile_api(self):
        # No support files (it generates it)
        # Uses SqliteStorageManager
        # uses API
        sql_jm_nofile = JIDManager(storage_manager=SqliteStorageManager())
        self.assertTrue(sql_jm_nofile.is_valid(self.valid_jid_1))
        self.assertTrue(sql_jm_nofile.is_valid(self.valid_jid_2))
        self.assertTrue(sql_jm_nofile.is_valid(self.valid_jid_3))
        self.assertFalse(sql_jm_nofile.is_valid(self.invalid_jid_1))
        self.assertFalse(sql_jm_nofile.is_valid(self.invalid_jid_2))
        # check that the support db was correctly created and that it contains all the validated ids
        self.assertTrue(os.path.exists("storage/id_valid_dict.db"))
        validated_ids = [self.valid_jid_1, self.valid_jid_2, self.valid_jid_3, self.invalid_jid_1, self.invalid_jid_2]
        all_ids_stored = sql_jm_nofile.storage_manager.get_all_keys()
        # check that all the validated ids are stored in the json file
        self.assertTrue(all(sql_jm_nofile.normalise(x, include_prefix=True) in all_ids_stored for x in validated_ids))
        sql_jm_nofile.storage_manager.delete_storage()
        # check that the support file was correctly deleted
        self.assertFalse(os.path.exists("storage/id_valid_dict.db"))

    def test_arxiv_sqlite_file_api(self):
        # Uses support file
        # Uses SqliteStorageManager
        # does not use API (so a syntactically correct id is considered to be valid)
        # db creation
        test_sqlite_db = os.path.join(self.test_dir, "database.db")
        if os.path.exists(test_sqlite_db):
            os.remove(test_sqlite_db)
        to_insert = [self.invalid_jid_1, self.valid_jid_1, self.valid_jid_3]
        sql_file = JIDManager(storage_manager=SqliteStorageManager(test_sqlite_db), use_api_service=True)
        for id in to_insert:
            norm_id = sql_file.normalise(id, include_prefix=True)
            is_valid = 1 if sql_file.is_valid(norm_id) else 0
            insert_tup = (norm_id, is_valid)
            sql_file.storage_manager.cur.execute(f"INSERT OR REPLACE INTO info VALUES (?,?)", insert_tup)
            sql_file.storage_manager.con.commit()
        sql_file.storage_manager.con.close()

        sql_no_api = JIDManager(storage_manager=SqliteStorageManager(test_sqlite_db), use_api_service=False)
        all_db_keys = sql_no_api.storage_manager.get_all_keys()
        # check that all the normalised ind in the list were correctly inserted in the db
        self.assertTrue(all(sql_no_api.normalise(x, include_prefix=True) in all_db_keys for x in to_insert))
        self.assertTrue(sql_no_api.is_valid(self.valid_jid_1)) # is stored in support file as valid
        self.assertTrue(sql_no_api.is_valid(self.valid_jid_3)) # is stored in support file as valid
        self.assertFalse(sql_no_api.is_valid(self.invalid_jid_1)) # is stored in support file as invalid
        self.assertTrue(sql_no_api.is_valid("jid:pjab1978")) # is not stored in support file as invalid, does not exist but has correct syntax
        sql_no_api.storage_manager.delete_storage()

    def test_arxiv_sqlite_nofile_noapi(self):
        # Does not use support file
        # Uses SqliteStorageManager
        # Does not use API (so a syntactically correct id which is not valid is considered to be valid)
        jm_nofile_noapi = JIDManager(storage_manager=SqliteStorageManager(), use_api_service=False)
        self.assertTrue(jm_nofile_noapi.is_valid(self.valid_jid_1))
        self.assertTrue(jm_nofile_noapi.is_valid(self.invalid_jid_1))
        jm_nofile_noapi.storage_manager.delete_storage()
        