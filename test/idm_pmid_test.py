from oc_ds_converter.oc_idmanager.pmid import PMIDManager
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

        pm_file = PMIDManager(use_api_service=False, storage_manager=InMemoryStorageManager(self.test_json_path))
        self.assertTrue(pm_file.normalise(self.valid_pmid_1, include_prefix=True) in self.data)
        self.assertTrue(pm_file.normalise(self.invalid_pmid_1, include_prefix=True) in self.data)
        self.assertTrue(pm_file.is_valid(self.valid_pmid_1))
        self.assertFalse(pm_file.is_valid(self.invalid_pmid_1))

        pm_nofile_noapi = PMIDManager(storage_manager=InMemoryStorageManager(self.test_json_path), use_api_service=False)
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

        dm_file = PMIDManager(storage_manager=InMemoryStorageManager(self.test_json_path), use_api_service=False)
        self.assertTrue(dm_file.normalise(self.valid_pmid_1, include_prefix=True) in self.data)
        self.assertTrue(dm_file.normalise(self.invalid_pmid_2, include_prefix=True) in self.data)
        self.assertTrue(dm_file.is_valid(self.valid_pmid_1))
        self.assertFalse(dm_file.is_valid(self.invalid_pmid_2))


    def test_pmid_default(self):
        am_nofile = PMIDManager()
        # No support files (it generates it)
        # Default storage manager : in Memory + generates file on method call (not automatically)
        # uses API
        self.assertTrue(am_nofile.is_valid(self.valid_pmid_1))
        self.assertTrue(am_nofile.is_valid(self.valid_pmid_2))
        self.assertFalse(am_nofile.is_valid(self.invalid_pmid_2))
        self.assertFalse(am_nofile.is_valid(self.invalid_pmid_1))
        am_nofile.storage_manager.store_file()
        validated_ids = [self.valid_pmid_1, self.valid_pmid_2, self.invalid_pmid_1, self.invalid_pmid_2]
        validated = [am_nofile.normalise(x, include_prefix=True) for x in validated_ids if am_nofile.normalise(x, include_prefix=True)]
        # check that the support file was correctly created
        self.assertTrue(os.path.exists("storage/id_value.json"))
        lj = open("storage/id_value.json")
        load_dict = json.load(lj)
        lj.close()
        stored = [am_nofile.normalise(x, include_prefix=True) for x in load_dict if am_nofile.normalise(x, include_prefix=True)]

        # check that all the validated ids are stored in the json file
        self.assertTrue(all(x in stored for x in validated))
        am_nofile.storage_manager.delete_storage()
        # check that the support file was correctly deleted
        self.assertFalse(os.path.exists("storage/id_value.json"))

    def test_pmid_memory_file_noapi(self):
        # Uses support file (without updating it)
        # Uses InMemoryStorageManager storage manager
        # does not use API (so a syntactically correct id is considered to be valid)
        am_file = PMIDManager(storage_manager=InMemoryStorageManager(self.test_json_path), use_api_service=False)
        self.assertTrue(am_file.normalise(self.valid_pmid_1, include_prefix=True) in self.data)
        self.assertTrue(am_file.normalise(self.invalid_pmid_2, include_prefix=True) in self.data)
        self.assertFalse(am_file.is_valid(self.invalid_pmid_2)) # is stored in support file as invalid
        self.assertTrue(am_file.is_valid(am_file.normalise(self.invalid_pmid_3, include_prefix=True))) # is not stored in support file as invalid, does not exist but has correct syntax

    def test_pmid_memory_file_api(self):
        # Uses support file (without updating it)
        # Uses InMemoryStorageManager storage manager
        # uses API (so a syntactically correct id which is not valid is considered to be invalid)
        am_file = PMIDManager(storage_manager=InMemoryStorageManager(self.test_json_path), use_api_service=True)
        self.assertFalse(am_file.is_valid(self.invalid_pmid_1))

    def test_pmid_memory_nofile_noapi(self):
        # Does not use support file
        # Uses InMemoryStorageManager storage manager
        # Does not use API (so a syntactically correct id which is not valid is considered to be valid)
        am_nofile_noapi = PMIDManager(storage_manager=InMemoryStorageManager(), use_api_service=False)
        self.assertTrue(am_nofile_noapi.is_valid(self.valid_pmid_1))
        self.assertTrue(am_nofile_noapi.is_valid(self.invalid_pmid_1))
        am_nofile_noapi.storage_manager.delete_storage()



    def test_pmid_sqlite_nofile_api(self):
        # No support files (it generates it)
        # storage manager : SqliteStorageManager
        # uses API
        sql_am_nofile = PMIDManager(storage_manager=SqliteStorageManager())
        self.assertTrue(sql_am_nofile.is_valid(self.valid_pmid_1))
        self.assertTrue(sql_am_nofile.is_valid(self.valid_pmid_2))
        self.assertFalse(sql_am_nofile.is_valid(self.invalid_pmid_1))
        self.assertFalse(sql_am_nofile.is_valid(self.invalid_pmid_2))
        # check that the support db was correctly created and that it contains all the validated ids
        self.assertTrue(os.path.exists("storage/id_valid_dict.db"))
        validated_ids = [self.valid_pmid_1, self.valid_pmid_2, self.invalid_pmid_1, self.invalid_pmid_2]
        all_ids_stored = sql_am_nofile.storage_manager.get_all_keys()
        # check that all the validated ids are stored in the json file
        stored = [x for x in all_ids_stored]
        validated = [sql_am_nofile.normalise(x, include_prefix=True) for x in validated_ids if sql_am_nofile.normalise(x, include_prefix=True)]
        self.assertTrue(all(x in stored for x in validated))
        sql_am_nofile.storage_manager.delete_storage()
        # check that the support file was correctly deleted
        self.assertFalse(os.path.exists("storage/id_valid_dict.db"))

    def test_pmid_sqlite_file_api(self):
        # Uses support file
        # Uses SqliteStorageManager storage manager
        # does not use API (so a syntactically correct id is considered to be valid)
        # db creation
        test_sqlite_db = os.path.join(self.test_dir, "database.db")
        if os.path.exists(test_sqlite_db):
            os.remove(test_sqlite_db)
        #con = sqlite3.connect(test_sqlite_db)
        #cur = con.cursor()
        to_insert = [self.invalid_pmid_1, self.valid_pmid_1]
        sql_file = PMIDManager(storage_manager=SqliteStorageManager(test_sqlite_db), use_api_service=True)
        for id in to_insert:
            norm_id = sql_file.normalise(id, include_prefix=True)
            is_valid = 1 if sql_file.is_valid(norm_id) else 0
            insert_tup = (norm_id, is_valid)
            sql_file.storage_manager.cur.execute( f"INSERT OR REPLACE INTO info VALUES (?,?)", insert_tup )
            sql_file.storage_manager.con.commit()
        sql_file.storage_manager.con.close()

        sql_no_api = PMIDManager(storage_manager=SqliteStorageManager(test_sqlite_db), use_api_service=False)
        all_db_keys = sql_no_api.storage_manager.get_all_keys()
        #check that all the normalised ind in the list were correctly inserted in the db
        self.assertTrue(all(sql_no_api.normalise(x,include_prefix=True) in all_db_keys for x in to_insert))
        self.assertTrue(sql_no_api.is_valid(self.valid_pmid_1)) # is stored in support file as valid
        self.assertFalse(sql_no_api.is_valid(self.invalid_pmid_1)) # is stored in support file as invalid
        self.assertTrue(sql_no_api.is_valid(sql_no_api.normalise(self.invalid_pmid_2, include_prefix=True))) # is not stored in support file as invalid, does not exist but has correct syntax
        sql_no_api.storage_manager.delete_storage()

    def test_pmid_sqlite_nofile_noapi(self):
        # Does not use support file
        # Uses SqliteStorageManager storage manager
        # Does not use API (so a syntactically correct id which is not valid is considered to be valid)
        am_nofile_noapi = PMIDManager(storage_manager=SqliteStorageManager(), use_api_service=False)
        self.assertTrue(am_nofile_noapi.is_valid(self.valid_pmid_1))
        self.assertTrue(am_nofile_noapi.is_valid(self.invalid_pmid_2))
        am_nofile_noapi.storage_manager.delete_storage()
