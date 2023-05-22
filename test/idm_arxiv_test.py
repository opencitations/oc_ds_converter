#!python
# Copyright 2019, Silvio Peroni <essepuntato@gmail.com>
# Copyright 2022, Giuseppe Grieco <giuseppe.grieco3@unibo.it>, Arianna Moretti <arianna.moretti4@unibo.it>, Elia Rizzetto <elia.rizzetto@studio.unibo.it>, Arcangelo Massari <arcangelo.massari@unibo.it>
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.


import json
import sqlite3
import os.path
import unittest
from os import makedirs
from os.path import exists, join

from oc_ds_converter.oc_idmanager import *
from oc_ds_converter.oc_idmanager.arxiv import ArXivManager
from oc_ds_converter.oc_idmanager.jid import JIDManager
from oc_ds_converter.oc_idmanager.url import URLManager
from oc_ds_converter.oc_idmanager.oc_data_storage.sqlite_manager import SqliteStorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.in_memory_manager import InMemoryStorageManager


class ArxivIdentifierManagerTest(unittest.TestCase):
    """This class aim at testing identifiers manager."""

    def setUp(self):
        if not exists("tmp"):
            makedirs("tmp")

        self.test_dir = join("test", "data")
        self.test_json_path = join(self.test_dir, "glob.json")
        with open(self.test_json_path, encoding="utf-8") as fp:
            self.data = json.load(fp)

        self.valid_arxiv_1 = "arXiv:2109.05583"
        self.valid_arxiv_1v = "2109.05583v2"
        self.valid_arxiv_2 = "arXiv:2109.05582"
        self.valid_arx_U_S = "2109.05583V2  "
        self.invalid_arxiv_1 = "1133.5582"
        self.invalid_arxiv_2v = "2109.05583v23"


    def test_arxiv_default(self):
        am_nofile = ArXivManager()
        # No support files (it generates it)
        # Default storage manager : in Memory + generates file on method call (not automatically)
        # uses API
        self.assertTrue(am_nofile.is_valid(self.valid_arxiv_1))
        self.assertTrue(am_nofile.is_valid(self.valid_arxiv_2))
        self.assertTrue(am_nofile.is_valid(self.valid_arxiv_1v))
        self.assertFalse(am_nofile.is_valid(self.invalid_arxiv_1))
        self.assertFalse(am_nofile.is_valid(self.invalid_arxiv_2v))
        am_nofile.storage_manager.store_file()
        validated_ids = [self.valid_arxiv_1, self.valid_arxiv_2, self.valid_arxiv_1v, self.invalid_arxiv_1, self.invalid_arxiv_2v]
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

    def test_arxiv_memory_file_noapi(self):
        # Uses support file (without updating it)
        # Uses InMemoryStorageManager storage manager
        # does not use API (so a syntactically correct id is considered to be valid)
        am_file = ArXivManager(storage_manager=InMemoryStorageManager(self.test_json_path), use_api_service=False)
        self.assertTrue(am_file.normalise(self.valid_arxiv_1.lower(), include_prefix=True) in self.data)
        self.assertTrue(am_file.normalise(self.valid_arx_U_S.strip().lower(), include_prefix=True) in self.data)
        self.assertTrue(am_file.normalise(self.invalid_arxiv_1.strip().lower(), include_prefix=True) in self.data)
        self.assertTrue(am_file.is_valid(self.valid_arxiv_1))
        self.assertFalse(am_file.is_valid(self.invalid_arxiv_1)) # is stored in support file as invalid
        self.assertTrue(am_file.is_valid("arxiv:2229.00851")) # is not stored in support file as invalid, does not exist but has correct syntax

    def test_arxiv_memory_file_api(self):
        # Uses support file (without updating it)
        # Uses InMemoryStorageManager storage manager
        # uses API (so a syntactically correct id which is not valid is considered to be invalid)
        am_file = ArXivManager(storage_manager=InMemoryStorageManager(self.test_json_path), use_api_service=True)
        self.assertFalse(am_file.is_valid(self.invalid_arxiv_1))

    def test_arxiv_memory_nofile_noapi(self):
        # Does not use support file
        # Uses InMemoryStorageManager storage manager
        # Does not API (so a syntactically correct id which is not valid is considered to be valid)
        am_nofile_noapi = ArXivManager(storage_manager=InMemoryStorageManager(), use_api_service=False)
        self.assertTrue(am_nofile_noapi.is_valid(self.valid_arxiv_1v))
        self.assertTrue(am_nofile_noapi.is_valid(self.invalid_arxiv_1))
        am_nofile_noapi.storage_manager.delete_storage()



    def test_arxiv_sqlite_nofile_api(self):
        # No support files (it generates it)
        # Default storage manager : SqliteStorageManager
        # uses API
        sql_am_nofile = ArXivManager(storage_manager=SqliteStorageManager())
        self.assertTrue(sql_am_nofile.is_valid(self.valid_arxiv_1))
        self.assertTrue(sql_am_nofile.is_valid(self.valid_arxiv_2))
        self.assertTrue(sql_am_nofile.is_valid(self.valid_arxiv_1v))
        self.assertFalse(sql_am_nofile.is_valid(self.invalid_arxiv_1))
        self.assertFalse(sql_am_nofile.is_valid(self.invalid_arxiv_2v))
        # check that the support db was correctly created and that it contains all the validated ids
        self.assertTrue(os.path.exists("storage/id_valid_dict.db"))
        validated_ids = [self.valid_arxiv_1, self.valid_arxiv_2, self.valid_arxiv_1v, self.invalid_arxiv_1, self.invalid_arxiv_2v]
        all_ids_stored = sql_am_nofile.storage_manager.get_all_keys()
        # check that all the validated ids are stored in the json file
        self.assertTrue(all(sql_am_nofile.normalise(x, include_prefix=True) in all_ids_stored for x in validated_ids))
        sql_am_nofile.storage_manager.delete_storage()
        # check that the support file was correctly deleted
        self.assertFalse(os.path.exists("storage/id_valid_dict.db"))

    def test_arxiv_sqlite_file_api(self):
        # Uses support file
        # Uses SqliteStorageManager storage manager
        # does not use API (so a syntactically correct id is considered to be valid)
        # db creation
        test_sqlite_db = os.path.join(self.test_dir, "database.db")
        if os.path.exists(test_sqlite_db):
            os.remove(test_sqlite_db)
        con = sqlite3.connect(test_sqlite_db)
        cur = con.cursor()
        to_insert = [self.invalid_arxiv_1, self.valid_arxiv_1, self.valid_arx_U_S]
        sql_file = ArXivManager(storage_manager=SqliteStorageManager(test_sqlite_db), use_api_service=True)
        for id in to_insert:
            norm_id = sql_file.normalise(id, include_prefix=True)
            is_valid = 1 if sql_file.is_valid(norm_id) else 0
            insert_tup = (norm_id, is_valid)
            cur.execute(f"INSERT OR REPLACE INTO info VALUES (?,?)", insert_tup)
            con.commit()
        sql_no_api = ArXivManager(storage_manager=SqliteStorageManager(test_sqlite_db), use_api_service=False)
        all_db_keys = sql_no_api.storage_manager.get_all_keys()
        #check that all the normalised ind in the list were correctly inserted in the db
        self.assertTrue(all(sql_no_api.normalise(x,include_prefix=True) in all_db_keys for x in to_insert))
        self.assertTrue(sql_no_api.is_valid(self.valid_arxiv_1)) # is stored in support file as valid
        self.assertTrue(sql_no_api.is_valid(self.valid_arx_U_S)) # is stored in support file as valid
        self.assertFalse(sql_no_api.is_valid(self.invalid_arxiv_1)) # is stored in support file as invalid
        self.assertTrue(sql_no_api.is_valid("arxiv:2229.00851")) # is not stored in support file as invalid, does not exist but has correct syntax
        sql_no_api.storage_manager.delete_storage()

    def test_arxiv_sqlite_nofile_noapi(self):
        # Does not use support file
        # Uses SqliteStorageManager storage manager
        # Does not API (so a syntactically correct id which is not valid is considered to be valid)
        am_nofile_noapi = ArXivManager(storage_manager=SqliteStorageManager(), use_api_service=False)
        self.assertTrue(am_nofile_noapi.is_valid(self.valid_arxiv_1v))
        self.assertTrue(am_nofile_noapi.is_valid(self.invalid_arxiv_1))
        am_nofile_noapi.storage_manager.delete_storage()