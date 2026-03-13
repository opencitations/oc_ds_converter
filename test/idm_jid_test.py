import json
import unittest
from os import makedirs, remove
from os.path import exists, join

from oc_ds_converter.oc_idmanager.jid import JIDManager
from oc_ds_converter.oc_idmanager.oc_data_storage.sqlite_manager import SqliteStorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.in_memory_manager import InMemoryStorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.redis_manager import RedisStorageManager


class JidIdentifierManagerTest(unittest.TestCase):
    """This class aim at testing jid identifiers manager."""

    def setUp(self):
        if not exists("tmp"):
            makedirs("tmp")

        self.test_dir = join("test","data")
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

        jm_file = JIDManager(testing=True)
        self.assertTrue(jm_file.normalise(self.valid_jid_1, include_prefix=True) in self.data)
        self.assertTrue(jm_file.normalise(self.valid_jid_2, include_prefix=True) in self.data)
        self.assertTrue(jm_file.normalise(self.valid_jid_3, include_prefix=True) in self.data)
        self.assertTrue(jm_file.is_valid(jm_file.normalise(self.valid_jid_1, include_prefix=True)))
        self.assertTrue(jm_file.is_valid(jm_file.normalise(self.valid_jid_2, include_prefix=True)))
        self.assertTrue(jm_file.is_valid(jm_file.normalise(self.valid_jid_3, include_prefix=True)))

        jm_nofile_noapi = JIDManager(testing=True, use_api_service=False)
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
        jm_nofile = JIDManager(testing=True)
        # Uses RedisStorageManager with testing=True (fakeredis)
        # uses API
        self.assertTrue(jm_nofile.is_valid(self.valid_jid_1))
        self.assertTrue(jm_nofile.is_valid(self.valid_jid_2))
        self.assertTrue(jm_nofile.is_valid(self.valid_jid_3))
        self.assertFalse(jm_nofile.is_valid(self.invalid_jid_1))
        self.assertFalse(jm_nofile.is_valid(self.invalid_jid_2))
        validated_ids = [self.valid_jid_1, self.valid_jid_2, self.valid_jid_3, self.invalid_jid_1, self.invalid_jid_2]
        # check that all the validated ids are stored in redis
        all_ids_stored = jm_nofile.storage_manager.get_all_keys()
        self.assertTrue(all(jm_nofile.normalise(x, include_prefix=True) in all_ids_stored for x in validated_ids))
        jm_nofile.storage_manager.delete_storage()
        # check that the storage was correctly deleted
        self.assertEqual(jm_nofile.storage_manager.get_all_keys(), set())

    #### IN MEMORY STORAGE MANAGER

    def test_jid_memory_file_noapi(self):
        # Uses pre-seeded data (without updating it)
        # Uses RedisStorageManager storage manager
        # does not use API (so a syntactically correct id is considered to be valid)
        jm_file = JIDManager(testing=True, use_api_service=False)
        # Pre-seed storage with data from glob.json
        for key, value in self.data.items():
            if key.startswith("jid:"):
                jm_file.storage_manager.set_value(key, value.get("valid", False))
        self.assertTrue(jm_file.normalise(self.valid_jid_1.replace("", " "), include_prefix=True) in self.data)
        self.assertTrue(jm_file.normalise("jid:" + self.valid_jid_2, include_prefix=True) in self.data)
        self.assertTrue(jm_file.is_valid(self.valid_jid_1))
        self.assertFalse(jm_file.is_valid(self.invalid_jid_2))  # is stored as invalid
        self.assertTrue(jm_file.is_valid("jid:pjab1978"))  # not stored as invalid, has correct syntax

    def test_jid_memory_file_api(self):
        # Uses support file (without updating it)
        # Uses RedisStorageManager storage manager
        # uses API (so a syntactically correct id which is not valid is considered to be invalid)
        jm_file = JIDManager(testing=True, use_api_service=True)
        self.assertFalse(jm_file.is_valid("jid:pjab1978"))

    def test_jid_memory_nofile_noapi(self):
        # Does not use support file
        # Uses RedisStorageManager storage manager
        # Does not use API (so a syntactically correct id which is not valid is considered to be valid)
        jm_nofile_noapi = JIDManager(testing=True, use_api_service=False)
        self.assertTrue(jm_nofile_noapi.is_valid(self.valid_jid_1))
        self.assertTrue(jm_nofile_noapi.is_valid(self.invalid_jid_1))
        jm_nofile_noapi.storage_manager.delete_storage()

    #### SQLITE STORAGE MANAGER

    def test_jid_sqlite_nofile_api(self):
        # No pre-existing data
        # Uses RedisStorageManager
        # uses API
        sql_jm_nofile = JIDManager(testing=True)
        self.assertTrue(sql_jm_nofile.is_valid(self.valid_jid_1))
        self.assertTrue(sql_jm_nofile.is_valid(self.valid_jid_2))
        self.assertTrue(sql_jm_nofile.is_valid(self.valid_jid_3))
        self.assertFalse(sql_jm_nofile.is_valid(self.invalid_jid_1))
        self.assertFalse(sql_jm_nofile.is_valid(self.invalid_jid_2))
        # check that the redis storage contains all the validated ids
        validated_ids = [self.valid_jid_1, self.valid_jid_2, self.valid_jid_3, self.invalid_jid_1, self.invalid_jid_2]
        all_ids_stored = sql_jm_nofile.storage_manager.get_all_keys()
        self.assertTrue(all(sql_jm_nofile.normalise(x, include_prefix=True) in all_ids_stored for x in validated_ids))
        sql_jm_nofile.storage_manager.delete_storage()
        # check that the storage was correctly deleted
        self.assertEqual(sql_jm_nofile.storage_manager.get_all_keys(), set())

    def test_jid_sqlite_file_api(self):
        # Uses support file
        # Uses SqliteStorageManager
        # does not use API (so a syntactically correct id is considered to be valid)
        # db creation
        test_sqlite_db = join(self.test_dir, "database.db")
        if exists(test_sqlite_db):
            remove(test_sqlite_db)
        to_insert = [self.invalid_jid_1, self.valid_jid_1, self.valid_jid_3]
        sql_file = JIDManager(testing=True, use_api_service=True)
        for jid in to_insert:
            norm_id = sql_file.normalise(jid, include_prefix=True)
            is_valid = sql_file.is_valid(norm_id)
            sql_file.storage_manager.set_value(norm_id, is_valid)

        sql_no_api = JIDManager(testing=True, use_api_service=False)
        # Copy values from the first manager to the second for testing
        for jid in to_insert:
            norm_id = sql_no_api.normalise(jid, include_prefix=True)
            value = sql_file.storage_manager.get_value(norm_id)
            if value is not None:
                sql_no_api.storage_manager.set_value(norm_id, value)
        all_db_keys = sql_no_api.storage_manager.get_all_keys()
        # check that all the normalised ids in the list were correctly inserted
        self.assertTrue(all(sql_no_api.normalise(x, include_prefix=True) in all_db_keys for x in to_insert))
        self.assertTrue(sql_no_api.is_valid(self.valid_jid_1))  # is stored as valid
        self.assertTrue(sql_no_api.is_valid(self.valid_jid_3))  # is stored as valid
        self.assertFalse(sql_no_api.is_valid(self.invalid_jid_1))  # is stored as invalid
        self.assertTrue(sql_no_api.is_valid("jid:pjab1978"))  # not stored as invalid, has correct syntax
        sql_no_api.storage_manager.delete_storage()

    def test_jid_sqlite_nofile_noapi(self):
        # Does not use support file
        # Uses RedisStorageManager
        # Does not use API (so a syntactically correct id which is not valid is considered to be valid)
        jm_nofile_noapi = JIDManager(testing=True, use_api_service=False)
        self.assertTrue(jm_nofile_noapi.is_valid(self.valid_jid_1))
        self.assertTrue(jm_nofile_noapi.is_valid(self.invalid_jid_1))
        jm_nofile_noapi.storage_manager.delete_storage()

    #### REDIS STORAGE MANAGER
    def test_jid_redis_nofile_api(self):
        # No available data in redis db
        # Storage manager : RedisStorageManager
        # uses API
        jm_nofile = JIDManager(storage_manager=RedisStorageManager(testing=True))
        self.assertTrue(jm_nofile.is_valid(self.valid_jid_1))
        self.assertTrue(jm_nofile.is_valid(self.valid_jid_2))
        self.assertTrue(jm_nofile.is_valid(self.valid_jid_3))
        self.assertFalse(jm_nofile.is_valid(self.invalid_jid_1))
        self.assertFalse(jm_nofile.is_valid(self.invalid_jid_2))
        # check that the redis db was correctly filled and that it contains all the validated ids

        validated_ids = {self.valid_jid_1, self.valid_jid_2, self.valid_jid_3, self.invalid_jid_1,
                         self.invalid_jid_2}
        validated_ids = {jm_nofile.normalise(x, include_prefix=True) for x in validated_ids}
        all_ids_stored = jm_nofile.storage_manager.get_all_keys()
        # check that all the validated ids are stored in the json file
        self.assertEqual(validated_ids, all_ids_stored)
        jm_nofile.storage_manager.delete_storage()
        # check that the support file was correctly deleted
        self.assertEqual(jm_nofile.storage_manager.get_all_keys(), set())

    def test_jid_redis_file_api(self):
        # Uses data in redis db
        # Uses RedisStorageManager
        # fills db

        #use API to save validity values
        to_insert = [self.invalid_jid_1, self.valid_jid_1, self.valid_jid_3]
        storage_manager = RedisStorageManager(testing=True)
        redis_file = JIDManager(storage_manager=storage_manager, use_api_service=True)
        for id in to_insert:
            norm_id = redis_file.normalise(id, include_prefix=True)
            is_valid = redis_file.is_valid(norm_id)
            # insert_tup = (norm_id, is_valid)
            redis_file.storage_manager.set_value(norm_id, is_valid)

        # does not use API, retrieve values from DB
        redis_no_api = JIDManager(storage_manager=storage_manager, use_api_service=False)
        all_db_keys = redis_no_api.storage_manager.get_all_keys()
        # check that all the normalised ids in the list were correctly inserted in the db
        self.assertTrue(all(redis_no_api.normalise(x, include_prefix=True) in all_db_keys for x in to_insert))
        self.assertTrue(redis_no_api.is_valid(self.valid_jid_1))  # is stored in support file as valid
        self.assertTrue(redis_no_api.is_valid(self.valid_jid_2))  # is stored in support file as valid
        self.assertFalse(redis_no_api.is_valid(self.invalid_jid_1))  # is stored in support file as invalid
        self.assertTrue(redis_no_api.is_valid(self.invalid_jid_2))  # is not stored in support file as invalid, does not exist but has correct syntax
        redis_no_api.storage_manager.delete_storage()

    def test_jid_redis_nofile_noapi(self):
        # No data in redis db
        # Uses RedisStorageManager
        # Does not use API (so a syntactically correct id which is not valid is considered to be valid)
        jm_nofile_noapi = JIDManager(storage_manager=RedisStorageManager(testing=True), use_api_service=False)
        self.assertTrue(jm_nofile_noapi.is_valid(self.valid_jid_1))
        self.assertTrue(jm_nofile_noapi.is_valid(self.invalid_jid_1))

        jm_nofile_noapi.storage_manager.delete_storage()