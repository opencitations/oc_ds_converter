import json
import unittest
from os import makedirs
from os.path import exists, join

from oc_ds_converter.oc_idmanager.openalex import OpenAlexManager

class OpenAlexIdentifierManagerTest(unittest.TestCase):
    """This class aim at testing identifiers manager."""

    def setUp(self):
        if not exists("tmp"):
            makedirs("tmp")

        self.test_dir = join("test", "data")
        self.test_json_path = join(self.test_dir, "glob.json")
        with open(self.test_json_path, encoding="utf-8") as fp:
            self.data = json.load(fp)

        self.valid_wid = "W2013228336"
        self.valid_sid = "S4210229581"
        self.invalid_wid = "W7836728310"
        self.invalid_sid = "S4263287381"

    def test_openalex_is_valid(self):
        oalm_nofile = OpenAlexManager()
        self.assertTrue(oalm_nofile.is_valid(self.valid_wid))
        self.assertTrue(oalm_nofile.is_valid(self.valid_sid))
        self.assertFalse(oalm_nofile.is_valid(self.invalid_wid))
        self.assertFalse(oalm_nofile.is_valid(self.invalid_sid))

        oalm_file = OpenAlexManager(use_api_service=False, testing=True)
        # Pre-seed storage with data from glob.json
        for key, value in self.data.items():
            if key.startswith("openalex:"):
                oalm_file.storage_manager.set_value(key, value.get("valid", False))
        self.assertTrue(oalm_file.normalise(self.valid_wid, include_prefix=True) in self.data)
        self.assertTrue(oalm_file.normalise(self.invalid_wid, include_prefix=True) in self.data)
        self.assertTrue(oalm_file.is_valid(self.valid_wid))
        self.assertFalse(oalm_file.is_valid(self.invalid_wid))

        oalm_nofile_noapi = OpenAlexManager(testing=True, use_api_service=False)
        self.assertTrue(oalm_nofile_noapi.is_valid(self.valid_wid))
        self.assertTrue(oalm_nofile_noapi.is_valid(self.valid_sid))

    def test_exists(self):
        with self.subTest(msg="get_extra_info=True, allow_extra_api=None"):
            oalm = OpenAlexManager()
            output = oalm.exists('openalex:W748315831', get_extra_info=True, allow_extra_api=None)
            expected_output = (True, {'valid': True})
            self.assertEqual(expected_output[0], output[0])
            # self.assertCountEqual({k:v for k,v in expected_output[1].items() if k!= "author"}, {k:v for k,v in output[1].items() if k!= "author"})
            # self.assertCountEqual(expected_output[1]["author"], output[1]["author"])

        with self.subTest(msg="get_extra_info=False, allow_extra_api=None"):
            oalm = OpenAlexManager()
            output = oalm.exists('S4210229581', get_extra_info=False, allow_extra_api=None)
            expected_output = True
            self.assertEqual(output, expected_output)


    def test_openalex_normalise(self):
        oalm = OpenAlexManager()

        self.assertEqual(
            self.valid_wid, oalm.normalise("openalex:" + self.valid_wid)
        )
        self.assertEqual(
            self.valid_wid, oalm.normalise(self.valid_wid.replace("", " "))
        )
        self.assertEqual(
            self.valid_wid,
            oalm.normalise("https://openalex.org/" + self.valid_wid),
        )
        self.assertEqual(
            oalm.normalise(self.valid_wid),
            oalm.normalise(' ' + self.valid_wid),
        )
        self.assertEqual(
            oalm.normalise(self.valid_sid),
            oalm.normalise("https://api.openalex.org/sources/" + self.valid_sid),
        )

        dm_file = OpenAlexManager(testing=True, use_api_service=False)
        # Pre-seed storage with data from glob.json
        for key, value in self.data.items():
            if key.startswith("openalex:"):
                dm_file.storage_manager.set_value(key, value.get("valid", False))
        self.assertTrue(dm_file.normalise(self.valid_wid, include_prefix=True) in self.data)
        self.assertTrue(dm_file.normalise(self.invalid_sid, include_prefix=True) in self.data)
        self.assertTrue(dm_file.is_valid(self.valid_wid))
        self.assertFalse(dm_file.is_valid(self.invalid_sid))

    def test_openalex_default(self):
        mngr = OpenAlexManager(testing=True)
        # Uses RedisStorageManager with testing=True (fakeredis)
        # uses API
        self.assertTrue(mngr.is_valid(self.valid_wid))
        self.assertTrue(mngr.is_valid(self.valid_sid))
        self.assertFalse(mngr.is_valid(self.invalid_sid))
        self.assertFalse(mngr.is_valid(self.invalid_wid))
        validated_ids = [self.valid_wid, self.valid_sid, self.invalid_wid, self.invalid_sid]
        validated = [mngr.normalise(x, include_prefix=True) for x in validated_ids if mngr.normalise(x, include_prefix=True)]
        # check that all the validated ids are stored in redis
        all_ids_stored = mngr.storage_manager.get_all_keys()
        self.assertTrue(all(x in all_ids_stored for x in validated))
        mngr.storage_manager.delete_storage()
        # check that the storage was correctly deleted
        self.assertEqual(mngr.storage_manager.get_all_keys(), set())

    def test_openalex_memory_file_noapi(self):
        # Uses pre-seeded data (without updating it)
        # Uses RedisStorageManager storage manager
        # does not use API (so a syntactically correct id is considered to be valid)
        am_file = OpenAlexManager(testing=True, use_api_service=False)
        # Pre-seed storage with data from glob.json
        for key, value in self.data.items():
            if key.startswith("openalex:"):
                am_file.storage_manager.set_value(key, value.get("valid", False))
        self.assertTrue(am_file.normalise(self.valid_wid, include_prefix=True) in self.data)
        self.assertTrue(am_file.normalise(self.invalid_sid, include_prefix=True) in self.data)
        self.assertFalse(am_file.is_valid(self.invalid_sid))  # is stored as invalid

    def test_openalex_memory_file_api(self):
        # Uses support file (without updating it)
        # Uses RedisStorageManager storage manager
        # uses API (so a syntactically correct id which is not valid is considered to be invalid)
        am_file = OpenAlexManager(testing=True, use_api_service=True)
        self.assertFalse(am_file.is_valid(self.invalid_wid))

    def test_openalex_memory_nofile_noapi(self):
        # Does not use support file
        # Uses RedisStorageManager storage manager
        # Does not use API (so a syntactically correct id which is not valid is considered to be valid)
        am_nofile_noapi = OpenAlexManager(testing=True, use_api_service=False)
        self.assertTrue(am_nofile_noapi.is_valid(self.valid_wid))
        self.assertTrue(am_nofile_noapi.is_valid(self.invalid_wid))
        am_nofile_noapi.storage_manager.delete_storage()

    def test_openalex_sqlite_nofile_api(self):
        # No pre-existing data
        # storage manager : RedisStorageManager
        # uses API
        sql_am_nofile = OpenAlexManager(testing=True)
        self.assertTrue(sql_am_nofile.is_valid(self.valid_wid))
        self.assertTrue(sql_am_nofile.is_valid(self.valid_sid))
        self.assertFalse(sql_am_nofile.is_valid(self.invalid_wid))
        self.assertFalse(sql_am_nofile.is_valid(self.invalid_sid))
        # check that the redis storage contains all the validated ids
        validated_ids = [self.valid_wid, self.valid_sid, self.invalid_wid, self.invalid_sid]
        all_ids_stored = sql_am_nofile.storage_manager.get_all_keys()
        validated = [sql_am_nofile.normalise(x, include_prefix=True) for x in validated_ids if sql_am_nofile.normalise(x, include_prefix=True)]
        self.assertTrue(all(x in all_ids_stored for x in validated))
        sql_am_nofile.storage_manager.delete_storage()
        # check that the storage was correctly deleted
        self.assertEqual(sql_am_nofile.storage_manager.get_all_keys(), set())

    def test_openalex_sqlite_file_api(self):
        # Uses pre-existing data in Redis
        # Uses RedisStorageManager storage manager
        # tests validation behavior with pre-seeded data
        to_insert = [self.invalid_wid, self.valid_wid]
        sql_file = OpenAlexManager(testing=True, use_api_service=True)
        for oalid in to_insert:
            norm_id = sql_file.normalise(oalid, include_prefix=True)
            is_valid = sql_file.is_valid(norm_id)
            sql_file.storage_manager.set_value(norm_id, is_valid)

        sql_no_api = OpenAlexManager(testing=True, use_api_service=False)
        # Copy values from the first manager to the second for testing
        for oalid in to_insert:
            norm_id = sql_no_api.normalise(oalid, include_prefix=True)
            value = sql_file.storage_manager.get_value(norm_id)
            if value is not None:
                sql_no_api.storage_manager.set_value(norm_id, value)
        all_db_keys = sql_no_api.storage_manager.get_all_keys()
        # check that all the normalised ids in the list were correctly inserted
        self.assertTrue(all(sql_no_api.normalise(x, include_prefix=True) in all_db_keys for x in to_insert))
        self.assertTrue(sql_no_api.is_valid(self.valid_wid))  # is stored as valid
        self.assertFalse(sql_no_api.is_valid(self.invalid_wid))  # is stored as invalid
        self.assertTrue(sql_no_api.is_valid(sql_no_api.normalise(self.invalid_sid, include_prefix=True)))  # not stored, has correct syntax
        sql_no_api.storage_manager.delete_storage()

    def test_openalex_sqlite_nofile_noapi(self):
        # Does not use support file
        # Uses RedisStorageManager storage manager
        # Does not use API (so a syntactically correct id which is not valid is considered to be valid)
        am_nofile_noapi = OpenAlexManager(testing=True, use_api_service=False)
        self.assertTrue(am_nofile_noapi.is_valid(self.valid_wid))
        self.assertTrue(am_nofile_noapi.is_valid(self.invalid_sid))
        am_nofile_noapi.storage_manager.delete_storage()
