import json
import re
import unittest
from os import makedirs
from os.path import exists, join

from oc_ds_converter.oc_idmanager.orcid import ORCIDManager


class orcidIdentifierManagerTest(unittest.TestCase):
    """This class aim at testing identifiers manager."""

    def setUp(self):
        if not exists("tmp"):
            makedirs("tmp")

        self.test_dir = join("test", "data")
        self.test_json_path = join(self.test_dir, "glob.json")
        with open(self.test_json_path, encoding="utf-8") as fp:
            self.data = json.load(fp)

        self.valid_orcid_1 = "0000-0003-0530-4305"
        self.valid_orcid_2 = "0000-0001-5506-523X"
        self.invalid_orcid_1 = "0000-0003-0530-430C"
        self.invalid_orcid_2 = "0000-0001-5506-5232"
        self.invalid_orcid_3 = "0000-0001-5506-523"
        self.invalid_orcid_4 = "1-5506-5232"
        self.invalid_orcid_5 = "0000-0001-2345-6789"


    def test_orcid_normalise(self):
        om = ORCIDManager()
        self.assertEqual(
            self.valid_orcid_1, om.normalise(self.valid_orcid_1.replace("-", "  "))
        )
        self.assertEqual(
            self.valid_orcid_1, om.normalise("https://orcid.org/" + self.valid_orcid_1)
        )
        self.assertEqual(
            self.valid_orcid_2, om.normalise(self.valid_orcid_2.replace("-", "  "))
        )
        self.assertEqual(
            self.invalid_orcid_3, om.normalise(self.invalid_orcid_3.replace("-", "  "))
        )

    def test_orcid_is_valid(self):
        om = ORCIDManager()
        self.assertTrue(om.is_valid(self.valid_orcid_1))
        self.assertTrue(om.is_valid(self.valid_orcid_2))
        self.assertFalse(om.is_valid(self.invalid_orcid_1))
        self.assertFalse(om.is_valid(self.invalid_orcid_2))
        self.assertFalse(om.is_valid(self.invalid_orcid_3))
        self.assertFalse(om.is_valid(self.invalid_orcid_4))

        om_file = ORCIDManager(testing=True, use_api_service=False)
        self.assertTrue(om_file.normalise(self.valid_orcid_1, include_prefix=True) in self.data)
        self.assertTrue(om_file.normalise(self.valid_orcid_2, include_prefix=True) in self.data)
        self.assertTrue(om_file.is_valid(om_file.normalise(self.valid_orcid_1, include_prefix=True)))
        self.assertTrue(om_file.is_valid(om_file.normalise(self.valid_orcid_2, include_prefix=True)))

        om_nofile_noapi = ORCIDManager(use_api_service=False)
        self.assertTrue(om_nofile_noapi.is_valid(self.valid_orcid_1))
        self.assertTrue(om_nofile_noapi.is_valid(self.valid_orcid_2))

    def test_exists(self):
        with self.subTest(msg="get_extra_info=True, allow_extra_api=None"):
            orcid_manager = ORCIDManager()
            output = orcid_manager.exists(self.valid_orcid_2, get_extra_info=True, allow_extra_api=None)
            self.assertTrue(output[0])  # Check if exists
            info = output[1]
            self.assertEqual(info['id'], '0000-0001-5506-523X')
            self.assertTrue(info['valid'])
            self.assertEqual(info['family_name'], 'Shotton')
            self.assertEqual(info['given_name'], 'David')
            self.assertEqual(info['email'], "")
            self.assertEqual(info['external_identifiers'], {})
            self.assertEqual(info['submission_date'], '2012-10-31')
            # Check if update_date is a valid date string and not earlier than submission_date
            self.assertTrue(re.match(r'\d{4}-\d{2}-\d{2}', info['update_date']))
            self.assertGreaterEqual(info['update_date'], info['submission_date'])
        with self.subTest(msg="get_extra_info=False, allow_extra_api=None"):
            orcid_manager = ORCIDManager()
            output = orcid_manager.exists(orcid_manager.normalise(self.valid_orcid_1), get_extra_info=False, allow_extra_api=None)
            expected_output = True
            self.assertEqual(output, expected_output)
        with self.subTest(msg="get_extra_info=False, allow_extra_api='None'"):
            orcid_manager = ORCIDManager()
            output = orcid_manager.exists(self.invalid_orcid_5, get_extra_info=False, allow_extra_api='None')
            expected_output = False
            self.assertEqual(output, expected_output)

    def test_orcid_default(self):
        am_nofile = ORCIDManager(testing=True)
        # Uses RedisStorageManager with testing=True (fakeredis)
        # uses API
        self.assertTrue(am_nofile.is_valid(self.valid_orcid_1))
        self.assertTrue(am_nofile.is_valid(self.valid_orcid_2))
        self.assertFalse(am_nofile.is_valid(self.invalid_orcid_2))
        self.assertFalse(am_nofile.is_valid(self.invalid_orcid_1))
        validated_ids = [self.valid_orcid_1, self.valid_orcid_2, self.invalid_orcid_1, self.invalid_orcid_2]
        # check that all the validated ids are stored in redis
        all_ids_stored = am_nofile.storage_manager.get_all_keys()
        self.assertTrue(all(am_nofile.normalise(x, include_prefix=True) in all_ids_stored for x in validated_ids))
        am_nofile.storage_manager.delete_storage()
        # check that the storage was correctly deleted
        self.assertEqual(am_nofile.storage_manager.get_all_keys(), set())

    def test_orcid_memory_file_noapi(self):
        # Uses support file (without updating it)
        # Uses RedisStorageManager storage manager
        # does not use API (so a syntactically correct id is considered to be valid)
        am_file = ORCIDManager(testing=True, use_api_service=False)
        self.assertTrue(am_file.normalise(self.valid_orcid_1, include_prefix=True) in self.data)
        self.assertTrue(am_file.normalise(self.invalid_orcid_2, include_prefix=True) in self.data)
        self.assertFalse(am_file.is_valid(self.invalid_orcid_2)) # is stored in support file as invalid
        self.assertTrue(am_file.is_valid(am_file.normalise(self.invalid_orcid_5, include_prefix=True))) # is not stored in support file as invalid, does not exist but has correct syntax

    def test_orcid_memory_file_api(self):
        # Uses support file (without updating it)
        # Uses RedisStorageManager storage manager
        # uses API (so a syntactically correct id which is not valid is considered to be invalid)
        am_file = ORCIDManager(testing=True, use_api_service=True)
        self.assertFalse(am_file.is_valid(self.invalid_orcid_1))

    def test_orcid_memory_nofile_noapi(self):
        # Does not use support file
        # Uses RedisStorageManager storage manager
        # Does not API (so a syntactically correct id which is not valid is considered to be valid)
        am_nofile_noapi = ORCIDManager(testing=True, use_api_service=False)
        self.assertTrue(am_nofile_noapi.is_valid(self.valid_orcid_1))
        self.assertTrue(am_nofile_noapi.is_valid(self.invalid_orcid_5))
        am_nofile_noapi.storage_manager.delete_storage()



    def test_orcid_sqlite_nofile_api(self):
        # No pre-existing data
        # storage manager : RedisStorageManager
        # uses API
        sql_am_nofile = ORCIDManager(testing=True)
        self.assertTrue(sql_am_nofile.is_valid(self.valid_orcid_1))
        self.assertTrue(sql_am_nofile.is_valid(self.valid_orcid_2))
        self.assertFalse(sql_am_nofile.is_valid(self.invalid_orcid_1))
        self.assertFalse(sql_am_nofile.is_valid(self.invalid_orcid_2))
        # check that the redis storage contains all the validated ids
        validated_ids = [self.valid_orcid_1, self.valid_orcid_2, self.invalid_orcid_1, self.invalid_orcid_2]
        all_ids_stored = sql_am_nofile.storage_manager.get_all_keys()
        # check that all the validated ids are stored
        self.assertTrue(all(sql_am_nofile.normalise(x, include_prefix=True) in all_ids_stored for x in validated_ids))
        sql_am_nofile.storage_manager.delete_storage()
        # check that the storage was correctly deleted
        self.assertEqual(sql_am_nofile.storage_manager.get_all_keys(), set())

    def test_orcid_sqlite_file_api(self):
        # Uses pre-existing data in Redis
        # Uses RedisStorageManager storage manager
        # tests validation behavior with pre-seeded data
        to_insert = [self.invalid_orcid_1, self.valid_orcid_1]
        sql_file = ORCIDManager(testing=True, use_api_service=True)
        for id in to_insert:
            norm_id = sql_file.normalise(id, include_prefix=True)
            is_valid = sql_file.is_valid(norm_id)
            sql_file.storage_manager.set_value(norm_id, is_valid)

        sql_no_api = ORCIDManager(testing=True, use_api_service=False)
        # Copy values from the first manager to the second for testing
        for id in to_insert:
            norm_id = sql_no_api.normalise(id, include_prefix=True)
            value = sql_file.storage_manager.get_value(norm_id)
            if value is not None:
                sql_no_api.storage_manager.set_value(norm_id, value)
        all_db_keys = sql_no_api.storage_manager.get_all_keys()
        # check that all the normalised ids in the list were correctly inserted
        self.assertTrue(all(sql_no_api.normalise(x, include_prefix=True) in all_db_keys for x in to_insert))
        self.assertTrue(sql_no_api.is_valid(self.valid_orcid_1))  # is stored as valid
        self.assertFalse(sql_no_api.is_valid(self.invalid_orcid_1))  # is stored as invalid
        self.assertTrue(sql_no_api.is_valid(sql_no_api.normalise(self.invalid_orcid_5, include_prefix=True)))  # not stored as invalid, has correct syntax
        sql_no_api.storage_manager.delete_storage()

    def test_orcid_sqlite_nofile_noapi(self):
        # Does not use support file
        # Uses RedisStorageManager storage manager
        # Does not use API (so a syntactically correct id which is not valid is considered to be valid)
        am_nofile_noapi = ORCIDManager(testing=True, use_api_service=False)
        self.assertTrue(am_nofile_noapi.is_valid(self.valid_orcid_1))
        self.assertTrue(am_nofile_noapi.is_valid(self.invalid_orcid_5))
        am_nofile_noapi.storage_manager.delete_storage()



    #### REDIS STORAGE MANAGER
    def test_orcid_redis_nofile_api(self):
        # No available data in redis db
        # Storage manager : RedisStorageManager
        # uses API
        sql_am_nofile = ORCIDManager(testing=True)
        self.assertTrue(sql_am_nofile.is_valid(self.valid_orcid_1))
        self.assertTrue(sql_am_nofile.is_valid(self.valid_orcid_2))
        self.assertFalse(sql_am_nofile.is_valid(self.invalid_orcid_1))
        self.assertFalse(sql_am_nofile.is_valid(self.invalid_orcid_2))
        # check that the redis db was correctly filled and that it contains all the validated ids

        validated_ids = {self.valid_orcid_1, self.valid_orcid_2, self.invalid_orcid_1, self.invalid_orcid_2}
        validated_ids = {sql_am_nofile.normalise(x, include_prefix=True) for x in validated_ids}
        all_ids_stored = sql_am_nofile.storage_manager.get_all_keys()
        # check that all the validated ids are stored in the json file
        self.assertEqual(validated_ids, all_ids_stored)
        sql_am_nofile.storage_manager.delete_storage()
        # check that the support file was correctly deleted
        self.assertEqual(sql_am_nofile.storage_manager.get_all_keys(), set())

    def test_orcid_redis_file_api(self):
        # Uses data in redis db
        # Uses RedisStorageManager
        # does not use API (so a syntactically correct id is considered to be valid)
        # fills db

        to_insert = [self.invalid_orcid_1, self.valid_orcid_1]
        sql_file = ORCIDManager(testing=True, use_api_service=True)
        for id in to_insert:
            norm_id = sql_file.normalise(id, include_prefix=True)
            is_valid = sql_file.is_valid(norm_id)
            sql_file.storage_manager.set_value(norm_id,is_valid)

        sql_no_api = ORCIDManager(testing=True, use_api_service=False)
        # Copy values from the first manager to the second for testing
        for id in to_insert:
            norm_id = sql_no_api.normalise(id, include_prefix=True)
            value = sql_file.storage_manager.get_value(norm_id)
            if value is not None:
                sql_no_api.storage_manager.set_value(norm_id, value)
        all_db_keys = sql_no_api.storage_manager.get_all_keys()
        #check that all the normalised ids in the list were correctly inserted in the db
        self.assertTrue(all(sql_no_api.normalise(x,include_prefix=True) in all_db_keys for x in to_insert))

        self.assertTrue(sql_no_api.is_valid(self.valid_orcid_1)) # is stored in support file as valid
        self.assertFalse(sql_no_api.is_valid(self.invalid_orcid_1)) # is stored in support file as invalid
        self.assertTrue(sql_no_api.is_valid(sql_no_api.normalise(self.invalid_orcid_5, include_prefix=True))) # is not stored in support file as invalid, does not exist but has correct syntax
        sql_no_api.storage_manager.delete_storage()

    def test_orcid_redis_nofile_noapi(self):
        # No data in redis db
        # Uses RedisStorageManager
        # Does not API (so a syntactically correct id which is not valid is considered to be valid)
        am_nofile_noapi = ORCIDManager(testing=True, use_api_service=False)
        self.assertTrue(am_nofile_noapi.is_valid(self.valid_orcid_1))
        self.assertTrue(am_nofile_noapi.is_valid(self.invalid_orcid_5))
        am_nofile_noapi.storage_manager.delete_storage()



