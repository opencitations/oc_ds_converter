import unittest
from oc_ds_converter.oc_idmanager.oc_data_storage.redis_manager import RedisStorageManager


class TestRedisStorageManager(unittest.TestCase):

    def test_storage_management_testing(self):
        rsm = RedisStorageManager(testing=True)
        rsm.set_value("pmid:9", True)
        rsm.set_value("pmid:0", False)
        # test that set value and get_all_keys work as expected
        self.assertCountEqual(rsm.get_all_keys(), {"pmid:9", "pmid:0"})

        #test delete_storage
        rsm.delete_storage()
        self.assertCountEqual(rsm.get_all_keys(), {})



if __name__ == '__main__':
    unittest.main()
