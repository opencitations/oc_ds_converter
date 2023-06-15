import unittest
from oc_ds_converter.oc_idmanager.oc_data_storage.redis_manager import RedisStorageManager


class TestRedisStorageManager(unittest.TestCase):

    def test_storage_management_testing(self):
        rsm = RedisStorageManager(testing=True)
        rsm.set_value("pmid:9", True)
        rsm.set_value("pmid:0", False)

        # test set_value
        # test get_all_keys
        self.assertCountEqual(rsm.get_all_keys(), {"pmid:9", "pmid:0"})

        #test delete_storage
        rsm.delete_storage()
        self.assertCountEqual(rsm.get_all_keys(), {})

        #test set_multi_value
        rsm.set_multi_value([("pmid:1020", True), ("pmid:2020", False)])
        self.assertCountEqual(rsm.get_all_keys(), {"pmid:1020", "pmid:2020"})

        #test get_value
        get_val = rsm.get_value("pmid:1020")
        exp = True
        self.assertEqual(get_val, exp)

        get_val = rsm.get_value("pmid:2020")
        exp = False
        self.assertEqual(get_val, exp)

        get_val = rsm.get_value("pmid:3020")
        exp = None
        self.assertEqual(get_val, exp)

        #test set_full_value

        rsm.set_full_value("pmid:1212", {"valid":True})
        get_val = rsm.get_value("pmid:1212")
        exp = True
        self.assertEqual(get_val, exp)

        rsm.delete_storage()
        emp_set = set()
        self.assertEqual(emp_set, rsm.get_all_keys())

    def test_storage_management_no_testing(self):
        try:
            rsm = RedisStorageManager(testing=False)
            rsm.set_value("TEST VALUE", False)
            run_test = True
        except:
            run_test = False
            print("test skipped: 'test_storage_management_no_testing': Connect to redis before running the test")

        if run_test:
            rsm.del_value("TEST VALUE")
            if not len(rsm.get_all_keys()):
                rsm.set_value("pmid:9", True)
                rsm.set_value("pmid:0", False)

                # test set_value
                # test get_all_keys
                self.assertCountEqual(rsm.get_all_keys(), {"pmid:9", "pmid:0"})

                #test delete_storage
                rsm.delete_storage()
                self.assertCountEqual(rsm.get_all_keys(), {})

                #test set_multi_value
                rsm.set_multi_value([("pmid:1020", True), ("pmid:2020", False)])
                self.assertCountEqual(rsm.get_all_keys(), {"pmid:1020", "pmid:2020"})

                #test get_value
                get_val = rsm.get_value("pmid:1020")
                exp = True
                self.assertEqual(get_val, exp)

                get_val = rsm.get_value("pmid:2020")
                exp = False
                self.assertEqual(get_val, exp)

                get_val = rsm.get_value("pmid:3020")
                exp = None
                self.assertEqual(get_val, exp)

                #test set_full_value

                rsm.set_full_value("pmid:1212", {"valid":True})
                get_val = rsm.get_value("pmid:1212")
                exp = True
                self.assertEqual(get_val, exp)

                rsm.delete_storage()
                emp_set = set()
                self.assertEqual(emp_set, rsm.get_all_keys())

                rsm.delete_storage()

            else:
                print("test skipped: 'test_storage_management_no_testing' because redis db 2 is not empty")


if __name__ == '__main__':
    unittest.main()
