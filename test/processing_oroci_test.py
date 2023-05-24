import os
import unittest
from pprint import pprint

from oc_ds_converter.lib.csvmanager import CSVManager
from oc_ds_converter.lib.jsonmanager import *

from oc_ds_converter.openaire.openaire_processing import OpenaireProcessing
from oc_ds_converter.oc_idmanager.oc_data_storage.storage_manager import StorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.in_memory_manager import InMemoryStorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.sqlite_manager import SqliteStorageManager

BASE = os.path.join('test', 'openaire_processing')
#IOD = os.path.join(BASE, 'iod')
#WANTED_DOIS = os.path.join(BASE, 'wanted_dois.csv') #?
#WANTED_DOIS_FOLDER = os.path.join(BASE, 'wanted_dois') #?
DATA = os.path.join(BASE, 'jSonFile_1.json')
DATA_DIR = BASE
TMP_SUPPORT_MATERIAL = os.path.join(BASE,"tmp_support")
OUTPUT = os.path.join(BASE, 'meta_input')
MULTIPROCESS_OUTPUT = os.path.join(BASE, 'multi_process_test')
#COMPR_INPUT = os.path.join(BASE, 'zst_test', "40228.json.zst")
#PUBLISHERS_MAPPING = os.path.join(BASE, 'publishers.csv')

# TEST OpenaireProcessing METHODS
import unittest


class TestOpenaireProcessing(unittest.TestCase):

    def delete_storege(self, storage_type=None, specific_path=None):
        if not specific_path:
            if storage_type == "sqlite":
                auto_db_created_path = os.path.join(os.getcwd(), "storage", "id_valid_dict.db")
                if os.path.exists(auto_db_created_path):
                    os.remove(auto_db_created_path)
            else:
                auto_db_created_path = os.path.join(os.getcwd(), "storage", "id_value.json")
                if os.path.exists(auto_db_created_path):
                    os.remove(auto_db_created_path)
        elif specific_path:
            if os.path.exists(specific_path):
                os.remove(specific_path)


    def test_validated_as_default(self):
        '''
        Check that, given an ID dict with keys "schema" (value: string of the schema) and "identifier" (value:
        string of the identifier, the method "validated_as" returns:
        - True if the id was already validated as valid
        - False if the id was already validated as invalid
        - None if the id was not validated before
        The procedure is tested
        - With default storage manager (sqlite) without a pre-existent db associated
        '''

        self.delete_storege(storage_type="sqlite")
        opp = OpenaireProcessing()
        validate_as_none = opp.validated_as({"schema":"pmid", "identifier": "pmid:23483834"})
        self.assertEqual(validate_as_none, None)
        self.delete_storege(storage_type="sqlite")


    def test_validated_as_sqlite(self):
        '''
        Check that, given an ID dict with keys "schema" (value: string of the schema) and "identifier" (value:
        string of the identifier, the method "validated_as" returns:
        - True if the id was already validated as valid
        - False if the id was already validated as invalid
        - None if the id was not validated before
        The procedure is tested
        - With sqlite storage manager without a pre-existent db associated
        - With sqlite storage manager and a pre-existent db associated
        '''

        db_path = os.path.join(TMP_SUPPORT_MATERIAL, "db_path.db")
        self.delete_storege(specific_path=db_path)

        sqlite_man = SqliteStorageManager(db_path)
        valid_pmid_not_in_db = {"identifier":"pmid:2938", "schema":"pmid"}
        valid_pmid_in_db = {"identifier":"pmid:23483834", "schema":"pmid"}
        invalid_pmid_in_db = {"identifier":"pmid:18328387372097", "schema":"pmid"}
        sqlite_man.set_value(valid_pmid_in_db["identifier"], True)
        sqlite_man.set_value(invalid_pmid_in_db["identifier"], False)

        # New class instance to check the correct task management with a sqlite db in input
        opp_sql = OpenaireProcessing(storage_manager=sqlite_man)
        validated_as_True = opp_sql.validated_as(valid_pmid_in_db)
        validated_as_False = opp_sql.validated_as(invalid_pmid_in_db)
        not_validated = opp_sql.validated_as(valid_pmid_not_in_db)

        self.assertEqual(validated_as_True, True)
        self.assertEqual(validated_as_False, False)
        self.assertEqual(not_validated, None)

        self.delete_storege(specific_path=db_path)


    def test_validated_as_inmemory(self):
        '''
        Check that, given an ID dict with keys "schema" (value: string of the schema) and "identifier" (value:
        string of the identifier, the method "validated_as" returns:
        - True if the id was already validated as valid
        - False if the id was already validated as invalid
        - None if the id was not validated before
        The procedure is tested
        - With in Memory + Json storage manager and a pre-existent db associated
        - With in Memory + Json storage manager without a pre-existent db associated
        '''

        db_json_path = os.path.join(TMP_SUPPORT_MATERIAL, "db_path.json")
        self.delete_storege(specific_path=db_json_path)

        inmemory_man = InMemoryStorageManager(db_json_path)
        valid_pmid_not_in_db = {"identifier":"pmid:2938", "schema":"pmid"}
        valid_pmid_in_db = {"identifier":"pmid:23483834", "schema":"pmid"}
        invalid_pmid_in_db = {"identifier":"pmid:18328387372097", "schema":"pmid"}
        inmemory_man.set_value(valid_pmid_in_db["identifier"], True)
        inmemory_man.set_value(invalid_pmid_in_db["identifier"], False)
        inmemory_man.store_file()

        # New class instance to check the correct task management with a sqlite db in input
        opp_sql = OpenaireProcessing(storage_manager=inmemory_man)
        validated_as_True = opp_sql.validated_as(valid_pmid_in_db)
        validated_as_False = opp_sql.validated_as(invalid_pmid_in_db)
        not_validated = opp_sql.validated_as(valid_pmid_not_in_db)

        self.assertEqual(validated_as_True, True)
        self.assertEqual(validated_as_False, False)
        self.assertEqual(not_validated, None)

        self.delete_storege(specific_path=db_json_path)



if __name__ == '__main__':
    unittest.main()