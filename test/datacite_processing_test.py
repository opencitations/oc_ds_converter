
import unittest
import json
from oc_ds_converter.lib.csvmanager import CSVManager
from oc_ds_converter.lib.jsonmanager import *
from oc_ds_converter.datacite.datacite_processing import DataciteProcessing

from oc_ds_converter.oc_idmanager.oc_data_storage.redis_manager import RedisStorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.in_memory_manager import InMemoryStorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.sqlite_manager import SqliteStorageManager

TEST_DIR = os.path.join("test","datacite_processing")
TMP_SUPPORT_MATERIAL = os.path.join(TEST_DIR, "tmp_support")
IOD = os.path.join(TEST_DIR, 'iod')
WANTED_DOIS = os.path.join(TEST_DIR, 'wanted_dois')
PUBLISHERS_MAPPING = os.path.join(TEST_DIR, 'publishers.csv')
DATA = os.path.join(TEST_DIR, 'jSonFile_1_new_dump.json')

class TestDataciteProcessing(unittest.TestCase):

    def setUp(self):
        # Create dirs
        for d in [TMP_SUPPORT_MATERIAL, IOD, WANTED_DOIS]:
            makedirs(d, exist_ok=True)

        # Load golden data
        with open(DATA, 'r', encoding='utf-8') as f:
            self.expected_entities = json.load(f)["data"]
        self.expected_count = len(self.expected_entities)

    def test_get_all_ids_citing(self):
        all_br = set()
        all_ra = set()
        dcp = DataciteProcessing()
        for entity in self.expected_entities:
            allids = dcp.extract_all_ids(entity, is_citing=True)
            all_br.update(set(allids[0]))
            all_ra.update(set(allids[1]))

        self.assertEqual(all_br, set())
        self.assertTrue({"orcid:0000-0002-8013-9947", "orcid:0000-0001-7392-1415",
                         "orcid:0000-0003-2328-5769", "orcid:0000-0002-6715-3533", "orcid:0000-0002-0801-0890",
                         "orcid:0000-0001-7543-3466", "orcid:0000-0002-6210-8370", "orcid:0000-0002-9747-4928",
                         "ror:03ztgj037"} == all_ra)

    def test_get_all_ids_cited(self):
        all_br = set()
        all_ra = set()
        dcp = DataciteProcessing()
        for entity in self.expected_entities:
            allids = dcp.extract_all_ids(entity, is_citing=False)

            all_br.update(set(allids[0]))
            all_ra.update(set(allids[1]))
        self.assertTrue({"doi:10.5281/zenodo.8249952", "doi:10.5281/zenodo.8249970", "doi:10.1017/9781009157896",
                         "doi:10.1017/9781009157896.005"} == all_br)

    def test_get_redis_validity_list_br(self):
        dcp = DataciteProcessing()
        br = {"doi:10.5281/zenodo.8249952", "doi:10.5281/zenodo.8249970", "doi:10.1017/9781009157896", "doi:10.1017/9781009157896.005"}
        br_valid_list = dcp.get_reids_validity_list(br, "br")
        exp_br_valid_list = []
        self.assertEqual(br_valid_list, exp_br_valid_list)
        dcp.storage_manager.delete_storage()

    def test_get_redis_validity_list_ra(self):
        dcp = DataciteProcessing()
        ra = {"orcid:0000-0002-8013-9947", "orcid:0000-0001-7392-1415",
             "orcid:0000-0003-2328-5769", "orcid:0000-0002-6715-3533", "orcid:0000-0002-0801-0890",
             "orcid:0000-0001-7543-3466", "orcid:0000-0002-6210-8370", "orcid:0000-0002-9747-4928",
             "ror:03ztgj037"}
        ra_valid_list = dcp.get_reids_validity_list(ra, "ra")
        exp_ra_valid_list = []
        self.assertEqual(ra_valid_list, exp_ra_valid_list)
        dcp.storage_manager.delete_storage()

    def test_get_redis_validity_list_br_redis(self):
        dcp = DataciteProcessing(storage_manager=RedisStorageManager(testing=True))
        br = {"doi:10.5281/zenodo.8249952", "doi:10.5281/zenodo.8249970", "doi:10.1017/9781009157896", "doi:10.1017/9781009157896.005"}
        br_valid_list = dcp.get_reids_validity_list(br, "br")
        exp_br_valid_list = []
        self.assertEqual(br_valid_list, exp_br_valid_list)
        dcp.storage_manager.delete_storage()

    def test_get_redis_validity_dict_w_fakeredis_db_values_sqlite(self):
        dcp = DataciteProcessing()
        dcp.BR_redis.sadd("doi:10.5281/zenodo.8249952", "omid:1")
        dcp.RA_redis.sadd("orcid:0000-0002-8013-9947", "omid:2")
        dcp.RA_redis.sadd("ror:03ztgj039", "omid:3")  # invalid ror

        br = {"doi:10.5281/zenodo.8249952", "doi:10.5281/zenodo.8249970", "doi:10.1017/9781009157896", "doi:10.1017/9781009157896.005"}

        ra = {"orcid:0000-0002-8013-9947", "orcid:0000-0001-7392-1415",
             "orcid:0000-0003-2328-5769", "orcid:0000-0002-6715-3533", "orcid:0000-0002-0801-0890",
             "orcid:0000-0001-7543-3466", "orcid:0000-0002-6210-8370", "orcid:0000-0002-9747-4928", "ror:03ztgj039"}

        br_validity_dict = dcp.get_reids_validity_list(br, "br")
        exp_br_valid_list = ["doi:10.5281/zenodo.8249952"]
        ra_validity_dict = dcp.get_reids_validity_list(ra, "ra")
        exp_ra_valid_list = ["orcid:0000-0002-8013-9947", "ror:03ztgj039"]
        self.assertEqual(set(br_validity_dict), set(exp_br_valid_list))
        self.assertEqual(set(ra_validity_dict), set(exp_ra_valid_list))

        dcp.storage_manager.delete_storage()

        dcp.BR_redis.delete("doi:10.5281/zenodo.8249952")
        dcp.RA_redis.delete("orcid:0000-0002-8013-9947")
        dcp.RA_redis.delete("ror:03ztgj039")

    def test_get_redis_validity_dict_w_fakeredis_db_values_redis(self):
        dcp = DataciteProcessing(storage_manager=RedisStorageManager())
        dcp.BR_redis.sadd("doi:10.5281/zenodo.8249970", "omid:1")
        dcp.RA_redis.sadd("orcid:0000-0002-6210-8370", "omid:2")
        dcp.RA_redis.sadd("ror:03ztgj039", "omid:3")  # invalid ror

        br = {"doi:10.5281/zenodo.8249952", "doi:10.5281/zenodo.8249970", "doi:10.1017/9781009157896", "doi:10.1017/9781009157896.005"}

        ra = {"orcid:0000-0002-8013-9947", "orcid:0000-0001-7392-1415",
              "orcid:0000-0003-2328-5769", "orcid:0000-0002-6715-3533", "orcid:0000-0002-0801-0890",
              "orcid:0000-0001-7543-3466", "orcid:0000-0002-6210-8370", "orcid:0000-0002-9747-4928", "ror:03ztgj039"}

        br_validity_dict = dcp.get_reids_validity_list(br, "br")
        exp_br_valid_list = ["doi:10.5281/zenodo.8249970"]
        ra_validity_dict = dcp.get_reids_validity_list(ra, "ra")
        exp_ra_valid_list = ["orcid:0000-0002-6210-8370", "ror:03ztgj039"]
        self.assertEqual(set(br_validity_dict), set(exp_br_valid_list))
        self.assertEqual(set(ra_validity_dict), set(exp_ra_valid_list))

        dcp.storage_manager.delete_storage()

        dcp.BR_redis.delete("doi:10.5281/zenodo.8249970")
        dcp.RA_redis.delete("orcid:0000-0002-6210-8370")
        dcp.RA_redis.delete("ror:03ztgj039")

    def test_validated_as_default(self):
        """
        Check that, given an ID dict with keys "schema" (value: string of the schema) and "identifier" (value:
        string of the identifier, the method "validated_as" returns:
        - True if the id was already validated as valid
        - False if the id was already validated as invalid
        - None if the id was not validated before
        The procedure is tested
        - With default storage manager (sqlite) without a pre-existent db associated
        """

        dcp = DataciteProcessing()
        validate_as_none_doi = dcp.validated_as({"schema": "doi", "identifier": "doi:10.11578/1480643"})
        validated_as_none_orcid = dcp.validated_as({"schema": "orcid", "identifier": "orcid:0000-0001-8513-8700"})
        validated_as_none_ror = dcp.validated_as({"schema": "ror", "identifier": "ror:03ztgj037"})
        self.assertEqual(validate_as_none_doi, None)
        self.assertEqual(validated_as_none_orcid, None)
        self.assertEqual(validated_as_none_ror, None)

        dcp.storage_manager.delete_storage()

    def test_validated_as_default_redis(self):
        """
        Check that, given an ID dict with keys "schema" (value: string of the schema) and "identifier" (value:
        string of the identifier, the method "validated_as" returns:
        - True if the id was already validated as valid
        - False if the id was already validated as invalid
        - None if the id was not validated before
        The procedure is tested
        - With redis storage manager without a pre-existent db associated
        """

        dcp = DataciteProcessing(storage_manager=RedisStorageManager(testing=True))
        validate_as_none_doi = dcp.validated_as({"schema": "doi", "identifier": "doi:10.11578/1480643"})
        validated_as_none_orcid = dcp.validated_as({"schema": "orcid", "identifier": "orcid:0000-0001-8513-8700"})
        validated_as_none_ror = dcp.validated_as({"schema": "ror", "identifier": "ror:03ztgj037"})
        self.assertEqual(validate_as_none_doi, None)
        self.assertEqual(validated_as_none_orcid, None)
        self.assertEqual(validated_as_none_ror, None)
        dcp.storage_manager.delete_storage()

    def test_validated_as_redis_with_preexistent_data(self):
        """
        Check that, given an ID dict with keys "schema" (value: string of the schema) and "identifier" (value:
        string of the identifier, the method "validated_as" returns:
        - True if the id was already validated as valid
        - False if the id was already validated as invalid
        - None if the id was not validated before
        The procedure is tested
        - With redis storage manager and pre-existent data associated
        """
        db_path = os.path.join(TMP_SUPPORT_MATERIAL, "db_path.db")
        sqlite_man = SqliteStorageManager(db_path)

        valid_doi_not_in_db = {"identifier": "doi:10.11578/1480643", "schema": "doi"}
        valid_doi_in_db = {"identifier": "doi:10.15407/scin11.06.057", "schema": "doi"}
        invalid_doi_in_db = {"identifier": "doi:10.1066/1741-4326/aa6b", "schema": "doi"}

        valid_orcid_not_in_db = {"schema": "orcid", "identifier": "orcid:0000-0001-8513-8700"}
        valid_orcid_in_db = {"schema": "orcid", "identifier": "orcid:0000-0002-9286-2630"}
        invalid_orcid_in_db = {"schema": "orcid", "identifier": "orcid:0000-0002-9286-26XX"}

        valid_ror_in_db = {"schema": "ror", "identifier": "ror:03ztgj037"}
        valid_ror_not_in_db = {"schema": "ror", "identifier": "ror:01111rn36"}
        invalid_ror_in_db = {"schema": "ror", "identifier": "ror:03ztgj039"}

        valid_viaf_not_in_db = {"identifier": "viaf:102333412", "schema": "viaf"}
        valid_viaf_in_db = {"identifier": "viaf:108389263", "schema": "viaf"}
        invalid_viaf_in_db = {"identifier": "viaf:12345ABC", "schema": "viaf"}

        valid_wikidata_not_in_db = {"identifier": "wikidata:Q2330656", "schema": "wikidata"}
        valid_wikidata_in_db = {"identifier": "wikidata:Q42", "schema": "wikidata"}
        invalid_wikidata_in_db = {"identifier": "wikidata:Q_invalid_123", "schema": "wikidata"}

        # --- POPOLAMENTO DATABASE SQLITE ---
        sqlite_man.set_value(valid_doi_in_db["identifier"], True)
        sqlite_man.set_value(invalid_doi_in_db["identifier"], False)

        sqlite_man.set_value(valid_orcid_in_db["identifier"], True)
        sqlite_man.set_value(invalid_orcid_in_db["identifier"], False)

        sqlite_man.set_value(valid_ror_in_db["identifier"], True)
        sqlite_man.set_value(invalid_ror_in_db["identifier"], False)

        sqlite_man.set_value(valid_viaf_in_db["identifier"], True)
        sqlite_man.set_value(invalid_viaf_in_db["identifier"], False)

        sqlite_man.set_value(valid_wikidata_in_db["identifier"], True)
        sqlite_man.set_value(invalid_wikidata_in_db["identifier"], False)

        # --- ESECUZIONE DEI METODI ---
        # New class instance to check the correct task management with a sqlite db in input
        d_processing_sql = DataciteProcessing(storage_manager=sqlite_man)

        doi_validated_as_True = d_processing_sql.validated_as(valid_doi_in_db)
        doi_validated_as_False = d_processing_sql.validated_as(invalid_doi_in_db)
        doi_not_validated = d_processing_sql.validated_as(valid_doi_not_in_db)

        orcid_validated_as_True = d_processing_sql.validated_as(valid_orcid_in_db)
        orcid_validated_as_False = d_processing_sql.validated_as(invalid_orcid_in_db)
        orcid_not_validated = d_processing_sql.validated_as(valid_orcid_not_in_db)

        ror_validated_as_True = d_processing_sql.validated_as(valid_ror_in_db)
        ror_validated_as_False = d_processing_sql.validated_as(invalid_ror_in_db)
        ror_not_validated = d_processing_sql.validated_as(valid_ror_not_in_db)

        viaf_validated_as_True = d_processing_sql.validated_as(valid_viaf_in_db)
        viaf_validated_as_False = d_processing_sql.validated_as(invalid_viaf_in_db)
        viaf_not_validated = d_processing_sql.validated_as(valid_viaf_not_in_db)

        wikidata_validated_as_True = d_processing_sql.validated_as(valid_wikidata_in_db)
        wikidata_validated_as_False = d_processing_sql.validated_as(invalid_wikidata_in_db)
        wikidata_not_validated = d_processing_sql.validated_as(valid_wikidata_not_in_db)

        # --- ASSERZIONI ---
        self.assertEqual(doi_validated_as_True, True)
        self.assertEqual(doi_validated_as_False, False)
        self.assertEqual(doi_not_validated, None)

        self.assertEqual(orcid_validated_as_True, True)
        self.assertEqual(orcid_validated_as_False, False)
        self.assertEqual(orcid_not_validated, None)

        self.assertEqual(ror_validated_as_True, True)
        self.assertEqual(ror_validated_as_False, False)
        self.assertEqual(ror_not_validated, None)

        self.assertEqual(viaf_validated_as_True, True)
        self.assertEqual(viaf_validated_as_False, False)
        self.assertEqual(viaf_not_validated, None)

        self.assertEqual(wikidata_validated_as_True, True)
        self.assertEqual(wikidata_validated_as_False, False)
        self.assertEqual(wikidata_not_validated, None)

        d_processing_sql.storage_manager.delete_storage()

    def test_validated_as_inmemory(self):
        '''
        Check that, given an ID dict with keys "schema" (value: string of the schema) and "identifier" (value:
        string of the identifier, the method "validated_as" returns:
        - True if the id was already validated as valid
        - False if the id was already validated as invalid
        - None if the id was not validated before
        The procedure is tested
        - With in Memory + Json storage manager and a pre-existent db associated
        '''

        db_json_path = os.path.join(TMP_SUPPORT_MATERIAL, "db_path.json")
        inmemory_man = InMemoryStorageManager(db_json_path)

        valid_doi_not_in_db = {"identifier": "doi:10.11578/1480643", "schema": "doi"}
        valid_doi_in_db = {"identifier": "doi:10.15407/scin11.06.057", "schema": "doi"}
        invalid_doi_in_db = {"identifier": "doi:10.1066/1741-4326/aa6b", "schema": "doi"}

        valid_orcid_not_in_db = {"schema": "orcid", "identifier": "orcid:0000-0001-8513-8700"}
        valid_orcid_in_db = {"schema": "orcid", "identifier": "orcid:0000-0002-9286-2630"}
        invalid_orcid_in_db = {"schema": "orcid", "identifier": "orcid:0000-0002-9286-26XX"}

        valid_ror_in_db = {"schema": "ror", "identifier": "ror:03ztgj037"}
        valid_ror_not_in_db = {"schema": "ror", "identifier": "ror:01111rn36"}
        invalid_ror_in_db = {"schema": "ror", "identifier": "ror:03ztgj039"}

        valid_viaf_not_in_db = {"identifier": "viaf:102333412", "schema": "viaf"}
        valid_viaf_in_db = {"identifier": "viaf:108389263", "schema": "viaf"}
        invalid_viaf_in_db = {"identifier": "viaf:12345ABC", "schema": "viaf"}

        valid_wikidata_not_in_db = {"identifier": "wikidata:Q2330656", "schema": "wikidata"}
        valid_wikidata_in_db = {"identifier": "wikidata:Q42", "schema": "wikidata"}
        invalid_wikidata_in_db = {"identifier": "wikidata:Q_invalid_123", "schema": "wikidata"}

        inmemory_man.set_value(valid_doi_in_db["identifier"], True)
        inmemory_man.set_value(invalid_doi_in_db["identifier"], False)

        inmemory_man.set_value(valid_orcid_in_db["identifier"], True)
        inmemory_man.set_value(invalid_orcid_in_db["identifier"], False)

        inmemory_man.set_value(valid_ror_in_db["identifier"], True)
        inmemory_man.set_value(invalid_ror_in_db["identifier"], False)

        inmemory_man.set_value(valid_viaf_in_db["identifier"], True)
        inmemory_man.set_value(invalid_viaf_in_db["identifier"], False)

        inmemory_man.set_value(valid_wikidata_in_db["identifier"], True)
        inmemory_man.set_value(invalid_wikidata_in_db["identifier"], False)


        # New class instance to check the correct task management with a sqlite db in input
        d_processing = DataciteProcessing(storage_manager=inmemory_man)

        doi_validated_as_True = d_processing.validated_as(valid_doi_in_db)
        doi_validated_as_False = d_processing.validated_as(invalid_doi_in_db)
        doi_not_validated = d_processing.validated_as(valid_doi_not_in_db)

        orcid_validated_as_True = d_processing.validated_as(valid_orcid_in_db)
        orcid_validated_as_False = d_processing.validated_as(invalid_orcid_in_db)
        orcid_not_validated = d_processing.validated_as(valid_orcid_not_in_db)

        ror_validated_as_True = d_processing.validated_as(valid_ror_in_db)
        ror_validated_as_False = d_processing.validated_as(invalid_ror_in_db)
        ror_not_validated = d_processing.validated_as(valid_ror_not_in_db)

        viaf_validated_as_True = d_processing.validated_as(valid_viaf_in_db)
        viaf_validated_as_False = d_processing.validated_as(invalid_viaf_in_db)
        viaf_not_validated = d_processing.validated_as(valid_viaf_not_in_db)

        wikidata_validated_as_True = d_processing.validated_as(valid_wikidata_in_db)
        wikidata_validated_as_False = d_processing.validated_as(invalid_wikidata_in_db)
        wikidata_not_validated = d_processing.validated_as(valid_wikidata_not_in_db)

        self.assertEqual(doi_validated_as_True, True)
        self.assertEqual(doi_validated_as_False, False)
        self.assertEqual(doi_not_validated, None)

        self.assertEqual(orcid_validated_as_True, True)
        self.assertEqual(orcid_validated_as_False, False)
        self.assertEqual(orcid_not_validated, None)

        self.assertEqual(ror_validated_as_True, True)
        self.assertEqual(ror_validated_as_False, False)
        self.assertEqual(ror_not_validated, None)

        self.assertEqual(viaf_validated_as_True, True)
        self.assertEqual(viaf_validated_as_False, False)
        self.assertEqual(viaf_not_validated, None)

        self.assertEqual(wikidata_validated_as_True, True)
        self.assertEqual(wikidata_validated_as_False, False)
        self.assertEqual(wikidata_not_validated, None)


        d_processing.storage_manager.delete_storage()

    def test_validated_as_redis(self):
        """
        Check that, given an ID dict with keys "schema" (value: string of the schema) and "identifier" (value:
        string of the identifier, the method "validated_as" returns:
        - True if the id was already validated as valid
        - False if the id was already validated as invalid
        - None if the id was not validated before
        The procedure is tested
        - With REDIS storage manager and a pre-existent db associated
        """
        redis_man = RedisStorageManager(testing=True)

        valid_doi_not_in_db = {"identifier": "doi:10.11578/1480643", "schema": "doi"}
        valid_doi_in_db = {"identifier": "doi:10.15407/scin11.06.057", "schema": "doi"}
        invalid_doi_in_db = {"identifier": "doi:10.1066/1741-4326/aa6b", "schema": "doi"}

        valid_orcid_not_in_db = {"schema": "orcid", "identifier": "orcid:0000-0001-8513-8700"}
        valid_orcid_in_db = {"schema": "orcid", "identifier": "orcid:0000-0002-9286-2630"}
        invalid_orcid_in_db = {"schema": "orcid", "identifier": "orcid:0000-0002-9286-26XX"}

        valid_ror_in_db = {"schema": "ror", "identifier": "ror:03ztgj037"}
        valid_ror_not_in_db = {"schema": "ror", "identifier": "ror:01111rn36"}
        invalid_ror_in_db = {"schema": "ror", "identifier": "ror:03ztgj039"}

        valid_viaf_not_in_db = {"identifier": "viaf:102333412", "schema": "viaf"}
        valid_viaf_in_db = {"identifier": "viaf:108389263", "schema": "viaf"}
        invalid_viaf_in_db = {"identifier": "viaf:12345ABC", "schema": "viaf"}

        valid_wikidata_not_in_db = {"identifier": "wikidata:Q2330656", "schema": "wikidata"}
        valid_wikidata_in_db = {"identifier": "wikidata:Q42", "schema": "wikidata"}
        invalid_wikidata_in_db = {"identifier": "wikidata:Q_invalid_123", "schema": "wikidata"}

        redis_man.set_value(valid_doi_in_db["identifier"], True)
        redis_man.set_value(invalid_doi_in_db["identifier"], False)

        redis_man.set_value(valid_orcid_in_db["identifier"], True)
        redis_man.set_value(invalid_orcid_in_db["identifier"], False)

        redis_man.set_value(valid_ror_in_db["identifier"], True)
        redis_man.set_value(invalid_ror_in_db["identifier"], False)

        redis_man.set_value(valid_viaf_in_db["identifier"], True)
        redis_man.set_value(invalid_viaf_in_db["identifier"], False)

        redis_man.set_value(valid_wikidata_in_db["identifier"], True)
        redis_man.set_value(invalid_wikidata_in_db["identifier"], False)

        d_processing_redis = DataciteProcessing(storage_manager=redis_man)

        doi_validated_as_True = d_processing_redis.validated_as(valid_doi_in_db)
        doi_validated_as_False = d_processing_redis.validated_as(invalid_doi_in_db)
        doi_not_validated = d_processing_redis.validated_as(valid_doi_not_in_db)

        orcid_validated_as_True = d_processing_redis.validated_as(valid_orcid_in_db)
        orcid_validated_as_False = d_processing_redis.validated_as(invalid_orcid_in_db)
        orcid_not_validated = d_processing_redis.validated_as(valid_orcid_not_in_db)

        ror_validated_as_True = d_processing_redis.validated_as(valid_ror_in_db)
        ror_validated_as_False = d_processing_redis.validated_as(invalid_ror_in_db)
        ror_not_validated = d_processing_redis.validated_as(valid_ror_not_in_db)

        viaf_validated_as_True = d_processing_redis.validated_as(valid_viaf_in_db)
        viaf_validated_as_False = d_processing_redis.validated_as(invalid_viaf_in_db)
        viaf_not_validated = d_processing_redis.validated_as(valid_viaf_not_in_db)

        wikidata_validated_as_True = d_processing_redis.validated_as(valid_wikidata_in_db)
        wikidata_validated_as_False = d_processing_redis.validated_as(invalid_wikidata_in_db)
        wikidata_not_validated = d_processing_redis.validated_as(valid_wikidata_not_in_db)

        self.assertEqual(doi_validated_as_True, True)
        self.assertEqual(doi_validated_as_False, False)
        self.assertEqual(doi_not_validated, None)

        self.assertEqual(orcid_validated_as_True, True)
        self.assertEqual(orcid_validated_as_False, False)
        self.assertEqual(orcid_not_validated, None)

        self.assertEqual(ror_validated_as_True, True)
        self.assertEqual(ror_validated_as_False, False)
        self.assertEqual(ror_not_validated, None)

        self.assertEqual(viaf_validated_as_True, True)
        self.assertEqual(viaf_validated_as_False, False)
        self.assertEqual(viaf_not_validated, None)

        self.assertEqual(wikidata_validated_as_True, True)
        self.assertEqual(wikidata_validated_as_False, False)
        self.assertEqual(wikidata_not_validated, None)

        d_processing_redis.storage_manager.delete_storage()

    def test_get_id_manager(self):
        """Check that, given in input the string of a schema (e.g.:'pmid') or an id with a prefix (e.g.: 'pmid:12334')
        and a dictionary mapping the strings of the schemas to their id managers, the method returns the correct
        id manager. Note that each instance of the Preprocessing class needs its own instances of the id managers,
        in order to avoid conflicts while validating data"""

        d_processing = DataciteProcessing()

        id_man_dict = d_processing.venue_id_man_dict
        ra_man_dict = d_processing.ra_man_dict

        issn_id = "issn:0003-987X"
        issn_string = "issn"

        isbn_id = "isbn:978-88-98719-08-2"
        isbn_string = "isbn"

        orcid_id = "orcid:0000-0001-8513-8700"
        orcid_string = "orcid"

        ror_id = "ror:03ztgj037"
        ror_string = "ror"

        viaf_id = "viaf:102333412"
        viaf_string = "viaf"

        wikidata_id = "wikidata:Q42"
        wikidata_string = "wikidata"

        issn_man_exp = d_processing.get_id_manager(issn_id, id_man_dict)
        issn_man_exp_2 = d_processing.get_id_manager(issn_string, id_man_dict)

        isbn_man_exp = d_processing.get_id_manager(isbn_id, id_man_dict)
        isbn_man_exp_2 = d_processing.get_id_manager(isbn_string, id_man_dict)

        orcid_man_exp = d_processing.get_id_manager(orcid_id, ra_man_dict)
        orcid_man_exp_2 = d_processing.get_id_manager(orcid_string, ra_man_dict)

        ror_man_exp = d_processing.get_id_manager(ror_id, ra_man_dict)
        ror_man_exp_2 = d_processing.get_id_manager(ror_string, ra_man_dict)

        viaf_man_exp = d_processing.get_id_manager(viaf_id, ra_man_dict)
        viaf_man_exp_2 = d_processing.get_id_manager(viaf_string, ra_man_dict)

        wikidata_man_exp = d_processing.get_id_manager(wikidata_id, ra_man_dict)
        wikidata_man_exp_2 = d_processing.get_id_manager(wikidata_string, ra_man_dict)

        # check that the idmanager for the issn was returned and that it works as expected
        self.assertTrue(issn_man_exp.is_valid(issn_id))
        self.assertTrue(issn_man_exp_2.is_valid(issn_id))

        # check that the idmanager for the isbn was returned and that it works as expected
        self.assertTrue(isbn_man_exp.is_valid(isbn_id))
        self.assertTrue(isbn_man_exp_2.is_valid(isbn_id))

        # check that the idmanager for the orcid was returned and that it works as expected
        self.assertTrue(orcid_man_exp.is_valid(orcid_id))
        self.assertTrue(orcid_man_exp_2.is_valid(orcid_id))

        # check that the idmanager for the ror was returned and that it works as expected
        self.assertTrue(ror_man_exp.is_valid(ror_id))
        self.assertTrue(ror_man_exp_2.is_valid(ror_id))

        # check that the idmanager for the viaf was returned and that it works as expected
        self.assertTrue(viaf_man_exp.is_valid(viaf_id))
        self.assertTrue(viaf_man_exp_2.is_valid(viaf_id))

        # check that the idmanager for the wikidata was returned and that it works as expected
        self.assertTrue(wikidata_man_exp.is_valid(wikidata_id))
        self.assertTrue(wikidata_man_exp_2.is_valid(wikidata_id))

        d_processing.storage_manager.delete_storage()

    def test_csv_creator(self):
        '''Add a test with all the data'''
        datacite_processor = DataciteProcessing()
        data = {
             'id': '10.34780/7510-t906',
             'type': 'dois',
             'attributes': {
                'container': {
                    'identifier': '2701-5572',
                    'firstPage': '2021',
                    'identifierType': 'ISSN',
                    'type': 'Series',
                    'title': 'Journal of Global Archaeology'
                },
                'reason': None,
                'prefix': '10.34780',
                'citationsOverTime': [],
                'registered': '2021-06-07T10:39:06Z',
                'language': 'en',
                'source': 'fabricaForm',
                'suffix': '7510-t906',
                 'relatedItems': [],
                 'descriptions': [
                    {'descriptionType': 'SeriesInformation', 'description': 'Journal of Global Archaeology, 2021'},
                    {'descriptionType': 'SeriesInformation', 'description': 'Journal of Global Archaeology, 2021'},
                    {'descriptionType': 'Abstract',
                     'description': 'The kingdom of Eswatini provides a rich archaeological sequence covering all time periods from the Early Stone Age to the Iron Age. For over 27 years though, no or very little archaeological research was conducted in the country. In the scope of a new project funded by the German Research Foundation (DFG) we aim to re-excavate and re-date Lion Cavern, the potentially oldest ochre mine in the world. In addition, we conduct a largescale geological survey for outcrops of ochre and test their geochemical signatures for comparative studies with archaeological ochre pieces from MSA and LSA assemblages in Eswatini. Here we present a review of the research history of the kingdom and some preliminary results from our ongoing project.',
                     'lang': 'en'}],
                 'sizes': ['§ 1–12'],
                 'versionOfCount': 0,
                 'relatedIdentifiers': [
                    {'relationType': 'IsPartOf', 'relatedIdentifier': '2701-5572', 'relatedIdentifierType': 'ISSN'},
                    {'relationType': 'IsPartOf', 'relatedIdentifierType': 'DOI'},
                    {'relationType': 'HasMetadata', 'relatedIdentifier': 'https://zenon.dainst.org/Record/002035353',
                     'relatedIdentifierType': 'URL'},
                    {'relationType': 'References', 'relatedIdentifier': '10.2307/3888317',
                     'relatedIdentifierType': 'DOI'},
                    {'relationType': 'References', 'relatedIdentifier': '10.1086/204793',
                     'relatedIdentifierType': 'DOI'},
                    {'relationType': 'References', 'relatedIdentifier': '10.1086/338292',
                     'relatedIdentifierType': 'DOI'},
                    {'relationType': 'References', 'relatedIdentifier': '10.1111/arcm.12202',
                     'relatedIdentifierType': 'DOI'},
                    {'relationType': 'References', 'relatedIdentifier': '10.1006/jasc.2000.0638',
                     'relatedIdentifierType': 'DOI'},
                    {'relationType': 'References', 'relatedIdentifier': '10.2307/3888015',
                     'relatedIdentifierType': 'DOI'},
                    {'relationType': 'References', 'relatedIdentifier': '10.3213/2191-5784-10199',
                     'relatedIdentifierType': 'DOI'},
                    {'relationType': 'References', 'relatedIdentifier': '10.1016/j.jhevol.2005.06.007',
                     'relatedIdentifierType': 'DOI'},
                    {'relationType': 'References', 'relatedIdentifier': '10.1017/s0003598x00113298',
                     'relatedIdentifierType': 'DOI'}], 'created': '2021-05-11T13:11:58Z',
                'dates': [{'date': '2021', 'dateType': 'Issued'}],
                'published': '2021',
                'geoLocations': [],
                'partCount': 0,
                'publicationYear': 2021,
                'partOfCount': 0,
                'updated': '2021-07-30T12:39:50Z',
                'formats': [],
                'fundingReferences': [],
                'creators': [
                    {
                        'nameType': 'Personal',
                        'affiliation': [
                            {'affiliationIdentifier': 'https://ror.org/03a1kwz48',
                             'name': 'University of Tübingen, Senckenberg Centre for Human Evolution and Palaeoenvironment',
                             'affiliationIdentifierScheme': 'ROR'}],
                        'givenName': 'Gregor D.',
                        'familyName': 'Bader',
                        'name': 'Bader, Gregor D.',
                        'nameIdentifiers': [
                        {'nameIdentifierScheme': 'ORCID', 'schemeUri': 'https://orcid.org',
                         'nameIdentifier': 'https://orcid.org/0000-0003-0621-9209'}]
                    },
                    {
                         'nameType': 'Personal',
                         'affiliation': [
                             {
                                 'affiliationIdentifier': 'https://ror.org/02vrphe47',
                                 'name': 'Swaziland National Trust Commission',
                                 'affiliationIdentifierScheme': 'ROR'}
                         ],
                         'givenName': 'Bob',
                         'familyName': 'Forrester',
                         'name': 'Forrester, Bob'
                    },
                    {
                        'nameType': 'Personal',
                        'affiliation': [
                            {
                                'affiliationIdentifier': 'https://ror.org/041qv0h25',
                                'name': 'Deutsches Archäologisches Institut, Kommission für Archäologie Außereuropäischer Kulturen',
                                'affiliationIdentifierScheme': 'ROR'}
                        ],
                        'givenName': 'Lisa',
                        'familyName': 'Ehlers',
                        'name': 'Ehlers, Lisa'
                    },
                    {
                        'nameType': 'Personal',
                        'affiliation': [
                            {
                                'affiliationIdentifier': 'https://ror.org/03zga2b32',
                                'name': 'University of Bergen, SFF Centre for Early Sapiens Behaviour',
                                'affiliationIdentifierScheme': 'ROR'}
                        ],
                        'givenName': 'Elizabeth',
                        'familyName': 'Velliky',
                        'name': 'Velliky, Elizabeth',
                        'nameIdentifiers': [
                            {
                                'nameIdentifierScheme': 'ORCID',
                                'schemeUri': 'https://orcid.org',
                                'nameIdentifier': 'https://orcid.org/0000-0002-3019-5377'}
                        ]
                    }],
                'schemaVersion': 'http://datacite.org/schema/kernel-4', 'versionCount': 0, 'metadataVersion': 2,
                'citationCount': 0,
                'types': {'schemaOrg': 'ScholarlyArticle', 'resourceTypeGeneral': 'Text', 'citeproc': 'article-journal',
                          'bibtex': 'article', 'ris': 'RPRT', 'resourceType': 'Article'}, 'isActive': True,
                'viewsOverTime': [], 'identifiers': [],
                'subjects': [{'subject': 'Eswatini'}, {'subject': 'Lion Cavern'}, {'subject': 'Ochre'},
                             {'subject': 'Provenance tracing'}], 'titles': [
                    {'lang': 'en', 'title': 'The Forgotten Kingdom. New investigations in the prehistory of Eswatini'}],
                'url': 'https://publications.dainst.org/journals/index.php/joga/article/view/3559', 'downloadCount': 0,
                'rightsList': [], 'contentUrl': None, 'contributors': [], 'referenceCount': 9, 'viewCount': 0,
                'downloadsOverTime': [], 'doi': '10.34780/7510-t906',
                'publisher': {
                    'publisherIdentifierScheme': 'ROR',
                    'schemeUri': 'https://ror.org',
                    'name': 'Deutsches Archäologisches Institut',
                    'publisherIdentifier': 'https://ror.org/041qv0h25'
                },
                'version': None,
                'state': 'findable',
                'alternateIdentifiers': []
             },
             'relationships': {'client': {'data': {'id': 'dai.avnrkz', 'type': 'clients'}},
                               'provider': {'data': {'id': 'dai', 'type': 'providers'}}, 'media': {'data': []},
                               'references': {'data': [{'id': '10.2307/3888317', 'type': 'dois'},
                                                       {'id': '10.1086/204793', 'type': 'dois'},
                                                       {'id': '10.1086/338292', 'type': 'dois'},
                                                       {'id': '10.1111/arcm.12202', 'type': 'dois'},
                                                       {'id': '10.1006/jasc.2000.0638', 'type': 'dois'},
                                                       {'id': '10.2307/3888015', 'type': 'dois'},
                                                       {'id': '10.3213/2191-5784-10199', 'type': 'dois'},
                                                       {'id': '10.1016/j.jhevol.2005.06.007', 'type': 'dois'},
                                                       {'id': '10.1017/s0003598x00113298', 'type': 'dois'}]},
                               'citations': {'data': []}, 'parts': {'data': []}, 'partOf': {'data': []},
                               'versions': {'data': []}, 'versionOf': {'data': []}}}
        output = list()
        tabular_data = datacite_processor.csv_creator(data)
        if tabular_data:
            output.append(tabular_data)

        expected_output = [
            {
                'id': 'doi:10.34780/7510-t906',
                'title': 'The Forgotten Kingdom. New investigations in the prehistory of Eswatini',
                'author': 'Bader, Gregor D. [orcid:0000-0003-0621-9209]; Forrester, Bob; Ehlers, Lisa; Velliky, Elizabeth [orcid:0000-0002-3019-5377]',
                'pub_date': '2021',
                'venue': 'journal of global archaeology [issn:2701-5572]',
                'volume': '',
                'issue': '',
                'page': '2021-2021',
                'type': 'journal article',
                'publisher': 'Deutsches Archäologisches Institut [ror:041qv0h25]',
                'editor': ''
            }
        ]
        self.assertEqual(output,expected_output)

    def test_csv_creator2(self):
        datacite_processor = DataciteProcessing()
        data = load_json(DATA, None)
        output = list()
        for item in data['data']:
            tabular_data = datacite_processor.csv_creator(item)
            if tabular_data:
                output.append(tabular_data)

        expected_output = [
            {'id': 'doi:10.5281/zenodo.8244010',
             'title': 'FIGURE 1A, B in Meeting the southern brothers: a revision of the Neotropical spider genus Hexapopha Platnick, Berniker & Víquez, 2014 (Araneae, Oonopidae)',
             'author': 'Feitosa, Níthomas M. [orcid:0000-0002-8013-9947]; Ott, Ricardo [orcid:0000-0001-7392-1415]; Bonaldo, Alexandre B. [orcid:0000-0002-8013-9947]',
             'pub_date': '2023-08-11',
             'venue': '',
             'volume': '',
             'issue': '',
             'page': '',
             'type': 'other',
             'publisher': 'Zenodo',
             'editor': ''},
            {'id': 'doi:10.26050/wdcc/ar6.c6gmipicl',
             'title': 'IPCC DDC: IPSL IPSL-CM6A-LR model output prepared for CMIP6 GMMIP',
             'author': 'Boucher, Olivier [orcid:0000-0003-2328-5769]; Denvil, Sébastien [orcid:0000-0002-6715-3533]; Levavasseur, Guillaume [orcid:0000-0002-0801-0890]; Cozic, Anne [orcid:0000-0001-7543-3466]; Caubel, Arnaud [orcid:0000-0002-6210-8370]; Foujols, Marie-Alice [orcid:0000-0002-9747-4928]; Meurdesoif, Yann; Mellul, Lidia',
             'pub_date': '2023',
             'venue': '',
             'volume': '',
             'issue': '',
             'page': '',
             'type': 'dataset',
             'publisher': 'World Data Center for Climate (WDCC) at DKRZ [ror:03ztgj037]',
             'editor': 'Boucher, Olivier [orcid:0000-0003-2328-5769]; Denvil, Sébastien [orcid:0000-0002-6715-3533]; Levavasseur, Guillaume [orcid:0000-0002-0801-0890]; Cozic, Anne [orcid:0000-0001-7543-3466]; Caubel, Arnaud [orcid:0000-0002-6210-8370]; Foujols, Marie-Alice [orcid:0000-0002-9747-4928]; Meurdesoif, Yann; Mellul, Lidia'},
        ]

        self.assertEqual(output, expected_output)

    def test_csv_creator_object(self):
        dcp = DataciteProcessing()
        doi_obj = "doi:10.1021/acs.jpclett.7b01097"
        expected_output = {
            'id': 'doi:10.1021/acs.jpclett.7b01097',
            'title': '',
            'author': '',
            'pub_date': '',
            'venue': '',
            'volume': '',
            'issue': '',
            'page': '',
            'type': '',
            'publisher': '',
            'editor': ''}

        out = dcp.csv_creator({"id": doi_obj, "type": "dois", "attributes": {"doi": doi_obj}})
        self.assertEqual(out, expected_output)

    def test_get_publisher_name_invalid_publishers(self):
        dcp = DataciteProcessing()
        item1 = {"publisher": {
            "name":"(:unav)"}
        }
        item2 = {"publisher": {
            "name":":unav"}
        }
        item3 = {"publisher": {
            "name":":unkn"}}
        item4 = {"publisher": {
            "name":"(:unkn)"}}
        item5 = {"publisher": {
            "name":"Edo : [publisher not identified]mon han"}}
        item6 = {"publisher": {
            "name":"[place of publication not identified]: [pubisher not identified]"
        }}
        item7 = {"publisher": {
            "name":"unknown unknown"
        }}
        item8 = {"publisher": {
            "name":"[unknown] : [unknown]"
        }}
        item9 = {"publisher": {
            "name":"[unknown] : College of Pharmacists of British Columbia"
        }}
        item10 = {"publisher": {
            "name":"[Edinburgh]: [Unknown]"
        }}
        item11 = {"publisher": {
            "name":"Unknown, National University of Singapore"
        }}
        item12 = {"publisher": {
            "name":"Not provided."
        }}
        item13 = {"publisher": {
            "name":"Soleure, s.n."
        }}
        item14 = {"publisher": {
            "name":"[s.l. , s.n]"
        }}
        item15 = {"publisher": {
            "name":"[ s.l. : s.n.]"
        }}
        item16 = {"publisher": {
            "name":"s.n.]"
        }}
        item17 = {"publisher": {
            "name":"Information not available, contact SND for more information"
        }}
        item18 = {"publisher": {
            "name":"Publisher Not Specified"
        }}
        result1 = dcp.get_publisher('doi:10.11578/dc.20191106.1', item1['publisher'])
        result2 = dcp.get_publisher('doi:10.11578/dc.20191106.1', item2['publisher'])
        result3 = dcp.get_publisher('doi:10.11578/dc.20191106.1', item3['publisher'])
        result4 = dcp.get_publisher('doi:10.11578/dc.20191106.1', item4['publisher'])
        result5 = dcp.get_publisher('doi:10.11578/dc.20191106.1', item5['publisher'])
        result6 = dcp.get_publisher('doi:10.11578/dc.20191106.1', item6['publisher'])
        result7 = dcp.get_publisher('doi:10.11578/dc.20191106.1', item7['publisher'])
        result8 = dcp.get_publisher('doi:10.11578/dc.20191106.1', item8['publisher'])
        result9 = dcp.get_publisher('doi:10.11578/dc.20191106.1', item9['publisher'])
        result10 = dcp.get_publisher('doi:10.11578/dc.20191106.1', item10['publisher'])
        result11 = dcp.get_publisher('doi:10.11578/dc.20191106.1', item11['publisher'])
        result12 = dcp.get_publisher('doi:10.11578/dc.20191106.1', item12['publisher'])
        result13 = dcp.get_publisher('doi:10.11578/dc.20191106.1', item13['publisher'])
        result14 = dcp.get_publisher('doi:10.11578/dc.20191106.1', item14['publisher'])
        result15 = dcp.get_publisher('doi:10.11578/dc.20191106.1', item15['publisher'])
        result16 = dcp.get_publisher('doi:10.11578/dc.20191106.1', item16['publisher'])
        result17 = dcp.get_publisher('doi:10.11578/dc.20191106.1', item17['publisher'])
        result18 = dcp.get_publisher('doi:10.11578/dc.20191106.1', item18['publisher'])
        expected_res = ""
        expected_res9 = "[unknown] : College of Pharmacists of British Columbia"
        expected_res10 = "[Edinburgh]: [Unknown]"
        expected_res11 = "Unknown, National University of Singapore"
        expected_res13 = "Soleure, s.n."

        self.assertEqual(result1, expected_res)
        self.assertEqual(result2, expected_res)
        self.assertEqual(result3, expected_res)
        self.assertEqual(result4, expected_res)
        self.assertEqual(result5, expected_res)
        self.assertEqual(result6, expected_res)
        self.assertEqual(result7, expected_res)
        self.assertEqual(result8, expected_res)
        self.assertEqual(result9, expected_res9)
        self.assertEqual(result10, expected_res10)
        self.assertEqual(result11, expected_res11)
        self.assertEqual(result12, expected_res)
        self.assertEqual(result13, expected_res13)
        self.assertEqual(result14, expected_res)
        self.assertEqual(result15, expected_res)
        self.assertEqual(result16, expected_res)
        self.assertEqual(result17, expected_res)
        self.assertEqual(result18, expected_res)

    def test_get_publisher_name_publisher_mapping(self):

        item = {
            "doi": "10.1594/pangaea.777220",
            "publisher": {"name":"PANGAEA - Data Publisher for Earth & Environmental Science"}
        }
        doi = '10.1594/pangaea.777220'
        datacite_processor = DataciteProcessing(orcid_index=None, doi_csv=None,
                                                publishers_filepath_dc=PUBLISHERS_MAPPING)
        publisher_name = datacite_processor.get_publisher(doi, item)
        self.assertEqual(publisher_name, 'PANGAEA - Data Publisher for Earth & Environmental Science [datacite:2]')

    def test_get_publisher_name_from_prefix(self):
        # The item has no declared publisher, but the DOI prefix is in the publishers' mapping
        item = {
            'publisher': '',
            'doi': '10.12753/sample_test_doi_with_known_prefix',
        }
        doi = '10.12753/sample_test_doi_with_known_prefix'
        datacite_processor = DataciteProcessing(orcid_index=None, doi_csv=None,
                                                publishers_filepath_dc=PUBLISHERS_MAPPING)
        publisher_name = datacite_processor.get_publisher(doi, item)
        self.assertEqual(publisher_name, 'ADLRO [datacite:3]')

    def test_to_validated_id_list(self):
        dcp = DataciteProcessing()
        # CASE_1:  is valid
        inp_1 = {'id': 'doi:10.11578/1367552', 'schema': 'doi'}
        out_1 = dcp.to_validated_id_list(inp_1)
        exp_1 = ['doi:10.11578/1367552']
        self.assertEqual(out_1, exp_1)
        dcp.storage_manager.delete_storage()

        dcp = DataciteProcessing()
        # CASE_2: is invalid
        inp_2 = {'id': 'doi:10.11578/136755', 'schema': 'doi'}
        out_2 = dcp.to_validated_id_list(inp_2)
        exp_2 = []
        self.assertEqual(out_2, exp_2)

        dcp = DataciteProcessing()
        # CASE_3:  valid orcid
        inp_3 = {'id': 'orcid:0000-0002-9286-2630', 'schema': 'orcid'}
        out_3 = dcp.to_validated_id_list(inp_3)
        exp_3 = ['orcid:0000-0002-9286-2630']
        self.assertEqual(out_3, exp_3)
        dcp.storage_manager.delete_storage()

        dcp = DataciteProcessing()
        # CASE_4: invalid doi in self._redis_values_br
        inp_4 = {'id': 'doi:10.1089/bsp.2008.002', 'schema': 'doi'}
        dcp._redis_values_br.append(inp_4['id'])
        out_4 = dcp.to_validated_id_list(inp_4)
        exp_4 = ['doi:10.1089/bsp.2008.002']
        self.assertEqual(out_4, exp_4)
        value = dcp.tmp_doi_m.storage_manager.get_value('doi:10.1089/bsp.2008.002')
        self.assertEqual(value, True)
        dcp.storage_manager.delete_storage()

    def test_to_validated_id_list_redis(self):
        dcp = DataciteProcessing(testing=True)
        # CASE_1:  is valid
        inp_1 = {'id': 'doi:10.11578/1367552', 'schema': 'doi'}
        out_1 = dcp.to_validated_id_list(inp_1)
        exp_1 = ['doi:10.11578/1367552']
        self.assertEqual(out_1, exp_1)
        dcp.storage_manager.delete_storage()

        dcp = DataciteProcessing(testing=True)
        # CASE_2: is invalid
        inp_2 = {'id': 'doi:10.11578/136755', 'schema': 'doi'}
        out_2 = dcp.to_validated_id_list(inp_2)
        exp_2 = []
        self.assertEqual(out_2, exp_2)

        dcp = DataciteProcessing(testing=True)
        # CASE_3:  valid orcid
        inp_3 = {'id': 'orcid:0000-0002-9286-2630', 'schema': 'orcid'}
        out_3 = dcp.to_validated_id_list(inp_3)
        exp_3 = ['orcid:0000-0002-9286-2630']
        self.assertEqual(out_3, exp_3)
        dcp.storage_manager.delete_storage()

        dcp = DataciteProcessing(testing=True)
        # CASE_4: invalid doi in self._redis_values_br
        inp_4 = {'id': 'doi:10.1089/bsp.2008.002', 'schema': 'doi'}
        dcp._redis_values_br.append(inp_4['id'])
        out_4 = dcp.to_validated_id_list(inp_4)
        exp_4 = ['doi:10.1089/bsp.2008.002']
        self.assertEqual(out_4, exp_4)
        value = dcp.tmp_doi_m.storage_manager.get_value('doi:10.1089/bsp.2008.002')
        self.assertEqual(value, True)
        dcp.storage_manager.delete_storage()

    def test_find_datacite_orcid(self):
        dcp = DataciteProcessing(testing=True)
        inp = ["https://orcid.org/0000-0002-9286-2630"]
        out = dcp.find_datacite_orcid(inp)
        exp_out = "orcid:0000-0002-9286-2630"
        self.assertEqual(out, exp_out)

        inp_invalid_id = ["https://orcid.org/0000-0002-9286-2631"]
        out_invalid_id = dcp.find_datacite_orcid(inp_invalid_id)
        exp_invalid_id = ""
        self.assertEqual(out_invalid_id, exp_invalid_id)

        dcp.orcid_m.storage_manager.delete_storage()

        # set a valid id as invalid in storage, so to check that the api check is
        # avoided if the info is already in storage
        dcp = DataciteProcessing(testing=True)
        dcp.orcid_m.storage_manager.set_value("orcid:0000-0002-9286-2630", False)
        inp = ["https://orcid.org/0000-0002-9286-2630"]
        out = dcp.find_datacite_orcid(inp)
        exp_out = ""
        self.assertEqual(out, exp_out)
        dcp.orcid_m.storage_manager.delete_storage()

        dcp = DataciteProcessing(testing=True)
        dcp.orcid_m.storage_manager.set_value("orcid:0000-0002-9286-2631", True)
        inp = ["https://orcid.org/0000-0002-9286-2631"]
        out = dcp.find_datacite_orcid(inp)
        exp_out = "orcid:0000-0002-9286-2631"
        self.assertEqual(out, exp_out)
        dcp.orcid_m.storage_manager.delete_storage()

    def test_find_datacite_orcid_api_disabled_not_in_index(self):
        """Se l'API è OFF e l'ORCID non è nell'indice, non deve essere risolto."""
        dp = DataciteProcessing(use_orcid_api=False)
        test_doi = "10.9999/noindex"
        candidate = "0000-0003-4082-1500"  # valido sintatticamente

        out = dp.find_datacite_orcid([candidate], test_doi)

        self.assertEqual(out, "")
        # Non deve essere stato scritto in tmp storage
        self.assertIsNone(dp.tmp_orcid_m.storage_manager.get_value(f"orcid:{candidate}"))

        dp.storage_manager.delete_storage()

    def test_find_datacite_orcid_api_disabled_from_index(self):
        """Se l'API è OFF ma l'ORCID è nell'indice DOI→ORCID, deve essere risolto e salvato in tmp storage."""
        dp = DataciteProcessing(use_orcid_api=False)
        test_doi = "10.1234/test"
        test_orcid = "0000-0002-1234-5678"
        test_name = "Smith, John"

        # l'indice DOI→ORCID viene popolato
        dp.orcid_index.data = {test_doi: {f"{test_name} [orcid:{test_orcid}]"}}

        out = dp.find_datacite_orcid([test_orcid], test_doi)

        self.assertEqual(out, f"orcid:{test_orcid}")
        self.assertTrue(dp.tmp_orcid_m.storage_manager.get_value(f"orcid:{test_orcid}"))

        dp.storage_manager.delete_storage()

    def test_get_venue_container(self):
        item={'container': {'type': 'DataRepository', 'title': 'GEM Datasets'}, 'reason': None, 'prefix': '10.13117', 'citationsOverTime': [], 'registered': '2014-03-24T10:51:17Z', 'language': 'en', 'source': None, 'suffix': 'gem.dataset.ghea-v1.0', 'relatedItems': [], 'descriptions': [{'descriptionType': 'SeriesInformation', 'description': 'GEM Datasets'}, {'descriptionType': 'SeriesInformation', 'description': 'GEM Catalogues'}], 'sizes': ['1011 records'], 'versionOfCount': 0, 'relatedIdentifiers': [{'relationType': 'IsIdenticalTo', 'relatedIdentifier': 'http://emidius.eu/GEH/', 'relatedIdentifierType': 'URL'}, {'relationType': 'IsDocumentedBy', 'relatedIdentifier': '10.13117/gem.gegd.tr2013.01', 'relatedIdentifierType': 'DOI'}, {'relationType': 'Compiles', 'relatedIdentifier': '10.13117/gem.dataset.ghec-v1.0', 'relatedIdentifierType': 'DOI'}, {'relationType': 'References', 'relatedIdentifier': '10.6092/ingv.it-ahead', 'relatedIdentifierType': 'DOI'}], 'created': '2014-03-24T10:51:17Z', 'dates': [{'date': '1008-04-27/1903-12-28', 'dateType': 'Collected'}, {'date': '2013-06-01', 'dateType': 'Available'}, {'date': '2010-11-01/2013-03-31', 'dateType': 'Created'}, {'date': '2013', 'dateType': 'Issued'}], 'published': '2013', 'geoLocations': [], 'partCount': 0, 'publicationYear': 2013, 'partOfCount': 0, 'updated': '2020-07-26T16:07:36Z', 'formats': ['text/html', 'image/svg+xml', 'application/pdf'], 'fundingReferences': [], 'creators': [{'nameType': 'Personal', 'affiliation': [], 'givenName': 'Paola', 'familyName': 'Albini', 'name': 'Albini, Paola', 'nameIdentifiers': [{'nameIdentifierScheme': 'ORCID', 'schemeUri': 'https://orcid.org', 'nameIdentifier': 'https://orcid.org/0000-0003-4149-9760'}]}, {'nameType': 'Personal', 'affiliation': [], 'givenName': 'Roger M.W.', 'familyName': 'Musson', 'name': 'Musson, Roger M.W.', 'nameIdentifiers': [{'nameIdentifierScheme': 'ISNI', 'nameIdentifier': '0000 0000 5424 2727'}]}, {'nameType': 'Personal', 'affiliation': [], 'givenName': 'Antonio A.', 'familyName': 'Gomez Capera', 'name': 'Gomez Capera, Antonio A.', 'nameIdentifiers': []}, {'nameType': 'Personal', 'affiliation': [], 'givenName': 'Mario', 'familyName': 'Locati', 'name': 'Locati, Mario', 'nameIdentifiers': [{'nameIdentifierScheme': 'ORCID', 'schemeUri': 'https://orcid.org', 'nameIdentifier': 'https://orcid.org/0000-0003-2185-3267'}]}, {'nameType': 'Personal', 'affiliation': [], 'givenName': 'Andrea', 'familyName': 'Rovida', 'name': 'Rovida, Andrea', 'nameIdentifiers': [{'nameIdentifierScheme': 'ORCID', 'schemeUri': 'https://orcid.org', 'nameIdentifier': 'https://orcid.org/0000-0001-6147-9981'}]}, {'nameType': 'Personal', 'affiliation': [], 'givenName': 'Massimiliano', 'familyName': 'Stucchi', 'name': 'Stucchi, Massimiliano', 'nameIdentifiers': [{'nameIdentifierScheme': 'ORCID', 'schemeUri': 'https://orcid.org', 'nameIdentifier': 'https://orcid.org/0000-0002-5870-1542'}]}, {'nameType': 'Personal', 'affiliation': [], 'givenName': 'Daniele', 'familyName': 'Viganò', 'name': 'Viganò, Daniele', 'nameIdentifiers': [{'nameIdentifierScheme': 'ORCID', 'schemeUri': 'https://orcid.org', 'nameIdentifier': 'https://orcid.org/0000-0003-2713-8387'}]}], 'schemaVersion': 'http://datacite.org/schema/kernel-3', 'versionCount': 0, 'metadataVersion': 3, 'citationCount': 0, 'types': {'schemaOrg': 'Dataset', 'resourceTypeGeneral': 'Dataset', 'citeproc': 'dataset', 'bibtex': 'misc', 'ris': 'DATA', 'resourceType': 'Dataset/Earthquakes'}, 'isActive': True, 'viewsOverTime': [], 'identifiers': [], 'subjects': [{'subject': 'Earthquake history'}, {'subject': 'Historical seismology'}, {'subject': 'Catalogue'}, {'subject': 'Archive'}, {'subject': 'Macroseismic data'}, {'subject': 'GEM'}], 'titles': [{'title': 'GEM Global Historical Earthquake Archive'}], 'url': 'https://www.emidius.eu/GEH/', 'downloadCount': 0, 'rightsList': [{'rights': 'Copyright © 2013 GEM Foundation, Albini, P., R.M.W. Musson, A.A. Gomez Capera, M. Locati, A. Rovida, M. Stucchi, and D. Viganò'}, {'rightsUri': 'http://creativecommons.org/licenses/by-nc-sa/4.0', 'rights': 'Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International'}], 'contentUrl': None, 'contributors': [{'affiliation': [], 'name': 'Istituto Nazionale Di Geofisica E Vulcanologia (INGV)', 'nameIdentifiers': [], 'contributorType': 'DataCollector'}, {'affiliation': [], 'name': 'British Geological Survey (BGS)', 'nameIdentifiers': [], 'contributorType': 'DataCollector'}], 'referenceCount': 1, 'viewCount': 0, 'downloadsOverTime': [], 'doi': '10.13117/gem.dataset.ghea-v1.0', 'publisher': {'name': 'GEM Foundation, Pavia, Italy'}, 'version': '1.0', 'state': 'findable', 'alternateIdentifiers': []}
        row = {'id': '', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '',
               'type': 'dataset', 'publisher': '', 'editor': ''}
        datacite_processor = DataciteProcessing(orcid_index=None, doi_csv=None,
                                                publishers_filepath_dc=PUBLISHERS_MAPPING)
        venue_name = datacite_processor.get_venue_name(item, row)
        self.assertEqual(venue_name, 'gem datasets')

    def test_get_venue_name_no_container(self):
        item = {
            "container": {},
            "relatedIdentifiers": [
                {
                    "relationType": "IsSupplementTo",
                    "resourceTypeGeneral": "Text",
                    "relatedIdentifier": "10.4230/LIPIcs.ECOOP.2023.39",
                    "relatedIdentifierType": "DOI"
                },
                {
                    "relationType": "IsPartOf",
                    "relatedIdentifier": "2509-8195",
                    "relatedIdentifierType": "ISSN"
                },
            ]
        }
        row = {'id': '', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '',
               'type': 'journal article', 'publisher': '', 'editor': ''}
        datacite_processor = DataciteProcessing(orcid_index=None, doi_csv=None,
                                                publishers_filepath_dc=PUBLISHERS_MAPPING)
        venue_name = datacite_processor.get_venue_name(item, row)
        self.assertEqual(venue_name, '[issn:2509-8195]')

    def test_get_venue_name_with_ISSN(self):
        item = {
            "container": {"type": "Series", "identifier": "2509-8195", "identifierType": "ISSN", "title": "DARTS",
                          "volume": "Vol. 9", "firstPage": "pages 25:1", "lastPage": "25:2"}
        }
        row = {'id': '', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '',
               'type': 'journal article', 'publisher': '', 'editor': ''}
        datacite_processor = DataciteProcessing(orcid_index=None, doi_csv=None,
                                                publishers_filepath_dc=PUBLISHERS_MAPPING)
        venue_name = datacite_processor.get_venue_name(item, row)
        self.assertEqual(venue_name,
                         "darts [issn:2509-8195]")
        # ISSN with wrong number of digits
        item1 = {
            "container": {"type": "Journal", "issue": "18", "title": "Geophysical Research Letters", "volume": "41",
                          "lastPage": "6451", "firstPage": "6443", "identifier": "00948276", "identifierType": "ISSN"}
        }
        row1 = {'id': '', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '',
                'type': 'journal article', 'publisher': '', 'editor': ''}
        venue_name1 = datacite_processor.get_venue_name(item1, row1)
        self.assertEqual(venue_name1,
                         "geophysical research letters [issn:0094-8276]")

    def test_get_pages(self):
        item = {
            "container": {"type": "Journal", "issue": "7", "title": "Global Biogeochemical Cycles", "volume": "29",
                          "lastPage": "1013", "firstPage": "994", "identifier": "08866236",
                          "identifierType": "ISSN"}
        }
        datacite_processor = DataciteProcessing(orcid_index=None,
                                                publishers_filepath_dc=PUBLISHERS_MAPPING)
        pages = datacite_processor.get_datacite_pages(item)
        self.assertEqual(pages, '994-1013')

    def test_get_pages_right_letter(self):
        item = {
            "container": {"type": "Journal", "issue": "4", "title": "Ecosphere", "volume": "10",
                          "firstPage": "e02701", "identifier": "2150-8925", "identifierType": "ISSN"}
        }
        datacite_processor = DataciteProcessing(orcid_index=None,
                                                publishers_filepath_dc=PUBLISHERS_MAPPING)
        pages = datacite_processor.get_datacite_pages(item)
        self.assertEqual(pages, 'e02701-e02701')

    def test_get_pages_wrong_letter(self):
        item = {
            "relatedIdentifiers": [
                {"relationType": "IsPartOf",
                 "relatedIdentifier": "0094-2405",
                 "relatedIdentifierType": "ISSN",
                 "firstPage": "583b",
                 "lastPage": "584"},
                {"relationType": "References",
                 "relatedIdentifier": "10.1016/j.ecl.2014.08.007",
                 "relatedIdentifierType": "DOI"}
            ]
        }
        datacite_processor = DataciteProcessing(orcid_index=None,
                                                publishers_filepath_dc=PUBLISHERS_MAPPING)
        pages = datacite_processor.get_datacite_pages(item)
        self.assertEqual(pages, '583-584')

    def test_get_pages_roman_letters(self):
        item = {
            "relatedIdentifiers": [
                {"relationType": "IsPartOf",
                 "relatedIdentifier": "0094-2405",
                 "relatedIdentifierType": "ISSN",
                 "firstPage": "iv",
                 "lastPage": "l"},
                {"relationType": "References",
                 "relatedIdentifier": "10.1016/j.ecl.2014.08.007",
                 "relatedIdentifierType": "DOI"}
            ]
        }
        datacite_processor = DataciteProcessing(orcid_index=None,
                                                publishers_filepath_dc=PUBLISHERS_MAPPING)
        pages = datacite_processor.get_datacite_pages(item)
        self.assertEqual(pages, 'iv-l')

    def test_get_pages_non_roman_letters(self):
        item = {
            "relatedIdentifiers": [
                {"relationType": "IsPartOf",
                 "relatedIdentifier": "0094-2405",
                 "relatedIdentifierType": "ISSN",
                 "firstPage": "kj",
                 "lastPage": "hh"},
                {"relationType": "References",
                 "relatedIdentifier": "10.1016/j.ecl.2014.08.007",
                 "relatedIdentifierType": "DOI"}
            ]
        }
        datacite_processor = DataciteProcessing(orcid_index=None,
                                                publishers_filepath_dc=PUBLISHERS_MAPPING)
        pages = datacite_processor.get_datacite_pages(item)
        self.assertEqual(pages, '')

    def test_get_pages_with_strings_no_venue_id(self):
        item = {'container': {
                        'firstPage': '13. Studi umanistici. Serie Antichistica',
                        'type': 'Series',
                        'title': 'Collana Studi e Ricerche'
                }}
        datacite_processor = DataciteProcessing(orcid_index=None, doi_csv=None,
                                                publishers_filepath_dc=PUBLISHERS_MAPPING)
        pages = datacite_processor.get_datacite_pages(item)
        self.assertEqual(pages, '')

    def test_venue_id_cont_and_rel_id(self):
        items = {'data': [
            {
                "id": "10.1002/2014jd022411",
                "type": "dois",
                "attributes": {
                    "doi": "10.1002/2014jd022411",
                    "identifiers": [],
                    "titles": [{
                        "title": "\n              Assessing the magnitude of CO\n              \n              flux uncertainty in atmospheric CO\n              \n              records using products from NASA's Carbon Monitoring Flux Pilot Project\n            "}],
                    "publisher": {
                        "name":"(:unav)"},
                    "container": {"type": "Journal", "issue": "2",
                                  "title": "Journal of Geophysical Research: Atmospheres", "volume": "120",
                                  "lastPage": "765", "firstPage": "734", "identifier": "2169897X",
                                  "identifierType": "ISSN"},
                    "types": {"ris": "JOUR", "bibtex": "article", "citeproc": "article-journal",
                              "schemaOrg": "ScholarlyArticle", "resourceType": "JournalArticle",
                              "resourceTypeGeneral": "Text"},
                    "relatedIdentifiers": [{"relationType": "IsPartOf", "relatedIdentifier": "2169897X",
                                            "resourceTypeGeneral": "Collection", "relatedIdentifierType": "ISSN"}]
                }
            }
        ]}
        datacite_processor = DataciteProcessing(orcid_index=None,
                                                publishers_filepath_dc=PUBLISHERS_MAPPING)
        output = list()
        for item in items['data']:
            output.append(datacite_processor.csv_creator(item))
        expected_output = [{'id': 'doi:10.1002/2014jd022411',
                            'title': "Assessing the magnitude of CO flux uncertainty in atmospheric CO records using products from NASA's Carbon Monitoring Flux Pilot Project",
                            'author': '', 'pub_date': '',
                            'venue': 'journal of geophysical research: atmospheres [issn:2169-897X]',
                            'volume': '120', 'issue': '2', 'page': '734-765', 'type': 'journal article',
                            'publisher': 'Wiley [datacite:1]', 'editor': ''}]
        self.assertEqual(output, expected_output)

    def test_venue_id_cont_and_rel_id_no_types(self):
        # the absence of publication types specified excludes the possibility
        # to assert whether the container can have an ISSN or not
        items = {'data': [
            {
                "id": "10.1002/2014jd022411",
                "type": "dois",
                "attributes": {
                    "doi": "10.1002/2014jd022411",
                    "identifiers": [],
                    "titles": [{
                        "title": "\n              Assessing the magnitude of CO\n              \n              flux uncertainty in atmospheric CO\n              \n              records using products from NASA's Carbon Monitoring Flux Pilot Project\n            "}],
                    "publisher": {"name":"(:unav)"},
                    "container": {"type": "Journal", "issue": "2",
                                  "title": "Journal of Geophysical Research: Atmospheres", "volume": "120",
                                  "lastPage": "765", "firstPage": "734", "identifier": "2169897X",
                                  "identifierType": "ISSN"},
                    "relatedIdentifiers": [{"relationType": "IsPartOf", "relatedIdentifier": "2169897X",
                                            "resourceTypeGeneral": "Collection", "relatedIdentifierType": "ISSN"}]
                }
            }
        ]}
        datacite_processor = DataciteProcessing(orcid_index=None,
                                                publishers_filepath_dc=PUBLISHERS_MAPPING)
        output = list()
        for item in items['data']:
            output.append(datacite_processor.csv_creator(item))
        expected_output = [{'id': 'doi:10.1002/2014jd022411',
                            'title': "Assessing the magnitude of CO flux uncertainty in atmospheric CO records using products from NASA's Carbon Monitoring Flux Pilot Project",
                            'author': '', 'pub_date': '', 'venue': 'journal of geophysical research: atmospheres',
                            'volume': '120', 'issue': '2', 'page': '734-765', 'type': '',
                            'publisher': 'Wiley [datacite:1]', 'editor': ''}]
        self.assertEqual(output, expected_output)

    def test_get_agents_strings_list_overlapping_surnames(self):
        # The surname of one author is included in the surname of another.
        entity_attr_dict = {
            "creators": [
                {"name": "Olivarez Lyle, Annette",
                 "givenName": "Annette",
                 "familyName": "Olivarez Lyle",
                 "affiliation": [],
                 "nameIdentifiers": []
                 },
                {"name": "Lyle, Mitchell W",
                 "givenName": "Mitchell W",
                 "familyName": "Lyle",
                 "nameIdentifiers": [
                     {"schemeUri": "https://orcid.org",
                      "nameIdentifier": "https://orcid.org/0000-0002-0861-0511",
                      "nameIdentifierScheme": "ORCID"}
                 ],
                 "affiliation": []
                 }
            ],
            "contributors": []
        }

        datacite_processor = DataciteProcessing(None, None)
        authors_list = datacite_processor.add_authors_to_agent_list(entity_attr_dict, [],
                                                                    doi="doi:10.1594/pangaea.777220")
        editors_list = datacite_processor.add_editors_to_agent_list(entity_attr_dict, [],
                                                                    doi="doi:10.1594/pangaea.777220")
        agents_list = authors_list + editors_list
        csv_manager = CSVManager()
        csv_manager.data = {'10.1594/pangaea.777220': {'Lyle, Mitchell W [0000-0002-0861-0511]'}}
        datacite_processor.orcid_index = csv_manager
        authors_strings_list, editors_strings_list = datacite_processor.get_agents_strings_list(
            '10.1594/pangaea.777220', agents_list)

        expected_authors_list = ['Olivarez Lyle, Annette',
                                 'Lyle, Mitchell W [orcid:0000-0002-0861-0511]']
        expected_editors_list = []
        self.assertEqual((authors_strings_list, editors_strings_list), (expected_authors_list, expected_editors_list))

    def test_get_agents_strings_list(self):
        entity_attr_dict = {
            "doi": "10.1002/2014jd022411",
            "creators": [
                {"name": "Ott, Lesley E.", "nameType": "Personal", "givenName": "Lesley E.", "familyName": "Ott",
                 "affiliation": [], "nameIdentifiers": []},
                {"name": "Pawson, Steven", "nameType": "Personal", "givenName": "Steven", "familyName": "Pawson",
                 "affiliation": [], "nameIdentifiers": []},
                {"name": "Collatz, George J.", "nameType": "Personal", "givenName": "George J.",
                 "familyName": "Collatz", "affiliation": [], "nameIdentifiers": []},
                {"name": "Gregg, Watson W.", "nameType": "Personal", "givenName": "Watson W.", "familyName": "Gregg",
                 "affiliation": [], "nameIdentifiers": []},
                {"name": "Menemenlis, Dimitris", "nameType": "Personal", "givenName": "Dimitris",
                 "familyName": "Menemenlis", "affiliation": [], "nameIdentifiers": [
                    {"schemeUri": "https://orcid.org", "nameIdentifier": "https://orcid.org/0000-0001-9940-8409",
                     "nameIdentifierScheme": "ORCID"}]},
                {"name": "Brix, Holger", "nameType": "Personal", "givenName": "Holger", "familyName": "Brix",
                 "affiliation": [], "nameIdentifiers": []},
                {"name": "Rousseaux, Cecile S.", "nameType": "Personal", "givenName": "Cecile S.",
                 "familyName": "Rousseaux", "affiliation": [], "nameIdentifiers": []},
                {"name": "Bowman, Kevin W.", "nameType": "Personal", "givenName": "Kevin W.", "familyName": "Bowman",
                 "affiliation": [], "nameIdentifiers": []},
                {"name": "Liu, Junjie", "nameType": "Personal", "givenName": "Junjie", "familyName": "Liu",
                 "affiliation": [], "nameIdentifiers": []},
                {"name": "Eldering, Annmarie", "nameType": "Personal", "givenName": "Annmarie",
                 "familyName": "Eldering", "affiliation": [], "nameIdentifiers": []},
                {"name": "Gunson, Michael R.", "nameType": "Personal", "givenName": "Michael R.",
                 "familyName": "Gunson", "affiliation": [], "nameIdentifiers": []},
                {"name": "Kawa, Stephan R.", "nameType": "Personal", "givenName": "Stephan R.", "familyName": "Kawa",
                 "affiliation": [], "nameIdentifiers": []}],
            "contributors": [{
                'name': 'AKMB-News: Informationen Zu Kunst, Museum Und Bibliothek',
                'nameType': 'Personal',
                'givenName': 'Museum Und Bibliothek',
                'familyName': 'AKMB-News: Informationen Zu Kunst',
                'affiliation': [],
                'contributorType': 'Editor',
                'nameIdentifiers': []}]}

        datacite_processor = DataciteProcessing()
        authors_list = datacite_processor.add_authors_to_agent_list(entity_attr_dict, [],
                                                                    doi="doi:10.1002/2014jd022411")
        editors_list = datacite_processor.add_editors_to_agent_list(entity_attr_dict, [],
                                                                    doi="doi:10.1002/2014jd022411")
        agents_list = authors_list + editors_list
        authors_strings_list, editors_strings_list = datacite_processor.get_agents_strings_list('10.1002/2014jd022411',
                                                                                                agents_list)
        expected_authors_list = ['Ott, Lesley E.', 'Pawson, Steven', 'Collatz, George J.', 'Gregg, Watson W.',
                                 'Menemenlis, Dimitris [orcid:0000-0001-9940-8409]', 'Brix, Holger',
                                 'Rousseaux, Cecile S.', 'Bowman, Kevin W.', 'Liu, Junjie', 'Eldering, Annmarie',
                                 'Gunson, Michael R.', 'Kawa, Stephan R.']
        expected_editors_list = ['AKMB-News: Informationen Zu Kunst, Museum Und Bibliothek']

        self.assertEqual(authors_strings_list, expected_authors_list)
        self.assertEqual(editors_strings_list, expected_editors_list)

    def test_get_agents_strings_list_same_family(self):
        # Two authors have the same family name and the same given name initials
        entity_attr_dict = {
            "creators": [
                {"name": "Schulz, Heide N",
                 "nameType": "Personal",
                 "givenName": "Heide N",
                 "familyName": "Schulz",
                 "nameIdentifiers":
                     [
                         {"schemeUri": "https://orcid.org", "nameIdentifier": "https://orcid.org/0000-0003-1445-0291",
                          "nameIdentifierScheme": "ORCID"}
                     ],
                 "affiliation": []},
                {"name": "Schulz, Horst D",
                 "nameType": "Personal",
                 "givenName": "Horst D",
                 "familyName": "Schulz",
                 "affiliation": [],
                 "nameIdentifiers": []}],
            "contributors": []
        }
        datacite_processor = DataciteProcessing()
        authors_list = datacite_processor.add_authors_to_agent_list(entity_attr_dict, [],
                                                                    doi="doi:10.1594/pangaea.231378")
        editors_list = datacite_processor.add_editors_to_agent_list(entity_attr_dict, [],
                                                                    doi="doi:10.1594/pangaea.231378")
        agents_list = authors_list + editors_list
        authors_strings_list, _ = datacite_processor.get_agents_strings_list('10.1594/pangaea.231378', agents_list)
        expected_authors_list = ['Schulz, Heide N [orcid:0000-0003-1445-0291]', 'Schulz, Horst D']
        self.assertEqual(authors_strings_list, expected_authors_list)

    def test_get_agents_strings_list_homonyms(self):
        # Two authors have the same family name and the same given name
        entity_attr_dict = {
            "creators":
                [
                    {"name": "Viorel, Cojocaru",
                     "nameType": "Personal",
                     "givenName": "Cojocaru",
                     "familyName": "Viorel",
                     "affiliation": [],
                     "nameIdentifiers": []},
                    {"name": "Viorel, Cojocaru",
                     "nameType": "Personal",
                     "givenName": "Cojocaru",
                     "familyName": "Viorel",
                     "affiliation": [],
                     "nameIdentifiers": []
                     },
                    {"name": "Ciprian, Panait",
                     "nameType": "Personal",
                     "givenName": "Panait",
                     "familyName": "Ciprian",
                     "affiliation": [],
                     "nameIdentifiers": []}
                ],
            "contributors": []
        }
        datacite_processor = DataciteProcessing(None, None)
        authors_list = datacite_processor.add_authors_to_agent_list(entity_attr_dict, [],
                                                                    doi="doi:10.12753/2066-026x-14-246")
        editors_list = datacite_processor.add_editors_to_agent_list(entity_attr_dict, [],
                                                                    doi="doi:10.12753/2066-026x-14-246")
        agents_list = authors_list + editors_list
        authors_strings_list, _ = datacite_processor.get_agents_strings_list('10.12753/2066-026x-14-246', agents_list)
        expected_authors_list = ['Viorel, Cojocaru', 'Viorel, Cojocaru', 'Ciprian, Panait']
        self.assertEqual(authors_strings_list, expected_authors_list)

    def test_get_agents_strings_list_inverted_names(self):
        # One author with an ORCID has as a name the surname of another
        entity_attr_dict = {
            "creators":
                [
                    {"name": "Viorel, Cojocaru",
                     "nameType": "Personal",
                     "givenName": "Cojocaru",
                     "familyName": "Viorel",
                     "affiliation": [],
                     "nameIdentifiers": []},

                    {"name": "Cojocaru, John",
                     "nameType": "Personal",
                     "givenName": "John",
                     "familyName": "Cojocaru",
                     "affiliation": [],
                     "nameIdentifiers": []
                     },
                    {"name": "Ciprian, Panait",
                     "nameType": "Personal",
                     "givenName": "Panait",
                     "familyName": "Ciprian",
                     "affiliation": [],
                     "nameIdentifiers": []}
                ],
            "contributors": []
        }
        # Note : 'Cojocaru, John' is not one of the authors of the item, the name was made up for testing purposes
        datacite_processor = DataciteProcessing(None, None)
        authors_list = datacite_processor.add_authors_to_agent_list(entity_attr_dict, [],
                                                                    doi="doi:10.12753/2066-026x-14-246")
        editors_list = datacite_processor.add_editors_to_agent_list(entity_attr_dict, [],
                                                                    doi="doi:10.12753/2066-026x-14-246")
        agents_list = authors_list + editors_list
        authors_strings_list, _ = datacite_processor.get_agents_strings_list('10.12753/2066-026x-14-246', agents_list)
        expected_authors_list = ['Viorel, Cojocaru', 'Cojocaru, John', 'Ciprian, Panait']
        self.assertEqual(authors_strings_list, expected_authors_list)

    def test_get_agents_strings_list_api_disabled_no_index(self):
        """Con API OFF e indice vuoto, gli ORCID presenti come nameIdentifier NON devono comparire in output."""
        entity_attr_dict = {
            "creators": [
                {
                    "name": "Doe, Jane",
                    "nameType": "Personal",
                    "givenName": "Jane",
                    "familyName": "Doe",
                    "nameIdentifiers": [
                        {
                            "schemeUri": "https://orcid.org",
                            "nameIdentifier": "https://orcid.org/0000-0003-4082-1500",
                            "nameIdentifierScheme": "ORCID",
                        }
                    ],
                }
            ],
            "contributors": [],
        }

        dp = DataciteProcessing(use_orcid_api=False)  # indice vuoto, nessuna API
        authors_list = dp.add_authors_to_agent_list(entity_attr_dict, [], doi="doi:10.9999/noindex")
        editors_list = dp.add_editors_to_agent_list(entity_attr_dict, [], doi="doi:10.9999/noindex")
        authors_strings, editors_strings = dp.get_agents_strings_list("10.9999/noindex", authors_list + editors_list)

        # L'ORCID NON deve essere aggiunto tra [] perché non c'è indice e l'API è OFF
        self.assertEqual(authors_strings, ["Doe, Jane"])
        self.assertEqual(editors_strings, [])

        dp.storage_manager.delete_storage()

    def test_find_datacite_orcid_with_index(self):
        """Test ORCID validation using ORCID index before API validation"""
        # Setup
        test_doi = "10.1234/test123"
        test_orcid = "0000-0002-1234-5678"
        test_name = "Smith, John"

        # Create DataciteProcessing instance with ORCID index
        dp = DataciteProcessing()
        dp.orcid_index.data = {test_doi: {f"{test_name} [orcid:{test_orcid}]"}}

        # Test Case 1: ORCID found in index
        inp_1 = [test_orcid]
        out_1 = dp.find_datacite_orcid([test_orcid], test_doi)
        exp_1 = f"orcid:{test_orcid}"
        self.assertEqual(out_1, exp_1)
        # Verify it was added to temporary storage
        self.assertTrue(dp.tmp_orcid_m.storage_manager.get_value(f"orcid:{test_orcid}"))

        # Test Case 2: ORCID not in index but valid via API
        inp_2 = ["0000-0003-4082-1500"]
        out_2 = dp.find_datacite_orcid(["0000-0003-4082-1500"], test_doi)
        exp_2 = "orcid:0000-0003-4082-1500"
        self.assertEqual(out_2, exp_2)

        # Test Case 3: ORCID not in index and invalid
        inp_3 = ["0000-0000-0000-0000"]
        out_3 = dp.find_datacite_orcid(["0000-0000-0000-0000"], test_doi)
        exp_3 = ""
        self.assertEqual(out_3, exp_3)

        # Test Case 4: Valid ORCID but no DOI provided (retrocompatibilità)
        inp_4 = [test_orcid]
        out_4 = dp.find_datacite_orcid(inp_4)  # No DOI
        exp_4 = f"orcid:{test_orcid}"  # Should still validate via API
        self.assertEqual(out_4, exp_4)

        # Test Case 5: Multiple ORCIDs, first one valid
        inp_5 = [test_orcid, "0000-0000-0000-0000"]
        out_5 = dp.find_datacite_orcid([test_orcid, "0000-0000-0000-0000"], test_doi)
        exp_5 = f"orcid:{test_orcid}"
        self.assertEqual(out_5, exp_5)

        # Cleanup
        dp.storage_manager.delete_storage()

    def test_find_datacite_orcid_api_enabled_invalid_in_storage(self):
        """API ON + ORCID marcato come invalid in storage: rifiuta subito (niente indice/API)."""
        dp = DataciteProcessing(use_orcid_api=True, testing=True)
        oid = "orcid:0000-0002-9286-2630"
        dp.orcid_m.storage_manager.set_value(oid, False)
        out = dp.find_datacite_orcid([oid.split(":")[1]], "10.9999/anything")
        self.assertEqual(out, "")
        # nessuna semina in tmp
        self.assertIsNone(dp.tmp_orcid_m.storage_manager.get_value(oid))
        dp.orcid_m.storage_manager.delete_storage()

    def test_find_datacite_orcid_api_enabled_from_redis_snapshot(self):
        """API ON + storage/indice vuoti, ma ORCID presente nello snapshot Redis RA: accetta senza rete."""
        dp = DataciteProcessing(use_orcid_api=True)
        oid = "orcid:0000-0003-4082-1500"
        dp.update_redis_values(br=[], ra=[oid])  # simula snapshot
        out = dp.find_datacite_orcid([oid.split(":")[1]], "10.9999/noindex")
        self.assertEqual(out, oid)
        self.assertTrue(dp.tmp_orcid_m.storage_manager.get_value(oid))
        dp.storage_manager.delete_storage()

    def test_find_datacite_orcid_api_disabled_from_redis_snapshot(self):
        """API OFF + storage/indice vuoti, ORCID nello snapshot Redis RA: accetta offline."""
        dp = DataciteProcessing(use_orcid_api=False)
        oid = "orcid:0000-0003-4082-1500"
        dp.update_redis_values(br=[], ra=[oid])
        out = dp.find_datacite_orcid([oid.split(":")[1]], "10.9999/noindex")
        self.assertEqual(out, oid)
        self.assertTrue(dp.tmp_orcid_m.storage_manager.get_value(oid))
        dp.storage_manager.delete_storage()

    def test_find_datacite_orcid_api_disabled_in_storage(self):
        """API OFF + ORCID già valido nello storage persistente: deve essere accettato."""
        dp = DataciteProcessing(use_orcid_api=False, testing=True)
        oid = "orcid:0000-0003-4082-1500"
        dp.orcid_m.storage_manager.set_value(oid, True)
        out = dp.find_datacite_orcid([oid.split(":")[1]], "10.9999/any")
        self.assertEqual(out, oid)
        dp.orcid_m.storage_manager.delete_storage()

    def test_find_datacite_orcid_index_with_normalized_doi(self):
        """La lookup nell'indice deve funzionare anche se DOI è passato senza prefisso o viceversa."""
        dp = DataciteProcessing()
        doi_no_prefix = "10.1234/test-norm"
        doi_with_prefix = f"doi:{doi_no_prefix}"
        orcid = "0000-0002-1234-5678"
        dp.orcid_index.add_value(doi_with_prefix, f"Rossi, Mario [orcid:{orcid}]")
        #  DOI senza prefisso: deve matchare comunque
        out = dp.find_datacite_orcid([orcid], doi_no_prefix)
        self.assertEqual(out, f"orcid:{orcid}")
        self.assertTrue(dp.tmp_orcid_m.storage_manager.get_value(f"orcid:{orcid}"))
        dp.storage_manager.delete_storage()

    #PUBLISHER IDENTIFIERS
    def test_find_datacite_publisher_id_api_enabled_no_value_in_storage(self):
        """API ON + id non salvato nello storage."""
        dp = DataciteProcessing(use_ror_api=True, use_wikidata_api=True, use_viaf_api=True)
        publisher1 = {
            'publisherIdentifierScheme': 'ROR',
            'schemeUri': 'https://ror.org',
            'name': 'DataCite',
            'publisherIdentifier': 'https://ror.org/04wxnsj81'
        }
        publisher2 = {
            'publisherIdentifierScheme': 'VIAF',
            'schemeUri': 'https://viaf.org/',
            'name': 'Deutsches archäologisches Institut',
            'publisherIdentifier': 'http://viaf.org/viaf/148463773'
        }
        publisher3 = {
            'publisherIdentifierScheme': 'Wikidata',
            'schemeUri': 'https://www.wikidata.org/wiki/',
            'name': 'University of Tokyo',
            'publisherIdentifier': 'https://wikidata.org/wiki/Q7842'
        }

        id1 = "ror:04wxnsj81"
        id2 = "viaf:148463773"
        id3 = "wikidata:Q7842"

        out1 = dp.get_publisher_id(publisher1)
        out2 = dp.get_publisher_id(publisher2)
        out3 = dp.get_publisher_id(publisher3)

        self.assertEqual(out1, id1)
        self.assertEqual(out2, id2)
        self.assertEqual(out3, id3)

        dp.storage_manager.delete_storage()

    def test_get_pubblisher_api_disabled_no_index(self):
        """Con API OFF e indice vuoto, i publisher id presenti NON devono comparire in output."""

        publisher1 = {
            'publisherIdentifierScheme': 'ROR',
            'schemeUri': 'https://ror.org',
            'name': 'DataCite',
            'publisherIdentifier': 'https://ror.org/04wxnsj81'
        }
        publisher2 = {
            'publisherIdentifierScheme': 'VIAF',
            'schemeUri': 'https://viaf.org/',
            'name': 'Deutsches archäologisches Institut',
            'publisherIdentifier': 'http://viaf.org/viaf/148463773'
        }
        publisher3 = {
            'publisherIdentifierScheme': 'Wikidata',
            'schemeUri': 'https://www.wikidata.org/wiki/',
            'name': 'University of Tokyo',
            'publisherIdentifier': 'https://wikidata.org/wiki/Q7842'
        }


        dp = DataciteProcessing(use_ror_api=False, use_viaf_api=False, use_wikidata_api=False)  # indice vuoto, nessuna API
        publisher_row1 = dp.get_publisher('10.60804/bpmz-jb79', publisher1)
        publisher_row2 = dp.get_publisher('10.60804/bpmz-jb79', publisher2)
        publisher_row3 = dp.get_publisher('10.60804/bpmz-jb79', publisher3)

        # L'id NON deve essere aggiunto tra [] perché non c'è indice e l'API è OFF
        self.assertEqual(publisher_row1, "DataCite")
        self.assertEqual(publisher_row2, "Deutsches archäologisches Institut")
        self.assertEqual(publisher_row3, "University of Tokyo")

        dp.storage_manager.delete_storage()

    def test_find_datacite_publisher_id_api_enabled_invalid_in_storage(self):
        """API ON + id marcato come invalid in storage: rifiuta subito (niente indice/API)."""
        dp = DataciteProcessing(use_ror_api=True, use_wikidata_api=True, use_viaf_api=True)
        publisher1 = {
            'publisherIdentifierScheme': 'ROR',
            'schemeUri': 'https://ror.org',
            'name': 'DataCite',
            'publisherIdentifier': 'https://ror.org/04wxnsj81'
        }
        publisher2 = {
            'publisherIdentifierScheme': 'VIAF',
            'schemeUri': 'https://viaf.org/',
            'name': 'Deutsches archäologisches Institut',
            'publisherIdentifier': 'http://viaf.org/viaf/148463773'
        }
        publisher3 = {
            'publisherIdentifierScheme': 'Wikidata',
            'schemeUri': 'https://www.wikidata.org/wiki/',
            'name': 'University of Tokyo',
            'publisherIdentifier': 'https://wikidata.org/wiki/Q7842'
        }

        id1 = "ror:04wxnsj81"
        id2 = "viaf:148463773"
        id3 = "wikidata:Q7842"

        dp.storage_manager.set_value(id1, False)
        dp.storage_manager.set_value(id2, False)
        dp.storage_manager.set_value(id3, False)

        out1 = dp.get_publisher_id(publisher1)
        out2 = dp.get_publisher_id(publisher2)
        out3 = dp.get_publisher_id(publisher3)

        self.assertEqual(out1, "")
        self.assertEqual(out2, "")
        self.assertEqual(out3, "")

        # nessuna semina in tmp
        self.assertIsNone(dp.tmp_viaf_m.storage_manager.get_value(id2))
        self.assertIsNone(dp.tmp_ror_m.storage_manager.get_value(id1))
        self.assertIsNone(dp.tmp_wikidata_m.storage_manager.get_value(id3))
        dp.storage_manager.delete_storage()

    def test_find_datacite_publisher_id_api_enabled_from_redis_snapshot(self):
        """API ON + storage/indice vuoti, ma id presente nello snapshot Redis RA: accetta senza rete."""
        dp = DataciteProcessing(use_viaf_api=True, use_wikidata_api=True, use_ror_api=True)

        id1 = "ror:04wxnsj81"
        id2 = "viaf:148463773"
        id3 = "wikidata:Q7842"

        publisher1 = {
            'publisherIdentifierScheme': 'ROR',
            'schemeUri': 'https://ror.org',
            'name': 'DataCite',
            'publisherIdentifier': 'https://ror.org/04wxnsj81'
        }
        publisher2 = {
            'publisherIdentifierScheme': 'VIAF',
            'schemeUri': 'https://viaf.org/',
            'name': 'Deutsches archäologisches Institut',
            'publisherIdentifier': 'http://viaf.org/viaf/148463773'
        }
        publisher3 = {
            'publisherIdentifierScheme': 'Wikidata',
            'schemeUri': 'https://www.wikidata.org/wiki/',
            'name': 'University of Tokyo',
            'publisherIdentifier': 'https://wikidata.org/wiki/Q7842'
        }

        dp.update_redis_values(br=[], ra=[id1, id2, id3])  # simula snapshot
        out1 = dp.get_publisher_id(publisher1)
        out2 = dp.get_publisher_id(publisher2)
        out3 = dp.get_publisher_id(publisher3)

        self.assertEqual(out1, id1)
        self.assertEqual(out2, id2)
        self.assertEqual(out3, id3)

        self.assertTrue(dp.tmp_ror_m.storage_manager.get_value(id1))
        self.assertTrue(dp.tmp_viaf_m.storage_manager.get_value(id2))
        self.assertTrue(dp.tmp_wikidata_m.storage_manager.get_value(id3))

        dp.storage_manager.delete_storage()

    def test_find_datacite_publisher_id_api_disabled_from_redis_snapshot(self):
        """API OFF + storage/indice vuoti, ORCID nello snapshot Redis RA: accetta offline."""
        dp = DataciteProcessing(use_ror_api=False, use_viaf_api=False, use_wikidata_api=False)

        id1 = "ror:04wxnsj81"
        id2 = "viaf:148463773"
        id3 = "wikidata:Q7842"
        id4 = "crossref:501100000739"

        publisher1 = {
            'publisherIdentifierScheme': 'ROR',
            'schemeUri': 'https://ror.org',
            'name': 'DataCite',
            'publisherIdentifier': 'https://ror.org/04wxnsj81'
        }
        publisher2 = {
            'publisherIdentifierScheme': 'VIAF',
            'schemeUri': 'https://viaf.org/',
            'name': 'Deutsches archäologisches Institut',
            'publisherIdentifier': 'http://viaf.org/viaf/148463773'
        }
        publisher3 = {
            'publisherIdentifierScheme': 'Wikidata',
            'schemeUri': 'https://www.wikidata.org/wiki/',
            'name': 'University of Tokyo',
            'publisherIdentifier': 'https://wikidata.org/wiki/Q7842'
        }

        dp.update_redis_values(br=[], ra=[id1, id2, id3, id4])  # simula snapshot
        out1 = dp.get_publisher_id(publisher1)
        out2 = dp.get_publisher_id(publisher2)
        out3 = dp.get_publisher_id(publisher3)

        self.assertEqual(out1, id1)
        self.assertEqual(out2, id2)
        self.assertEqual(out3, id3)

        self.assertTrue(dp.tmp_ror_m.storage_manager.get_value(id1))
        self.assertTrue(dp.tmp_viaf_m.storage_manager.get_value(id2))
        self.assertTrue(dp.tmp_wikidata_m.storage_manager.get_value(id3))

        dp.storage_manager.delete_storage()


    def test_find_datacite_publisher_id_api_disabled_in_storage(self):
        """API OFF + publisher id già valido nello storage persistente: deve essere accettato."""
        dp = DataciteProcessing(use_viaf_api=False, use_wikidata_api=False, use_ror_api=False)
        id1 = "ror:04wxnsj89" #invalid
        id2 = "viaf:148463773"
        id3 = "wikidata:Q7842"

        publisher1 = {
            'publisherIdentifierScheme': 'ROR',
            'schemeUri': 'https://ror.org',
            'name': 'DataCite',
            'publisherIdentifier': 'https://ror.org/04wxnsj89'
        }
        publisher2 = {
            'publisherIdentifierScheme': 'VIAF',
            'schemeUri': 'https://viaf.org/',
            'name': 'Deutsches archäologisches Institut',
            'publisherIdentifier': 'http://viaf.org/viaf/148463773'
        }
        publisher3 = {
            'publisherIdentifierScheme': 'Wikidata',
            'schemeUri': 'https://www.wikidata.org/wiki/',
            'name': 'University of Tokyo',
            'publisherIdentifier': 'https://wikidata.org/wiki/Q7842'
        }


        dp.storage_manager.set_value(id1, True)
        dp.storage_manager.set_value(id2, True)
        dp.storage_manager.set_value(id3, True)

        out1 = dp.get_publisher_id(publisher1)
        out2 = dp.get_publisher_id(publisher2)
        out3 = dp.get_publisher_id(publisher3)

        self.assertEqual(out1, id1)
        self.assertEqual(out2, id2)
        self.assertEqual(out3, id3)

        dp.storage_manager.delete_storage()

    def test_publisher_id_replaced_by_mapping(self):

        publisher3 = {
            'publisherIdentifierScheme': 'Wikidata',
            'schemeUri': 'https://www.wikidata.org/wiki/',
            'name': 'University of Tokyo',
            'publisherIdentifier': 'https://wikidata.org/wiki/Q7842'
        }

        dp = DataciteProcessing(publishers_filepath_dc=PUBLISHERS_MAPPING)
        doi = "10.12753/2066-026X-17-015"
        publisher3 = dp.get_publisher(doi, publisher3)
        publisher3_exp = "ADLRO [datacite:3]"
        self.assertEqual(publisher3, publisher3_exp)


    def test_update_redis_values_normalization(self):
        """update_redis_values deve normalizzare gli ID (doi:/orcid:) così i confronti funzionano."""
        dp = DataciteProcessing()
        dp.update_redis_values(
            br=["10.1002/2014jd022411"],  # senza prefisso
            ra=["https://orcid.org/0000-0001-8513-8700"]  # URL
        )
        # validazione via snapshot deve riuscire
        out_ra = dp.find_datacite_orcid(["0000-0001-8513-8700"], "10.9999/noindex")
        self.assertEqual(out_ra, "orcid:0000-0001-8513-8700")
        # DOI in BR: check via to_validated_id_list
        out_br = dp.to_validated_id_list({"id": "doi:10.1002/2014jd022411", "schema": "doi"})
        self.assertEqual(out_br, ["doi:10.1002/2014jd022411"])
        dp.storage_manager.delete_storage()

    def test_memory_to_storage_flushes_and_clears(self):
        """Gli aggiornamenti in tmp vengono persistiti in blocco e la memoria temporanea viene svuotata."""
        dp = DataciteProcessing(testing=True)
        # usa Redis snapshot per marcare True in tmp_orcid_m
        oid = "orcid:0000-0001-8513-8700"
        dp.update_redis_values(br=[], ra=[oid])
        _ = dp.find_datacite_orcid([oid.split(":")[1]], "10.9999/noindex")
        # dopo la validazione: il valore è in tmp_orcid_m.storage_manager
        self.assertTrue(dp.tmp_orcid_m.storage_manager.get_value(oid))
        # memory_to_storage svuota temporary_manager (che è già vuoto in questo caso)
        dp.memory_to_storage()
        # la memoria tmp è svuotata (nessun valore residuo)
        self.assertEqual(dp.temporary_manager.get_validity_list_of_tuples(), [])
        dp.tmp_orcid_m.storage_manager.delete_storage()

    def test_csv_creator_offline_uses_index_for_orcid(self):
        """API OFF: se l'ORCID è nell'indice DOI→ORCID, l'autore deve uscire con [orcid:...] anche offline."""
        dp = DataciteProcessing(use_orcid_api=False)
        doi = "10.2000/test-offline-index"
        orcid = "0000-0002-1234-5678"
        name = "Doe, Jane"
        dp.orcid_index.add_value(doi, f"{name} [orcid:{orcid}]")
        item = {
            "id": doi,
            "type": "dois",
            "attributes": {
                "doi": doi,
                "titles": [{"title": "Sample"}],
                "types": {"ris": "JOUR"},
                "creators": [{
                    "nameType": "Personal",
                    "familyName": "Doe",
                    "givenName": "Jane",
                    "nameIdentifiers": [{
                        "nameIdentifierScheme": "ORCID",
                        "nameIdentifier": f"https://orcid.org/{orcid}",
                        "schemeUri": "https://orcid.org"
                    }]
                }]
            }
        }
        row = dp.csv_creator(item)
        self.assertIn("[orcid:0000-0002-1234-5678]", row["author"])
        dp.storage_manager.delete_storage()

    def test_get_agents_strings_list_uses_index_with_doi_normalization(self):
        """get_agents_strings_list deve arricchire da indice anche se DOI arriva senza prefisso."""
        dp = DataciteProcessing()
        doi_no_prefix = "10.3000/abc"
        orcid = "0000-0003-1445-0291"
        dp.orcid_index.add_value(f"doi:{doi_no_prefix}", f"Schulz, Heide N [orcid:{orcid}]")
        entity_attr_dict = {
            "creators": [
                {"name": "Schulz, Heide N", "nameType": "Personal",
                 "givenName": "Heide N", "familyName": "Schulz", "nameIdentifiers": []}
            ],
            "contributors": []
        }
        authors = dp.add_authors_to_agent_list(entity_attr_dict, [], doi="doi:10.3000/abc")
        editors = dp.add_editors_to_agent_list(entity_attr_dict, [], doi="doi:10.3000/abc")
        authors_strings, editors_strings = dp.get_agents_strings_list(doi_no_prefix, authors + editors)
        self.assertEqual(authors_strings, [f"Schulz, Heide N [orcid:{orcid}]"])
        self.assertEqual(editors_strings, [])
        dp.storage_manager.delete_storage()
