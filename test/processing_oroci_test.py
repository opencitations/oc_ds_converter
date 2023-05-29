from oc_ds_converter.lib.jsonmanager import *
import os
from oc_ds_converter.openaire.openaire_processing import OpenaireProcessing

# TEST OpenaireProcessing METHODS
import unittest
from oc_ds_converter.oc_idmanager.oc_data_storage.in_memory_manager import InMemoryStorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.sqlite_manager import SqliteStorageManager
#

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
MEMO_JSON_PATH = "test/openaire_processing/tmp_support/memo.json"
SAMPLE_ENTITY = {'collectedFrom': [{'completionStatus': 'complete', 'provider': {'identifiers': [{'identifier': '10|openaire____::081b82f96300b6a6e3d282bad31cb6e2', 'schema': 'DNET Identifier'}], 'name': 'Crossref'}, 'provisionMode': 'collected'}, {'completionStatus': 'complete', 'provider': {'identifiers': [{'identifier': '10|openaire____::8ac8380272269217cb09a928c8caa993', 'schema': 'DNET Identifier'}], 'name': 'UnpayWall'}, 'provisionMode': 'collected'}, {'completionStatus': 'complete', 'provider': {'identifiers': [{'identifier': '10|openaire____::806360c771262b4d6770e7cdf04b5c5a', 'schema': 'DNET Identifier'}], 'name': 'ORCID'}, 'provisionMode': 'collected'}, {'completionStatus': 'complete', 'provider': {'identifiers': [{'identifier': '10|openaire____::5f532a3fc4f1ea403f37070f59a7a53a', 'schema': 'DNET Identifier'}], 'name': 'Microsoft Academic Graph'}, 'provisionMode': 'collected'}, {'completionStatus': 'complete', 'provider': {'identifiers': [{'identifier': '10|openaire____::9e3be59865b2c1c335d32dae2fe7b254', 'schema': 'DNET Identifier'}], 'name': 'Datacite'}, 'provisionMode': 'collected'}, {'completionStatus': 'complete', 'provider': {'identifiers': [{'identifier': '10|opendoar____::6f4922f45568161a8cdf4ad2299f6d23', 'schema': 'DNET Identifier'}], 'name': 'arXiv.org e-Print Archive'}, 'provisionMode': 'collected'}], 'creator': [{'name': 'Matteo Serra'}, {'name': 'Salvatore Mignemi'}, {'identifiers': [{'identifier': '0000-0001-5595-7537', 'schema': 'ORCID', 'url': 'https://orcid.org/0000-0001-5595-7537'}], 'name': 'Mariano Cadoni'}], 'dnetIdentifier': '50|doi_dedup___::41074cd388749ccbdb6668caaf059f4a', 'identifier': [{'identifier': '10.1103/physrevd.84.084046', 'schema': 'doi', 'url': 'https://doi.org/10.1103/physrevd.84.084046'}, {'identifier': '10.1103/physrevd.84.084046', 'schema': 'doi'}, {'identifier': '10.48550/arxiv.1107.5979', 'schema': 'doi', 'url': 'https://dx.doi.org/10.48550/arxiv.1107.5979'}, {'identifier': '1107.5979', 'schema': 'arXiv', 'url': 'http://arxiv.org/abs/1107.5979'}], 'objectSubType': 'Article', 'objectType': 'publication', 'publicationDate': '2011-10-21', 'publisher': [{'name': 'American Physical Society (APS)'}], 'title': 'Exact solutions with AdS asymptotics of Einstein and Einstein-Maxwell gravity minimally coupled to a scalar field'}
SAMPLE_ENTITY_FOR_CSV_CREATOR = {'collectedFrom': [{'completionStatus': 'complete', 'provider': {'identifiers': [{'identifier': '10|openaire____::0a836ef43dcb67bb7cbd4dd509b11b73', 'schema': 'DNET Identifier'}], 'name': 'CORE (RIOXX-UK Aggregator)'}, 'provisionMode': 'collected'}, {'completionStatus': 'complete', 'provider': {'identifiers': [{'identifier': '10|opendoar____::eda80a3d5b344bc40f3bc04f65b7a357', 'schema': 'DNET Identifier'}], 'name': 'PubMed Central'}, 'provisionMode': 'collected'}, {'completionStatus': 'complete', 'provider': {'identifiers': [{'identifier': '10|opendoar____::8b6dd7db9af49e67306feb59a8bdc52c', 'schema': 'DNET Identifier'}], 'name': 'Europe PubMed Central'}, 'provisionMode': 'collected'}, {'completionStatus': 'complete', 'provider': {'identifiers': [{'identifier': '10|driver______::bee53aa31dc2cbb538c10c2b65fa5824', 'schema': 'DNET Identifier'}], 'name': 'DOAJ-Articles'}, 'provisionMode': 'collected'}, {'completionStatus': 'complete', 'provider': {'identifiers': [{'identifier': '10|opendoar____::566a9968b43628588e76be5a85a0f9e8', 'schema': 'DNET Identifier'}], 'name': "King's Research Portal"}, 'provisionMode': 'collected'}, {'completionStatus': 'complete', 'provider': {'identifiers': [{'identifier': '10|openaire____::c2cdfa5866e03cdd07d313cbc8fb8311', 'schema': 'DNET Identifier'}], 'name': 'Multidisciplinary Digital Publishing Institute'}, 'provisionMode': 'collected'}], 'creator': [{'name': 'Smith, Lee'}, {'name': 'Sawyer, Alexia'}, {'name': 'Gardner, Benjamin'}, {'name': 'Seppala, Katri'}, {'name': 'Ucci, Marcella'}, {'name': 'Marmot, Alexi'}, {'name': 'Lally, Pippa'}, {'name': 'Fisher, Abi'}], 'dnetIdentifier': '50|pmid_dedup__::a1a8687c2378a0d68314566dec29dafb', 'objectSubType': 'Article', 'objectType': 'publication', 'publicationDate': '2018-06-09', 'publisher': [{'name': 'MDPI'}], 'title': 'Occupational physical activity habits of UK office workers: cross-sectional data from the Active Buildings Study', 'identifier': {'valid': [], 'not_valid': [], 'to_be_val': [{'schema': 'pmid', 'identifier': 'pmid:29890726', 'valid': None}]}}
# memo_sqlite_path = "test/openaire_processing/tmp_support/memo.json"
# storage_manager_m = InMemoryStorageManager(memo_json_path)
# storage_manager_s = SqliteStorageManager(memo_sqlite_path)
# doi_m = DOIManager(storage_manager=storage_manager_s)
# pmid_m = PMIDManager(storage_manager=storage_manager_s)
# pmc_m = PMCIDManager(storage_manager=storage_manager_s)
# arxiv_m = ArXivManager(storage_manager=storage_manager_s)
# orcid_m = ORCIDManager()


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

    def test_get_id_manager(self):
        """Check that, given in input the string of a schema (e.g.:'pmid') or an id with a prefix (e.g.: 'pmid:12334')
        and a dictionary mapping the strings of the schemas to their id managers, the method returns the correct
        id manager. Note that each instance of the Preprocessing class needs its own instances of the id managers,
        in order to avoid conflicts while validating data"""

        op = OpenaireProcessing()
        id_man_dict = op._id_man_dict

        pmid_id = "pmid:12345"
        pmid_string = "pmid"
        pmid_man_exp = op.get_id_manager(pmid_id, id_man_dict)
        pmid_man_exp_2 = op.get_id_manager(pmid_string, id_man_dict)

        #check that the idmanager for the pmid was returned and that it works as expected
        self.assertTrue(pmid_man_exp.is_valid(pmid_id))
        self.assertTrue(pmid_man_exp_2.is_valid(pmid_id))

        doi_id = "doi:10.1103/physrevd.84.084046"
        doi_string = "doi"
        doi_man_exp = op.get_id_manager(doi_id, id_man_dict)
        doi_man_exp_2 = op.get_id_manager(doi_string, id_man_dict)

        #check that the idmanager for the doi was returned and that it works as expected
        self.assertTrue(doi_man_exp.is_valid(doi_id))
        self.assertTrue(doi_man_exp_2.is_valid(doi_id))

        pmc_id = "pmcid:PMC5555555"
        pmc_string = "pmcid"
        pmc_man_exp = op.get_id_manager(pmc_id, id_man_dict)
        pmc_man_exp_2 = op.get_id_manager(pmc_string, id_man_dict)

        #check that the idmanager for the pmc was returned and that it works as expected
        self.assertTrue(pmc_man_exp.is_valid(pmc_id))
        self.assertTrue(pmc_man_exp_2.is_valid(pmc_id))

        arxiv_id = "arxiv:1509.08217"
        arxiv_string = "arxiv"
        arxiv_man_exp = op.get_id_manager(arxiv_id, id_man_dict)
        arxiv_man_exp_2 = op.get_id_manager(arxiv_string, id_man_dict)

        #check that the idmanager for the arxiv was returned and that it works as expected
        self.assertTrue(arxiv_man_exp.is_valid(arxiv_id))
        self.assertTrue(arxiv_man_exp_2.is_valid(arxiv_id))

        self.delete_storege(storage_type="sqlite")

    def test_normalise_any_id(self):
        '''
        Check that, given an id with a prefix, any doi, pmid, pmcid and arxiv id is correctly normalised
        '''
        op = OpenaireProcessing()

        pmid_id = "pmid:12345"
        doi_id = "doi:10.1103/physrevd.84.084046"
        arxiv_id = "arxiv:1509.08217"
        pmc_id = "pmcid:PMC5555555"

        self.assertEqual(pmid_id, op.normalise_any_id(pmid_id+"abc"))
        self.assertEqual(doi_id, op.normalise_any_id("doi:" + doi_id.split(":")[1].upper()))
        self.assertEqual(arxiv_id, op.normalise_any_id(arxiv_id.replace(".", "....")))
        self.assertEqual(pmc_id, op.normalise_any_id(pmc_id+"     "))

        self.delete_storege(storage_type="sqlite")

    def test_get_norm_ids(self):
        '''
        Check that, given a list of dictionaries representing the ids of an entity, the method returns a reduced version
        of the same list, containing only the normalised version of the ids of the schemas managed by opencitations.
        Each reduced dictionary only contains two key-value pairs, i.e.: "identifier" and "schema".
        '''
        op = OpenaireProcessing()

        list_of_ids_to_norm_with_duplicates = [
            {'identifier': '10.1103/PHYSREVD.84.084046', 'schema': 'doi',
             'url': 'https://doi.org/10.1103/physrevd.84.084046'},
            {'identifier': '10.1103/physrevd.84.084046', 'schema': 'doi'},
            {'identifier': '10.48550/arxiv.1107.5979', 'schema': 'doi',
             'url': 'https://dx.doi.org/10.48550/arxiv.1107.5979'},
            {'identifier': '1107.5979', 'schema': 'arXiv', 'url': 'http://arxiv.org/abs/1107.5979'}]
        norm_ids = op.get_norm_ids(list_of_ids_to_norm_with_duplicates)
        exp_norm_ids = [{'identifier': 'doi:10.1103/physrevd.84.084046', 'schema': 'doi'},
                        {'identifier': 'doi:10.48550/arxiv.1107.5979', 'schema': 'doi'},
                        {'identifier': 'arxiv:1107.5979', 'schema': 'arxiv'}]

        list_of_ids_w_not_managed_schema = [
            {'identifier': '11245/1.357137', 'schema': 'handle', 'url': 'https://hdl.handle.net/11245/1.357137'},
            {'identifier': '21887584', 'schema': 'pmid', 'url': 'https://pubmed.ncbi.nlm.nih.gov/21887584'},
            {'identifier': '10.1007/s12160-011-9282-0', 'schema': 'doi','url': 'https://doi.org/10.1007/s12160-011-9282-0'}]
        norm_ids_2 = op.get_norm_ids(list_of_ids_w_not_managed_schema)
        exp_norm_ids_2 = [{'identifier': 'pmid:21887584', 'schema': 'pmid'},
                          {'identifier': 'doi:10.1007/s12160-011-9282-0', 'schema': 'doi'}]

        list_of_ids_not_managed_and_not_normalisable_only = [
            {'identifier': '11245/1.357137', 'schema': 'handle', 'url': 'https://hdl.handle.net/11245/1.357137'},
            {'identifier': '20.ABC/s12160-011-9282-FAKEID', 'schema': 'doi','url': 'https://doi.org/10.1007/s12160-011-9282-0'}]
        norm_ids_3 = op.get_norm_ids(list_of_ids_not_managed_and_not_normalisable_only)
        exp_norm_ids_3 = []

        self.assertEqual(norm_ids, exp_norm_ids)
        self.assertEqual(norm_ids_2, exp_norm_ids_2)
        self.assertEqual(norm_ids_3, exp_norm_ids_3)
        self.delete_storege(storage_type="sqlite")


    def test_dict_to_cache(self):
        op = OpenaireProcessing()
        sample_dict = {"dict_type": "sample"}
        if os.path.exists(MEMO_JSON_PATH):
            os.remove(MEMO_JSON_PATH)
        self.assertFalse(os.path.exists(MEMO_JSON_PATH))
        op.dict_to_cache(sample_dict,MEMO_JSON_PATH)
        self.assertTrue(os.path.exists(MEMO_JSON_PATH))
        self.delete_storege(specific_path=MEMO_JSON_PATH)
        self.assertFalse(os.path.exists(MEMO_JSON_PATH))
        self.delete_storege(storage_type="sqlite")


    def test_csv_creator_base(self):
        '''
        Check that, given an updated openaire entity (i.e.: where the "identifier" field was modified
        after having checked the presence of the given identifiers in the storage memory) a meta csv
        table for the entity is created
        '''

        op = OpenaireProcessing()
        csv_row = op.csv_creator(SAMPLE_ENTITY_FOR_CSV_CREATOR)
        expected_row = {
            'id': 'pmid:29890726',
            'title': 'Occupational physical activity habits of UK office workers: cross-sectional data from the Active Buildings Study',
            'author': 'Smith Lee; Sawyer Alexia; Gardner Benjamin; Seppala Katri; Ucci Marcella; Marmot Alexi; Lally Pippa; Fisher Abi',
            'pub_date': '2018-06-09',
            'venue': '',
            'volume': '',
            'issue': '',
            'page': '',
            'type': 'journal article',
            'publisher': 'MDPI',
            'editor': ''
        }
        self.assertEqual(csv_row, expected_row)

        self.delete_storege(storage_type="sqlite")

    def test_csv_creator_not_accepted_id(self):
        '''
        Check that, given an updated openaire entity with NO ids managed by opencitations (i.e.: an handle id),
        no meta csv rows are created.
        '''

        op = OpenaireProcessing()

        replaced_entity = {'schema': 'handle', 'identifier': 'handle:11245/1.357137', 'valid': None}
        MODIFIED_ENTITY = {k:v for k,v in SAMPLE_ENTITY_FOR_CSV_CREATOR.items()}
        MODIFIED_ENTITY["identifier"]["to_be_val"]= []
        MODIFIED_ENTITY["identifier"]["to_be_val"].append(replaced_entity)
        csv_row = op.csv_creator(MODIFIED_ENTITY)
        expected_row = {} #because there is no ID accepted in opencitations for this entity
        self.assertEqual(csv_row, expected_row)

        self.delete_storege(storage_type="sqlite")

    def test_csv_creator_invalid_id(self):
        '''
        Check that, given an updated openaire entity with NO ids managed by opencitations (i.e.: an handle id),
        no meta csv rows are created.
        '''

        op = OpenaireProcessing()

        replaced_entity = {'schema': 'doi', 'identifier': 'doi:10.1000/FAKE_ID', 'valid': None}
        MODIFIED_ENTITY = {k: v for k, v in SAMPLE_ENTITY_FOR_CSV_CREATOR.items()}
        MODIFIED_ENTITY["identifier"]["to_be_val"] = []
        MODIFIED_ENTITY["identifier"]["to_be_val"].append(replaced_entity)
        csv_row = op.csv_creator(MODIFIED_ENTITY)
        expected_row = {}  # because there is no ID accepted in opencitations for this entity
        self.assertEqual(csv_row, expected_row)

        self.delete_storege(storage_type="sqlite")

    def test_get_publisher_name_base(self):
        '''
        Check that, given a doi and a dictionary representing a  publisher's data, the string of the publisher's
        normalised name (and possibly its crossref ID) is returned.

        Base functionalities: No publisher mapping in input -> only Publisher name retrieved from the datasource dump
        '''
        op = OpenaireProcessing()
        no_doi_pub_input = {'name': 'Blackwell Publishing Ltd'}

        doi_pub_1_input = {'name': 'Frontiers Media SA'}
        doi1 = "10.3389/fnana.2012.00034"

        doi_pub_2_input = {'name': 'Oxford University Press (OUP)'}
        doi2 = "10.2527/1995.7392834x"

        no_doi_pub_output = op.get_publisher_name("", no_doi_pub_input)
        doi_pub_output_1 = op.get_publisher_name(doi1, doi_pub_1_input)
        doi_pub_output_2 = op.get_publisher_name(doi2, doi_pub_2_input)

        self.assertEqual(doi_pub_output_1, "Frontiers Media SA")
        self.assertEqual(no_doi_pub_output, "Blackwell Publishing Ltd")
        self.assertEqual(doi_pub_output_2, "Oxford University Press (OUP)")

        self.delete_storege(storage_type="sqlite")

    def test_get_publisher_name_publishers_mapping(self):
        '''
        Check that, given a doi and a dictionary representing a  publisher's data, the string of the publisher's
        normalised name (and possibly its crossref ID) is returned.

        Mapping Provided: Publisher name retrieved + crossref member returned,
        only if :
        - the doi prefix is a crossref doi prefix,
        - it is present in the mapping,
         -the name of the publisher provided by the datasource corresponds to the from the datasource dump
        '''
        # MODIFICARE IL FUNZIONAMENTO PER DOI MULTIPLI (per evitare che il doi selezionato non sia quello a cui è associato il publisher in crossref.
        # Usare una lista
        op = OpenaireProcessing(publishers_filepath_openaire="test/openaire_processing/support_material/publishers.json")

        no_doi_pub_input = {'name': 'Blackwell Publishing Ltd'}

        doi_pub_1_input = {'name': 'Frontiers Media SA'}
        doi1 = "10.3389/fnana.2012.00034"

        doi_pub_2_input = {'name': 'Oxford University Press (OUP)'}
        doi2 = "10.2527/1995.7392834x"

        no_doi_pub_output = op.get_publisher_name("", no_doi_pub_input)
        doi_pub_output_1 = op.get_publisher_name(doi1, doi_pub_1_input)
        doi_pub_output_2 = op.get_publisher_name(doi2, doi_pub_2_input)

        self.assertEqual(doi_pub_output_1, "Frontiers Media SA")
        self.assertEqual(no_doi_pub_output, "Blackwell Publishing Ltd")
        self.assertEqual(doi_pub_output_2, "Oxford University Press (OUP)")

        self.delete_storege(storage_type="sqlite")

        pass








if __name__ == '__main__':
    unittest.main()