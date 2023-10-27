from oc_ds_converter.lib.jsonmanager import *
import os
from oc_ds_converter.openaire.openaire_processing import OpenaireProcessing

import unittest
from oc_ds_converter.oc_idmanager.oc_data_storage.in_memory_manager import InMemoryStorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.sqlite_manager import SqliteStorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.redis_manager import RedisStorageManager
from fakeredis import FakeStrictRedis
#

BASE = os.path.join('test', 'openaire_processing')
DATA = os.path.join(BASE, 'jSonFile_1.json')
DATA_DIR = BASE
TMP_SUPPORT_MATERIAL = os.path.join(BASE, "tmp_support")
OUTPUT = os.path.join(BASE, 'meta_input')
MULTIPROCESS_OUTPUT = os.path.join(BASE, 'multi_process_test')
MEMO_JSON_PATH = "test/openaire_processing/tmp_support/memo.json"
SAMPLE_ENTITY = {'collectedFrom': [{'completionStatus': 'complete', 'provider': {'identifiers': [{'identifier': '10|openaire____::081b82f96300b6a6e3d282bad31cb6e2', 'schema': 'DNET Identifier'}], 'name': 'Crossref'}, 'provisionMode': 'collected'}, {'completionStatus': 'complete', 'provider': {'identifiers': [{'identifier': '10|openaire____::8ac8380272269217cb09a928c8caa993', 'schema': 'DNET Identifier'}], 'name': 'UnpayWall'}, 'provisionMode': 'collected'}, {'completionStatus': 'complete', 'provider': {'identifiers': [{'identifier': '10|openaire____::806360c771262b4d6770e7cdf04b5c5a', 'schema': 'DNET Identifier'}], 'name': 'ORCID'}, 'provisionMode': 'collected'}, {'completionStatus': 'complete', 'provider': {'identifiers': [{'identifier': '10|openaire____::5f532a3fc4f1ea403f37070f59a7a53a', 'schema': 'DNET Identifier'}], 'name': 'Microsoft Academic Graph'}, 'provisionMode': 'collected'}, {'completionStatus': 'complete', 'provider': {'identifiers': [{'identifier': '10|openaire____::9e3be59865b2c1c335d32dae2fe7b254', 'schema': 'DNET Identifier'}], 'name': 'Datacite'}, 'provisionMode': 'collected'}, {'completionStatus': 'complete', 'provider': {'identifiers': [{'identifier': '10|opendoar____::6f4922f45568161a8cdf4ad2299f6d23', 'schema': 'DNET Identifier'}], 'name': 'arXiv.org e-Print Archive'}, 'provisionMode': 'collected'}], 'creator': [{'name': 'Matteo Serra'}, {'name': 'Salvatore Mignemi'}, {'identifiers': [{'identifier': '0000-0001-5595-7537', 'schema': 'ORCID', 'url': 'https://orcid.org/0000-0001-5595-7537'}], 'name': 'Mariano Cadoni'}], 'dnetIdentifier': '50|doi_dedup___::41074cd388749ccbdb6668caaf059f4a', 'identifier': [{'identifier': '10.1103/physrevd.84.084046', 'schema': 'doi', 'url': 'https://doi.org/10.1103/physrevd.84.084046'}, {'identifier': '10.1103/physrevd.84.084046', 'schema': 'doi'}, {'identifier': '10.48550/arxiv.1107.5979', 'schema': 'doi', 'url': 'https://dx.doi.org/10.48550/arxiv.1107.5979'}, {'identifier': '1107.5979', 'schema': 'arXiv', 'url': 'http://arxiv.org/abs/1107.5979'}], 'objectSubType': 'Article', 'objectType': 'publication', 'publicationDate': '2011-10-21', 'publisher': [{'name': 'American Physical Society (APS)'}], 'title': 'Exact solutions with AdS asymptotics of Einstein and Einstein-Maxwell gravity minimally coupled to a scalar field'}
SAMPLE_ENT2 = {"identifier":"000017d2c913b28e09291b811ce3609a","linkprovider":[{"identifiers":[{"identifier":"10|openaire____::0a836ef43dcb67bb7cbd4dd509b11b73","schema":"DNET Identifier"}],"name":"CORE (RIOXX-UK Aggregator)"},{"identifiers":[{"identifier":"10|opendoar____::eda80a3d5b344bc40f3bc04f65b7a357","schema":"DNET Identifier"}],"name":"PubMed Central"},{"identifiers":[{"identifier":"10|opendoar____::8b6dd7db9af49e67306feb59a8bdc52c","schema":"DNET Identifier"}],"name":"Europe PubMed Central"},{"identifiers":[{"identifier":"10|opendoar____::229754d7799160502a143a72f6789927","schema":"DNET Identifier"}],"name":"Publications at Bielefeld University"}],"publicationDate":"2014-02-01","publisher":[{"name":"Springer Nature"}],"relationship":{"inverse":"IsCitedBy","name":"Cites","schema":"datacite"},"source":{"collectedFrom":[{"completionStatus":"complete","provider":{"identifiers":[{"identifier":"10|openaire____::0a836ef43dcb67bb7cbd4dd509b11b73","schema":"DNET Identifier"}],"name":"CORE (RIOXX-UK Aggregator)"},"provisionMode":"collected"},{"completionStatus":"complete","provider":{"identifiers":[{"identifier":"10|opendoar____::eda80a3d5b344bc40f3bc04f65b7a357","schema":"DNET Identifier"}],"name":"PubMed Central"},"provisionMode":"collected"},{"completionStatus":"complete","provider":{"identifiers":[{"identifier":"10|opendoar____::8b6dd7db9af49e67306feb59a8bdc52c","schema":"DNET Identifier"}],"name":"Europe PubMed Central"},"provisionMode":"collected"},{"completionStatus":"complete","provider":{"identifiers":[{"identifier":"10|opendoar____::229754d7799160502a143a72f6789927","schema":"DNET Identifier"}],"name":"Publications at Bielefeld University"},"provisionMode":"collected"}],"creator":[{"identifiers":[{"identifier":"0000-0002-6491-0754","schema":"ORCID","url":"https://orcid.org/0000-0002-6491-0754"}],"name":"Sattler, Sebastian"},{"name":"Mehlkop, Guido"},{"name":"Graeff, Peter"},{"identifiers":[{"identifier":"0000-0002-8090-6886","schema":"ORCID","url":"https://orcid.org/0000-0002-8090-6886"}],"name":"Sauer, Carsten"}],"dnetIdentifier":"50|pmid_dedup__::8936076da7a86820c24ede7ca3ff15b3","identifier":[{"identifier":"PMC3928621","schema":"pmc","url":"http://europepmc.org/articles/PMC3928621"},{"identifier":"24484640","schema":"pmid"},{"identifier":"24484640","schema":"pmid","url":"https://pubmed.ncbi.nlm.nih.gov/24484640"},{"identifier":"PMC3928621","schema":"pmc"}],"objectSubType":"Article","objectType":"publication","publicationDate":"2014-02-01","publisher":[{"name":"Springer Nature"}],"title":"Evaluating the drivers of and obstacles to the willingness to use cognitive enhancement drugs: the influence of drug characteristics, social environment, and personal characteristics"},"target":{"collectedFrom":[{"completionStatus":"complete","provider":{"identifiers":[{"identifier":"10|openaire____::081b82f96300b6a6e3d282bad31cb6e2","schema":"DNET Identifier"}],"name":"Crossref"},"provisionMode":"collected"},{"completionStatus":"complete","provider":{"identifiers":[{"identifier":"10|openaire____::5f532a3fc4f1ea403f37070f59a7a53a","schema":"DNET Identifier"}],"name":"Microsoft Academic Graph"},"provisionMode":"collected"}],"creator":[{"name":"Harold G. Grasmick"},{"name":"Robert J. Bursik"}],"dnetIdentifier":"50|doi_________::816648c63de74835ec2b0a753a68f037","identifier":[{"identifier":"10.2307/3053861","schema":"doi","url":"https://doi.org/10.2307/3053861"}],"objectSubType":"Article","objectType":"publication","publicationDate":"1990-01-01","publisher":[{"name":"JSTOR"}],"title":"Conscience, significant others, and rational choice: Extending the deterrence model."}}
SAMPLE_ENTITY_FOR_CSV_CREATOR = {'collectedFrom': [{'completionStatus': 'complete', 'provider': {'identifiers': [{'identifier': '10|openaire____::0a836ef43dcb67bb7cbd4dd509b11b73', 'schema': 'DNET Identifier'}], 'name': 'CORE (RIOXX-UK Aggregator)'}, 'provisionMode': 'collected'}, {'completionStatus': 'complete', 'provider': {'identifiers': [{'identifier': '10|opendoar____::eda80a3d5b344bc40f3bc04f65b7a357', 'schema': 'DNET Identifier'}], 'name': 'PubMed Central'}, 'provisionMode': 'collected'}, {'completionStatus': 'complete', 'provider': {'identifiers': [{'identifier': '10|opendoar____::8b6dd7db9af49e67306feb59a8bdc52c', 'schema': 'DNET Identifier'}], 'name': 'Europe PubMed Central'}, 'provisionMode': 'collected'}, {'completionStatus': 'complete', 'provider': {'identifiers': [{'identifier': '10|driver______::bee53aa31dc2cbb538c10c2b65fa5824', 'schema': 'DNET Identifier'}], 'name': 'DOAJ-Articles'}, 'provisionMode': 'collected'}, {'completionStatus': 'complete', 'provider': {'identifiers': [{'identifier': '10|opendoar____::566a9968b43628588e76be5a85a0f9e8', 'schema': 'DNET Identifier'}], 'name': "King's Research Portal"}, 'provisionMode': 'collected'}, {'completionStatus': 'complete', 'provider': {'identifiers': [{'identifier': '10|openaire____::c2cdfa5866e03cdd07d313cbc8fb8311', 'schema': 'DNET Identifier'}], 'name': 'Multidisciplinary Digital Publishing Institute'}, 'provisionMode': 'collected'}], 'creator': [{'name': 'Smith, Lee'}, {'name': 'Sawyer, Alexia'}, {'name': 'Gardner, Benjamin'}, {'name': 'Seppala, Katri'}, {'name': 'Ucci, Marcella'}, {'name': 'Marmot, Alexi'}, {'name': 'Lally, Pippa'}, {'name': 'Fisher, Abi'}], 'dnetIdentifier': '50|pmid_dedup__::a1a8687c2378a0d68314566dec29dafb', 'objectSubType': 'Article', 'objectType': 'publication', 'publicationDate': '2018-06-09', 'publisher': [{'name': 'MDPI'}], 'title': 'Occupational physical activity habits of UK office workers: cross-sectional data from the Active Buildings Study', 'identifier': {'valid': [], 'not_valid': [], 'to_be_val': [{'schema': 'pmid', 'identifier': 'pmid:29890726', 'valid': None}]}, "redis_validity_lists":[[],[]]}


class TestOpenaireProcessing(unittest.TestCase):

    def delete_storege(self, storage_type=None, specific_path=None):
        if not specific_path:
            if storage_type == "sqlite":
                auto_db_created_path = os.path.join(os.getcwd(), "storage", "id_valid_dict.db")
                auto_db_created_path = auto_db_created_path if os.path.exists(auto_db_created_path) else auto_db_created_path+"?mode=rw"
                if os.path.exists(auto_db_created_path):
                    os.remove(auto_db_created_path)
            else:
                auto_db_created_path = os.path.join(os.getcwd(), "storage", "id_value.json")
                if os.path.exists(auto_db_created_path):
                    os.remove(auto_db_created_path)
        elif specific_path:
            if os.path.exists(specific_path):
                os.remove(specific_path)

    def test_get_all_ids(self):
        opp = OpenaireProcessing()
        allids = opp.extract_all_ids(SAMPLE_ENT2)
        self.assertCountEqual(['pmid:24484640', 'pmcid:PMC3928621', 'doi:10.2307/3053861'], allids[0])
        self.assertCountEqual(['orcid:0000-0002-8090-6886', 'orcid:0000-0002-6491-0754'], allids[1])

        opp.storage_manager.delete_storage()

    def test_get_all_ids_redis(self):
        opp = OpenaireProcessing(storage_manager=RedisStorageManager(testing=True))
        allids = opp.extract_all_ids(SAMPLE_ENT2)
        self.assertCountEqual(['pmid:24484640', 'pmcid:PMC3928621', 'doi:10.2307/3053861'], allids[0])
        self.assertCountEqual(['orcid:0000-0002-8090-6886', 'orcid:0000-0002-6491-0754'], allids[1])
        opp.storage_manager.delete_storage()

    def test_get_reids_validity_list(self):
        br = {'pmid:24484640', 'pmcid:PMC3928621', 'doi:10.2307/3053861'}
        ra = {'orcid:0000-0002-8090-6886', 'orcid:0000-0002-6491-0754'}

        opp = OpenaireProcessing()
        br_valid_list = opp.get_reids_validity_list(br, "br")
        exp_exp_br_valid_list = []
        ra_valid_list = opp.get_reids_validity_list(ra, "ra")
        exp_exp_ra_valid_list = []
        self.assertEqual(ra_valid_list, exp_exp_ra_valid_list)
        self.assertEqual(br_valid_list, exp_exp_br_valid_list)

        opp.storage_manager.delete_storage()

    def test_get_reids_validity_list_redis(self):
        br = {'pmid:24484640', 'pmcid:PMC3928621', 'doi:10.2307/3053861'}
        ra = {'orcid:0000-0002-8090-6886', 'orcid:0000-0002-6491-0754'}

        opp = OpenaireProcessing(storage_manager=RedisStorageManager(testing=True))
        br_valid_list = opp.get_reids_validity_list(br, "br")
        exp_exp_br_valid_list = []
        ra_valid_list = opp.get_reids_validity_list(ra, "ra")
        exp_exp_ra_valid_list = []
        self.assertEqual(ra_valid_list, exp_exp_ra_valid_list)
        self.assertEqual(br_valid_list, exp_exp_br_valid_list)
        opp.storage_manager.delete_storage()

    def test_get_reids_validity_dict_w_fakeredis_db_values_sqlite(self):
        opp = OpenaireProcessing()
        opp.BR_redis.set('pmid:24484640', "omid:1")
        opp.RA_redis.set('orcid:0000-0002-8090-6886', "omid:2")


        br = {'pmid:24484640', 'pmcid:PMC3928621', 'doi:10.2307/3053861'}
        ra = {'orcid:0000-0002-8090-6886', 'orcid:0000-0002-6491-0754'}

        br_validity_dict = opp.get_reids_validity_list(br, "br")
        exp_br_valid_list = ["pmid:24484640"]
        ra_validity_dict = opp.get_reids_validity_list(ra, "ra")
        exp_ra_valid_list = ['orcid:0000-0002-8090-6886']
        self.assertEqual(br_validity_dict, exp_br_valid_list)
        self.assertEqual(ra_validity_dict, exp_ra_valid_list)

        opp.storage_manager.delete_storage()

        opp.BR_redis.delete('pmid:24484640')
        opp.BR_redis.delete('pmcid:PMC3928621')
        opp.RA_redis.delete('orcid:0000-0002-8090-6886')

    def test_get_reids_validity_dict_w_fakeredis_db_values_redis(self):

        opp = OpenaireProcessing(storage_manager=RedisStorageManager())
        opp.BR_redis.set('pmid:24484640', "omid:1")
        opp.RA_redis.set('orcid:0000-0002-8090-6886', "omid:2")


        br = {'pmid:24484640', 'pmcid:PMC3928621', 'doi:10.2307/3053861'}
        ra = {'orcid:0000-0002-8090-6886', 'orcid:0000-0002-6491-0754'}

        br_validity_dict = opp.get_reids_validity_list(br, "br")
        exp_br_valid_list = ["pmid:24484640"]
        ra_validity_dict = opp.get_reids_validity_list(ra, "ra")
        exp_ra_valid_list = ['orcid:0000-0002-8090-6886']
        self.assertEqual(br_validity_dict, exp_br_valid_list)
        self.assertEqual(ra_validity_dict, exp_ra_valid_list)

        opp.storage_manager.delete_storage()
        opp.BR_redis.delete('pmid:24484640')
        opp.BR_redis.delete('pmcid:PMC3928621')
        opp.RA_redis.delete('orcid:0000-0002-8090-6886')

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

        opp = OpenaireProcessing()
        validate_as_none = opp.validated_as({"schema":"pmid", "identifier": "pmid:23483834"})
        self.assertEqual(validate_as_none, None)
        opp.storage_manager.delete_storage()

    def test_validated_as_default_redis(self):
        '''
        Check that, given an ID dict with keys "schema" (value: string of the schema) and "identifier" (value:
        string of the identifier, the method "validated_as" returns:
        - True if the id was already validated as valid
        - False if the id was already validated as invalid
        - None if the id was not validated before
        The procedure is tested
        - With redis storage manager without a pre-existent db associated
        '''

        opp = OpenaireProcessing(storage_manager=RedisStorageManager(testing=True))
        validate_as_none = opp.validated_as({"schema":"pmid", "identifier": "pmid:23483834"})
        self.assertEqual(validate_as_none, None)
        opp.storage_manager.delete_storage()

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

        opp_sql.storage_manager.delete_storage()


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

        opp_sql.storage_manager.delete_storage()


    def test_validated_as_redis(self):
        '''
        Check that, given an ID dict with keys "schema" (value: string of the schema) and "identifier" (value:
        string of the identifier, the method "validated_as" returns:
        - True if the id was already validated as valid
        - False if the id was already validated as invalid
        - None if the id was not validated before
        The procedure is tested
        - With REDIS storage manager and a pre-existent db associated
        - With REDIS storage manager without a pre-existent db associated
        '''

        redis_man = RedisStorageManager(testing=True)
        valid_pmid_not_in_db = {"identifier":"pmid:2938", "schema":"pmid"}
        valid_pmid_in_db = {"identifier":"pmid:23483834", "schema":"pmid"}
        invalid_pmid_in_db = {"identifier":"pmid:18328387372097", "schema":"pmid"}
        redis_man.set_value(valid_pmid_in_db["identifier"], True)
        redis_man.set_value(invalid_pmid_in_db["identifier"], False)

        # New class instance to check the correct task management with a redis manager using a db with data
        opp_redis = OpenaireProcessing(storage_manager=redis_man)
        validated_as_True = opp_redis.validated_as(valid_pmid_in_db)
        validated_as_False = opp_redis.validated_as(invalid_pmid_in_db)
        not_validated = opp_redis.validated_as(valid_pmid_not_in_db)

        self.assertEqual(validated_as_True, True)
        self.assertEqual(validated_as_False, False)
        self.assertEqual(not_validated, None)
        opp_redis.storage_manager.delete_storage()

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

        op.storage_manager.delete_storage()

    def test_get_id_manager_redis(self):
        """Check that, given in input the string of a schema (e.g.:'pmid') or an id with a prefix (e.g.: 'pmid:12334')
        and a dictionary mapping the strings of the schemas to their id managers, the method returns the correct
        id manager. Note that each instance of the Preprocessing class needs its own instances of the id managers,
        in order to avoid conflicts while validating data"""

        op = OpenaireProcessing(storage_manager=RedisStorageManager(testing=True))
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

        op.storage_manager.delete_storage()


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
        self.assertEqual(arxiv_id + "v1", op.normalise_any_id(arxiv_id.replace(".", "....")))
        self.assertEqual(pmc_id, op.normalise_any_id(pmc_id+"     "))

        op.storage_manager.delete_storage()

    def test_normalise_any_id_redis(self):
        '''
        Check that, given an id with a prefix, any doi, pmid, pmcid and arxiv id is correctly normalised
        '''
        op = OpenaireProcessing(storage_manager=RedisStorageManager(testing=True))

        pmid_id = "pmid:12345"
        doi_id = "doi:10.1103/physrevd.84.084046"
        arxiv_id = "arxiv:1509.08217"
        pmc_id = "pmcid:PMC5555555"

        self.assertEqual(pmid_id, op.normalise_any_id(pmid_id+"abc"))
        self.assertEqual(doi_id, op.normalise_any_id("doi:" + doi_id.split(":")[1].upper()))
        self.assertEqual(arxiv_id + "v1", op.normalise_any_id(arxiv_id.replace(".", "....")))
        self.assertEqual(pmc_id, op.normalise_any_id(pmc_id+"     "))

        op.storage_manager.delete_storage()

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
                        {'identifier': 'arxiv:1107.5979v1', 'schema': 'arxiv'}]

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
        op.storage_manager.delete_storage()


    def test_get_norm_ids_redis(self):
        '''
        Check that, given a list of dictionaries representing the ids of an entity, the method returns a reduced version
        of the same list, containing only the normalised version of the ids of the schemas managed by opencitations.
        Each reduced dictionary only contains two key-value pairs, i.e.: "identifier" and "schema".
        '''
        op = OpenaireProcessing(storage_manager=RedisStorageManager(testing=True))

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
                        {'identifier': 'arxiv:1107.5979v1', 'schema': 'arxiv'}]

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
        op.storage_manager.delete_storage()

    def test_dict_to_cache(self):
        op = OpenaireProcessing()
        sample_dict = {"dict_type": "sample"}
        if os.path.exists(MEMO_JSON_PATH):
            os.remove(MEMO_JSON_PATH)
        self.assertFalse(os.path.exists(MEMO_JSON_PATH))
        op.dict_to_cache(sample_dict, MEMO_JSON_PATH)
        self.assertTrue(os.path.exists(MEMO_JSON_PATH))
        self.delete_storege(specific_path=MEMO_JSON_PATH)
        self.assertFalse(os.path.exists(MEMO_JSON_PATH))
        op.storage_manager.delete_storage()


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

        op.storage_manager.delete_storage()

    def test_csv_creator_base_redis(self):
        '''
        Check that, given an updated openaire entity (i.e.: where the "identifier" field was modified
        after having checked the presence of the given identifiers in the storage memory) a meta csv
        table for the entity is created
        '''

        op = OpenaireProcessing(storage_manager=RedisStorageManager(testing=True))
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

        op.storage_manager.delete_storage()

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

        op.storage_manager.delete_storage()

    def test_csv_creator_not_accepted_id_redis(self):
        '''
        Check that, given an updated openaire entity with NO ids managed by opencitations (i.e.: an handle id),
        no meta csv rows are created.
        '''

        op = OpenaireProcessing(storage_manager=RedisStorageManager(testing=True))

        replaced_entity = {'schema': 'handle', 'identifier': 'handle:11245/1.357137', 'valid': None}
        MODIFIED_ENTITY = {k:v for k,v in SAMPLE_ENTITY_FOR_CSV_CREATOR.items()}
        MODIFIED_ENTITY["identifier"]["to_be_val"]= []
        MODIFIED_ENTITY["identifier"]["to_be_val"].append(replaced_entity)
        csv_row = op.csv_creator(MODIFIED_ENTITY)
        expected_row = {} #because there is no ID accepted in opencitations for this entity
        self.assertEqual(csv_row, expected_row)

        op.storage_manager.delete_storage()

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

        op.storage_manager.delete_storage()


    def test_csv_creator_invalid_id_redis(self):
        '''
        Check that, given an updated openaire entity with NO ids managed by opencitations (i.e.: an handle id),
        no meta csv rows are created.
        '''

        op = OpenaireProcessing(storage_manager=RedisStorageManager(testing=True))

        replaced_entity = {'schema': 'doi', 'identifier': 'doi:10.1000/FAKE_ID', 'valid': None}
        MODIFIED_ENTITY = {k: v for k, v in SAMPLE_ENTITY_FOR_CSV_CREATOR.items()}
        MODIFIED_ENTITY["identifier"]["to_be_val"] = []
        MODIFIED_ENTITY["identifier"]["to_be_val"].append(replaced_entity)
        csv_row = op.csv_creator(MODIFIED_ENTITY)
        expected_row = {}  # because there is no ID accepted in opencitations for this entity
        self.assertEqual(csv_row, expected_row)

        op.storage_manager.delete_storage()

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

        no_doi_pub_output = op.get_publisher_name([""], no_doi_pub_input)
        doi_pub_output_1 = op.get_publisher_name([doi1], doi_pub_1_input)
        doi_pub_output_2 = op.get_publisher_name([doi2], doi_pub_2_input)

        self.assertEqual(doi_pub_output_1, "Frontiers Media SA")
        self.assertEqual(no_doi_pub_output, "Blackwell Publishing Ltd")
        self.assertEqual(doi_pub_output_2, "Oxford University Press (OUP)")

        op.storage_manager.delete_storage()

    def test_get_publisher_name_base_redis(self):
        '''
        Check that, given a doi and a dictionary representing a  publisher's data, the string of the publisher's
        normalised name (and possibly its crossref ID) is returned.

        Base functionalities: No publisher mapping in input -> only Publisher name retrieved from the datasource dump
        '''
        op = OpenaireProcessing(storage_manager=RedisStorageManager(testing=True))
        no_doi_pub_input = {'name': 'Blackwell Publishing Ltd'}

        doi_pub_1_input = {'name': 'Frontiers Media SA'}
        doi1 = "10.3389/fnana.2012.00034"

        doi_pub_2_input = {'name': 'Oxford University Press (OUP)'}
        doi2 = "10.2527/1995.7392834x"

        no_doi_pub_output = op.get_publisher_name([""], no_doi_pub_input)
        doi_pub_output_1 = op.get_publisher_name([doi1], doi_pub_1_input)
        doi_pub_output_2 = op.get_publisher_name([doi2], doi_pub_2_input)

        self.assertEqual(doi_pub_output_1, "Frontiers Media SA")
        self.assertEqual(no_doi_pub_output, "Blackwell Publishing Ltd")
        self.assertEqual(doi_pub_output_2, "Oxford University Press (OUP)")

        op.storage_manager.delete_storage()

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

        op = OpenaireProcessing(publishers_filepath_openaire="test/openaire_processing/support_material/publishers.json")

        no_doi_pub_input = {'name': 'Blackwell Publishing Ltd'}

        doi_pub_1_input = {'name': 'Frontiers Media SA'}
        doi1 = "10.3389/fnana.2012.00034"

        doi_pub_2_input = {'name': 'Oxford University Press (OUP)'}
        doi2 = "10.2527/1995.7392834x"

        no_doi_pub_output = op.get_publisher_name([""], no_doi_pub_input)
        doi_pub_output_1 = op.get_publisher_name([doi1], doi_pub_1_input)
        doi_pub_output_2 = op.get_publisher_name([doi2], doi_pub_2_input)

        self.assertEqual(doi_pub_output_1, "Frontiers Media SA")
        self.assertEqual(no_doi_pub_output, "Blackwell Publishing Ltd")
        self.assertEqual(doi_pub_output_2, "Oxford University Press (OUP)")

        op.storage_manager.delete_storage()

    def test_get_publisher_name_publishers_mapping_redis(self):
        '''
        Check that, given a doi and a dictionary representing a  publisher's data, the string of the publisher's
        normalised name (and possibly its crossref ID) is returned.

        Mapping Provided: Publisher name retrieved + crossref member returned,
        only if :
        - the doi prefix is a crossref doi prefix,
        - it is present in the mapping,
         -the name of the publisher provided by the datasource corresponds to the from the datasource dump
        '''

        op = OpenaireProcessing(storage_manager=RedisStorageManager(testing=True),publishers_filepath_openaire="test/openaire_processing/support_material/publishers.json")

        no_doi_pub_input = {'name': 'Blackwell Publishing Ltd'}

        doi_pub_1_input = {'name': 'Frontiers Media SA'}
        doi1 = "10.3389/fnana.2012.00034"

        doi_pub_2_input = {'name': 'Oxford University Press (OUP)'}
        doi2 = "10.2527/1995.7392834x"

        no_doi_pub_output = op.get_publisher_name([""], no_doi_pub_input)
        doi_pub_output_1 = op.get_publisher_name([doi1], doi_pub_1_input)
        doi_pub_output_2 = op.get_publisher_name([doi2], doi_pub_2_input)

        self.assertEqual(doi_pub_output_1, "Frontiers Media SA")
        self.assertEqual(no_doi_pub_output, "Blackwell Publishing Ltd")
        self.assertEqual(doi_pub_output_2, "Oxford University Press (OUP)")

        op.storage_manager.delete_storage()

    def test_get_publisher_name_publishers_mapping_multi_dois(self):
        '''
        Check that, given a doi and a dictionary representing a  publisher's data, the string of the publisher's
        normalised name (and possibly its crossref ID) is returned.

        Mapping Provided: Publisher name retrieved + crossref member returned,
        only if :
        - the doi prefix is a crossref doi prefix,
        - it is present in the mapping,
         -the name of the publisher provided by the datasource corresponds to the from the datasource dump
        '''

        op = OpenaireProcessing(publishers_filepath_openaire="test/openaire_processing/support_material/publishers.json")

        # CASE 1: The Publisher Name provided by OPENAIRE corresponds to the Publisher Name mapped to one of the
        # entity's dois prefixes in the prefix-to-publisher-data mapping in input
        # EXPECTED OUTPUT: The publisher name is retrieved with its crossref member

        ent_1_doi_1 = "10.1152/sample_doi" #this prefix is in the mapping and corresponds to American Physiological Society
        ent_1_doi_2 = "10.1153/sample_doi"
        pub_input_1 = {'name': 'American Physiological Society'}

        no_doi_pub_output = op.get_publisher_name([ent_1_doi_1, ent_1_doi_2], pub_input_1)

        self.assertEqual(no_doi_pub_output, "American Physiological Society [crossref:24]")

        # CASE 2: The Publisher Name provided by OPENAIRE does not correspond to the Publisher Name mapped to one of the
        # entity's dois prefixes in the prefix-to-publisher-data mapping in input
        # EXPECTED OUTPUT: The publisher name provided by Openaire is retrieved without any crossref member

        ent_2_doi_1 = "10.1152/sample_doi" #this prefix is in the mapping and corresponds to American Physiological Society
        ent_2_doi_2 = "10.1153/sample_doi"
        pub_input_2 = {'name': 'Sample Publisher Name'}

        no_doi_pub_output2 = op.get_publisher_name([ent_2_doi_1, ent_2_doi_2], pub_input_2)
        self.assertEqual(no_doi_pub_output2, "Sample Publisher Name")

        # CASE 3: The Publisher Name provided by OPENAIRE corresponds to the Publisher Name mapped to one of the
        # entity's dois prefixes in the prefix-to-publisher-data mapping in input BUT it is not the first doi of the list
        # EXPECTED OUTPUT: The publisher name is retrieved with its crossref member

        ent_3_doi_1 = "10.1152/sample_doi" #this prefix is in the mapping and corresponds to American Physiological Society
        ent_3_doi_2 = "10.1153/sample_doi"
        pub_input_3 = {'name': 'American Physiological Society'}

        doi_pub_output3 = op.get_publisher_name([ent_3_doi_2, ent_3_doi_1], pub_input_3)

        self.assertEqual(doi_pub_output3, "American Physiological Society [crossref:24]")

        op.storage_manager.delete_storage()

        # CASE 4: OPENAIRE does not provide a publisher name but one of the entity's DOI prefixes is in the
        # prefix-to-publisher-data mapping in input
        # EXPECTED OUTPUT: empty string

        ent_4_doi_1 = "10.1152/sample_doi" #this prefix is in the mapping and corresponds to American Physiological Society
        ent_4_doi_2 = "10.1153/sample_doi"
        pub_input_4 = {'name': ''}
        pub_input_4_1 = {}
        pub_input_4_2 = ''

        doi_pub_output4 = op.get_publisher_name([ent_4_doi_1, ent_4_doi_2], pub_input_4)
        doi_pub_output4_1 = op.get_publisher_name([ent_4_doi_1, ent_4_doi_2], pub_input_4_1)
        doi_pub_output4_2= op.get_publisher_name([ent_4_doi_1, ent_4_doi_2], pub_input_4_2)

        self.assertEqual(doi_pub_output4, "")
        self.assertEqual(doi_pub_output4_1, "")
        self.assertEqual(doi_pub_output4_2, "")

        op.storage_manager.delete_storage()

    def test_get_publisher_name_publishers_mapping_multi_dois_redis(self):
        '''
        Check that, given a doi and a dictionary representing a  publisher's data, the string of the publisher's
        normalised name (and possibly its crossref ID) is returned.

        Mapping Provided: Publisher name retrieved + crossref member returned,
        only if :
        - the doi prefix is a crossref doi prefix,
        - it is present in the mapping,
         -the name of the publisher provided by the datasource corresponds to the from the datasource dump
        '''

        op = OpenaireProcessing(storage_manager=RedisStorageManager(testing=True), publishers_filepath_openaire="test/openaire_processing/support_material/publishers.json")

        # CASE 1: The Publisher Name provided by OPENAIRE corresponds to the Publisher Name mapped to one of the
        # entity's dois prefixes in the prefix-to-publisher-data mapping in input
        # EXPECTED OUTPUT: The publisher name is retrieved with its crossref member

        ent_1_doi_1 = "10.1152/sample_doi" #this prefix is in the mapping and corresponds to American Physiological Society
        ent_1_doi_2 = "10.1153/sample_doi"
        pub_input_1 = {'name': 'American Physiological Society'}

        no_doi_pub_output = op.get_publisher_name([ent_1_doi_1, ent_1_doi_2], pub_input_1)

        self.assertEqual(no_doi_pub_output, "American Physiological Society [crossref:24]")

        # CASE 2: The Publisher Name provided by OPENAIRE does not correspond to the Publisher Name mapped to one of the
        # entity's dois prefixes in the prefix-to-publisher-data mapping in input
        # EXPECTED OUTPUT: The publisher name provided by Openaire is retrieved without any crossref member

        ent_2_doi_1 = "10.1152/sample_doi" #this prefix is in the mapping and corresponds to American Physiological Society
        ent_2_doi_2 = "10.1153/sample_doi"
        pub_input_2 = {'name': 'Sample Publisher Name'}

        no_doi_pub_output2 = op.get_publisher_name([ent_2_doi_1, ent_2_doi_2], pub_input_2)
        self.assertEqual(no_doi_pub_output2, "Sample Publisher Name")

        # CASE 3: The Publisher Name provided by OPENAIRE corresponds to the Publisher Name mapped to one of the
        # entity's dois prefixes in the prefix-to-publisher-data mapping in input BUT it is not the first doi of the list
        # EXPECTED OUTPUT: The publisher name is retrieved with its crossref member

        ent_3_doi_1 = "10.1152/sample_doi" #this prefix is in the mapping and corresponds to American Physiological Society
        ent_3_doi_2 = "10.1153/sample_doi"
        pub_input_3 = {'name': 'American Physiological Society'}

        doi_pub_output3 = op.get_publisher_name([ent_3_doi_2, ent_3_doi_1], pub_input_3)

        self.assertEqual(doi_pub_output3, "American Physiological Society [crossref:24]")

        op.storage_manager.delete_storage()

        # CASE 4: OPENAIRE does not provide a publisher name but one of the entity's DOI prefixes is in the
        # prefix-to-publisher-data mapping in input
        # EXPECTED OUTPUT: empty string

        ent_4_doi_1 = "10.1152/sample_doi" #this prefix is in the mapping and corresponds to American Physiological Society
        ent_4_doi_2 = "10.1153/sample_doi"
        pub_input_4 = {'name': ''}
        pub_input_4_1 = {}
        pub_input_4_2 = ''

        doi_pub_output4 = op.get_publisher_name([ent_4_doi_1, ent_4_doi_2], pub_input_4)
        doi_pub_output4_1 = op.get_publisher_name([ent_4_doi_1, ent_4_doi_2], pub_input_4_1)
        doi_pub_output4_2= op.get_publisher_name([ent_4_doi_1, ent_4_doi_2], pub_input_4_2)

        self.assertEqual(doi_pub_output4, "")
        self.assertEqual(doi_pub_output4_1, "")
        self.assertEqual(doi_pub_output4_2, "")

        op.storage_manager.delete_storage()

    def test_manage_arxiv_single_id(self):
        '''Check the correct management of entities with only one ID, in particular in
        case it is an arxiv. In this case, if it is an arxiv DOI, we return the normalised
        version of the correspondent arxiv. Both in case of an arxiv id and of an arxiv doi,
        we return the versioned arxiv id where the version is available (never in ARXIV doi).
        If no version is provided, we normalise the arxiv id as arxiv id version 1.
        In all the other id cases (pmid, pmc, handle (which is discarded in a later step) '''
        sample_doi_any = [{'schema': 'doi', 'identifier': 'doi:10.1000/FAKE_ID', 'valid': None}]
        sample_doi_arxiv = [{'schema': 'doi', 'identifier': 'doi:10.48550/arXiv.1509.08217', 'valid': None}]
        sample_arxiv_no_ver = [{'schema': 'arxiv', 'identifier': 'arxiv:1509.08217', 'valid': None}]
        sample_arxiv_ver = [{'schema': 'arxiv', 'identifier': 'arxiv:1509...08217v3', 'valid': None}]

        op = OpenaireProcessing()

        # CASE 1: the unique input id dict in list is a not-arxiv doi : the input list is returned
        out_sample_doi_any = op.manage_arxiv_single_id(sample_doi_any)
        self.assertEqual(out_sample_doi_any, [{'schema': 'doi', 'identifier': 'doi:10.1000/FAKE_ID', 'valid': None}])

        # CASE 2: the unique input id dict in list is an arxiv doi: the doi is replaced with its correspondent arxiv v1
        out_sample_doi_arxiv = op.manage_arxiv_single_id(sample_doi_arxiv)
        self.assertEqual(out_sample_doi_arxiv, [{'schema': 'arxiv', 'identifier': 'arxiv:1509.08217v1'}])

        # CASE 3: the unique input id dict in list is an arxiv id without version:
        # the arxiv id is replaced with its v1
        out_sample_arxiv_no_ver = op.manage_arxiv_single_id(sample_arxiv_no_ver)
        self.assertEqual(out_sample_arxiv_no_ver, [{'schema': 'arxiv', 'identifier': 'arxiv:1509.08217v1'}])

        # CASE 4: the unique input id dict in list is an arxiv id with version: the id is just normalised
        out_sample_arxiv_ver = op.manage_arxiv_single_id(sample_arxiv_ver)
        self.assertEqual(out_sample_arxiv_ver, [{'schema': 'arxiv', 'identifier': 'arxiv:1509.08217v3'}])

        op.storage_manager.delete_storage()

    def test_manage_arxiv_single_id_redis(self):
        '''Check the correct management of entities with only one ID, in particular in
        case it is an arxiv. In this case, if it is an arxiv DOI, we return the normalised
        version of the correspondent arxiv. Both in case of an arxiv id and of an arxiv doi,
        we return the versioned arxiv id where the version is available (never in ARXIV doi).
        If no version is provided, we normalise the arxiv id as arxiv id version 1.
        In all the other id cases (pmid, pmc, handle (which is discarded in a later step) '''
        sample_doi_any = [{'schema': 'doi', 'identifier': 'doi:10.1000/FAKE_ID', 'valid': None}]
        sample_doi_arxiv = [{'schema': 'doi', 'identifier': 'doi:10.48550/arXiv.1509.08217', 'valid': None}]
        sample_arxiv_no_ver = [{'schema': 'arxiv', 'identifier': 'arxiv:1509.08217', 'valid': None}]
        sample_arxiv_ver = [{'schema': 'arxiv', 'identifier': 'arxiv:1509...08217v3', 'valid': None}]

        op = OpenaireProcessing(storage_manager=RedisStorageManager(testing=True))

        # CASE 1: the unique input id dict in list is a not-arxiv doi : the input list is returned
        out_sample_doi_any = op.manage_arxiv_single_id(sample_doi_any)
        self.assertEqual(out_sample_doi_any, [{'schema': 'doi', 'identifier': 'doi:10.1000/FAKE_ID', 'valid': None}])

        # CASE 2: the unique input id dict in list is an arxiv doi: the doi is replaced with its correspondent arxiv v1
        out_sample_doi_arxiv = op.manage_arxiv_single_id(sample_doi_arxiv)
        self.assertEqual(out_sample_doi_arxiv, [{'schema': 'arxiv', 'identifier': 'arxiv:1509.08217v1'}])

        # CASE 3: the unique input id dict in list is an arxiv id without version:
        # the arxiv id is replaced with its v1
        out_sample_arxiv_no_ver = op.manage_arxiv_single_id(sample_arxiv_no_ver)
        self.assertEqual(out_sample_arxiv_no_ver, [{'schema': 'arxiv', 'identifier': 'arxiv:1509.08217v1'}])

        # CASE 4: the unique input id dict in list is an arxiv id with version: the id is just normalised
        out_sample_arxiv_ver = op.manage_arxiv_single_id(sample_arxiv_ver)
        self.assertEqual(out_sample_arxiv_ver, [{'schema': 'arxiv', 'identifier': 'arxiv:1509.08217v3'}])

        op.storage_manager.delete_storage()

    def test_manage_doi_prefixes_priorities(self):
        op = OpenaireProcessing()

        # CASE1: 1 figshare doi (priority 1) with version --> returned as it is
        es_1 = [{'schema': 'doi', 'identifier': 'doi:10.6084/1234.1234v3', 'valid': None}]
        out_1 = op.manage_doi_prefixes_priorities(es_1)
        self.assertEqual(out_1, es_1)

        # CASE2: 1 figshare doi (priority 1) without version --> returned with version v1
        es_2 = [{'schema': 'doi', 'identifier': 'doi:10.6084/1234.1234', 'valid': None}]
        exp_2 = [{'schema': 'doi', 'identifier': 'doi:10.6084/1234.1234v1', 'valid': None}]
        out_2 = op.manage_doi_prefixes_priorities(es_2)
        self.assertEqual(exp_2, out_2)

        # CASE3: 1 arxiv doi (always without and version) --> returned as correspondent arxiv id version v1
        es_3 = [{'schema': 'doi', 'identifier': 'doi:10.48550/1234.1234', 'valid': None}]
        out_3 = op.manage_doi_prefixes_priorities(es_3)
        exp_3 = [{'identifier': 'arxiv:1234.1234v1', 'schema': 'arxiv'}]
        self.assertEqual(exp_3, out_3)

        # CASE4: >1 arxiv doi or figshare and at least one has version --> return the one(s) with version
        es_4 = [{'schema': 'doi', 'identifier': 'doi:10.48550/1234.1234', 'valid': None}, {'schema': 'doi', 'identifier': 'doi:10.6084/5678v3', 'valid': None}]
        out_4 = op.manage_doi_prefixes_priorities(es_4)
        exp_4 = [{'schema': 'doi', 'identifier': 'doi:10.6084/5678v3', 'valid': None}]
        self.assertEqual(exp_4, out_4)

        # CASE5: >1 arxiv doi or figshare and none has version --> return, as first choice, the arxiv version v1 of the first arxiv doi encountered
        es_5 = [{'schema': 'doi', 'identifier': 'doi:10.6084/5678', 'valid': None}, {'schema': 'doi', 'identifier': 'doi:10.48550/1234.1234', 'valid': None}]
        out_5 = op.manage_doi_prefixes_priorities(es_5)
        exp_5 = [{'identifier': 'arxiv:1234.1234v1', 'schema': 'arxiv'}]
        self.assertEqual(exp_5, out_5)

        # CASE6: >1 figshare dois and none has version --> return, return version v1 doi of the first figshare doi encountered
        es_6 = [{'schema': 'doi', 'identifier': 'doi:10.6084/5678', 'valid': None}, {'schema': 'doi', 'identifier': 'doi:10.6084/1234', 'valid': None}]
        out_6 = op.manage_doi_prefixes_priorities(es_6)
        exp_6 = [{'identifier': 'doi:10.6084/5678v1', 'schema': 'doi', 'valid': None}]
        self.assertEqual(exp_6, out_6)

        # CASE7: >1 more than one zenodo doi --> return the one with the highest number: it is the last one assigned and thus it
        # is a version doi and not the collector doi (which is the first one to be assigned when a publication is uploaded on zenodo).
        es_7 = [{'schema': 'doi', 'identifier': '10.5281/zenodo.111', 'valid': None}, {'schema': 'doi', 'identifier': '10.5281/zenodo.112', 'valid': None}]
        es_7_1 = [{'schema': 'doi', 'identifier': 'doi:10.5281/zenodo.111', 'valid': None}, {'schema': 'doi', 'identifier': 'doi:10.5281/zenodo.112', 'valid': None}]
        out_7 = op.manage_doi_prefixes_priorities(es_7)
        out_7_1 = op.manage_doi_prefixes_priorities(es_7_1)
        exp_7 = [{'identifier': '10.5281/zenodo.112', 'schema': 'doi', 'valid': None}]
        exp_7_1 = [{'identifier': 'doi:10.5281/zenodo.112', 'schema': 'doi', 'valid': None}]
        self.assertEqual(exp_7, out_7)
        self.assertEqual(exp_7_1, out_7_1)

        # CASE8: None of the previous cases: return the first VALID DOI with highest priority prefix
        #No one of the ids is valid, return an empty list
        es_8 = [
            {'schema': 'doi', 'identifier': 'doi:10.5281/zenodo.111', 'valid': None},
            {'schema': 'doi', 'identifier': 'doi:10.1184/abc', 'valid': None},
            {'schema': 'doi', 'identifier': 'doi:10.25384/efg', 'valid': None},
        ]

        out_8 = op.manage_doi_prefixes_priorities(es_8)
        exp_8 = []
        self.assertEqual(exp_8, out_8)

        # CASE8_1:
        # No valid id among the ones with a max priority prefix -->  return the first valid ID in order of prefix priority
        es_8_1 = [
            {'schema': 'doi', 'identifier': '10.5281/zenodo.4725899', 'valid': None},
            {'schema': 'doi', 'identifier': 'doi:10.1184/abc', 'valid': None},
            {'schema': 'doi', 'identifier': 'doi:10.25384/efg', 'valid': None},
        ]

        out_8_1 = op.manage_doi_prefixes_priorities(es_8_1)
        exp_8_1 = [{'schema': 'doi', 'identifier': '10.5281/zenodo.4725899', 'valid': None}]
        self.assertEqual(exp_8_1, out_8_1)

        # CASE8_2:
        # more valid ids among the ones with a max priority prefix -->  return the first one encountered
        es_8_2 = [
            {'schema': 'doi', 'identifier': 'doi:10.5281/zenodo.4725899', 'valid': None},
            {'schema': 'doi', 'identifier': 'doi:10.1184/R1/12841247.v1', 'valid': None},
            {'schema': 'doi', 'identifier': 'doi:10.25384/sage.c.4112909', 'valid': None},
        ]

        out_8_2 = op.manage_doi_prefixes_priorities(es_8_2)
        exp_8_2 = [{'schema': 'doi', 'identifier': 'doi:10.1184/R1/12841247.v1', 'valid': None}]
        self.assertEqual(exp_8_2, out_8_2)

        op.storage_manager.delete_storage()

    def test_manage_doi_prefixes_priorities_redis(self):
        op = OpenaireProcessing(storage_manager=RedisStorageManager(testing=True))

        # CASE1: 1 figshare doi (priority 1) with version --> returned as it is
        es_1 = [{'schema': 'doi', 'identifier': 'doi:10.6084/1234.1234v3', 'valid': None}]
        out_1 = op.manage_doi_prefixes_priorities(es_1)
        self.assertEqual(out_1, es_1)

        # CASE2: 1 figshare doi (priority 1) without version --> returned with version v1
        es_2 = [{'schema': 'doi', 'identifier': 'doi:10.6084/1234.1234', 'valid': None}]
        exp_2 = [{'schema': 'doi', 'identifier': 'doi:10.6084/1234.1234v1', 'valid': None}]
        out_2 = op.manage_doi_prefixes_priorities(es_2)
        self.assertEqual(exp_2, out_2)

        # CASE3: 1 arxiv doi (always without and version) --> returned as correspondent arxiv id version v1
        es_3 = [{'schema': 'doi', 'identifier': 'doi:10.48550/1234.1234', 'valid': None}]
        out_3 = op.manage_doi_prefixes_priorities(es_3)
        exp_3 = [{'identifier': 'arxiv:1234.1234v1', 'schema': 'arxiv'}]
        self.assertEqual(exp_3, out_3)

        # CASE4: >1 arxiv doi or figshare and at least one has version --> return the one(s) with version
        es_4 = [{'schema': 'doi', 'identifier': 'doi:10.48550/1234.1234', 'valid': None}, {'schema': 'doi', 'identifier': 'doi:10.6084/5678v3', 'valid': None}]
        out_4 = op.manage_doi_prefixes_priorities(es_4)
        exp_4 = [{'schema': 'doi', 'identifier': 'doi:10.6084/5678v3', 'valid': None}]
        self.assertEqual(exp_4, out_4)

        # CASE5: >1 arxiv doi or figshare and none has version --> return, as first choice, the arxiv version v1 of the first arxiv doi encountered
        es_5 = [{'schema': 'doi', 'identifier': 'doi:10.6084/5678', 'valid': None}, {'schema': 'doi', 'identifier': 'doi:10.48550/1234.1234', 'valid': None}]
        out_5 = op.manage_doi_prefixes_priorities(es_5)
        exp_5 = [{'identifier': 'arxiv:1234.1234v1', 'schema': 'arxiv'}]
        self.assertEqual(exp_5, out_5)

        # CASE6: >1 figshare dois and none has version --> return, return version v1 doi of the first figshare doi encountered
        es_6 = [{'schema': 'doi', 'identifier': 'doi:10.6084/5678', 'valid': None}, {'schema': 'doi', 'identifier': 'doi:10.6084/1234', 'valid': None}]
        out_6 = op.manage_doi_prefixes_priorities(es_6)
        exp_6 = [{'identifier': 'doi:10.6084/5678v1', 'schema': 'doi', 'valid': None}]
        self.assertEqual(exp_6, out_6)

        # CASE7: >1 more than one zenodo doi --> return the one with the highest number: it is the last one assigned and thus it
        # is a version doi and not the collector doi (which is the first one to be assigned when a publication is uploaded on zenodo).
        es_7 = [{'schema': 'doi', 'identifier': '10.5281/zenodo.111', 'valid': None}, {'schema': 'doi', 'identifier': '10.5281/zenodo.112', 'valid': None}]
        es_7_1 = [{'schema': 'doi', 'identifier': 'doi:10.5281/zenodo.111', 'valid': None}, {'schema': 'doi', 'identifier': 'doi:10.5281/zenodo.112', 'valid': None}]
        out_7 = op.manage_doi_prefixes_priorities(es_7)
        out_7_1 = op.manage_doi_prefixes_priorities(es_7_1)
        exp_7 = [{'identifier': '10.5281/zenodo.112', 'schema': 'doi', 'valid': None}]
        exp_7_1 = [{'identifier': 'doi:10.5281/zenodo.112', 'schema': 'doi', 'valid': None}]
        self.assertEqual(exp_7, out_7)
        self.assertEqual(exp_7_1, out_7_1)

        # CASE8: None of the previous cases: return the first VALID DOI with highest priority prefix
        #No one of the ids is valid, return an empty list
        es_8 = [
            {'schema': 'doi', 'identifier': 'doi:10.5281/zenodo.111', 'valid': None},
            {'schema': 'doi', 'identifier': 'doi:10.1184/abc', 'valid': None},
            {'schema': 'doi', 'identifier': 'doi:10.25384/efg', 'valid': None},
        ]

        out_8 = op.manage_doi_prefixes_priorities(es_8)
        exp_8 = []
        self.assertEqual(exp_8, out_8)

        # CASE8_1:
        # No valid id among the ones with a max priority prefix -->  return the first valid ID in order of prefix priority
        es_8_1 = [
            {'schema': 'doi', 'identifier': '10.5281/zenodo.4725899', 'valid': None},
            {'schema': 'doi', 'identifier': 'doi:10.1184/abc', 'valid': None},
            {'schema': 'doi', 'identifier': 'doi:10.25384/efg', 'valid': None},
        ]

        out_8_1 = op.manage_doi_prefixes_priorities(es_8_1)
        exp_8_1 = [{'schema': 'doi', 'identifier': '10.5281/zenodo.4725899', 'valid': None}]
        self.assertEqual(exp_8_1, out_8_1)

        # CASE8_2:
        # more valid ids among the ones with a max priority prefix -->  return the first one encountered
        es_8_2 = [
            {'schema': 'doi', 'identifier': 'doi:10.5281/zenodo.4725899', 'valid': None},
            {'schema': 'doi', 'identifier': 'doi:10.1184/R1/12841247.v1', 'valid': None},
            {'schema': 'doi', 'identifier': 'doi:10.25384/sage.c.4112909', 'valid': None},
        ]

        out_8_2 = op.manage_doi_prefixes_priorities(es_8_2)
        exp_8_2 = [{'schema': 'doi', 'identifier': 'doi:10.1184/R1/12841247.v1', 'valid': None}]
        self.assertEqual(exp_8_2, out_8_2)

        op.storage_manager.delete_storage()

    def test_to_validated_id_list(self):
        # NOTE: in tests using the sqlite storage method it must be avoided to delete the storage
        # while using the same OpenaireProcessing() instance, otherwise the process would try to
        # store data in a filepath that has just been deleted, with no new connection created after it.

        # 2 OPTIONS: 1) instantiate OpenaireProcessing only once at the beginning and delete the
        # storage only at the end; 2) create a new OpenaireProcessing instance at every check and
        # delete the storage each time after the check is done.

        op = OpenaireProcessing()
        # CASE1_1: No already validated ids + 1 id to be validated, which is valid
        inp_1 =  {'valid': [], 'not_valid': [], 'to_be_val': [{'schema': 'pmid', 'identifier': 'pmid:20662931', 'valid': None}]}
        out_1 = op.to_validated_id_list(inp_1)
        exp_1 = ['pmid:20662931']
        self.assertEqual(out_1, exp_1)
        op.storage_manager.delete_storage()

        op = OpenaireProcessing()
        # CASE1_2: No already validated ids + 1 id to be validated, which is invalid
        inp_2 =  {'valid': [], 'not_valid': [], 'to_be_val': [{'schema': 'pmid', 'identifier': 'pmid:999920662931', 'valid': None}]}
        out_2 = op.to_validated_id_list(inp_2)
        exp_2 = []
        self.assertEqual(out_2, exp_2)

        op = OpenaireProcessing()
        # CASE1_3: No already validated ids + 1 id to be validated, which is a valid arxiv doi
        inp_3 =  {'valid': [], 'not_valid': [], 'to_be_val': [{'schema': 'doi', 'identifier': 'doi:10.48550/arXiv.1509.08217', 'valid': None}]}
        out_3 = op.to_validated_id_list(inp_3)
        exp_3 = ['arxiv:1509.08217v1']
        self.assertEqual(out_3, exp_3)
        op.storage_manager.delete_storage()


        op = OpenaireProcessing()
        # CASE1_4: No already validated ids + 1 id to be validated, which hasn't a valid schema
        inp_4 =  {'valid': [], 'not_valid': [], 'to_be_val': [{'schema': "0", 'identifier': 'doi:10.48550/arXiv.1509.08217', 'valid': None}]}
        out_4 = op.to_validated_id_list(inp_4)
        exp_4 = []
        self.assertEqual(out_4, exp_4)
        op.storage_manager.delete_storage()

        op = OpenaireProcessing()
        # CASE1_5: No already validated ids + 1 id to be validated, which is not valid
        inp_5 =  {'valid': [], 'not_valid': [], 'to_be_val': [{'schema': "doi", 'identifier': 'doi:10.0000/fake_id', 'valid': None}]}
        out_5 = op.to_validated_id_list(inp_5)
        exp_5 = []
        self.assertEqual(out_5, exp_5)
        op.storage_manager.delete_storage()

        op = OpenaireProcessing()
        # CASE1_9: No already validated ids + 1 id to be validated, which is a valid PMC
        inp_9 =  {'valid': [], 'not_valid': [], 'to_be_val': [{'schema': "pmcid", 'identifier': 'pmcid:PMC2873764', 'valid': None}]}
        out_9 = op.to_validated_id_list(inp_9)
        exp_9 = ['pmcid:PMC2873764']
        self.assertEqual(out_9, exp_9)
        op.storage_manager.delete_storage()

        op = OpenaireProcessing()
        # CASE2_1: No already validated ids + >1 id to be validated, both valid and with accepted schemas
        inp_6 =  {'valid': [], 'not_valid': [], 'to_be_val': [{'schema': 'pmid', 'identifier': 'pmid:20662931', 'valid': None},
                                                              {'schema': 'doi', 'identifier': 'doi:10.1007/s12160-011-9282-0', 'valid': None}]}
        out_6 = op.to_validated_id_list(inp_6)
        exp_6 = ['pmid:20662931', 'doi:10.1007/s12160-011-9282-0']
        self.assertCountEqual(out_6, exp_6) #Test that sequence first contains the same elements as second, regardless of their order
        op.storage_manager.delete_storage()

        op = OpenaireProcessing()
        # CASE2_2: No already validated ids + >1 id to be validated, both valid, one of the two is an arxiv id
        inp_8 =  {'valid': [], 'not_valid': [], 'to_be_val': [{'schema': 'pmid', 'identifier': 'pmid:20662931', 'valid': None},
                                                              {'schema': 'arxiv', 'identifier': 'arxiv:1107.5979', 'valid': None}]}
        out_8 = op.to_validated_id_list(inp_8)
        exp_8 = ['pmid:20662931']
        self.assertEqual(out_8, exp_8)
        op.storage_manager.delete_storage()

        op = OpenaireProcessing()
        # CASE2_3: No already validated ids + >1 id to be validated, both valid, one of the two is an arxiv doi
        inp_7 =  {'valid': [], 'not_valid': [], 'to_be_val': [{'schema': 'pmid', 'identifier': 'pmid:20662931', 'valid': None}, {'schema': "doi", 'identifier': 'doi:10.48550/arXiv.1509.08217', 'valid': None}]}
        out_7 = op.to_validated_id_list(inp_7)
        exp_7 = ['pmid:20662931']
        self.assertEqual(out_7, exp_7)
        op.storage_manager.delete_storage()

        op = OpenaireProcessing()
        # CASE2_4: No already validated ids + >1 id to be validated, both valid, one of the two is a PMC
        inp_10 =  {'valid': [], 'not_valid': [], 'to_be_val': [{'schema': 'pmid', 'identifier': 'pmid:20662931', 'valid': None},
                                                              {'schema': "pmcid", 'identifier': 'pmcid:PMC2873764', 'valid': None}]}
        out_10 = op.to_validated_id_list(inp_10)
        exp_10 = ['pmid:20662931']
        self.assertEqual(out_10, exp_10)
        op.storage_manager.delete_storage()

        op = OpenaireProcessing()
        # CASE2_5: No already validated ids + >1 id to be validated, 1 valid pmid, 1 valid doi, 1 valid doi with a "critic" prefix
        # for opencitations entities management

        inp_11 =  {'valid': [], 'not_valid': [], 'to_be_val': [{'schema': 'pmid', 'identifier': 'pmid:20662931', 'valid': None},
                                                              {'schema': 'doi', 'identifier': 'doi:10.1007/s12160-011-9282-0', 'valid': None},
                                                               {'schema': 'doi',
                                                                'identifier': 'doi:10.48550/arXiv.1509.08217',
                                                                'valid': None}
                                                               ]}
        out_11 = op.to_validated_id_list(inp_11)
        exp_11 = ['pmid:20662931', 'doi:10.1007/s12160-011-9282-0']
        self.assertCountEqual(out_11, exp_11) #Test that sequence first contains the same elements as second, regardless of their order
        op.storage_manager.delete_storage()

        op = OpenaireProcessing()
        # CASE2_6: No already validated ids + >1 id to be validated, one doi with a "critic" prefix and a PMCID
        # for opencitations entities management

        inp_12 =  {'valid': [], 'not_valid': [], 'to_be_val': [{'schema': 'pmcid', 'identifier': 'pmcid:PMC5555555', 'valid': None},
                                                               {'schema': 'doi',
                                                                'identifier': 'doi:10.48550/arXiv.1509.08217',
                                                                'valid': None}
                                                        ]}
        out_12 = op.to_validated_id_list(inp_12)
        exp_12 = ['pmcid:PMC5555555']
        self.assertEqual(out_12, exp_12)
        op.storage_manager.delete_storage()

        op = OpenaireProcessing()
        # CASE2_7: no already validated ids + >1 id to be validated, one doi with a "critic" prefix for opencitations
        # ingestion workflow and an ARXIV

        inp_13 =  {'valid': [], 'not_valid': [], 'to_be_val': [
            {'schema': 'arxiv', 'identifier': 'arxiv:1107.5979v1', 'valid': None},
            {'schema': 'doi', 'identifier': 'doi:10.1184/R1/12841247.v1', 'valid': None}
                                                        ]}
        out_13 = op.to_validated_id_list(inp_13)
        exp_13 = ['arxiv:1107.5979v1']
        self.assertEqual(out_13, exp_13)
        op.storage_manager.delete_storage()

        op = OpenaireProcessing()
        # CASE2_8: no already validated ids and more dois with "critic" prefixes for opencitations
        # ingestion workflow

        inp_14 =  {'valid': [], 'not_valid': [], 'to_be_val': [
            {'schema': 'doi', 'identifier': 'doi:10.5281/zenodo.4725899', 'valid': None},
            {'schema': 'doi', 'identifier': 'doi:10.1184/r1/12841247.v1', 'valid': None}
                                                        ]}
        out_14 = op.to_validated_id_list(inp_14)
        exp_14 = ['doi:10.1184/r1/12841247.v1']
        self.assertEqual(out_14, exp_14)
        op.storage_manager.delete_storage()

        op = OpenaireProcessing()
        # CASE3: an already validated id and more dois with "critic" prefixes for opencitations
        # ingestion workflow

        inp_15 =  {'valid': [], 'not_valid': [], 'to_be_val': [
            {'schema': 'doi', 'identifier': 'doi:10.5281/zenodo.4725899', 'valid': None},
            {'schema': 'doi', 'identifier': 'doi:10.1184/r1/12841247.v1', 'valid': None},
            {'schema': 'doi', 'identifier': 'doi:10.7557/5.5607', 'valid': None},
            {}
                                                        ]}
        out_15 = op.to_validated_id_list(inp_15)
        exp_15 = ['doi:10.7557/5.5607']
        self.assertEqual(out_15, exp_15)
        op.storage_manager.delete_storage()

    def test_to_validated_id_list_redis(self):
        # NOTE: in tests using the sqlite storage method it must be avoided to delete the storage
        # while using the same OpenaireProcessing() instance, otherwise the process would try to
        # store data in a filepath that has just been deleted, with no new connection created after it.

        # 2 OPTIONS: 1) instantiate OpenaireProcessing only once at the beginning and delete the
        # storage only at the end; 2) create a new OpenaireProcessing instance at every check and
        # delete the storage each time after the check is done.

        op = OpenaireProcessing(storage_manager=RedisStorageManager(testing=True))
        # CASE1_1: No already validated ids + 1 id to be validated, which is valid
        inp_1 =  {'valid': [], 'not_valid': [], 'to_be_val': [{'schema': 'pmid', 'identifier': 'pmid:20662931', 'valid': None}]}
        out_1 = op.to_validated_id_list(inp_1)
        exp_1 = ['pmid:20662931']
        self.assertEqual(out_1, exp_1)
        op.storage_manager.delete_storage()

        op = OpenaireProcessing(storage_manager=RedisStorageManager(testing=True))
        # CASE1_2: No already validated ids + 1 id to be validated, which is invalid
        inp_2 =  {'valid': [], 'not_valid': [], 'to_be_val': [{'schema': 'pmid', 'identifier': 'pmid:999920662931', 'valid': None}]}
        out_2 = op.to_validated_id_list(inp_2)
        exp_2 = []
        self.assertEqual(out_2, exp_2)
        op.storage_manager.delete_storage()

        op = OpenaireProcessing(storage_manager=RedisStorageManager(testing=True))
        # CASE1_3: No already validated ids + 1 id to be validated, which is a valid arxiv doi
        inp_3 =  {'valid': [], 'not_valid': [], 'to_be_val': [{'schema': 'doi', 'identifier': 'doi:10.48550/arXiv.1509.08217', 'valid': None}]}
        out_3 = op.to_validated_id_list(inp_3)
        exp_3 = ['arxiv:1509.08217v1']
        self.assertEqual(out_3, exp_3)
        op.storage_manager.delete_storage()

        op = OpenaireProcessing(storage_manager=RedisStorageManager(testing=True))
        # CASE1_4: No already validated ids + 1 id to be validated, which hasn't a valid schema
        inp_4 =  {'valid': [], 'not_valid': [], 'to_be_val': [{'schema': "0", 'identifier': 'doi:10.48550/arXiv.1509.08217', 'valid': None}]}
        out_4 = op.to_validated_id_list(inp_4)
        exp_4 = []
        self.assertEqual(out_4, exp_4)
        op.storage_manager.delete_storage()

        op = OpenaireProcessing(storage_manager=RedisStorageManager(testing=True))
        # CASE1_5: No already validated ids + 1 id to be validated, which is not valid
        inp_5 =  {'valid': [], 'not_valid': [], 'to_be_val': [{'schema': "doi", 'identifier': 'doi:10.0000/fake_id', 'valid': None}]}
        out_5 = op.to_validated_id_list(inp_5)
        exp_5 = []
        self.assertEqual(out_5, exp_5)
        op.storage_manager.delete_storage()

        op = OpenaireProcessing(storage_manager=RedisStorageManager(testing=True))
        # CASE1_9: No already validated ids + 1 id to be validated, which is a valid PMC
        inp_9 =  {'valid': [], 'not_valid': [], 'to_be_val': [{'schema': "pmcid", 'identifier': 'pmcid:PMC2873764', 'valid': None}]}
        out_9 = op.to_validated_id_list(inp_9)
        exp_9 = ['pmcid:PMC2873764']
        self.assertEqual(out_9, exp_9)
        op.storage_manager.delete_storage()

        op = OpenaireProcessing(storage_manager=RedisStorageManager(testing=True))
        # CASE2_1: No already validated ids + >1 id to be validated, both valid and with accepted schemas
        inp_6 =  {'valid': [], 'not_valid': [], 'to_be_val': [{'schema': 'pmid', 'identifier': 'pmid:20662931', 'valid': None},
                                                              {'schema': 'doi', 'identifier': 'doi:10.1007/s12160-011-9282-0', 'valid': None}]}
        out_6 = op.to_validated_id_list(inp_6)
        exp_6 = ['pmid:20662931', 'doi:10.1007/s12160-011-9282-0']
        self.assertCountEqual(out_6, exp_6) #Test that sequence first contains the same elements as second, regardless of their order
        op.storage_manager.delete_storage()

        op = OpenaireProcessing(storage_manager=RedisStorageManager(testing=True))
        # CASE2_2: No already validated ids + >1 id to be validated, both valid, one of the two is an arxiv id
        inp_8 =  {'valid': [], 'not_valid': [], 'to_be_val': [{'schema': 'pmid', 'identifier': 'pmid:20662931', 'valid': None},
                                                              {'schema': 'arxiv', 'identifier': 'arxiv:1107.5979', 'valid': None}]}
        out_8 = op.to_validated_id_list(inp_8)
        exp_8 = ['pmid:20662931']
        self.assertEqual(out_8, exp_8)
        op.storage_manager.delete_storage()

        op = OpenaireProcessing(storage_manager=RedisStorageManager(testing=True))
        # CASE2_3: No already validated ids + >1 id to be validated, both valid, one of the two is an arxiv doi
        inp_7 =  {'valid': [], 'not_valid': [], 'to_be_val': [{'schema': 'pmid', 'identifier': 'pmid:20662931', 'valid': None}, {'schema': "doi", 'identifier': 'doi:10.48550/arXiv.1509.08217', 'valid': None}]}
        out_7 = op.to_validated_id_list(inp_7)
        exp_7 = ['pmid:20662931']
        self.assertEqual(out_7, exp_7)
        op.storage_manager.delete_storage()

        op = OpenaireProcessing(storage_manager=RedisStorageManager(testing=True))
        # CASE2_4: No already validated ids + >1 id to be validated, both valid, one of the two is a PMC
        inp_10 =  {'valid': [], 'not_valid': [], 'to_be_val': [{'schema': 'pmid', 'identifier': 'pmid:20662931', 'valid': None},
                                                              {'schema': "pmcid", 'identifier': 'pmcid:PMC2873764', 'valid': None}]}
        out_10 = op.to_validated_id_list(inp_10)
        exp_10 = ['pmid:20662931']
        self.assertEqual(out_10, exp_10)
        op.storage_manager.delete_storage()

        op = OpenaireProcessing(storage_manager=RedisStorageManager(testing=True))
        # CASE2_5: No already validated ids + >1 id to be validated, 1 valid pmid, 1 valid doi, 1 valid doi with a "critic" prefix
        # for opencitations entities management

        inp_11 =  {'valid': [], 'not_valid': [], 'to_be_val': [{'schema': 'pmid', 'identifier': 'pmid:20662931', 'valid': None},
                                                              {'schema': 'doi', 'identifier': 'doi:10.1007/s12160-011-9282-0', 'valid': None},
                                                               {'schema': 'doi',
                                                                'identifier': 'doi:10.48550/arXiv.1509.08217',
                                                                'valid': None}
                                                               ]}
        out_11 = op.to_validated_id_list(inp_11)
        exp_11 = ['pmid:20662931', 'doi:10.1007/s12160-011-9282-0']
        self.assertCountEqual(out_11, exp_11) #Test that sequence first contains the same elements as second, regardless of their order
        op.storage_manager.delete_storage()

        op = OpenaireProcessing(storage_manager=RedisStorageManager(testing=True))
        # CASE2_6: No already validated ids + >1 id to be validated, one doi with a "critic" prefix and a PMCID
        # for opencitations entities management

        inp_12 =  {'valid': [], 'not_valid': [], 'to_be_val': [{'schema': 'pmcid', 'identifier': 'pmcid:PMC5555555', 'valid': None},
                                                               {'schema': 'doi',
                                                                'identifier': 'doi:10.48550/arXiv.1509.08217',
                                                                'valid': None}
                                                        ]}
        out_12 = op.to_validated_id_list(inp_12)
        exp_12 = ['pmcid:PMC5555555']
        self.assertEqual(out_12, exp_12)
        op.storage_manager.delete_storage()

        op = OpenaireProcessing(storage_manager=RedisStorageManager(testing=True))
        # CASE2_7: no already validated ids + >1 id to be validated, one doi with a "critic" prefix for opencitations
        # ingestion workflow and an ARXIV

        inp_13 =  {'valid': [], 'not_valid': [], 'to_be_val': [
            {'schema': 'arxiv', 'identifier': 'arxiv:1107.5979v1', 'valid': None},
            {'schema': 'doi', 'identifier': 'doi:10.1184/R1/12841247.v1', 'valid': None}
                                                        ]}
        out_13 = op.to_validated_id_list(inp_13)
        exp_13 = ['arxiv:1107.5979v1']
        self.assertEqual(out_13, exp_13)
        op.storage_manager.delete_storage()

        op = OpenaireProcessing(storage_manager=RedisStorageManager(testing=True))
        # CASE2_8: no already validated ids and more dois with "critic" prefixes for opencitations
        # ingestion workflow

        inp_14 =  {'valid': [], 'not_valid': [], 'to_be_val': [
            {'schema': 'doi', 'identifier': 'doi:10.5281/zenodo.4725899', 'valid': None},
            {'schema': 'doi', 'identifier': 'doi:10.1184/r1/12841247.v1', 'valid': None}
                                                        ]}
        out_14 = op.to_validated_id_list(inp_14)
        exp_14 = ['doi:10.1184/r1/12841247.v1']
        self.assertEqual(out_14, exp_14)
        op.storage_manager.delete_storage()

        op = OpenaireProcessing(storage_manager=RedisStorageManager(testing=True))
        # CASE3: an already validated id and more dois with "critic" prefixes for opencitations
        # ingestion workflow

        inp_15 =  {'valid': [], 'not_valid': [], 'to_be_val': [
            {'schema': 'doi', 'identifier': 'doi:10.5281/zenodo.4725899', 'valid': None},
            {'schema': 'doi', 'identifier': 'doi:10.1184/r1/12841247.v1', 'valid': None},
            {'schema': 'doi', 'identifier': 'doi:10.7557/5.5607', 'valid': None},
            {}
                                                        ]}
        out_15 = op.to_validated_id_list(inp_15)
        exp_15 = ['doi:10.7557/5.5607']
        self.assertEqual(out_15, exp_15)
        op.storage_manager.delete_storage()


    def test_add_authors_to_agent_list(self):
        op = OpenaireProcessing()
        sample_inp = {'creator': [{'name': 'Carlos Hoyos'}, {'name': 'Yaron Oz'}, {'identifiers': [{'identifier': '0000-0001-6946-5074', 'schema': 'ORCID', 'url': 'https://orcid.org/0000-0001-6946-5074'}], 'name': 'Bom Soo Kim'}]}
        sample_exp = op.add_authors_to_agent_list(sample_inp, [])
        sample_out = [{'role': 'author', 'name': 'Carlos Hoyos', 'family': '', 'given': ''}, {'role': 'author', 'name': 'Yaron Oz', 'family': '', 'given': ''}, {'role': 'author', 'name': 'Bom Soo Kim', 'family': '', 'given': '', 'orcid': 'orcid:0000-0001-6946-5074'}]
        self.assertEqual(sample_out, sample_exp)
        op.storage_manager.delete_storage()


    def test_add_authors_to_agent_list_redis(self):
        op = OpenaireProcessing(storage_manager=RedisStorageManager(testing=True))
        sample_inp = {'creator': [{'name': 'Carlos Hoyos'}, {'name': 'Yaron Oz'}, {'identifiers': [{'identifier': '0000-0001-6946-5074', 'schema': 'ORCID', 'url': 'https://orcid.org/0000-0001-6946-5074'}], 'name': 'Bom Soo Kim'}]}
        sample_exp = op.add_authors_to_agent_list(sample_inp, [])
        sample_out = [{'role': 'author', 'name': 'Carlos Hoyos', 'family': '', 'given': ''}, {'role': 'author', 'name': 'Yaron Oz', 'family': '', 'given': ''}, {'role': 'author', 'name': 'Bom Soo Kim', 'family': '', 'given': '', 'orcid': 'orcid:0000-0001-6946-5074'}]
        self.assertEqual(sample_out, sample_exp)
        op.storage_manager.delete_storage()

    def test_add_authors_to_agent_list_no_creator(self):
        op = OpenaireProcessing()
        sample_inp = {'creator': []}
        sample_exp = op.add_authors_to_agent_list(sample_inp, [])
        sample_out = []
        self.assertEqual(sample_out, sample_exp)
        op.storage_manager.delete_storage()


    def test_add_authors_to_agent_list_no_creator_redis(self):
        op = OpenaireProcessing(storage_manager=RedisStorageManager(testing=True))
        sample_inp = {'creator': []}
        sample_exp = op.add_authors_to_agent_list(sample_inp, [])
        sample_out = []
        self.assertEqual(sample_out, sample_exp)
        op.storage_manager.delete_storage()

    def test_get_agents_strings_list(self):
        best_doi = "doi:10.1007/jhep03(2014)050"
        agents_list_2 = [{'role': 'author', 'name': 'Hoyos, Carlos', 'family': '', 'given': ''}, {'role': 'author', 'name': 'Oz, Yaron', 'family': '', 'given': ''}, {'role': 'author', 'name': 'Kim, Bom Soo', 'family': '', 'given': '', 'orcid': 'orcid:0000-0001-6946-5074'}]
        op = OpenaireProcessing()
        sample_exp = op.get_agents_strings_list(best_doi, agents_list_2)
        self.assertEqual(sample_exp, (['Hoyos Carlos', 'Oz Yaron', 'Kim Bom Soo [orcid:0000-0001-6946-5074]'], []))
        op.storage_manager.delete_storage()

    def test_get_agents_strings_list_redis(self):
        best_doi = "doi:10.1007/jhep03(2014)050"
        agents_list_2 = [{'role': 'author', 'name': 'Hoyos, Carlos', 'family': '', 'given': ''}, {'role': 'author', 'name': 'Oz, Yaron', 'family': '', 'given': ''}, {'role': 'author', 'name': 'Kim, Bom Soo', 'family': '', 'given': '', 'orcid': 'orcid:0000-0001-6946-5074'}]
        op = OpenaireProcessing(storage_manager=RedisStorageManager(testing=True))
        sample_exp = op.get_agents_strings_list(best_doi, agents_list_2)
        self.assertEqual(sample_exp, (['Hoyos Carlos', 'Oz Yaron', 'Kim Bom Soo [orcid:0000-0001-6946-5074]'], []))
        op.storage_manager.delete_storage()

    def test_find_openaire_orcid(self):
        op = OpenaireProcessing()
        inp = [{'identifier': '0000-0001-9759-3938', 'schema': 'ORCID', 'url': 'https://orcid.org/0000-0001-9759-3938'}]
        out = op.find_openaire_orcid(inp)
        exp = "orcid:0000-0001-9759-3938"
        self.assertEqual(out, exp)

        inp_wrong_schema = [{'identifier': '0000-0001-9759-3938', 'schema': 'fake_schema', 'url': 'https://orcid.org/0000-0001-9759-3938'}]
        out_wrong_schema = op.find_openaire_orcid(inp_wrong_schema)
        exp_wrong_schema = ""
        self.assertEqual(out_wrong_schema, exp_wrong_schema)

        inp_invalid_id = [{'identifier': '5500-0001-9759-3938', 'schema': 'ORCID', 'url': 'https://orcid.org/0000-0001-9759-3938'}]
        out_invalid_id = op.find_openaire_orcid(inp_invalid_id)
        exp_invalid_id = ""
        self.assertEqual(out_invalid_id, exp_invalid_id)

        op.storage_manager.delete_storage()

        # set a valid id as invalid in storage, so to check that the api check is
        # avoided if the info is already in storage
        op = OpenaireProcessing()
        op.storage_manager.set_value("orcid:0000-0001-9759-3938", False)

        inp = [{'identifier': '0000-0001-9759-3938', 'schema': 'ORCID', 'url': 'https://orcid.org/0000-0001-9759-3938'}]
        out = op.find_openaire_orcid(inp)
        exp = ""
        self.assertEqual(out, exp)

        op.storage_manager.delete_storage()
        op = OpenaireProcessing()
        op.storage_manager.set_value("orcid:0000-0001-9759-3938", True)
        inp = [{'identifier': '0000-0001-9759-3938', 'schema': 'ORCID', 'url': 'https://orcid.org/0000-0001-9759-3938'}]
        out = op.find_openaire_orcid(inp)
        exp = "orcid:0000-0001-9759-3938"
        self.assertEqual(out, exp)
        op.storage_manager.delete_storage()


    def test_find_openaire_orcid_redis(self):
        op = OpenaireProcessing(storage_manager=RedisStorageManager(testing=True))
        inp = [{'identifier': '0000-0001-9759-3938', 'schema': 'ORCID', 'url': 'https://orcid.org/0000-0001-9759-3938'}]
        out = op.find_openaire_orcid(inp)
        exp = "orcid:0000-0001-9759-3938"
        self.assertEqual(out, exp)

        inp_wrong_schema = [{'identifier': '0000-0001-9759-3938', 'schema': 'fake_schema', 'url': 'https://orcid.org/0000-0001-9759-3938'}]
        out_wrong_schema = op.find_openaire_orcid(inp_wrong_schema)
        exp_wrong_schema = ""
        self.assertEqual(out_wrong_schema, exp_wrong_schema)

        inp_invalid_id = [{'identifier': '5500-0001-9759-3938', 'schema': 'ORCID', 'url': 'https://orcid.org/0000-0001-9759-3938'}]
        out_invalid_id = op.find_openaire_orcid(inp_invalid_id)
        exp_invalid_id = ""
        self.assertEqual(out_invalid_id, exp_invalid_id)

        op.storage_manager.delete_storage()

        # set a valid id as invalid in storage, so to check that the api check is
        # avoided if the info is already in storage
        op = OpenaireProcessing(storage_manager=RedisStorageManager(testing=True))
        op.storage_manager.set_value("orcid:0000-0001-9759-3938", False)

        inp = [{'identifier': '0000-0001-9759-3938', 'schema': 'ORCID', 'url': 'https://orcid.org/0000-0001-9759-3938'}]
        out = op.find_openaire_orcid(inp)
        exp = ""
        self.assertEqual(out, exp)

        op.storage_manager.delete_storage()
        op = OpenaireProcessing(storage_manager=RedisStorageManager(testing=True))
        op.storage_manager.set_value("orcid:0000-0001-9759-3938", True)
        inp = [{'identifier': '0000-0001-9759-3938', 'schema': 'ORCID', 'url': 'https://orcid.org/0000-0001-9759-3938'}]
        out = op.find_openaire_orcid(inp)
        exp = "orcid:0000-0001-9759-3938"
        self.assertEqual(out, exp)
        op.storage_manager.delete_storage()

    def test_update_redis_values(self):
        br = ["pmid:2", "pmid:3"]
        ra = ["orcid:0000-0003-0530-4305"]
        op = OpenaireProcessing(storage_manager=RedisStorageManager(testing=True))
        op.update_redis_values(br,ra)
        self.assertEqual(op._redis_values_br, br)
        self.assertEqual(op._redis_values_ra, ra)


    #### REAL REDIS TESTS (SKIPPED IF REDIS IS NOT CONNECTED // REDIS DB 14 IS NOT EMPTY)

    def test_get_reids_validity_list_real_redis(self):
        function_to_execute = "test_get_reids_validity_list_real_redis"
        try:
            rsm = RedisStorageManager(testing=False)
            rsm.set_value("TEST VALUE", False)
            run_test = True
        except:
            run_test = False
            print(f'test skipped: {function_to_execute}: Connect to redis before running the test')
        if run_test:
            rsm.del_value("TEST VALUE")
            op = OpenaireProcessing(storage_manager=RedisStorageManager(testing=False))
            if not op.BR_redis.get("pmcid:PMC4005913"):
                op.BR_redis.set("pmcid:PMC4005913", "ra/061randomra")
            list_br = ['pmcid:PMC4005913', 'pmid:24632350', 'pmid:1581770']
            out_list = op.get_reids_validity_list(list_br, "br")
            exp = ['pmcid:PMC4005913']
            self.assertEqual(exp, out_list)
            op.BR_redis.delete("pmcid:PMC4005913")
            self.assertFalse(op.BR_redis.get("pmcid:PMC4005913"))

    def real_redis_test_case(self, function_to_execute):
        try:
            rsm = RedisStorageManager(testing=False)
            rsm.set_value("TEST VALUE", False)
            run_test = True
        except:
            run_test = False
            print(f'test skipped: {function_to_execute}: Connect to redis before running the test')

        if run_test:
            rsm.del_value("TEST VALUE")
            if not len(rsm.get_all_keys()):
                function_to_execute()
                rsm.delete_storage()

            else:
                # print("get_all_keys()", rsm.get_all_keys())
                # rsm.delete_storage()
                print(f'test skipped: {function_to_execute}: Redis db 2 is not empty')

    def update_redis_values_real_redis(self):
        br = ["pmid:2", "pmid:3"]
        ra = ["orcid:0000-0003-0530-4305"]
        op = OpenaireProcessing(storage_manager=RedisStorageManager(testing=False))
        op.update_redis_values(br,ra)
        self.assertEqual(op._redis_values_br, br)
        self.assertEqual(op._redis_values_ra, ra)

    def test_update_redis_values_real_redis(self):
        self.real_redis_test_case(self.update_redis_values_real_redis)



if __name__ == '__main__':
    unittest.main()