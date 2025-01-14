from oc_ds_converter.crossref.crossref_processing import CrossrefProcessing
import unittest
import os
import json
from oc_ds_converter.lib.csvmanager import CSVManager
from oc_ds_converter.oc_idmanager.oc_data_storage.in_memory_manager import InMemoryStorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.sqlite_manager import SqliteStorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.redis_manager import RedisStorageManager
from oc_ds_converter.lib.jsonmanager import *
TEST_DIR = os.path.join("test", "crossref_processing")
JSON_FILE = os.path.join(TEST_DIR, "0.json")
TMP_SUPPORT_MATERIAL = os.path.join(TEST_DIR, "tmp_support")
IOD = os.path.join(TEST_DIR, 'iod')
WANTED_DOIS_FOLDER = os.path.join(TEST_DIR, 'wanted_dois')
DATA = os.path.join(TEST_DIR, '40228.json')
PUBLISHERS_MAPPING = os.path.join(TEST_DIR, 'publishers.csv')



class TestCrossrefProcessing(unittest.TestCase):
    def test_extract_all_ids_cited(self):
        c_processing = CrossrefProcessing()
        with open(JSON_FILE, encoding="utf8") as f:
             result = json.load(f)
        for entity_dict in result['items']:
            results_ids = c_processing.extract_all_ids(entity_dict, False)
            br = results_ids[0]
            expected_br = ['doi:10.2105/ajph.2006.101626', 'doi:10.1001/jama.299.12.1471', 'doi:10.1177/003335490812300219', 'doi:10.1089/bsp.2008.0020', 'doi:10.1097/01.ccm.0000151067.76074.21', 'doi:10.1177/003335490912400218', 'doi:10.1097/dmp.0b013e31817196bf', 'doi:10.1056/nejmsa021807', 'doi:10.1097/dmp.0b013e31819d977c', 'doi:10.1097/dmp.0b013e31819f1ae2', 'doi:10.1097/dmp.0b013e318194898d', 'doi:10.1378/chest.07-2693', 'doi:10.1016/s0196-0644(99)70224-6', 'doi:10.1097/01.ccm.0000151072.17826.72', 'doi:10.1097/01.bcr.0000155527.76205.a2', 'doi:10.2105/ajph.2009.162677']
            self.assertEqual(set(expected_br), set(br))
        c_processing.storage_manager.delete_storage()

    def test_extract_all_ids_cited_redis(self):
        c_processing = CrossrefProcessing(storage_manager=RedisStorageManager(testing=True))
        with open(JSON_FILE, encoding="utf8") as f:
             result = json.load(f)
        for entity_dict in result['items']:
            results_ids = c_processing.extract_all_ids(entity_dict, False)
            br = results_ids[0]
            expected_br = ['doi:10.2105/ajph.2006.101626', 'doi:10.1001/jama.299.12.1471',
                           'doi:10.1177/003335490812300219', 'doi:10.1089/bsp.2008.0020',
                           'doi:10.1097/01.ccm.0000151067.76074.21', 'doi:10.1177/003335490912400218',
                           'doi:10.1097/dmp.0b013e31817196bf', 'doi:10.1056/nejmsa021807',
                           'doi:10.1097/dmp.0b013e31819d977c', 'doi:10.1097/dmp.0b013e31819f1ae2',
                           'doi:10.1097/dmp.0b013e318194898d', 'doi:10.1378/chest.07-2693',
                           'doi:10.1016/s0196-0644(99)70224-6', 'doi:10.1097/01.ccm.0000151072.17826.72',
                           'doi:10.1097/01.bcr.0000155527.76205.a2', 'doi:10.2105/ajph.2009.162677']
            self.assertEqual(set(expected_br), set(br))
        c_processing.storage_manager.delete_storage()

    def test_get_redis_validity_list(self):
        c_processing = CrossrefProcessing()
        br = {'doi:10.2105/ajph.2006.101626', 'doi:10.1001/jama.299.12.1471',
               'doi:10.1177/003335490812300219', 'doi:10.1089/bsp.2008.0020',
               'doi:10.1097/01.ccm.0000151067.76074.21', 'doi:10.1177/003335490912400218',
               'doi:10.1097/dmp.0b013e31817196bf', 'doi:10.1056/nejmsa021807',
               'doi:10.1097/dmp.0b013e31819d977c', 'doi:10.1097/dmp.0b013e31819f1ae2',
               'doi:10.1097/dmp.0b013e318194898d', 'doi:10.1378/chest.07-2693',
               'doi:10.1016/s0196-0644(99)70224-6', 'doi:10.1097/01.ccm.0000151072.17826.72',
               'doi:10.1097/01.bcr.0000155527.76205.a2', 'doi:10.2105/ajph.2009.162677'}
        br_valid_list = c_processing.get_reids_validity_list(br, "br")
        exp_br_valid_list = []
        self.assertEqual(br_valid_list, exp_br_valid_list)
        c_processing.storage_manager.delete_storage()

    def test_get_redis_validity_list_redis(self):
        c_processing = CrossrefProcessing(storage_manager=RedisStorageManager(testing=True))
        br = {'doi:10.2105/ajph.2006.101626', 'doi:10.1001/jama.299.12.1471',
              'doi:10.1177/003335490812300219', 'doi:10.1089/bsp.2008.0020',
              'doi:10.1097/01.ccm.0000151067.76074.21', 'doi:10.1177/003335490912400218',
              'doi:10.1097/dmp.0b013e31817196bf', 'doi:10.1056/nejmsa021807',
              'doi:10.1097/dmp.0b013e31819d977c', 'doi:10.1097/dmp.0b013e31819f1ae2',
              'doi:10.1097/dmp.0b013e318194898d', 'doi:10.1378/chest.07-2693',
              'doi:10.1016/s0196-0644(99)70224-6', 'doi:10.1097/01.ccm.0000151072.17826.72',
              'doi:10.1097/01.bcr.0000155527.76205.a2', 'doi:10.2105/ajph.2009.162677'}
        ra = {'orcid:0000-0002-8090-6886', 'orcid:0000-0002-6491-0754'}
        br_valid_list = c_processing.get_reids_validity_list(br, "br")
        exp_br_valid_list = []
        ra_valid_list = c_processing.get_reids_validity_list(ra, "ra")
        self.assertEqual(br_valid_list, exp_br_valid_list)
        exp_ra_valid_list = []
        self.assertEqual(ra_valid_list, exp_ra_valid_list)
        c_processing.storage_manager.delete_storage()

    def test_get_redis_validity_dict_w_fakeredis_db_values_sqlite(self):
        c_processing = CrossrefProcessing()
        c_processing.BR_redis.set('doi:10.2105/ajph.2006.101626', "omid:1")
        c_processing.RA_redis.set('orcid:0000-0002-8090-6886', "omid:2")


        br = {'doi:10.2105/ajph.2006.101626', 'doi:10.1001/jama.299.12.1471',
              'doi:10.1177/003335490812300219'}
        ra = {'orcid:0000-0002-8090-6886', 'orcid:0000-0002-6491-0754'}

        br_validity_dict = c_processing.get_reids_validity_list(br, "br")
        exp_br_valid_list = ['doi:10.2105/ajph.2006.101626']
        ra_validity_dict = c_processing.get_reids_validity_list(ra, "ra")
        exp_ra_valid_list = ['orcid:0000-0002-8090-6886']
        self.assertEqual(br_validity_dict, exp_br_valid_list)
        self.assertEqual(ra_validity_dict, exp_ra_valid_list)

        c_processing.storage_manager.delete_storage()

        c_processing.BR_redis.delete('doi:10.2105/ajph.2006.101626')
        c_processing.RA_redis.delete('orcid:0000-0002-8090-6886')


    def test_get_redis_validity_dict_w_fakeredis_db_values_redis(self):
        c_processing = CrossrefProcessing(storage_manager=RedisStorageManager(testing=True))
        c_processing.BR_redis.set('doi:10.2105/ajph.2006.101626', "omid:1")
        c_processing.RA_redis.set('orcid:0000-0002-8090-6886', "omid:2")


        br = {'doi:10.2105/ajph.2006.101626', 'doi:10.1001/jama.299.12.1471',
              'doi:10.1177/003335490812300219'}
        ra = {'orcid:0000-0002-8090-6886', 'orcid:0000-0002-6491-0754'}

        br_validity_dict = c_processing.get_reids_validity_list(br, "br")
        exp_br_valid_list = ['doi:10.2105/ajph.2006.101626']
        ra_validity_dict = c_processing.get_reids_validity_list(ra, "ra")
        exp_ra_valid_list = ['orcid:0000-0002-8090-6886']
        self.assertEqual(br_validity_dict, exp_br_valid_list)
        self.assertEqual(ra_validity_dict, exp_ra_valid_list)

        c_processing.storage_manager.delete_storage()

        c_processing.BR_redis.delete('doi:10.2105/ajph.2006.101626')
        c_processing.RA_redis.delete('orcid:0000-0002-8090-6886')

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

        c_processing = CrossrefProcessing()
        validate_as_none = c_processing.validated_as({"schema":"doi", "identifier": "doi:10.1001/10-v4n2-hsf10003"})
        self.assertEqual(validate_as_none, None)
        c_processing.storage_manager.delete_storage()

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
        c_processing = CrossrefProcessing(storage_manager=RedisStorageManager(testing=True))
        validate_as_none = c_processing.validated_as({"schema": "doi", "identifier": "doi:10.1001/10-v4n2-hsf10003"})
        self.assertEqual(validate_as_none, None)
        c_processing.storage_manager.delete_storage()

    def test_validated_as_sqlite(self):
        """
        Check that, given an ID dict with keys "schema" (value: string of the schema) and "identifier" (value:
        string of the identifier, the method "validated_as" returns:
        - True if the id was already validated as valid
        - False if the id was already validated as invalid
        - None if the id was not validated before
        The procedure is tested
        - With sqlite storage manager without a pre-existent db associated
        - With sqlite storage manager and a pre-existent db associated
        """

        db_path = os.path.join(TMP_SUPPORT_MATERIAL, "db_path.db")

        sqlite_man = SqliteStorageManager(db_path)
        valid_doi_not_in_db = {"identifier":"doi:10.1001/2012.jama.10158", "schema":"doi"}
        valid_doi_in_db = {"identifier":"doi:10.1001/2012.jama.10368", "schema":"doi"}
        invalid_doi_in_db = {"identifier":"doi:10.1001/2012.jama.1036", "schema":"doi"}
        sqlite_man.set_value(valid_doi_in_db["identifier"], True)
        sqlite_man.set_value(invalid_doi_in_db["identifier"], False)

        # New class instance to check the correct task management with a sqlite db in input
        c_processing_sql = CrossrefProcessing(storage_manager=sqlite_man)
        validated_as_True = c_processing_sql.validated_as(valid_doi_in_db)
        validated_as_False = c_processing_sql.validated_as(invalid_doi_in_db)
        not_validated = c_processing_sql.validated_as(valid_doi_not_in_db)

        self.assertEqual(validated_as_True, True)
        self.assertEqual(validated_as_False, False)
        self.assertEqual(not_validated, None)

        c_processing_sql.storage_manager.delete_storage()

    def test_validated_as_inmemory(self):
        """
        Check that, given an ID dict with keys "schema" (value: string of the schema) and "identifier" (value:
        string of the identifier, the method "validated_as" returns:
        - True if the id was already validated as valid
        - False if the id was already validated as invalid
        - None if the id was not validated before
        The procedure is tested
        - With in Memory + Json storage manager and a pre-existent db associated
        - With in Memory + Json storage manager without a pre-existent db associated
        """
        db_json_path = os.path.join(TMP_SUPPORT_MATERIAL, "db_path.json")

        inmemory_man = InMemoryStorageManager(db_json_path)
        valid_doi_not_in_db = {"identifier": "doi:10.1001/2012.jama.10158", "schema": "doi"}
        valid_doi_in_db = {"identifier": "doi:10.1001/2012.jama.10368", "schema": "doi"}
        invalid_doi_in_db = {"identifier": "doi:10.1001/2012.jama.1036", "schema": "doi"}
        inmemory_man.set_value(valid_doi_in_db["identifier"], True)
        inmemory_man.set_value(invalid_doi_in_db["identifier"], False)

        c_processing = CrossrefProcessing(storage_manager=inmemory_man)
        validated_as_True = c_processing.validated_as(valid_doi_in_db)
        validated_as_False = c_processing.validated_as(invalid_doi_in_db)
        not_validated = c_processing.validated_as(valid_doi_not_in_db)

        self.assertEqual(validated_as_True, True)
        self.assertEqual(validated_as_False, False)
        self.assertEqual(not_validated, None)

        c_processing.storage_manager.delete_storage()

    def test_validated_as_redis(self):
        """
        Check that, given an ID dict with keys "schema" (value: string of the schema) and "identifier" (value:
        string of the identifier, the method "validated_as" returns:
        - True if the id was already validated as valid
        - False if the id was already validated as invalid
        - None if the id was not validated before
        The procedure is tested
        - With REDIS storage manager and a pre-existent db associated
        - With REDIS storage manager without a pre-existent db associated
        """

        redis_man = RedisStorageManager(testing=True)
        valid_doi_not_in_db = {"identifier": "doi:10.1001/2012.jama.10158", "schema": "doi"}
        valid_doi_in_db = {"identifier": "doi:10.1001/2012.jama.10368", "schema": "doi"}
        invalid_doi_in_db = {"identifier": "doi:10.1001/2012.jama.1036", "schema": "doi"}
        redis_man.set_value(valid_doi_in_db["identifier"], True)
        redis_man.set_value(invalid_doi_in_db["identifier"], False)

        # New class instance to check the correct task management with a redis manager using a db with data
        c_processing_redis = CrossrefProcessing(storage_manager=redis_man)
        validated_as_True = c_processing_redis.validated_as(valid_doi_in_db)
        validated_as_False = c_processing_redis.validated_as(invalid_doi_in_db)
        not_validated = c_processing_redis.validated_as(valid_doi_not_in_db)
        self.assertEqual(validated_as_True, True)
        self.assertEqual(validated_as_False, False)
        self.assertEqual(not_validated, None)
        c_processing_redis.storage_manager.delete_storage()

    def test_get_id_manager(self):
        """Check that, given in input the string of a schema (e.g.:'pmid') or an id with a prefix (e.g.: 'pmid:12334')
        and a dictionary mapping the strings of the schemas to their id managers, the method returns the correct
        id manager. Note that each instance of the Preprocessing class needs its own instances of the id managers,
        in order to avoid conflicts while validating data"""

        c_processing = CrossrefProcessing()
        id_man_dict = c_processing.venue_id_man_dict

        issn_id = "issn:0003-987X"
        issn_string = "issn"
        issn_man_exp = c_processing.get_id_manager(issn_id, id_man_dict)
        issn_man_exp_2 = c_processing.get_id_manager(issn_string, id_man_dict)

        #check that the idmanager for the issn was returned and that it works as expected
        self.assertTrue(issn_man_exp.is_valid(issn_id))
        self.assertTrue(issn_man_exp_2.is_valid(issn_id))

    def test_csv_creator(self):
        c_processing = CrossrefProcessing(orcid_index=IOD, doi_csv=WANTED_DOIS_FOLDER, publishers_filepath=None)
        data = load_json(DATA, None)
        output = list()
        for item in data['items']:
            tabular_data = c_processing.csv_creator(item)
            if tabular_data:
                output.append(tabular_data)
        expected_output = [
            {'id': 'doi:10.47886/9789251092637.ch7', 'title': 'Freshwater, Fish and the Future: Proceedings of the Global Cross-Sectoral Conference', 'author': '', 'pub_date': '2016', 'venue': 'Freshwater, Fish and the Future: Proceedings of the Global Cross-Sectoral Conference', 'volume': '', 'issue': '', 'page': '', 'type': 'book chapter', 'publisher': 'American Fisheries Society [crossref:460]', 'editor': 'Lymer, David; Food and Agriculture Organization of the United Nations Fisheries and Aquaculture Department Viale delle Terme di Caracalla Rome 00153 Italy; Marttin, Felix; Marmulla, Gerd; Bartley, Devin M.'},
            {'id': 'doi:10.9799/ksfan.2012.25.1.069', 'title': 'Nonthermal Sterilization and Shelf-life Extension of Seafood Products by Intense Pulsed Light Treatment', 'author': 'Cheigh, Chan-Ick [orcid:0000-0002-6227-4053]; Mun, Ji-Hye [orcid:0000-0002-6227-4053]; Chung, Myong-Soo', 'pub_date': '2012-3-31', 'venue': 'The Korean Journal of Food And Nutrition [issn:1225-4339]', 'volume': '25', 'issue': '1', 'page': '69-76', 'type': 'journal article', 'publisher': 'The Korean Society of Food and Nutrition [crossref:4768]', 'editor': ''},
            {'id': 'doi:10.9799/ksfan.2012.25.1.105', 'title': 'A Study on Dietary Habit and Eating Snack Behaviors of Middle School Students with Different Obesity Indexes in Chungnam Area', 'author': 'Kim, Myung-Hee; Seo, Jin-Seon; Choi, Mi-Kyeong [orcid:0000-0002-6227-4053]; Kim, Eun-Young', 'pub_date': '2012-3-31', 'venue': 'The Korean Journal of Food And Nutrition [issn:1225-4339]', 'volume': '25', 'issue': '1', 'page': '105-115', 'type': 'journal article', 'publisher': 'The Korean Society of Food and Nutrition [crossref:4768]', 'editor': ''},
            {'id': 'doi:10.9799/ksfan.2012.25.1.123', 'title': 'The Protective Effects of Chrysanthemum cornarium L. var. spatiosum Extract on HIT-T15 Pancreatic β-Cells against Alloxan-induced Oxidative Stress', 'author': 'Kim, In-Hye; Cho, Kang-Jin; Ko, Jeong-Sook; Kim, Jae-Hyun; Om, Ae-Son', 'pub_date': '2012-3-31', 'venue': 'The Korean Journal of Food And Nutrition [issn:1225-4339]', 'volume': '25', 'issue': '1', 'page': '123-131', 'type': 'journal article', 'publisher': 'The Korean Society of Food and Nutrition [crossref:4768]', 'editor': ''}
        ]
        self.assertEqual(output, expected_output)

    def test_csv_creator_cited(self):
        c_processing_cited = CrossrefProcessing(orcid_index=IOD, publishers_filepath=None, citing=False)
        with open(JSON_FILE, encoding="utf8") as f:
             result = json.load(f)
        output = list()
        for item in result['items']:
            if item.get("reference"):
                # filtering out entities without citations
                has_doi_references = [x for x in item["reference"] if x.get("DOI")]
                if has_doi_references:
                    for reference_dict in has_doi_references:
                        tabular_data = c_processing_cited.csv_creator(reference_dict)
                        if tabular_data:
                            output.append(tabular_data)
        expected_output =[
            {'id': 'doi:10.2105/ajph.2006.101626', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''},
            {'id': 'doi:10.1001/jama.299.12.1471', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''},
            {'id': 'doi:10.1177/003335490812300219', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''},
            {'id': 'doi:10.1089/bsp.2008.0020', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''},
            {'id': 'doi:10.1097/01.ccm.0000151067.76074.21', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''},
            {'id': 'doi:10.1177/003335490912400218', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''},
            {'id': 'doi:10.1097/dmp.0b013e31817196bf', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''},
            {'id': 'doi:10.1056/nejmsa021807', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''},
            {'id': 'doi:10.1097/dmp.0b013e31819d977c', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''},
            {'id': 'doi:10.1097/dmp.0b013e31819f1ae2', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''},
            {'id': 'doi:10.1097/dmp.0b013e318194898d', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''},
            {'id': 'doi:10.1378/chest.07-2693', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''},
            {'id': 'doi:10.1016/s0196-0644(99)70224-6', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''},
            {'id': 'doi:10.1097/01.ccm.0000151072.17826.72', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''},
            {'id': 'doi:10.1097/01.bcr.0000155527.76205.a2', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''},
            {'id': 'doi:10.2105/ajph.2009.162677', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''}]
        self.assertEqual(output, expected_output)

    def test_get_pages(self):
        item = {
            'page': '469-476'
        }
        crossref_processor = CrossrefProcessing(orcid_index=None, doi_csv=None, publishers_filepath=PUBLISHERS_MAPPING)
        pages = crossref_processor.get_crossref_pages(item)
        self.assertEqual(pages, '469-476')

    def test_get_pages_right_letter(self):
        item = {
            'page': 'G22'
        }
        crossref_processor = CrossrefProcessing(orcid_index=None, doi_csv=None, publishers_filepath=PUBLISHERS_MAPPING)
        pages = crossref_processor.get_crossref_pages(item)
        self.assertEqual(pages, 'G22-G22')

    def test_get_pages_wrong_letter(self):
        item = {
            'page': '583b-584'
        }
        crossref_processor = CrossrefProcessing(orcid_index=None, doi_csv=None, publishers_filepath=PUBLISHERS_MAPPING)
        pages = crossref_processor.get_crossref_pages(item)
        self.assertEqual(pages, '583-584')

    def test_get_pages_roman_letters(self):
        item = {
            'page': 'iv-l'
        }
        crossref_processor = CrossrefProcessing(orcid_index=None, doi_csv=None, publishers_filepath=PUBLISHERS_MAPPING)
        pages = crossref_processor.get_crossref_pages(item)
        self.assertEqual(pages, 'iv-l')

    def test_get_pages_non_roman_letters(self):
        item = {
            'page': 'kj-hh'
        }
        crossref_processor = CrossrefProcessing(orcid_index=None, doi_csv=None, publishers_filepath=PUBLISHERS_MAPPING)
        pages = crossref_processor.get_crossref_pages(item)
        self.assertEqual(pages, '')

    def test_load_publishers_mapping(self):
        output = CrossrefProcessing.load_publishers_mapping(publishers_filepath=PUBLISHERS_MAPPING)
        expected_output = {
            '1': {'name': 'Annals of Family Medicine', 'prefixes': {'10.1370'}},
            '2': {'name': 'American Association of Petroleum Geologists AAPG/Datapages', 'prefixes': {'10.15530', '10.1306'}},
            '3': {'name': 'American Association of Physics Teachers (AAPT)','prefixes': {'10.1119'}},
            '6': {'name': 'American College of Medical Physics (ACMP)','prefixes': {'10.1120'}},
            '9': {'name': 'Allen Press', 'prefixes': {'10.1043'}},
            '10': {'name': 'American Medical Association (AMA)', 'prefixes': {'10.1001'}},
            '11': {'name': 'American Economic Association', 'prefixes': {'10.1257'}},
            '460': {'name': 'American Fisheries Society', 'prefixes': {'10.1577', '10.47886'}}
        }
        self.assertEqual(output, expected_output)

    def test_get_publisher_name(self):
        # The item's member is in the publishers' mapping
        item = {
            'publisher': 'American Fisheries Society',
            'DOI': '10.47886\/9789251092637.ch7',
            'prefix': '10.47886',
            'member': '460'
        }
        doi = '10.47886/9789251092637.ch7'
        crossref_processor = CrossrefProcessing(orcid_index=None, doi_csv=None, publishers_filepath=PUBLISHERS_MAPPING)
        publisher_name = crossref_processor.get_publisher_name(doi, item)
        self.assertEqual(publisher_name, 'American Fisheries Society [crossref:460]')

    def test_get_publisher_name_no_member(self):
        # The item has no member, but the DOI prefix is the publishers' mapping
        item = {
            'publisher': 'American Fisheries Society',
            'DOI': '10.47886/9789251092637.ch7',
            'prefix': '10.47886'
        }
        doi = '10.47886/9789251092637.ch7'
        crossref_processor = CrossrefProcessing(orcid_index=None, doi_csv=None, publishers_filepath=PUBLISHERS_MAPPING)
        publisher_name = crossref_processor.get_publisher_name(doi, item)
        self.assertEqual(publisher_name, 'American Fisheries Society [crossref:460]')

    def test_get_venue_name(self):
        item = {
            'container-title': ['Cerebrospinal Fluid [Working Title]'],
        }
        row = {'id': '', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': 'journal article', 'publisher': '', 'editor': ''}
        crossref_processor = CrossrefProcessing(orcid_index=None, doi_csv=None, publishers_filepath=PUBLISHERS_MAPPING)
        venue_name = crossref_processor.get_venue_name(item, row)
        self.assertEqual(venue_name, 'Cerebrospinal Fluid [Working Title]')

    def test_get_venue_name_with_ISSN(self):
        item = {
            "container-title": ["Disaster Medicine and Public Health Preparedness"],
            "ISSN": ["1935-7893", "1938-744X"]
        }
        row = {'id': '', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '',
               'type': 'journal article', 'publisher': '', 'editor': ''}
        crossref_processor = CrossrefProcessing(orcid_index=None, doi_csv=None, publishers_filepath=PUBLISHERS_MAPPING)
        venue_name = crossref_processor.get_venue_name(item, row)
        self.assertEqual(venue_name, 'Disaster Medicine and Public Health Preparedness [issn:1935-7893 issn:1938-744X]')

    def test_find_crossref_orcid(self):
        """Test that, given in input a string representing an ORCID, the method returns:
        - the ORCID itself if it is valid
        - an empty string if it is not valid
        The procedure is tested with:
        - a valid ORCID
        - an invalid ORCID
        - a non-string input
        """
        c_processing = CrossrefProcessing()
        test_doi = "10.1234/test123"  # Added test DOI

        # Valid ORCID
        inp = '0000-0003-4082-1500'
        out = c_processing.find_crossref_orcid(inp, test_doi)
        exp = "orcid:0000-0003-4082-1500"
        self.assertEqual(out, exp)

        # Invalid ORCID
        inp_invalid_id = '5500-0001-9759-3938'
        out_invalid_id = c_processing.find_crossref_orcid(inp_invalid_id, test_doi)
        exp_invalid_id = ""
        self.assertEqual(out_invalid_id, exp_invalid_id)

        # Non-string input
        inp_non_string = None
        out_non_string = c_processing.find_crossref_orcid(inp_non_string, test_doi)
        exp_non_string = ""
        self.assertEqual(out_non_string, exp_non_string)

        c_processing.storage_manager.delete_storage()

        # Set a valid id as invalid in storage to check that the api check is
        # avoided if the info is already in storage
        c_processing = CrossrefProcessing()
        c_processing.storage_manager.set_value("orcid:0000-0001-9759-3938", False)

        inp = '0000-0001-9759-3938'
        out = c_processing.find_crossref_orcid(inp, test_doi)
        exp = ""
        self.assertEqual(out, exp)
        c_processing.storage_manager.delete_storage()

        c_processing = CrossrefProcessing()
        c_processing.storage_manager.set_value("orcid:0000-0001-9759-3938", True)
        inp = '0000-0001-9759-3938'
        out = c_processing.find_crossref_orcid(inp, test_doi)
        exp = "orcid:0000-0001-9759-3938"
        self.assertEqual(out, exp)
        c_processing.storage_manager.delete_storage()

    def test_report_series_venue_id(self):
        crossref_processor = CrossrefProcessing(orcid_index=IOD, doi_csv=WANTED_DOIS_FOLDER, publishers_filepath=None)
        items = {'items': [{
            'DOI': '10.1007/978-3-030-00668-6_8',
            'container-title': ["troitel'stvo: nauka i obrazovanie [Construction: Science and Education]"],
            'ISSN': '2305-5502',
            'type': 'report-series'
        }]}
        crossref_processor = CrossrefProcessing(orcid_index=None, doi_csv=None, publishers_filepath=PUBLISHERS_MAPPING)
        output = list()
        for item in items['items']:
            output.append(crossref_processor.csv_creator(item))
        expected_output = [{'id': 'doi:10.1007/978-3-030-00668-6_8', 'title': '', 'author': '', 'pub_date': '', 'venue': "troitel'stvo: nauka i obrazovanie [Construction: Science and Education] [issn:2305-5502]", 'volume': '', 'issue': '', 'page': '', 'type': 'report series', 'publisher': '', 'editor': ''}]
        self.assertEqual(output, expected_output)

    def test_report_series_br_id(self):
        crossref_processor = CrossrefProcessing(orcid_index=IOD, doi_csv=WANTED_DOIS_FOLDER, publishers_filepath=None)
        items = {'items': [{
            'DOI': '10.1007/978-3-030-00668-6_8',
            'container-title': [],
            'ISSN': '2305-5502',
            'type': 'report-series'
        }]}
        crossref_processor = CrossrefProcessing(orcid_index=None, doi_csv=None, publishers_filepath=PUBLISHERS_MAPPING)
        output = list()
        for item in items['items']:
            output.append(crossref_processor.csv_creator(item))
        expected_output = [{'id': 'doi:10.1007/978-3-030-00668-6_8 issn:2305-5502', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': 'report series', 'publisher': '', 'editor': ''}]
        self.assertEqual(output, expected_output)

    def test_get_agents_strings_list(self):
        authors_list = [
            {
                'given': 'Myung-Hee',
                'family': 'Kim',
                'affiliation': [],
                "role": "author"
            },
            {
                'given': 'Jin-Seon',
                'family': 'Seo',
                'affiliation': [],
                "role": "author"
            },
            {
                'given': 'Mi-Kyeong',
                'family': 'Choi',
                'affiliation': [],
                "role": "author"
            },
            {
                'given': 'Eun-Young',
                'family': 'Kim',
                'affiliation': [],
                "role": "author"
            }
        ]
        crossref_processor = CrossrefProcessing(IOD, WANTED_DOIS_FOLDER)
        authors_strings_list, _ = crossref_processor.get_agents_strings_list('10.9799/ksfan.2012.25.1.105',
                                                                             authors_list)
        expected_authors_list = ['Kim, Myung-Hee', 'Seo, Jin-Seon', 'Choi, Mi-Kyeong [orcid:0000-0002-6227-4053]',
                                 'Kim, Eun-Young']
        self.assertEqual(authors_strings_list, expected_authors_list)

    def test_get_agents_strings_list_same_family(self):
        # Two authors have the same family name and the same given name initials
        authors_list = [
            {
                'given': 'Mi-Kyeong',
                'family': 'Choi',
                'affiliation': [],
                "role": "author"
            },
            {
                'given': 'Mi-Hong',
                'family': 'Choi',
                'affiliation': [],
                "role": "author"
            }
        ]
        crossref_processor = CrossrefProcessing(IOD, WANTED_DOIS_FOLDER)
        authors_strings_list, _ = crossref_processor.get_agents_strings_list('10.9799/ksfan.2012.25.1.105',
                                                                             authors_list)
        expected_authors_list = ['Choi, Mi-Kyeong [orcid:0000-0002-6227-4053]', 'Choi, Mi-Hong']
        self.assertEqual(authors_strings_list, expected_authors_list)

    def test_get_agents_strings_list_homonyms(self):
        # Two authors have the same family name and the same given name
        authors_list = [
            {
                'given': 'Mi-Kyeong',
                'family': 'Choi',
                'affiliation': [],
                "role": "author"
            },
            {
                'given': 'Mi-Kyeong',
                'family': 'Choi',
                'affiliation': [],
                "role": "author"
            }
        ]
        crossref_processor = CrossrefProcessing(IOD, WANTED_DOIS_FOLDER)
        authors_strings_list, _ = crossref_processor.get_agents_strings_list('10.9799/ksfan.2012.25.1.105',
                                                                             authors_list)
        expected_authors_list = ['Choi, Mi-Kyeong', 'Choi, Mi-Kyeong']
        self.assertEqual(authors_strings_list, expected_authors_list)

    def test_get_agents_strings_list_inverted_names(self):
        # One author with an ORCID has as a name the surname of another
        authors_list = [
            {
                'given': 'Choi',
                'family': 'Mi-Kyeong',
                'affiliation': [],
                "role": "author"
            },
            {
                'given': 'Mi-Hong',
                'family': 'Choi',
                'affiliation': [],
                "role": "author"
            }
        ]
        crossref_processor = CrossrefProcessing(IOD, WANTED_DOIS_FOLDER)
        authors_strings_list, _ = crossref_processor.get_agents_strings_list('10.9799/ksfan.2012.25.1.105',
                                                                             authors_list)
        expected_authors_list = ['Mi-Kyeong, Choi', 'Choi, Mi-Hong']
        self.assertEqual(authors_strings_list, expected_authors_list)

    def test_get_agents_strings_list_overlapping_surnames(self):
        # The surname of one author is included in the surname of another.
        authors_list = [
            {
                "given": "Puvaneswari",
                "family": "Paravamsivam",
                "sequence": "first",
                "affiliation": [],
                "role": "author"
            },
            {
                "given": "Chua Kek",
                "family": "Heng",
                "sequence": "additional",
                "affiliation": [],
                "role": "author"
            },
            {
                "given": "Sri Nurestri Abdul",
                "family": "Malek",
                "sequence": "additional",
                "affiliation": [],
                "role": "author"
            },
            {
                "given": "Vikineswary",
                "family": "Sabaratnam",
                "sequence": "additional",
                "affiliation": [],
                "role": "author"
            },
            {
                "given": "Ravishankar Ram",
                "family": "M",
                "sequence": "additional",
                "affiliation": [],
                "role": "author"
            },
            {
                "given": "Sri Nurestri Abdul",
                "family": "Malek",
                "sequence": "additional",
                "affiliation": [],
                "role": "editor"
            },
            {
                "given": "Umah Rani",
                "family": "Kuppusamy",
                "sequence": "additional",
                "affiliation": [],
                "role": "author"
            }
        ]
        crossref_processor = CrossrefProcessing(None, None)
        csv_manager = CSVManager()
        csv_manager.data = {'10.9799/ksfan.2012.25.1.105': {'Malek, Sri Nurestri Abdul [0000-0001-6278-8559]'}}
        crossref_processor.orcid_index = csv_manager
        authors_strings_list, editors_strings_list = crossref_processor.get_agents_strings_list('10.9799/ksfan.2012.25.1.105', authors_list)
        expected_authors_list = ['Paravamsivam, Puvaneswari', 'Heng, Chua Kek', 'Malek, Sri Nurestri Abdul [orcid:0000-0001-6278-8559]', 'Sabaratnam, Vikineswary', 'M, Ravishankar Ram', 'Kuppusamy, Umah Rani']
        expected_editors_list = ['Malek, Sri Nurestri Abdul [orcid:0000-0001-6278-8559]']
        self.assertEqual((authors_strings_list, editors_strings_list), (expected_authors_list, expected_editors_list))

    def test_id_worker(self):
        field_issn = 'ISSN 1050-124X'
        field_isbn = ['978-1-56619-909-4']
        issn_list = list()
        isbn_list = list()
        CrossrefProcessing.id_worker(field_issn, issn_list, CrossrefProcessing.issn_worker)
        CrossrefProcessing.id_worker(field_isbn, isbn_list, CrossrefProcessing.isbn_worker)
        expected_issn_list = ['issn:1050-124X']
        expected_isbn_list = ['isbn:9781566199094']
        self.assertEqual((issn_list, isbn_list), (expected_issn_list, expected_isbn_list))

    def test_to_validated_id_list(self):
        # NOTE: in tests using the sqlite storage method it must be avoided to delete the storage
        # while using the same CrossrefProcessing() instance, otherwise the process would try to
        # store data in a filepath that has just been deleted, with no new connection created after it.

        # 2 OPTIONS: 1) instantiate CrossrefProcessing only once at the beginning and delete the
        # storage only at the end; 2) create a new CrossrefProcessing instance at every check and
        # delete the storage each time after the check is done.

        cp = CrossrefProcessing()
        # CASE_1:  is valid
        inp_1 = {'id':'doi:10.13039/100005522', 'schema':'doi'}
        out_1 = cp.to_validated_id_list(inp_1)
        exp_1 = ['doi:10.13039/100005522']
        self.assertEqual(out_1, exp_1)
        cp.storage_manager.delete_storage()

        cp = CrossrefProcessing()
        # CASE_2: is invalid
        inp_2 = {'id':'doi:10.1089/bsp.2008.002', 'schema':'doi'}
        out_2 = cp.to_validated_id_list(inp_2)
        exp_2 = []
        self.assertEqual(out_2, exp_2)

        cp = CrossrefProcessing()
        # CASE_3:  valid orcid
        inp_3 =  {'id': 'orcid:0000-0003-4082-1500', 'schema':'orcid'}
        out_3 = cp.to_validated_id_list(inp_3)
        exp_3 = ['orcid:0000-0003-4082-1500']
        self.assertEqual(out_3, exp_3)
        cp.storage_manager.delete_storage()

        cp= CrossrefProcessing()
        #CASE_4: invalid doi in self._redis_values_br
        inp_4 = {'id': 'doi:10.1089/bsp.2008.002', 'schema': 'doi'}
        cp._redis_values_br.append(inp_4['id'])
        out_4 = cp.to_validated_id_list(inp_4)
        exp_4 = ['doi:10.1089/bsp.2008.002']
        self.assertEqual(out_4, exp_4)
        value=cp.tmp_doi_m.storage_manager.get_value('doi:10.1089/bsp.2008.002')
        self.assertEqual(value, True)
        cp.storage_manager.delete_storage()


    def test_to_validated_id_list_redis(self):
        cp = CrossrefProcessing(storage_manager=RedisStorageManager(testing=True))
        # CASE_1:  is valid
        inp_1 = {'id': 'doi:10.13039/100005522', 'schema': 'doi'}
        out_1 = cp.to_validated_id_list(inp_1)
        exp_1 = ['doi:10.13039/100005522']
        self.assertEqual(out_1, exp_1)
        cp.storage_manager.delete_storage()

        cp = CrossrefProcessing(storage_manager=RedisStorageManager(testing=True))
        # CASE_2: is invalid
        inp_2 = {'id': 'doi:10.1089/bsp.2008.002', 'schema': 'doi'}
        out_2 = cp.to_validated_id_list(inp_2)
        exp_2 = []
        self.assertEqual(out_2, exp_2)

        cp = CrossrefProcessing(storage_manager=RedisStorageManager(testing=True))
        # CASE_3:  valid orcid
        inp_3 = {'id': 'orcid:0000-0003-4082-1500', 'schema': 'orcid'}
        out_3 = cp.to_validated_id_list(inp_3)
        exp_3 = ['orcid:0000-0003-4082-1500']
        self.assertEqual(out_3, exp_3)
        cp.storage_manager.delete_storage()

        cp = CrossrefProcessing(storage_manager=RedisStorageManager(testing=True))
        # CASE_4: invalid doi in self._redis_values_br
        inp_4 = {'id': 'doi:10.1089/bsp.2008.002', 'schema': 'doi'}
        cp._redis_values_br.append(inp_4['id'])
        out_4 = cp.to_validated_id_list(inp_4)
        exp_4 = ['doi:10.1089/bsp.2008.002']
        self.assertEqual(out_4, exp_4)
        value = cp.tmp_doi_m.storage_manager.get_value('doi:10.1089/bsp.2008.002')
        self.assertEqual(value, True)
        cp.storage_manager.delete_storage()

    def test_find_crossref_orcid_with_index(self):
        """Test ORCID validation using ORCID index before API validation"""
        # Setup
        test_doi = "10.1234/test123"
        test_orcid = "0000-0002-1234-5678"
        test_name = "Smith, John"
        
        # Create CrossrefProcessing instance with ORCID index
        cp = CrossrefProcessing()
        cp.orcid_index.add_value(test_doi, f"{test_name} [orcid:{test_orcid}]")
        
        # Test Case 1: ORCID found in index
        out_1 = cp.find_crossref_orcid(test_orcid, test_doi)
        exp_1 = f"orcid:{test_orcid}"
        self.assertEqual(out_1, exp_1)
        # Verify it was added to temporary storage
        self.assertTrue(cp.tmp_orcid_m.storage_manager.get_value(f"orcid:{test_orcid}"))
        
        # Test Case 2: ORCID not in index but valid via API
        out_2 = cp.find_crossref_orcid("0000-0003-4082-1500", test_doi)
        exp_2 = "orcid:0000-0003-4082-1500"
        self.assertEqual(out_2, exp_2)
        
        # Test Case 3: ORCID not in index and invalid
        out_3 = cp.find_crossref_orcid("0000-0000-0000-0000", test_doi)
        exp_3 = ""
        self.assertEqual(out_3, exp_3)
        
        # Cleanup
        cp.storage_manager.delete_storage()














