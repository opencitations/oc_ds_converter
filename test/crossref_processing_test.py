import json
import os
import unittest

from oc_ds_converter.crossref.crossref_processing import CrossrefProcessing
from oc_ds_converter.datasource.orcid_index import PublishersRedis
from oc_ds_converter.lib.csvmanager import CSVManager
from oc_ds_converter.lib.jsonmanager import load_json

TEST_DIR = os.path.join("test", "crossref_processing")
JSON_FILE = os.path.join(TEST_DIR, "0.json")
TMP_SUPPORT_MATERIAL = os.path.join(TEST_DIR, "tmp_support")
IOD = os.path.join(TEST_DIR, 'iod')
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
        c_processing = CrossrefProcessing(testing=True)
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
        br_valid_list = c_processing.get_redis_validity_list(br, "br")
        exp_br_valid_list = []
        self.assertEqual(br_valid_list, exp_br_valid_list)
        c_processing.storage_manager.delete_storage()

    def test_get_redis_validity_list_redis(self):
        c_processing = CrossrefProcessing(testing=True)
        br = {'doi:10.2105/ajph.2006.101626', 'doi:10.1001/jama.299.12.1471',
              'doi:10.1177/003335490812300219', 'doi:10.1089/bsp.2008.0020',
              'doi:10.1097/01.ccm.0000151067.76074.21', 'doi:10.1177/003335490912400218',
              'doi:10.1097/dmp.0b013e31817196bf', 'doi:10.1056/nejmsa021807',
              'doi:10.1097/dmp.0b013e31819d977c', 'doi:10.1097/dmp.0b013e31819f1ae2',
              'doi:10.1097/dmp.0b013e318194898d', 'doi:10.1378/chest.07-2693',
              'doi:10.1016/s0196-0644(99)70224-6', 'doi:10.1097/01.ccm.0000151072.17826.72',
              'doi:10.1097/01.bcr.0000155527.76205.a2', 'doi:10.2105/ajph.2009.162677'}
        ra = {'orcid:0000-0002-8090-6886', 'orcid:0000-0002-6491-0754'}
        br_valid_list = c_processing.get_redis_validity_list(br, "br")
        exp_br_valid_list = []
        ra_valid_list = c_processing.get_redis_validity_list(ra, "ra")
        self.assertEqual(br_valid_list, exp_br_valid_list)
        exp_ra_valid_list = []
        self.assertEqual(ra_valid_list, exp_ra_valid_list)
        c_processing.storage_manager.delete_storage()

    def test_get_redis_validity_dict_w_fakeredis_db_values_sqlite(self):
        c_processing = CrossrefProcessing()
        c_processing.BR_redis.sadd('doi:10.2105/ajph.2006.101626', "omid:1")
        c_processing.RA_redis.sadd('orcid:0000-0002-8090-6886', "omid:2")

        br = {'doi:10.2105/ajph.2006.101626', 'doi:10.1001/jama.299.12.1471',
              'doi:10.1177/003335490812300219'}
        ra = {'orcid:0000-0002-8090-6886', 'orcid:0000-0002-6491-0754'}

        br_validity_dict = c_processing.get_redis_validity_list(br, "br")
        exp_br_valid_list = ['doi:10.2105/ajph.2006.101626']
        ra_validity_dict = c_processing.get_redis_validity_list(ra, "ra")
        exp_ra_valid_list = ['orcid:0000-0002-8090-6886']
        self.assertEqual(br_validity_dict, exp_br_valid_list)
        self.assertEqual(ra_validity_dict, exp_ra_valid_list)

        c_processing.storage_manager.delete_storage()

        c_processing.BR_redis.delete('doi:10.2105/ajph.2006.101626')
        c_processing.RA_redis.delete('orcid:0000-0002-8090-6886')

    def test_get_redis_validity_dict_w_fakeredis_db_values_redis(self):
        c_processing = CrossrefProcessing(testing=True)
        c_processing.BR_redis.sadd('doi:10.2105/ajph.2006.101626', "omid:1")
        c_processing.RA_redis.sadd('orcid:0000-0002-8090-6886', "omid:2")

        br = {'doi:10.2105/ajph.2006.101626', 'doi:10.1001/jama.299.12.1471',
              'doi:10.1177/003335490812300219'}
        ra = {'orcid:0000-0002-8090-6886', 'orcid:0000-0002-6491-0754'}

        br_validity_dict = c_processing.get_redis_validity_list(br, "br")
        exp_br_valid_list = ['doi:10.2105/ajph.2006.101626']
        ra_validity_dict = c_processing.get_redis_validity_list(ra, "ra")
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
        c_processing = CrossrefProcessing(testing=True)
        validate_as_none = c_processing.validated_as({"schema": "doi", "identifier": "doi:10.1001/10-v4n2-hsf10003"})
        self.assertEqual(validate_as_none, None)
        c_processing.storage_manager.delete_storage()

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
        valid_doi_not_in_db = {"identifier":"doi:10.1001/2012.jama.10158", "schema":"doi"}
        valid_doi_in_db = {"identifier":"doi:10.1001/2012.jama.10368", "schema":"doi"}
        invalid_doi_in_db = {"identifier":"doi:10.1001/2012.jama.1036", "schema":"doi"}

        # New class instance and set values directly on the DOIManager's storage_manager
        c_processing_redis = CrossrefProcessing(testing=True)
        c_processing_redis.doi_m.storage_manager.set_value(valid_doi_in_db["identifier"], True)
        c_processing_redis.doi_m.storage_manager.set_value(invalid_doi_in_db["identifier"], False)
        validated_as_True = c_processing_redis.validated_as(valid_doi_in_db)
        validated_as_False = c_processing_redis.validated_as(invalid_doi_in_db)
        not_validated = c_processing_redis.validated_as(valid_doi_not_in_db)

        self.assertEqual(validated_as_True, True)
        self.assertEqual(validated_as_False, False)
        self.assertEqual(not_validated, None)

        c_processing_redis.doi_m.storage_manager.delete_storage()

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
        valid_doi_not_in_db = {"identifier": "doi:10.1001/2012.jama.10158", "schema": "doi"}
        valid_doi_in_db = {"identifier": "doi:10.1001/2012.jama.10368", "schema": "doi"}
        invalid_doi_in_db = {"identifier": "doi:10.1001/2012.jama.1036", "schema": "doi"}

        c_processing = CrossrefProcessing(testing=True)
        c_processing.doi_m.storage_manager.set_value(valid_doi_in_db["identifier"], True)
        c_processing.doi_m.storage_manager.set_value(invalid_doi_in_db["identifier"], False)
        validated_as_True = c_processing.validated_as(valid_doi_in_db)
        validated_as_False = c_processing.validated_as(invalid_doi_in_db)
        not_validated = c_processing.validated_as(valid_doi_not_in_db)

        self.assertEqual(validated_as_True, True)
        self.assertEqual(validated_as_False, False)
        self.assertEqual(not_validated, None)

        c_processing.doi_m.storage_manager.delete_storage()

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

        valid_doi_not_in_db = {"identifier": "doi:10.1001/2012.jama.10158", "schema": "doi"}
        valid_doi_in_db = {"identifier": "doi:10.1001/2012.jama.10368", "schema": "doi"}
        invalid_doi_in_db = {"identifier": "doi:10.1001/2012.jama.1036", "schema": "doi"}

        # New class instance and set values directly on the DOIManager's storage_manager
        c_processing_redis = CrossrefProcessing(testing=True)
        c_processing_redis.doi_m.storage_manager.set_value(valid_doi_in_db["identifier"], True)
        c_processing_redis.doi_m.storage_manager.set_value(invalid_doi_in_db["identifier"], False)
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
        assert issn_man_exp is not None
        assert issn_man_exp_2 is not None
        self.assertTrue(issn_man_exp.is_valid(issn_id))
        self.assertTrue(issn_man_exp_2.is_valid(issn_id))

    def test_csv_creator(self):
        c_processing = CrossrefProcessing(orcid_index=IOD, publishers_filepath=None)
        data = load_json(DATA, None)  # type: ignore[arg-type]
        assert data is not None
        dois_to_prefetch = [item.get("DOI") for item in data['items'] if item.get("DOI")]
        c_processing.prefetch_doi_orcid_index(dois_to_prefetch)
        output = list()
        for item in data['items']:
            tabular_data = c_processing.csv_creator(item)
            if tabular_data:
                output.append(tabular_data)
        self.assertEqual(len(output), 11)
        output_ids = [row['id'] for row in output]
        self.assertIn('doi:10.47886/9789251092637.ch7', output_ids)
        self.assertIn('doi:10.9799/ksfan.2012.25.1.069', output_ids)
        self.assertIn('doi:10.9799/ksfan.2012.25.1.105', output_ids)
        first_item = next(row for row in output if row['id'] == 'doi:10.47886/9789251092637.ch7')
        self.assertEqual(first_item['type'], 'book chapter')
        self.assertEqual(first_item['publisher'], 'American Fisheries Society [crossref:460]')

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
        crossref_processor = CrossrefProcessing(orcid_index=None, publishers_filepath=PUBLISHERS_MAPPING)
        pages = crossref_processor.get_crossref_pages(item)
        self.assertEqual(pages, '469-476')

    def test_get_pages_right_letter(self):
        item = {
            'page': 'G22'
        }
        crossref_processor = CrossrefProcessing(orcid_index=None, publishers_filepath=PUBLISHERS_MAPPING)
        pages = crossref_processor.get_crossref_pages(item)
        self.assertEqual(pages, 'G22-G22')

    def test_get_pages_wrong_letter(self):
        item = {
            'page': '583b-584'
        }
        crossref_processor = CrossrefProcessing(orcid_index=None, publishers_filepath=PUBLISHERS_MAPPING)
        pages = crossref_processor.get_crossref_pages(item)
        self.assertEqual(pages, '583-584')

    def test_get_pages_roman_letters(self):
        item = {
            'page': 'iv-l'
        }
        crossref_processor = CrossrefProcessing(orcid_index=None, publishers_filepath=PUBLISHERS_MAPPING)
        pages = crossref_processor.get_crossref_pages(item)
        self.assertEqual(pages, 'iv-l')

    def test_get_pages_non_roman_letters(self):
        item = {
            'page': 'kj-hh'
        }
        crossref_processor = CrossrefProcessing(orcid_index=None, publishers_filepath=PUBLISHERS_MAPPING)
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
            'DOI': '10.47886/9789251092637.ch7',
            'prefix': '10.47886',
            'member': '460'
        }
        doi = '10.47886/9789251092637.ch7'
        crossref_processor = CrossrefProcessing(orcid_index=None, publishers_filepath=PUBLISHERS_MAPPING)
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
        crossref_processor = CrossrefProcessing(orcid_index=None, publishers_filepath=PUBLISHERS_MAPPING)
        publisher_name = crossref_processor.get_publisher_name(doi, item)
        self.assertEqual(publisher_name, 'American Fisheries Society [crossref:460]')

    def test_get_publisher_name_redis_by_member(self):
        publishers_redis = PublishersRedis(testing=True)
        publishers_redis.set_publisher("460", "American Fisheries Society", {"10.47886"})

        item = {
            'publisher': 'American Fisheries Society',
            'DOI': '10.47886/9789251092637.ch7',
            'prefix': '10.47886',
            'member': '460'
        }
        doi = '10.47886/9789251092637.ch7'
        crossref_processor = CrossrefProcessing(
            orcid_index=None, publishers_filepath=None,
            use_redis_publishers=True, testing=True
        )
        crossref_processor._publishers_redis = publishers_redis
        publisher_name = crossref_processor.get_publisher_name(doi, item)
        self.assertEqual(publisher_name, 'American Fisheries Society [crossref:460]')

    def test_get_publisher_name_redis_by_prefix(self):
        publishers_redis = PublishersRedis(testing=True)
        publishers_redis.set_publisher("460", "American Fisheries Society", {"10.47886"})

        item = {
            'publisher': 'American Fisheries Society',
            'DOI': '10.47886/9789251092637.ch7',
            'prefix': '10.47886'
        }
        doi = '10.47886/9789251092637.ch7'
        crossref_processor = CrossrefProcessing(
            orcid_index=None, publishers_filepath=None,
            use_redis_publishers=True, testing=True
        )
        crossref_processor._publishers_redis = publishers_redis
        publisher_name = crossref_processor.get_publisher_name(doi, item)
        self.assertEqual(publisher_name, 'American Fisheries Society [crossref:460]')

    def test_get_publisher_name_redis_not_found(self):
        publishers_redis = PublishersRedis(testing=True)

        item = {
            'publisher': 'Unknown Publisher',
            'DOI': '10.9999/unknown',
            'prefix': '10.9999'
        }
        doi = '10.9999/unknown'
        crossref_processor = CrossrefProcessing(
            orcid_index=None, publishers_filepath=None,
            use_redis_publishers=True, testing=True
        )
        crossref_processor._publishers_redis = publishers_redis
        publisher_name = crossref_processor.get_publisher_name(doi, item)
        self.assertEqual(publisher_name, 'Unknown Publisher')

    def test_get_venue_name(self):
        item = {
            'container-title': ['Cerebrospinal Fluid [Working Title]'],
        }
        row = {'id': '', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': 'journal article', 'publisher': '', 'editor': ''}
        crossref_processor = CrossrefProcessing(orcid_index=None, publishers_filepath=PUBLISHERS_MAPPING)
        venue_name = crossref_processor.get_venue_name(item, row)
        self.assertEqual(venue_name, 'Cerebrospinal Fluid [Working Title]')

    def test_get_venue_name_with_ISSN(self):
        item = {
            "container-title": ["Disaster Medicine and Public Health Preparedness"],
            "ISSN": ["1935-7893", "1938-744X"]
        }
        row = {'id': '', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '',
               'type': 'journal article', 'publisher': '', 'editor': ''}
        crossref_processor = CrossrefProcessing(orcid_index=None, publishers_filepath=PUBLISHERS_MAPPING)
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

        c_processing.orcid_m.storage_manager.delete_storage()

        # Set a valid id as invalid in storage to check that the api check is
        # avoided if the info is already in storage
        c_processing = CrossrefProcessing(testing=True)
        c_processing.orcid_m.storage_manager.set_value("orcid:0000-0001-9759-3938", False)

        inp = '0000-0001-9759-3938'
        out = c_processing.find_crossref_orcid(inp, test_doi)
        exp = ""
        self.assertEqual(out, exp)
        c_processing.orcid_m.storage_manager.delete_storage()

        c_processing = CrossrefProcessing(testing=True)
        c_processing.orcid_m.storage_manager.set_value("orcid:0000-0001-9759-3938", True)
        inp = '0000-0001-9759-3938'
        out = c_processing.find_crossref_orcid(inp, test_doi)
        exp = "orcid:0000-0001-9759-3938"
        self.assertEqual(out, exp)
        c_processing.orcid_m.storage_manager.delete_storage()

    def test_report_series_venue_id(self):
        crossref_processor = CrossrefProcessing(orcid_index=IOD, publishers_filepath=None)
        items = {'items': [{
            'DOI': '10.1007/978-3-030-00668-6_8',
            'container-title': ["troitel'stvo: nauka i obrazovanie [Construction: Science and Education]"],
            'ISSN': '2305-5502',
            'type': 'report-series'
        }]}
        crossref_processor = CrossrefProcessing(orcid_index=None, publishers_filepath=PUBLISHERS_MAPPING)
        output = list()
        for item in items['items']:
            output.append(crossref_processor.csv_creator(item))
        expected_output = [{'id': 'doi:10.1007/978-3-030-00668-6_8', 'title': '', 'author': '', 'pub_date': '', 'venue': "troitel'stvo: nauka i obrazovanie [Construction: Science and Education] [issn:2305-5502]", 'volume': '', 'issue': '', 'page': '', 'type': 'report series', 'publisher': '', 'editor': ''}]
        self.assertEqual(output, expected_output)

    def test_report_series_br_id(self):
        crossref_processor = CrossrefProcessing(orcid_index=IOD, publishers_filepath=None)
        items = {'items': [{
            'DOI': '10.1007/978-3-030-00668-6_8',
            'container-title': [],
            'ISSN': '2305-5502',
            'type': 'report-series'
        }]}
        crossref_processor = CrossrefProcessing(orcid_index=None, publishers_filepath=PUBLISHERS_MAPPING)
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
        crossref_processor = CrossrefProcessing(IOD)
        crossref_processor.prefetch_doi_orcid_index(['10.9799/ksfan.2012.25.1.105'])
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
        crossref_processor = CrossrefProcessing(IOD)
        crossref_processor.prefetch_doi_orcid_index(['10.9799/ksfan.2012.25.1.105'])
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
        crossref_processor = CrossrefProcessing(IOD)
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
        crossref_processor = CrossrefProcessing(IOD)
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
        crossref_processor = CrossrefProcessing(None)
        csv_manager = CSVManager()
        csv_manager.data = {'doi:10.9799/ksfan.2012.25.1.105': {'Malek, Sri Nurestri Abdul [0000-0001-6278-8559]'}}
        crossref_processor.orcid_index = csv_manager
        crossref_processor.prefetch_doi_orcid_index(['10.9799/ksfan.2012.25.1.105'])
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
        cp = CrossrefProcessing(testing=True)
        # CASE_1:  is valid
        inp_1 = {'id':'doi:10.13039/100005522', 'schema':'doi'}
        out_1 = cp.to_validated_id_list(inp_1)
        exp_1 = ['doi:10.13039/100005522']
        self.assertEqual(out_1, exp_1)
        cp.doi_m.storage_manager.delete_storage()

        cp = CrossrefProcessing(testing=True)
        # CASE_2: is invalid
        inp_2 = {'id':'doi:10.1089/bsp.2008.002', 'schema':'doi'}
        out_2 = cp.to_validated_id_list(inp_2)
        exp_2 = []
        self.assertEqual(out_2, exp_2)

        cp = CrossrefProcessing(testing=True)
        # CASE_3:  valid orcid
        inp_3 =  {'id': 'orcid:0000-0003-4082-1500', 'schema':'orcid'}
        out_3 = cp.to_validated_id_list(inp_3)
        exp_3 = ['orcid:0000-0003-4082-1500']
        self.assertEqual(out_3, exp_3)
        cp.orcid_m.storage_manager.delete_storage()

        cp = CrossrefProcessing(testing=True)
        #CASE_4: invalid doi in self._redis_values_br
        inp_4 = {'id': 'doi:10.1089/bsp.2008.002', 'schema': 'doi'}
        cp._redis_values_br.append(inp_4['id'])
        out_4 = cp.to_validated_id_list(inp_4)
        exp_4 = ['doi:10.1089/bsp.2008.002']
        self.assertEqual(out_4, exp_4)
        value=cp.tmp_doi_m.storage_manager.get_value('doi:10.1089/bsp.2008.002')
        self.assertEqual(value, True)
        cp.doi_m.storage_manager.delete_storage()


    def test_to_validated_id_list_redis(self):
        cp = CrossrefProcessing(testing=True)
        # CASE_1:  is valid
        inp_1 = {'id': 'doi:10.13039/100005522', 'schema': 'doi'}
        out_1 = cp.to_validated_id_list(inp_1)
        exp_1 = ['doi:10.13039/100005522']
        self.assertEqual(out_1, exp_1)
        cp.doi_m.storage_manager.delete_storage()

        cp = CrossrefProcessing(testing=True)
        # CASE_2: is invalid
        inp_2 = {'id': 'doi:10.1089/bsp.2008.002', 'schema': 'doi'}
        out_2 = cp.to_validated_id_list(inp_2)
        exp_2 = []
        self.assertEqual(out_2, exp_2)

        cp = CrossrefProcessing(testing=True)
        # CASE_3:  valid orcid
        inp_3 = {'id': 'orcid:0000-0003-4082-1500', 'schema': 'orcid'}
        out_3 = cp.to_validated_id_list(inp_3)
        exp_3 = ['orcid:0000-0003-4082-1500']
        self.assertEqual(out_3, exp_3)
        cp.orcid_m.storage_manager.delete_storage()

        cp = CrossrefProcessing(testing=True)
        # CASE_4: invalid doi in self._redis_values_br
        inp_4 = {'id': 'doi:10.1089/bsp.2008.002', 'schema': 'doi'}
        cp._redis_values_br.append(inp_4['id'])
        out_4 = cp.to_validated_id_list(inp_4)
        exp_4 = ['doi:10.1089/bsp.2008.002']
        self.assertEqual(out_4, exp_4)
        value = cp.tmp_doi_m.storage_manager.get_value('doi:10.1089/bsp.2008.002')
        self.assertEqual(value, True)
        cp.doi_m.storage_manager.delete_storage()

    def test_find_crossref_orcid_with_index(self):
        """Test ORCID validation using ORCID index before API validation"""
        # Setup
        test_doi = "10.1234/test123"
        test_doi_prefixed = "doi:10.1234/test123"
        test_orcid = "0000-0002-1234-5678"
        test_name = "Smith, John"

        # Create CrossrefProcessing instance with ORCID index
        cp = CrossrefProcessing(testing=True)
        cp.orcid_index.add_value(test_doi_prefixed, f"{test_name} [orcid:{test_orcid}]")  # type: ignore[attr-defined]
        cp.prefetch_doi_orcid_index([test_doi])

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
        cp.orcid_m.storage_manager.delete_storage()

    def test_find_crossref_orcid_api_disabled_not_in_index(self):
        """API OFF + empty index: a syntactically valid ORCID must NOT be resolved."""
        cp = CrossrefProcessing(use_orcid_api=False, testing=True)
        test_doi = "10.9999/noindex"
        candidate = "0000-0003-4082-1500"  # syntactically valid

        out = cp.find_crossref_orcid(candidate, test_doi)
        self.assertEqual(out, "")
        # Must NOT be written to tmp storage
        self.assertIsNone(cp.tmp_orcid_m.storage_manager.get_value(f"orcid:{candidate}"))

        cp.orcid_m.storage_manager.delete_storage()

    def test_find_crossref_orcid_api_disabled_from_index(self):
        """API OFF + present in DOI→ORCID index: must resolve and be saved in tmp storage."""
        cp = CrossrefProcessing(use_orcid_api=False, testing=True)
        test_doi = "10.1234/test"
        test_doi_prefixed = "doi:10.1234/test"
        test_orcid = "0000-0002-1234-5678"
        test_name = "Smith, John"

        cp.orcid_index.add_value(test_doi_prefixed, f"{test_name} [orcid:{test_orcid}]")  # type: ignore[attr-defined]
        cp.prefetch_doi_orcid_index([test_doi])

        out = cp.find_crossref_orcid(test_orcid, test_doi)
        self.assertEqual(out, f"orcid:{test_orcid}")
        self.assertTrue(cp.tmp_orcid_m.storage_manager.get_value(f"orcid:{test_orcid}"))

        cp.orcid_m.storage_manager.delete_storage()

    def test_find_crossref_orcid_api_disabled_in_storage(self):
        """API OFF + ORCID already valid in persistent storage: must be accepted."""
        cp = CrossrefProcessing(use_orcid_api=False, testing=True)
        oid = "orcid:0000-0003-4082-1500"
        cp.orcid_m.storage_manager.set_value(oid, True)  # mark valid
        out = cp.find_crossref_orcid(oid.split(":")[1], "10.9999/any")
        self.assertEqual(out, oid)
        cp.orcid_m.storage_manager.delete_storage()

    def test_find_crossref_orcid_api_disabled_from_redis_snapshot(self):
        """API OFF + empty index/storage, but ORCID present in Redis snapshot: accept and seed tmp storage."""
        cp = CrossrefProcessing(use_orcid_api=False, testing=True)
        oid = "orcid:0000-0003-4082-1500"
        cp.update_redis_values(br=[], ra=[oid])  # emulate per-chunk snapshot

        out = cp.find_crossref_orcid(oid.split(":")[1], "10.9999/noindex")
        self.assertEqual(out, oid)
        self.assertTrue(cp.tmp_orcid_m.storage_manager.get_value(oid))
        cp.orcid_m.storage_manager.delete_storage()

    def test_find_crossref_orcid_api_enabled_invalid_in_storage(self):
        """API ON + ORCID explicitly invalid in storage: reject immediately (no API/index)."""
        cp = CrossrefProcessing(use_orcid_api=True, testing=True)
        oid = "orcid:0000-0002-9286-2630"
        cp.orcid_m.storage_manager.set_value(oid, False)
        out = cp.find_crossref_orcid(oid.split(":")[1], "10.9999/anything")
        self.assertEqual(out, "")
        cp.orcid_m.storage_manager.delete_storage()

    def test_find_crossref_orcid_api_enabled_from_redis_snapshot(self):
        """API ON + empty storage/index, but ORCID present in Redis snapshot: accept without API call."""
        cp = CrossrefProcessing(use_orcid_api=True, testing=True)
        oid = "orcid:0000-0003-4082-1500"
        cp.update_redis_values(br=[], ra=[oid])

        out = cp.find_crossref_orcid(oid.split(":")[1], "10.9999/noindex")
        self.assertEqual(out, oid)
        self.assertTrue(cp.tmp_orcid_m.storage_manager.get_value(oid))
        cp.orcid_m.storage_manager.delete_storage()

    def test_get_agents_strings_list_api_disabled_no_index(self):
        """API OFF + empty index: ORCIDs provided in agent dict MUST NOT be appended to the author string."""
        agents_list = [
            {
                "given": "Jane",
                "family": "Doe",
                "role": "author",
                "ORCID": "https://orcid.org/0000-0003-4082-1500",  # present in metadata
            }
        ]
        cp = CrossrefProcessing(use_orcid_api=False, testing=True)
        authors_strings, editors_strings = cp.get_agents_strings_list("10.9999/noindex", agents_list)
        self.assertEqual(authors_strings, ["Doe, Jane"])  # no [orcid:...] tag
        self.assertEqual(editors_strings, [])
        cp.orcid_m.storage_manager.delete_storage()

    def test_get_agents_strings_list_api_disabled_index_requires_prefixed_doi(self):
        """
        API OFF + indice DOI→ORCID popolato con chiave DOI prefissata (doi:...).
        Il DOI passato a get_agents_strings_list è senza prefisso: la funzione deve
        normalizzarlo prima di interrogare l'indice, altrimenti l'ORCID non viene trovato.
        """
        cp = CrossrefProcessing(use_orcid_api=False, testing=True)

        # Indice popolato con DOI **prefissato**
        doi_pref = "doi:10.1234/test-idx"
        test_orcid = "0000-0002-9999-8888"
        cp.orcid_index.add_value(doi_pref, f"Smith, John [orcid:{test_orcid}]")  # type: ignore[attr-defined]
        cp.prefetch_doi_orcid_index(["10.1234/test-idx"])

        # Autore senza ORCID in metadati; DOI passato **senza prefisso**
        agents = [{
            "given": "John",
            "family": "Smith",
            "role": "author"
        }]

        authors, editors = cp.get_agents_strings_list("10.1234/test-idx", agents)
        # Deve risolvere via indice e apporre il tag [orcid:...]
        self.assertEqual(authors, ["Smith, John [orcid:0000-0002-9999-8888]"])
        self.assertEqual(editors, [])
        cp.orcid_m.storage_manager.delete_storage()

    def test_find_crossref_orcid_api_disabled_redis_snapshot_unprefixed_orcid(self):
        """
        API OFF + indice vuoto + storage vuoto, ma Redis snapshot contiene ORCID **senza prefisso**.
        La funzione deve riconoscerlo (normalizzando) e validarlo.
        """
        cp = CrossrefProcessing(use_orcid_api=False, testing=True)

        # Redis snapshot con ORCID **senza prefisso**
        raw_orcid = "0000-0003-4082-1500"
        cp.update_redis_values(br=[], ra=[raw_orcid])

        out = cp.find_crossref_orcid(raw_orcid, "10.9999/noindex")
        self.assertEqual(out, f"orcid:{raw_orcid}")
        self.assertTrue(cp.tmp_orcid_m.storage_manager.get_value(f"orcid:{raw_orcid}"))
        cp.orcid_m.storage_manager.delete_storage()

    def test_update_redis_values_normalizes_inputs(self):
        """
        update_redis_values deve normalizzare sempre:
        - DOI → con prefisso 'doi:'
        - ORCID → con prefisso 'orcid:'
        ed eliminare voci non normalizzabili.
        """
        cp = CrossrefProcessing(testing=True)

        cp.update_redis_values(
            br=["10.1001/jama.299.12.1471", "doi:10.2105/ajph.2006.101626", "xxx-bad"],
            ra=["0000-0002-1234-5678", "orcid:0000-0003-4082-1500", "bad-orcid"]
        )

        # Tutti normalizzati (e 'bad' scartati)
        self.assertIn("doi:10.1001/jama.299.12.1471", cp._redis_values_br)
        self.assertIn("doi:10.2105/ajph.2006.101626", cp._redis_values_br)
        self.assertNotIn("xxx-bad", cp._redis_values_br)

        self.assertIn("orcid:0000-0002-1234-5678", cp._redis_values_ra)
        self.assertIn("orcid:0000-0003-4082-1500", cp._redis_values_ra)
        self.assertNotIn("bad-orcid", cp._redis_values_ra)
        cp.storage_manager.delete_storage()


def test_validated_as_with_storage_manager(storage_manager):
    valid_doi_not_in_db = {"identifier": "doi:10.1001/2012.jama.10158", "schema": "doi"}
    valid_doi_in_db = {"identifier": "doi:10.1001/2012.jama.10368", "schema": "doi"}
    invalid_doi_in_db = {"identifier": "doi:10.1001/2012.jama.1036", "schema": "doi"}

    c_processing = CrossrefProcessing(storage_manager=storage_manager, testing=True)
    c_processing.doi_m.storage_manager.set_value(valid_doi_in_db["identifier"], True)
    c_processing.doi_m.storage_manager.set_value(invalid_doi_in_db["identifier"], False)

    assert c_processing.validated_as(valid_doi_in_db) is True
    assert c_processing.validated_as(invalid_doi_in_db) is False
    assert c_processing.validated_as(valid_doi_not_in_db) is None


class TestCrossrefProcessingWithMockedAPI(unittest.TestCase):
    """Integration tests using mocked Crossref API responses from conftest.py."""

    def test_csv_creator_nature_article(self):
        """Test with Nature article from mocked API (doi:10.1038/nature12373)."""
        item = {
            "DOI": "10.1038/nature12373",
            "type": "journal-article",
            "title": ["Nanometre-scale thermometry in a living cell"],
            "author": [
                {"given": "G.", "family": "Kucsko", "sequence": "first"},
                {"given": "P. C.", "family": "Maurer", "sequence": "additional"},
                {"given": "M. D.", "family": "Lukin", "sequence": "additional"}
            ],
            "container-title": ["Nature"],
            "volume": "500",
            "issue": "7460",
            "page": "54-58",
            "issued": {"date-parts": [[2013, 7, 31]]},
            "ISSN": ["0028-0836", "1476-4687"],
            "publisher": "Springer Science and Business Media LLC",
            "member": "297",
            "prefix": "10.1038"
        }
        processor = CrossrefProcessing(testing=True)
        row = processor.csv_creator(item)

        expected = {
            'id': 'doi:10.1038/nature12373',
            'title': 'Nanometre-scale thermometry in a living cell',
            'author': 'Kucsko, G.; Maurer, P. C.; Lukin, M. D.',
            'pub_date': '2013-7-31',
            'venue': 'Nature [issn:0028-0836 issn:1476-4687]',
            'volume': '500',
            'issue': '7460',
            'page': '54-58',
            'type': 'journal article',
            'publisher': 'Springer Science and Business Media LLC [crossref:297]',
            'editor': ''
        }
        self.assertEqual(row, expected)
        processor.storage_manager.delete_storage()

    def test_csv_creator_plos_with_orcid_url(self):
        """Test PLOS article with ORCID in URL format from mocked API."""
        item = {
            "DOI": "10.1371/journal.pone.0284601",
            "type": "journal-article",
            "title": ["Biochemical evaluation of vaccination in rats"],
            "author": [
                {"given": "Mahsa", "family": "Teymoorzadeh", "sequence": "first"},
                {"given": "Razieh", "family": "Yazdanparast", "sequence": "additional",
                 "ORCID": "https://orcid.org/0000-0003-0530-4305", "authenticated-orcid": True}
            ],
            "container-title": ["PLOS ONE"],
            "volume": "18",
            "issue": "5",
            "page": "e0284601",
            "issued": {"date-parts": [[2023, 5, 4]]},
            "ISSN": ["1932-6203"],
            "publisher": "Public Library of Science (PLoS)"
        }
        processor = CrossrefProcessing(testing=True)
        row = processor.csv_creator(item)

        expected = {
            'id': 'doi:10.1371/journal.pone.0284601',
            'title': 'Biochemical evaluation of vaccination in rats',
            'author': 'Teymoorzadeh, Mahsa; Yazdanparast, Razieh [orcid:0000-0003-0530-4305]',
            'pub_date': '2023-5-4',
            'venue': 'PLOS ONE [issn:1932-6203]',
            'volume': '18',
            'issue': '5',
            'page': 'e0284601-e0284601',
            'type': 'journal article',
            'publisher': 'Public Library of Science (PLoS)',
            'editor': ''
        }
        self.assertEqual(row, expected)
        processor.storage_manager.delete_storage()

    def test_csv_creator_book_chapter_multiple_containers(self):
        """Test book chapter with multiple container-titles from mocked API."""
        item = {
            "DOI": "10.1007/978-3-030-00668-6_8",
            "type": "book-chapter",
            "title": ["The SPAR Ontologies"],
            "author": [
                {"given": "Silvio", "family": "Peroni", "sequence": "first"},
                {"given": "David", "family": "Shotton", "sequence": "additional"}
            ],
            "container-title": ["Lecture Notes in Computer Science", "The Semantic Web – ISWC 2018"],
            "page": "119-136",
            "issued": {"date-parts": [[2018]]},
            "ISBN": ["9783030006679", "9783030006686"],
            "publisher": "Springer International Publishing"
        }
        processor = CrossrefProcessing(testing=True)
        row = processor.csv_creator(item)

        expected = {
            'id': 'doi:10.1007/978-3-030-00668-6_8',
            'title': 'The SPAR Ontologies',
            'author': 'Peroni, Silvio; Shotton, David',
            'pub_date': '2018',
            'venue': 'Lecture Notes in Computer Science [isbn:9783030006679 isbn:9783030006686]',
            'volume': '',
            'issue': '',
            'page': '119-136',
            'type': 'book chapter',
            'publisher': 'Springer International Publishing',
            'editor': ''
        }
        self.assertEqual(row, expected)
        processor.storage_manager.delete_storage()

    def test_csv_creator_date_parts_null(self):
        """Test handling of date-parts with null value: [[null]] from mocked API."""
        item = {
            "DOI": "10.1234/null-date",
            "type": "journal-article",
            "title": ["Article with null date"],
            "issued": {"date-parts": [[None]]}
        }
        processor = CrossrefProcessing(testing=True)
        row = processor.csv_creator(item)

        expected = {
            'id': 'doi:10.1234/null-date',
            'title': 'Article with null date',
            'author': '',
            'pub_date': '',
            'venue': '',
            'volume': '',
            'issue': '',
            'page': '',
            'type': 'journal article',
            'publisher': '',
            'editor': ''
        }
        self.assertEqual(row, expected)
        processor.storage_manager.delete_storage()

    def test_csv_creator_date_parts_empty(self):
        """Test handling of date-parts as empty list: [[]] from mocked API."""
        item = {
            "DOI": "10.1234/empty-date",
            "type": "journal-article",
            "title": ["Article with empty date-parts"],
            "issued": {"date-parts": [[]]}
        }
        processor = CrossrefProcessing(testing=True)
        row = processor.csv_creator(item)

        expected = {
            'id': 'doi:10.1234/empty-date',
            'title': 'Article with empty date-parts',
            'author': '',
            'pub_date': '',
            'venue': '',
            'volume': '',
            'issue': '',
            'page': '',
            'type': 'journal article',
            'publisher': '',
            'editor': ''
        }
        self.assertEqual(row, expected)
        processor.storage_manager.delete_storage()

    def test_csv_creator_date_parts_missing(self):
        """Test handling of issued without date-parts key from mocked API."""
        item = {
            "DOI": "10.1234/no-dateparts",
            "type": "journal-article",
            "title": ["Article without date-parts key"],
            "issued": {}
        }
        processor = CrossrefProcessing(testing=True)
        row = processor.csv_creator(item)

        expected = {
            'id': 'doi:10.1234/no-dateparts',
            'title': 'Article without date-parts key',
            'author': '',
            'pub_date': '',
            'venue': '',
            'volume': '',
            'issue': '',
            'page': '',
            'type': 'journal article',
            'publisher': '',
            'editor': ''
        }
        self.assertEqual(row, expected)
        processor.storage_manager.delete_storage()

    def test_csv_creator_html_in_title(self):
        """Test HTML markup in title is cleaned (from mocked API structure)."""
        item = {
            "DOI": "10.1234/html-title",
            "type": "journal-article",
            "title": ["A study of <i>Escherichia coli</i> in <b>biofilms</b>"],
            "issued": {"date-parts": [[2024, 1, 15]]}
        }
        processor = CrossrefProcessing(testing=True)
        row = processor.csv_creator(item)

        expected = {
            'id': 'doi:10.1234/html-title',
            'title': 'A study of Escherichia coli in biofilms',
            'author': '',
            'pub_date': '2024-1-15',
            'venue': '',
            'volume': '',
            'issue': '',
            'page': '',
            'type': 'journal article',
            'publisher': '',
            'editor': ''
        }
        self.assertEqual(row, expected)
        processor.storage_manager.delete_storage()

    def test_csv_creator_with_editor(self):
        """Test article with both author and editor from mocked API structure."""
        item = {
            "DOI": "10.1234/with-editor",
            "type": "edited-book",
            "title": ["Edited volume test"],
            "author": [{"given": "John", "family": "Doe", "sequence": "first"}],
            "editor": [{"given": "Jane", "family": "Smith", "sequence": "first"}],
            "issued": {"date-parts": [[2024, 6, 20]]}
        }
        processor = CrossrefProcessing(testing=True)
        row = processor.csv_creator(item)

        expected = {
            'id': 'doi:10.1234/with-editor',
            'title': 'Edited volume test',
            'author': 'Doe, John',
            'pub_date': '2024-6-20',
            'venue': '',
            'volume': '',
            'issue': '',
            'page': '',
            'type': 'edited book',
            'publisher': '',
            'editor': 'Smith, Jane'
        }
        self.assertEqual(row, expected)
        processor.storage_manager.delete_storage()

    def test_csv_creator_no_inplace_modification(self):
        """Test that csv_creator does not modify the input item dict."""
        item = {
            "DOI": "10.1234/with-editor",
            "type": "edited-book",
            "title": ["Edited volume test"],
            "author": [{"given": "John", "family": "Doe", "sequence": "first"}],
            "editor": [{"given": "Jane", "family": "Smith", "sequence": "first"}],
            "issued": {"date-parts": [[2024, 6, 20]]}
        }
        original_author = {"given": "John", "family": "Doe", "sequence": "first"}
        original_editor = {"given": "Jane", "family": "Smith", "sequence": "first"}

        processor = CrossrefProcessing(testing=True)
        processor.csv_creator(item)

        self.assertEqual(item['author'][0], original_author)
        self.assertEqual(item['editor'][0], original_editor)
        processor.storage_manager.delete_storage()

    def test_csv_creator_member_as_string(self):
        """Test that member field as string (API format) is handled."""
        item = {
            "DOI": "10.1001/test.12345",
            "type": "journal-article",
            "title": ["Test"],
            "publisher": "American Medical Association (AMA)",
            "member": "10",
            "prefix": "10.1001",
            "issued": {"date-parts": [[2024]]}
        }
        processor = CrossrefProcessing(
            publishers_filepath=PUBLISHERS_MAPPING,
            testing=True
        )
        row = processor.csv_creator(item)

        expected = {
            'id': 'doi:10.1001/test.12345',
            'title': 'Test',
            'author': '',
            'pub_date': '2024',
            'venue': '',
            'volume': '',
            'issue': '',
            'page': '',
            'type': 'journal article',
            'publisher': 'American Medical Association (AMA) [crossref:10]',
            'editor': ''
        }
        self.assertEqual(row, expected)
        processor.storage_manager.delete_storage()














