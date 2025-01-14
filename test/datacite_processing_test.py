import os
import unittest
from pprint import pprint
import json
from oc_ds_converter.lib.csvmanager import CSVManager
from oc_ds_converter.lib.jsonmanager import *

from oc_ds_converter.datacite.datacite_processing import DataciteProcessing
from oc_ds_converter.oc_idmanager.oc_data_storage.redis_manager import RedisStorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.in_memory_manager import InMemoryStorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.sqlite_manager import SqliteStorageManager


TEST_DIR = os.path.join("test", "datacite_processing")
TMP_SUPPORT_MATERIAL = os.path.join(TEST_DIR, "tmp_support")
IOD = os.path.join(TEST_DIR, 'iod')
WANTED_DOIS = os.path.join(TEST_DIR, 'wanted_dois')
PUBLISHERS_MAPPING = os.path.join(TEST_DIR, 'publishers.csv')
DATA2 = os.path.join(TEST_DIR, 'jSonFile_1.json')
DATA = os.path.join(TEST_DIR, 'sample_datacite.ndjson')

class TestDataciteProcessing(unittest.TestCase):

    def read_ndjson_chunk(self, file_path, chunk_size):
        with open(file_path, 'r', encoding='utf-8') as file:
            while True:
                chunk = []
                for _ in range(chunk_size):
                    line = file.readline()
                    if not line:
                        break
                    try:
                        data = json.loads(line)
                        chunk.append(data)
                    except json.JSONDecodeError as e:
                        # Handle JSON decoding errors if necessary
                        print(f"Error decoding JSON: {e}")
                if not chunk:
                    break
                yield chunk

    def test_get_all_ids_first_iteration(self):
        all_br = set()
        all_ra = set()
        dcp = DataciteProcessing()
        for idx, chunk in enumerate(self.read_ndjson_chunk(DATA, 100), start=1):
            for item in chunk:
                allids = dcp.extract_all_ids(item, is_first_iteration=True)
                all_br.update(set(allids[0]))
                all_ra.update(set(allids[1]))
        self.assertEqual(all_br, set())
        self.assertTrue({"orcid:0000-0001-8513-8700", "orcid:0000-0002-9286-2630"} == all_ra)

    def test_get_all_ids_second_iteration(self):
        all_br = set()
        all_ra = set()
        dcp = DataciteProcessing()
        for idx, chunk in enumerate(self.read_ndjson_chunk(DATA, 100), start=1):
            for item in chunk:
                allids = dcp.extract_all_ids(item, is_first_iteration=False)
                all_br.update(set(allids[0]))
                all_ra.update(set(allids[1]))
        self.assertTrue({"doi:10.1063/1.4973421", "doi:10.15407/scin11.06.057", "doi:10.1066/1741-4326/aa6b25", "doi:10.1063/1.4973421", "doi:10.1021/acs.jpclett.7b01097"} == all_br)

    def test_get_redis_validity_list_br(self):
        dcp = DataciteProcessing()
        br = {"doi:10.1063/1.4973421", "doi:10.15407/scin11.06.057", "doi:10.1066/1741-4326/aa6b25", "doi:10.1063/1.4973421", "doi:10.1021/acs.jpclett.7b01097"}
        br_valid_list = dcp.get_reids_validity_list(br, "br")
        exp_br_valid_list = []
        self.assertEqual(br_valid_list, exp_br_valid_list)
        dcp.storage_manager.delete_storage()

    def test_get_redis_validity_list_ra(self):
        dcp = DataciteProcessing()
        ra = {"orcid:0000-0001-8513-8700", "orcid:0000-0002-9286-2630"}
        ra_valid_list = dcp.get_reids_validity_list(ra, "ra")
        exp_ra_valid_list = []
        self.assertEqual(ra_valid_list, exp_ra_valid_list)
        dcp.storage_manager.delete_storage()

    def test_get_redis_validity_list_br_redis(self):
        dcp = DataciteProcessing(storage_manager=RedisStorageManager(testing=True))
        br = {"doi:10.1063/1.4973421", "doi:10.15407/scin11.06.057", "doi:10.1066/1741-4326/aa6b25",
              "doi:10.1063/1.4973421", "doi:10.1021/acs.jpclett.7b01097"}
        br_valid_list = dcp.get_reids_validity_list(br, "br")
        exp_br_valid_list = []
        self.assertEqual(br_valid_list, exp_br_valid_list)
        dcp.storage_manager.delete_storage()

    def test_get_redis_validity_dict_w_fakeredis_db_values_sqlite(self):
        dcp = DataciteProcessing()
        dcp.BR_redis.set("doi:10.15407/scin11.06.057", "omid:1")
        dcp.RA_redis.set("orcid:0000-0001-8513-8700", "omid:2")

        br = {"doi:10.1063/1.4973421", "doi:10.15407/scin11.06.057", "doi:10.1066/1741-4326/aa6b25",
              "doi:10.1063/1.4973421", "doi:10.1021/acs.jpclett.7b01097"}

        ra = {"orcid:0000-0001-8513-8700", "orcid:0000-0002-9286-2630"}
        br_validity_dict = dcp.get_reids_validity_list(br, "br")
        exp_br_valid_list = ["doi:10.15407/scin11.06.057"]
        ra_validity_dict = dcp.get_reids_validity_list(ra, "ra")
        exp_ra_valid_list = ["orcid:0000-0001-8513-8700"]
        self.assertEqual(br_validity_dict, exp_br_valid_list)
        self.assertEqual(ra_validity_dict, exp_ra_valid_list)

        dcp.storage_manager.delete_storage()

        dcp.BR_redis.delete("doi:10.15407/scin11.06.057")
        dcp.RA_redis.delete("orcid:0000-0001-8513-8700")

    def test_get_redis_validity_dict_w_fakeredis_db_values_redis(self):
        dcp = DataciteProcessing(storage_manager=RedisStorageManager())
        dcp.BR_redis.set("doi:10.15407/scin11.06.057", "omid:1")
        dcp.RA_redis.set("orcid:0000-0001-8513-8700", "omid:2")

        br = {"doi:10.1063/1.4973421", "doi:10.15407/scin11.06.057", "doi:10.1066/1741-4326/aa6b25",
              "doi:10.1063/1.4973421", "doi:10.1021/acs.jpclett.7b01097"}

        ra = {"orcid:0000-0001-8513-8700", "orcid:0000-0002-9286-2630"}
        br_validity_dict = dcp.get_reids_validity_list(br, "br")
        exp_br_valid_list = ["doi:10.15407/scin11.06.057"]
        ra_validity_dict = dcp.get_reids_validity_list(ra, "ra")
        exp_ra_valid_list = ["orcid:0000-0001-8513-8700"]
        self.assertEqual(br_validity_dict, exp_br_valid_list)
        self.assertEqual(ra_validity_dict, exp_ra_valid_list)

        dcp.storage_manager.delete_storage()

        dcp.BR_redis.delete("doi:10.15407/scin11.06.057")
        dcp.RA_redis.delete("orcid:0000-0001-8513-8700")

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
        validate_as_none_doi = dcp.validated_as({"schema":"doi", "identifier": "doi:10.11578/1480643"})
        validated_as_none_orcid = dcp.validated_as({"schema": "orcid", "identifier": "orcid:0000-0001-8513-8700"})
        self.assertEqual(validate_as_none_doi, None)
        self.assertEqual(validated_as_none_orcid, None)

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
        validate_as_none_doi = dcp.validated_as({"schema":"doi", "identifier": "doi:10.11578/1480643"})
        validated_as_none_orcid = dcp.validated_as({"schema": "orcid", "identifier": "orcid:0000-0001-8513-8700"})
        self.assertEqual(validate_as_none_doi, None)
        self.assertEqual(validated_as_none_orcid, None)

        dcp.storage_manager.delete_storage()

    def test_validated_as_sqlite(self):
        """
        Check that, given an ID dict with keys "schema" (value: string of the schema) and "identifier" (value:
        string of the identifier, the method "validated_as" returns:
        - True if the id was already validated as valid
        - False if the id was already validated as invalid
        - None if the id was not validated before
        The procedure is tested
        - With sqlite storage manager and a pre-existent db associated
        """
        db_path = os.path.join(TMP_SUPPORT_MATERIAL, "db_path.db")
        sqlite_man = SqliteStorageManager(db_path)
        valid_doi_not_in_db = {"identifier": "doi:10.11578/1480643", "schema": "doi"}
        valid_doi_in_db = {"identifier": "doi:10.15407/scin11.06.057", "schema": "doi"}
        invalid_doi_in_db = {"identifier": "doi:10.1066/1741-4326/aa6b", "schema": "doi"}
        valid_orcid_not_in_db = {"schema": "orcid", "identifier": "orcid:0000-0001-8513-8700"}
        valid_orcid_in_db = {"schema": "orcid", "identifier": "orcid:0000-0002-9286-2630"}
        invalid_orcid_in_db = {"schema": "orcid", "identifier": "orcid:0000-0002-9286-26XX"}
        sqlite_man.set_value(valid_doi_in_db["identifier"], True)
        sqlite_man.set_value(invalid_doi_in_db["identifier"], False)
        sqlite_man.set_value(valid_orcid_in_db["identifier"], True)
        sqlite_man.set_value(invalid_orcid_in_db["identifier"], False)

        # New class instance to check the correct task management with a sqlite db in input
        d_processing_sql = DataciteProcessing(storage_manager=sqlite_man)
        doi_validated_as_True = d_processing_sql.validated_as(valid_doi_in_db)
        doi_validated_as_False = d_processing_sql.validated_as(invalid_doi_in_db)
        doi_not_validated = d_processing_sql.validated_as(valid_doi_not_in_db)
        orcid_validated_as_True = d_processing_sql.validated_as(valid_orcid_in_db)
        orcid_validated_as_False = d_processing_sql.validated_as(invalid_orcid_in_db)
        orcid_not_validated = d_processing_sql.validated_as(valid_orcid_not_in_db)

        self.assertEqual(doi_validated_as_True, True)
        self.assertEqual(doi_validated_as_False, False)
        self.assertEqual(doi_not_validated, None)
        self.assertEqual(orcid_validated_as_True, True)
        self.assertEqual(orcid_validated_as_False, False)
        self.assertEqual(orcid_not_validated, None)

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
        inmemory_man.set_value(valid_doi_in_db["identifier"], True)
        inmemory_man.set_value(invalid_doi_in_db["identifier"], False)
        inmemory_man.set_value(valid_orcid_in_db["identifier"], True)
        inmemory_man.set_value(invalid_orcid_in_db["identifier"], False)

        # New class instance to check the correct task management with a sqlite db in input
        d_processing = DataciteProcessing(storage_manager=inmemory_man)
        doi_validated_as_True = d_processing.validated_as(valid_doi_in_db)
        doi_validated_as_False = d_processing.validated_as(invalid_doi_in_db)
        doi_not_validated = d_processing.validated_as(valid_doi_not_in_db)
        orcid_validated_as_True = d_processing.validated_as(valid_orcid_in_db)
        orcid_validated_as_False = d_processing.validated_as(invalid_orcid_in_db)
        orcid_not_validated = d_processing.validated_as(valid_orcid_not_in_db)

        self.assertEqual(doi_validated_as_True, True)
        self.assertEqual(doi_validated_as_False, False)
        self.assertEqual(doi_not_validated, None)
        self.assertEqual(orcid_validated_as_True, True)
        self.assertEqual(orcid_validated_as_False, False)
        self.assertEqual(orcid_not_validated, None)

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
        redis_man.set_value(valid_doi_in_db["identifier"], True)
        redis_man.set_value(invalid_doi_in_db["identifier"], False)
        redis_man.set_value(valid_orcid_in_db["identifier"], True)
        redis_man.set_value(invalid_orcid_in_db["identifier"], False)

        d_processing_redis = DataciteProcessing(storage_manager=redis_man)
        doi_validated_as_True = d_processing_redis.validated_as(valid_doi_in_db)
        doi_validated_as_False = d_processing_redis.validated_as(invalid_doi_in_db)
        doi_not_validated = d_processing_redis.validated_as(valid_doi_not_in_db)
        orcid_validated_as_True = d_processing_redis.validated_as(valid_orcid_in_db)
        orcid_validated_as_False = d_processing_redis.validated_as(invalid_orcid_in_db)
        orcid_not_validated = d_processing_redis.validated_as(valid_orcid_not_in_db)

        self.assertEqual(doi_validated_as_True, True)
        self.assertEqual(doi_validated_as_False, False)
        self.assertEqual(doi_not_validated, None)
        self.assertEqual(orcid_validated_as_True, True)
        self.assertEqual(orcid_validated_as_False, False)
        self.assertEqual(orcid_not_validated, None)

        d_processing_redis.storage_manager.delete_storage()

    def test_get_id_manager(self):
        """Check that, given in input the string of a schema (e.g.:'pmid') or an id with a prefix (e.g.: 'pmid:12334')
        and a dictionary mapping the strings of the schemas to their id managers, the method returns the correct
        id manager. Note that each instance of the Preprocessing class needs its own instances of the id managers,
        in order to avoid conflicts while validating data"""

        d_processing = DataciteProcessing()
        id_man_dict = d_processing.venue_id_man_dict

        issn_id = "issn:0003-987X"
        issn_string = "issn"
        isbn_id = "isbn:978-88-98719-08-2"
        isbn_string = "isbn"
        issn_man_exp = d_processing.get_id_manager(issn_id, id_man_dict)
        issn_man_exp_2 = d_processing.get_id_manager(issn_string, id_man_dict)
        isbn_man_exp = d_processing.get_id_manager(isbn_id, id_man_dict)
        isbn_man_exp_2 = d_processing.get_id_manager(isbn_string, id_man_dict)

        #check that the idmanager for the issn was returned and that it works as expected
        self.assertTrue(issn_man_exp.is_valid(issn_id))
        self.assertTrue(issn_man_exp_2.is_valid(issn_id))
        # check that the idmanager for the isbn was returned and that it works as expected
        self.assertTrue(isbn_man_exp.is_valid(isbn_id))
        self.assertTrue(isbn_man_exp_2.is_valid(isbn_id))
    def test_csv_creator2(self):
        datacite_processor = DataciteProcessing(orcid_index=IOD, doi_csv=WANTED_DOIS, publishers_filepath_dc=None)
        data = load_json(DATA2, None)
        output = list()
        for item in data['data']:
            tabular_data = datacite_processor.csv_creator(item)
            if tabular_data:
                output.append(tabular_data)

        expected_output = [
            {'id': 'doi:10.1002/2014jc009965',
             'title': 'On the physical and biogeochemical processes driving the high frequency variability of CO fugacity at 6°S, 10°W: Potential role of the internal waves',
             'author': 'Parard, Gaëlle; Boutin, J.; Cuypers, Y.; Bouruet-Aubertot, P.; Caniaux, G.',
             'pub_date': '2014-12',
             'venue': 'journal of geophysical research: oceans [issn:2169-9275]',
             'volume': '119',
             'issue': '12',
             'page': '8357-8374',
             'type': 'journal article',
             'publisher': '',
             'editor': ''},
            {'id': 'doi:10.1002/2014jd022411',
             'title': "Assessing the magnitude of CO flux uncertainty in atmospheric CO records using products from NASA's Carbon Monitoring Flux Pilot Project",
             'author': 'Ott, Lesley E.; Pawson, Steven; Collatz, George J.; Gregg, Watson W.; Menemenlis, Dimitris [orcid:0000-0001-9940-8409]; Brix, Holger; Rousseaux, Cecile S.; Bowman, Kevin W.; Liu, Junjie; Eldering, Annmarie; Gunson, Michael R.; Kawa, Stephan R.',
             'pub_date': '2015-01-27',
             'venue': 'journal of geophysical research: atmospheres [issn:2169-897X]',
             'volume': '120',
             'issue': '2',
             'page': '734-765',
             'type': 'journal article',
             'publisher': '',
             'editor': ''},
            {'id': 'doi:10.1002/2015gb005314',
             'title': 'Satellite estimates of net community production based on O /Ar observations and comparison to other estimates',
             'author': 'Li, Zuchuan; Cassar, Nicolas',
             'pub_date': '2016-05',
             'venue': 'global biogeochemical cycles [issn:0886-6236]',
             'volume': '30',
             'issue': '5',
             'page': '735-752',
             'type': 'journal article',
             'publisher': '',
             'editor': ''},
            {'id': 'doi:10.1002/2015gl065259',
             'title': 'Observed eastward progression of the Fukushima Cs signal across the North Pacific',
             'author': 'Yoshida, Sachiko; Macdonald, Alison M.; Jayne, Steven R.; Rypina, Irina I.; Buesseler, Ken O.',
             'pub_date': '2015-09-16',
             'venue': 'geophysical research letters [issn:0094-8276]',
             'volume': '42',
             'issue': '17',
             'page': '7139-7147',
             'type': 'journal article',
             'publisher': '',
             'editor': ''},
            {'id': 'doi:10.1594/pangaea.231378',
             'title': 'Phosphate, fluoride and cell abundance of bacteria Thiomargarita namibiensis in porewater of sediment profile M57/3_203 from Walvis Ridge',
             'author': 'Schulz, Heide N [orcid:0000-0003-1445-0291]; Schulz, Horst D',
             'pub_date': '2005',
             'venue': '',
             'volume': '',
             'issue': '',
             'page': '',
             'type': 'dataset',
             'publisher': 'PANGAEA - Data Publisher for Earth & Environmental Science', 'editor': ''}

        ]

        self.assertEqual(output, expected_output)
    def test_csv_creator(self):
        dcp = DataciteProcessing()
        output = list()
        for idx, chunk in enumerate(self.read_ndjson_chunk(DATA, 100), start=1):
            for item in chunk:
                tabular_data = dcp.csv_creator(item)
                if tabular_data:
                    output.append(tabular_data)
        expected_output = [
            {
                'id': 'doi:10.11578/1480643',
                'title': 'Dataset Title - Award',
                'author': 'Last, First',
                'pub_date': '2018',
                'venue': '',
                'volume': '',
                'issue': '',
                'page': '',
                'type': 'other',
                'publisher': 'Desert Research Institute (DRI), Nevada System of Higher Education, Reno,NV (United States)',
                'editor': ''
            },
            {
                'id': 'doi:10.11575/jet.v46i3.52198',
                'title': 'Intercultural Research and Education on the Alberta Prairies: Findings from a Doctoral Study',
                'author': 'Hamm, Lyle D.',
                'pub_date': '2018-05-17',
                'venue': 'journal of educational thought / revue de la pensée educative',
                'volume': '',
                'issue': '',
                'page': '',
                'type': 'report',
                'publisher': 'Journal of Educational Thought / Revue de la Pensée Educative',
                'editor': ''
            },
            {
                'id': 'doi:10.11578/1367548',
                'title': 'Stabilizing effect of resistivity towards ELM-free H-mode discharge in lithium-conditioned NSTX',
                'author': 'Banerjee, D.; Zhu, P.; Maingi, R.',
                'pub_date': '2017',
                'venue': '',
                'volume': '',
                'issue': '',
                'page': '',
                'type': 'dataset',
                'publisher': 'Princeton Plasma Physics Laboratory (PPPL), Princeton, NJ (United States)',
                'editor': ''
            },
            {
                'id': 'doi:10.11578/1372474',
                'title': 'Influence of Molecular Shape on the Thermal Stability and Molecular Orientation of Vapor-Deposited Organic Semiconductors',
                'author': 'Walters, Diane; Antony, Lucas; De Pablo, Juan; Ediger, Mark',
                'pub_date': '2017',
                'venue': '',
                'volume': '',
                'issue': '',
                'page': '',
                'type': 'dataset',
                'publisher': 'University of Wisconsin-Madison\nUniversity of Chicago',
                'editor': ''
            },
            {
                'id': 'doi:10.11578/1367552',
                'title': 'Application of IR imaging for free-surface velocity measurement in liquid-metal systems',
                'author': 'Hvasta, M.G.; Kolemen, E.; Fisher, A.',
                'pub_date': '2017',
                'venue': '',
                'volume': '',
                'issue': '',
                'page': '',
                'type': 'dataset',
                'publisher': 'Princeton Plasma Physics Laboratory (PPPL), Princeton, NJ (United States)',
                'editor': ''
            },
            {
                'id': 'doi:10.11578/dc.20191106.1',
                'title': 'STJ_PV: Subtropical Jet Finding Framework',
                'author': 'Kelleher, Michael [orcid:0000-0002-9286-2630]; Maher, Penelope [orcid:0000-0001-8513-8700]',
                'pub_date': '2019',
                'venue': '',
                'volume': '',
                'issue': '',
                'page': '',
                'type': 'computer program',
                'publisher': '',
                'editor': ''
            }
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
        item1 = {"publisher": "(:unav)"}
        item2 = {"publisher": ":unav"}
        item3 = {"publisher": ":unkn"}
        item4 = {"publisher": "(:unkn)"}
        item5 = {"publisher": "Edo : [publisher not identified]mon han"}
        item6 = {"publisher": "[place of publication not identified]: [pubisher not identified]"}
        item7 = {"publisher": "unknown unknown"}
        item8 = {"publisher": "[unknown] : [unknown]"}
        item9 = {"publisher": "[unknown] : College of Pharmacists of British Columbia"}
        item10 = {"publisher": "[Edinburgh]: [Unknown]"}
        item11 = {"publisher": "Unknown, National University of Singapore"}
        item12 = {"publisher": "Not provided."}
        item13 = {"publisher": "Soleure, s.n."}
        item14 = {"publisher": "[s.l. , s.n]"}
        item15 = {"publisher": "[ s.l. : s.n.]"}
        item16 = {"publisher": "s.n.]"}
        item17 = {"publisher": "Information not available, contact SND for more information"}
        item18 = {"publisher": "Publisher Not Specified"}
        result1 = dcp.get_publisher_name('doi:10.11578/dc.20191106.1', item1)
        result2 = dcp.get_publisher_name('doi:10.11578/dc.20191106.1', item2)
        result3 = dcp.get_publisher_name('doi:10.11578/dc.20191106.1', item3)
        result4 = dcp.get_publisher_name('doi:10.11578/dc.20191106.1', item4)
        result5 = dcp.get_publisher_name('doi:10.11578/dc.20191106.1', item5)
        result6 = dcp.get_publisher_name('doi:10.11578/dc.20191106.1', item6)
        result7 = dcp.get_publisher_name('doi:10.11578/dc.20191106.1', item7)
        result8 = dcp.get_publisher_name('doi:10.11578/dc.20191106.1', item8)
        result9 = dcp.get_publisher_name('doi:10.11578/dc.20191106.1', item9)
        result10 = dcp.get_publisher_name('doi:10.11578/dc.20191106.1', item10)
        result11 = dcp.get_publisher_name('doi:10.11578/dc.20191106.1', item11)
        result12 = dcp.get_publisher_name('doi:10.11578/dc.20191106.1', item12)
        result13 = dcp.get_publisher_name('doi:10.11578/dc.20191106.1', item13)
        result14 = dcp.get_publisher_name('doi:10.11578/dc.20191106.1', item14)
        result15 = dcp.get_publisher_name('doi:10.11578/dc.20191106.1', item15)
        result16 = dcp.get_publisher_name('doi:10.11578/dc.20191106.1', item16)
        result17 = dcp.get_publisher_name('doi:10.11578/dc.20191106.1', item17)
        result18 = dcp.get_publisher_name('doi:10.11578/dc.20191106.1', item18)
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
            "publisher": "PANGAEA - Data Publisher for Earth & Environmental Science"
        }
        doi = '10.1594/pangaea.777220'
        datacite_processor = DataciteProcessing(orcid_index=None, doi_csv=None, publishers_filepath_dc=PUBLISHERS_MAPPING)
        publisher_name = datacite_processor.get_publisher_name(doi, item)
        self.assertEqual(publisher_name, 'PANGAEA - Data Publisher for Earth & Environmental Science [datacite:2]')

    def test_get_publisher_name_from_prefix(self):
        # The item has no declared publisher, but the DOI prefix is in the publishers' mapping
        item = {
            'publisher': '',
            'doi': '10.12753/sample_test_doi_with_known_prefix',
        }
        doi = '10.12753/sample_test_doi_with_known_prefix'
        datacite_processor = DataciteProcessing(orcid_index=None, doi_csv=None, publishers_filepath_dc=PUBLISHERS_MAPPING)
        publisher_name = datacite_processor.get_publisher_name(doi, item)
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
        dcp = DataciteProcessing(storage_manager=RedisStorageManager(testing=True))
        # CASE_1:  is valid
        inp_1 = {'id': 'doi:10.11578/1367552', 'schema': 'doi'}
        out_1 = dcp.to_validated_id_list(inp_1)
        exp_1 = ['doi:10.11578/1367552']
        self.assertEqual(out_1, exp_1)
        dcp.storage_manager.delete_storage()

        dcp = DataciteProcessing(storage_manager=RedisStorageManager(testing=True))
        # CASE_2: is invalid
        inp_2 = {'id': 'doi:10.11578/136755', 'schema': 'doi'}
        out_2 = dcp.to_validated_id_list(inp_2)
        exp_2 = []
        self.assertEqual(out_2, exp_2)

        dcp = DataciteProcessing(storage_manager=RedisStorageManager(testing=True))
        # CASE_3:  valid orcid
        inp_3 = {'id': 'orcid:0000-0002-9286-2630', 'schema': 'orcid'}
        out_3 = dcp.to_validated_id_list(inp_3)
        exp_3 = ['orcid:0000-0002-9286-2630']
        self.assertEqual(out_3, exp_3)
        dcp.storage_manager.delete_storage()

        dcp = DataciteProcessing(storage_manager=RedisStorageManager(testing=True))
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
        dcp = DataciteProcessing()
        inp = ["https://orcid.org/0000-0002-9286-2630"]
        out = dcp.find_datacite_orcid(inp)
        exp_out = "orcid:0000-0002-9286-2630"
        self.assertEqual(out, exp_out)

        inp_invalid_id = ["https://orcid.org/0000-0002-9286-2631"]
        out_invalid_id = dcp.find_datacite_orcid(inp_invalid_id)
        exp_invalid_id = ""
        self.assertEqual(out_invalid_id, exp_invalid_id)

        dcp.storage_manager.delete_storage()

        # set a valid id as invalid in storage, so to check that the api check is
        # avoided if the info is already in storage
        dcp = DataciteProcessing()
        dcp.storage_manager.set_value("orcid:0000-0002-9286-2630", False)
        inp = ["https://orcid.org/0000-0002-9286-2630"]
        out = dcp.find_datacite_orcid(inp)
        exp_out = ""
        self.assertEqual(out, exp_out)
        dcp.storage_manager.delete_storage()

        dcp = DataciteProcessing()
        dcp.storage_manager.set_value("orcid:0000-0002-9286-2631", True)
        inp = ["https://orcid.org/0000-0002-9286-2631"]
        out = dcp.find_datacite_orcid(inp)
        exp_out = "orcid:0000-0002-9286-2631"
        self.assertEqual(out, exp_out)
        dcp.storage_manager.delete_storage()

    def test_get_venue_name(self):
        item = {
            "container": {"type": "Series", "title": "Journal of Educational Thought / Revue de la Pensée Educative", "firstPage": "Vol 46 No 3 (2012)", "identifier": "10.11575/jet.v46i3", "identifierType": "DOI"}
        }
        row = {'id': '', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '',
               'type': 'journal article', 'publisher': '', 'editor': ''}
        datacite_processor = DataciteProcessing(orcid_index=None, doi_csv=None, publishers_filepath_dc=PUBLISHERS_MAPPING)
        venue_name = datacite_processor.get_venue_name(item, row)
        self.assertEqual(venue_name, 'journal of educational thought / revue de la pensée educative')

    def test_get_venue_name_with_ISSN(self):
        item = {
            "container": {"type": "Journal", "issue": "18", "title": "Geophysical Research Letters", "volume": "41",
                          "lastPage": "6451", "firstPage": "6443", "identifier": "00948276", "identifierType": "ISSN"}
        }
        row = {'id': '', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '',
               'type': 'journal article', 'publisher': '', 'editor': ''}
        datacite_processor = DataciteProcessing(orcid_index=None, doi_csv=None, publishers_filepath_dc=PUBLISHERS_MAPPING)
        venue_name = datacite_processor.get_venue_name(item, row)
        self.assertEqual(venue_name,
                         "geophysical research letters [issn:0094-8276]")
        # ISSN with wrong number of digits
        item1 = {
            "container": {"type": "Journal", "issue": "18", "title": "Geophysical Research Letters", "volume": "41",
                          "lastPage": "6451", "firstPage": "6443", "identifier": "00948276", "identifierType": "ISSN"}
        }
        row1= {'id': '', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '',
               'type': 'journal article', 'publisher': '', 'editor': ''}
        venue_name1 = datacite_processor.get_venue_name(item1, row1)
        self.assertEqual(venue_name1,
                         "geophysical research letters [issn:0094-8276]")


    def test_get_venue_ISSN_from_rel_id(self):
        item = {"relatedIdentifiers": [
            {"relationType": "IsPartOf", "relatedIdentifier": "00948276", "resourceTypeGeneral": "Collection",
             "relatedIdentifierType": "ISSN"}]
                }
        row = {'id': '', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '',
               'page': '',
               'type': 'journal article', 'publisher': '', 'editor': ''}
        datacite_processor = DataciteProcessing(orcid_index=None, doi_csv=None,
                                                publishers_filepath_dc=PUBLISHERS_MAPPING)
        venue_name = datacite_processor.get_venue_name(item, row)
        self.assertEqual(venue_name, "[issn:0094-8276]")

    def test_get_pages(self):
        item = {
            "container": {"type": "Journal", "issue": "7", "title": "Global Biogeochemical Cycles", "volume": "29",
                          "lastPage": "1013", "firstPage": "994", "identifier": "08866236",
                          "identifierType": "ISSN"}
        }
        datacite_processor = DataciteProcessing(orcid_index=None, doi_csv=None,
                                                publishers_filepath_dc=PUBLISHERS_MAPPING)
        pages = datacite_processor.get_datacite_pages(item)
        self.assertEqual(pages, '994-1013')

    def test_get_pages_right_letter(self):
        item = {
            "container": {"type": "Journal", "issue": "4", "title": "Ecosphere", "volume": "10",
                          "firstPage": "e02701", "identifier": "2150-8925", "identifierType": "ISSN"}
        }
        datacite_processor = DataciteProcessing(orcid_index=None, doi_csv=None,
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
        datacite_processor = DataciteProcessing(orcid_index=None, doi_csv=None,
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
        datacite_processor = DataciteProcessing(orcid_index=None, doi_csv=None,
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
                    "publisher": "(:unav)",
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
        datacite_processor = DataciteProcessing(orcid_index=None, doi_csv=None,
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
                    "publisher": "(:unav)",
                    "container": {"type": "Journal", "issue": "2",
                                  "title": "Journal of Geophysical Research: Atmospheres", "volume": "120",
                                  "lastPage": "765", "firstPage": "734", "identifier": "2169897X",
                                  "identifierType": "ISSN"},
                    "relatedIdentifiers": [{"relationType": "IsPartOf", "relatedIdentifier": "2169897X",
                                            "resourceTypeGeneral": "Collection", "relatedIdentifierType": "ISSN"}]
                }
            }
        ]}
        datacite_processor = DataciteProcessing(orcid_index=None, doi_csv=None,
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

    def test_venue_id_rel_id_only(self):
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
                    "publisher": "(:unav)",
                    "container": {},
                    "types": {"ris": "JOUR", "bibtex": "article", "citeproc": "article-journal",
                              "schemaOrg": "ScholarlyArticle", "resourceType": "JournalArticle",
                              "resourceTypeGeneral": "Text"},
                    "relatedIdentifiers": [{"relationType": "IsPartOf", "relatedIdentifier": "2169897X",
                                            "resourceTypeGeneral": "Collection", "relatedIdentifierType": "ISSN"}]
                }
            }
        ]}
        datacite_processor = DataciteProcessing(orcid_index=None, doi_csv=None,
                                                publishers_filepath_dc=PUBLISHERS_MAPPING)
        output = list()
        for item in items['data']:
            output.append(datacite_processor.csv_creator(item))
        expected_output = [{'id': 'doi:10.1002/2014jd022411',
                            'title': "Assessing the magnitude of CO flux uncertainty in atmospheric CO records using products from NASA's Carbon Monitoring Flux Pilot Project",
                            'author': '', 'pub_date': '', 'venue': '[issn:2169-897X]', 'volume': '', 'issue': '',
                            'page': '', 'type': 'journal article', 'publisher': 'Wiley [datacite:1]', 'editor': ''}]
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
        authors_list = datacite_processor.add_authors_to_agent_list(entity_attr_dict, [])
        editors_list = datacite_processor.add_editors_to_agent_list(entity_attr_dict, [])
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
            "creators": [{"name": "Ott, Lesley E.", "nameType": "Personal", "givenName": "Lesley E.", "familyName": "Ott", "affiliation": [], "nameIdentifiers": []}, {"name": "Pawson, Steven", "nameType": "Personal", "givenName": "Steven", "familyName": "Pawson", "affiliation": [], "nameIdentifiers": []}, {"name": "Collatz, George J.", "nameType": "Personal", "givenName": "George J.", "familyName": "Collatz", "affiliation": [], "nameIdentifiers": []}, {"name": "Gregg, Watson W.", "nameType": "Personal", "givenName": "Watson W.", "familyName": "Gregg", "affiliation": [], "nameIdentifiers": []}, {"name": "Menemenlis, Dimitris", "nameType": "Personal", "givenName": "Dimitris", "familyName": "Menemenlis", "affiliation": [], "nameIdentifiers": [{"schemeUri": "https://orcid.org", "nameIdentifier": "https://orcid.org/0000-0001-9940-8409", "nameIdentifierScheme": "ORCID"}]}, {"name": "Brix, Holger", "nameType": "Personal", "givenName": "Holger", "familyName": "Brix", "affiliation": [], "nameIdentifiers": []}, {"name": "Rousseaux, Cecile S.", "nameType": "Personal", "givenName": "Cecile S.", "familyName": "Rousseaux", "affiliation": [], "nameIdentifiers": []}, {"name": "Bowman, Kevin W.", "nameType": "Personal", "givenName": "Kevin W.", "familyName": "Bowman", "affiliation": [], "nameIdentifiers": []}, {"name": "Liu, Junjie", "nameType": "Personal", "givenName": "Junjie", "familyName": "Liu", "affiliation": [], "nameIdentifiers": []}, {"name": "Eldering, Annmarie", "nameType": "Personal", "givenName": "Annmarie", "familyName": "Eldering", "affiliation": [], "nameIdentifiers": []}, {"name": "Gunson, Michael R.", "nameType": "Personal", "givenName": "Michael R.", "familyName": "Gunson", "affiliation": [], "nameIdentifiers": []}, {"name": "Kawa, Stephan R.", "nameType": "Personal", "givenName": "Stephan R.", "familyName": "Kawa", "affiliation": [], "nameIdentifiers": []}],
            "contributors": [{
                            'name': 'AKMB-News: Informationen Zu Kunst, Museum Und Bibliothek',
                            'nameType': 'Personal',
                            'givenName': 'Museum Und Bibliothek',
                            'familyName': 'AKMB-News: Informationen Zu Kunst',
                            'affiliation': [],
                            'contributorType': 'Editor',
                            'nameIdentifiers': []}]}

        datacite_processor = DataciteProcessing(IOD, WANTED_DOIS)
        authors_list = datacite_processor.add_authors_to_agent_list(entity_attr_dict, [])
        editors_list = datacite_processor.add_editors_to_agent_list(entity_attr_dict, [])
        agents_list = authors_list + editors_list


        authors_strings_list, editors_strings_list = datacite_processor.get_agents_strings_list('10.1002/2014jd022411',
                                                                             agents_list)
        expected_authors_list = ['Ott, Lesley E.', 'Pawson, Steven', 'Collatz, George J.', 'Gregg, Watson W.', 'Menemenlis, Dimitris [orcid:0000-0001-9940-8409]', 'Brix, Holger', 'Rousseaux, Cecile S.', 'Bowman, Kevin W.', 'Liu, Junjie', 'Eldering, Annmarie', 'Gunson, Michael R.', 'Kawa, Stephan R.']
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
                         {"schemeUri": "https://orcid.org", "nameIdentifier": "https://orcid.org/0000-0003-1445-0291", "nameIdentifierScheme": "ORCID"}
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
        datacite_processor = DataciteProcessing(IOD, WANTED_DOIS)
        authors_list = datacite_processor.add_authors_to_agent_list(entity_attr_dict, [])
        editors_list = datacite_processor.add_editors_to_agent_list(entity_attr_dict, [])
        agents_list = authors_list + editors_list
        authors_strings_list, _ = datacite_processor.get_agents_strings_list('10.1594/pangaea.231378',
                                                                             agents_list)
        expected_authors_list = ['Schulz, Heide N [orcid:0000-0003-1445-0291]', 'Schulz, Horst D']
        self.assertEqual(authors_strings_list, expected_authors_list)

    def test_get_agents_strings_list_homonyms(self):
        # Two authors have the same family name and the same given name
        entity_attr_dict = {
            "creators":
                [
                    {"name":  "Viorel, Cojocaru",
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
        authors_list = datacite_processor.add_authors_to_agent_list(entity_attr_dict, [])
        editors_list = datacite_processor.add_editors_to_agent_list(entity_attr_dict, [])
        agents_list = authors_list + editors_list
        authors_strings_list, _ = datacite_processor.get_agents_strings_list('10.12753/2066-026x-14-246',
                                                                             agents_list)
        expected_authors_list = ['Viorel, Cojocaru', 'Viorel, Cojocaru', 'Ciprian, Panait']
        self.assertEqual(authors_strings_list, expected_authors_list)


    def test_get_agents_strings_list_inverted_names(self):
        # One author with an ORCID has as a name the surname of another
        entity_attr_dict = {
            "creators":
                [
                    {"name":  "Viorel, Cojocaru",
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
        authors_list = datacite_processor.add_authors_to_agent_list(entity_attr_dict, [])
        editors_list = datacite_processor.add_editors_to_agent_list(entity_attr_dict, [])
        agents_list = authors_list + editors_list
        authors_strings_list, _ = datacite_processor.get_agents_strings_list('10.12753/2066-026x-14-246',
                                                                             agents_list)
        expected_authors_list = ['Viorel, Cojocaru', 'Cojocaru, John', 'Ciprian, Panait']
        self.assertEqual(authors_strings_list, expected_authors_list)

    def test_find_datacite_orcid_with_index(self):
        """Test ORCID validation using ORCID index before API validation"""
        # Setup
        test_doi = "10.1234/test123"
        test_orcid = "0000-0002-1234-5678"
        test_name = "Smith, John"
        
        # Create DataciteProcessing instance with ORCID index
        dp = DataciteProcessing()
        dp.orcid_index.add_value(test_doi, f"{test_name} [orcid:{test_orcid}]")
        
        # Test Case 1: ORCID found in index
        inp_1 = [test_orcid]
        out_1 = dp.find_datacite_orcid(inp_1, test_doi)
        exp_1 = f"orcid:{test_orcid}"
        self.assertEqual(out_1, exp_1)
        # Verify it was added to temporary storage
        self.assertTrue(dp.tmp_orcid_m.storage_manager.get_value(f"orcid:{test_orcid}"))
        
        # Test Case 2: ORCID not in index but valid via API
        inp_2 = ["0000-0003-4082-1500"]
        out_2 = dp.find_datacite_orcid(inp_2, test_doi)
        exp_2 = "orcid:0000-0003-4082-1500"
        self.assertEqual(out_2, exp_2)
        
        # Test Case 3: ORCID not in index and invalid
        inp_3 = ["0000-0000-0000-0000"]
        out_3 = dp.find_datacite_orcid(inp_3, test_doi)
        exp_3 = ""
        self.assertEqual(out_3, exp_3)
        
        # Test Case 4: Valid ORCID but no DOI provided (retrocompatibilità)
        inp_4 = [test_orcid]
        out_4 = dp.find_datacite_orcid(inp_4)  # No DOI
        exp_4 = f"orcid:{test_orcid}"  # Should still validate via API
        self.assertEqual(out_4, exp_4)
        
        # Test Case 5: Multiple ORCIDs, first one valid
        inp_5 = [test_orcid, "0000-0000-0000-0000"]
        out_5 = dp.find_datacite_orcid(inp_5, test_doi)
        exp_5 = f"orcid:{test_orcid}"
        self.assertEqual(out_5, exp_5)
        
        # Cleanup
        dp.storage_manager.delete_storage()