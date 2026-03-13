import csv
import json
import os
import shutil
import unittest

from oc_ds_converter.run.datacite_process import preprocess


class DataciteProcessTest(unittest.TestCase):

    def setUp(self) -> None:
        self.test_dir = os.path.join("test",'datacite_process')
        self.json_dir = os.path.join(self.test_dir, 'jsonFiles')
        self.output_dir = os.path.join(self.test_dir, 'output_dir')
        self.processing_test_dir = os.path.join('test', 'datacite_processing')
        self.publisher_mapping = os.path.join(self.processing_test_dir, 'publishers.csv')
        self.wanted_dois = os.path.join(self.processing_test_dir, 'wanted_dois')
        self.iod = os.path.join(self.processing_test_dir, 'iod')
        self.cache = os.path.join(self.test_dir, 'cache.json')
        self.cache_test = os.path.join(self.test_dir, 'cache_test.json')
        self.db = os.path.join(self.test_dir, 'anydb.db')
        # percorso input con il file NDJSON malformato
        self.error_input_folder = os.path.join(self.test_dir, 'sample_dc_error')
        # percorsi per report errori
        self.bad_dir = os.path.join(self.output_dir, '_bad')
        self.citations_output_path = self.output_dir + "_citations"

    def test_preprocess_base_decompress_and_read(self):
        """Test base functionalities of the Datacite processor for producing META csv tables and INDEX tables:
        1) All the files in input dir are correctly processed
        2) The number of files in input corresponds to the number of files in output for citations
        3) The number of files in input are duplicated in the output folder for both citing and cited entities
        """

        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)

        citations_output_path = self.output_dir + "_citations"
        if os.path.exists(citations_output_path):
            shutil.rmtree(citations_output_path)

        # assicura corretto funzionamento di _bad
        bad_dir = os.path.join(self.output_dir, '_bad')
        if os.path.exists(bad_dir):
            shutil.rmtree(bad_dir)

        if os.path.exists(self.db):
            os.remove(self.db)
        if os.path.exists(self.cache):
            os.remove(self.cache)

        preprocess(datacite_json_dir=self.json_dir, publishers_filepath=self.publisher_mapping,
                   orcid_doi_filepath=self.iod, csv_dir=self.output_dir, redis_storage_manager=False,
                   storage_path=self.db, cache=self.cache)

        citations_in_output = 0

        for file in os.listdir(citations_output_path):
            with open(os.path.join(citations_output_path, file), 'r', encoding='utf-8') as f:
                cits_rows = list(csv.DictReader(f))
                citations_in_output += len(cits_rows)

        #one self citation must not be considered
        expected_citations_in_output = 19

        #excluding duplicated entities and one invalid doi 10.46979/rbn.v52i4.5546
        expected_entities_in_output = 22

        self.assertEqual(expected_citations_in_output, citations_in_output)

        citations_files_n = len(list(os.listdir(citations_output_path)))

        shutil.rmtree(citations_output_path)

        entities_in_meta_output = 0
        for file in os.listdir(self.output_dir):
            with open(os.path.join(self.output_dir, file), 'r', encoding='utf-8') as f:
                entities_in_meta_output += len(list(csv.DictReader(f)))

        self.assertEqual(expected_entities_in_output, entities_in_meta_output)

        input_files_n = 2
        self.assertTrue(citations_files_n == input_files_n)

        # CLEAN: output, _bad, decompressioni e db
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)
        bad_dir = os.path.join(self.output_dir, '_bad')
        if os.path.exists(bad_dir):
            shutil.rmtree(bad_dir)
        if os.path.exists(self.db):
            os.remove(self.db)
        if os.path.exists(self.cache):
            os.remove(self.cache)

    def test_preprocess_orcid_api_disabled_no_index(self):
        """
        With the ORCID API disabled and without a DOI->ORCID index,
        ORCIDs must not appear in _subject.csv files.
        """
        # Pre-clean
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)

        citations_output_path = self.output_dir + "_citations"
        if os.path.exists(citations_output_path):
            shutil.rmtree(citations_output_path)

        # assicura corretto funzionamento di _bad
        bad_dir = os.path.join(self.output_dir, '_bad')
        if os.path.exists(bad_dir):
            shutil.rmtree(bad_dir)

        if os.path.exists(self.db):
            os.remove(self.db)
        if os.path.exists(self.cache):
            os.remove(self.cache)

        # Run with API disabled and no index
        preprocess(
            datacite_json_dir=self.json_dir,
            publishers_filepath=self.publisher_mapping,
            orcid_doi_filepath=None,
            csv_dir=self.output_dir,
            cache=self.cache,
            use_orcid_api=False
        )

        # Verify: no "[orcid:" in any _subject.csv "author" field
        found_orcid = False
        for file in os.listdir(self.output_dir):
            if file.endswith("_subject.csv"):
                with open(os.path.join(self.output_dir, file), "r", encoding="utf-8") as f:
                    for row in csv.DictReader(f):
                        if "[orcid:" in (row.get("author", "") or ""):
                            found_orcid = True
                            break
            if found_orcid:
                break

        self.assertFalse(found_orcid)

        # Post-clean
        # CLEAN: output, _bad, decompressioni e db
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)
        bad_dir = os.path.join(self.output_dir, '_bad')
        if os.path.exists(bad_dir):
            shutil.rmtree(bad_dir)
        if os.path.exists(self.db):
            os.remove(self.db)
        if os.path.exists(self.cache):
            os.remove(self.cache)


    def test_preprocess_orcid_api_disabled_no_leak(self):
        """With ORCID API disabled, authors should not contain [orcid:] unless the DOI is in the provided index.
        Our sample input DOIs with authors having ORCID nameIdentifiers are not covered by the sample index (iod),
        so no [orcid:] should appear in the subject CSVs."""

        # Pre-clean
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)

        citations_output_path = self.output_dir + "_citations"
        if os.path.exists(citations_output_path):
            shutil.rmtree(citations_output_path)

        # assicura corretto funzionamento di _bad
        bad_dir = os.path.join(self.output_dir, '_bad')
        if os.path.exists(bad_dir):
            shutil.rmtree(bad_dir)

        if os.path.exists(self.db):
            os.remove(self.db)
        if os.path.exists(self.cache):
            os.remove(self.cache)

        # Run the process with ORCID API disabled
        preprocess(
            datacite_json_dir=self.json_dir,
            publishers_filepath=self.publisher_mapping,
            orcid_doi_filepath=self.iod,
            csv_dir=self.output_dir,
            cache=self.cache,
            use_orcid_api=False,
        )

        # Scan subject CSVs and ensure authors contain no “[orcid:” token
        subject_rows = 0
        orcid_mentions = 0
        for fname in os.listdir(self.output_dir):
            if fname.endswith("_subject.csv"):
                with open(os.path.join(self.output_dir, fname), encoding="utf-8") as f:
                    rdr = csv.DictReader(f)
                    for row in rdr:
                        subject_rows += 1
                        if "[orcid:" in row.get("author", ""):
                            orcid_mentions += 1

        self.assertGreater(subject_rows, 0)
        self.assertEqual(orcid_mentions, 0)

        # Post-clean
        # CLEAN: output, _bad, decompressioni e db
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)
        bad_dir = os.path.join(self.output_dir, '_bad')
        if os.path.exists(bad_dir):
            shutil.rmtree(bad_dir)
        if os.path.exists(self.db):
            os.remove(self.db)
        if os.path.exists(self.cache):
            os.remove(self.cache)

    def test_any_db_creation_redis_no_testing(self):

        # Pre-clean
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)

        citations_output_path = self.output_dir + "_citations"
        if os.path.exists(citations_output_path):
            shutil.rmtree(citations_output_path)

        # assicura corretto funzionamento di _bad
        bad_dir = os.path.join(self.output_dir, '_bad')
        if os.path.exists(bad_dir):
            shutil.rmtree(bad_dir)

        if os.path.exists(self.db):
            os.remove(self.db)
        if os.path.exists(self.cache):
            os.remove(self.cache)

        try:
            rsm = RedisStorageManager(testing=False)
            rsm.set_value("TEST VALUE", False)
            run_test = True
        except:
            run_test = False
            print("test skipped: 'test_any_db_creation_redis_no_testing': Connect to redis before running the test")

        if run_test:
            rsm.del_value("TEST VALUE")
            if not len(rsm.get_all_keys()):
                preprocess(datacite_json_dir=self.json_dir, publishers_filepath=self.publisher_mapping,
                           orcid_doi_filepath=self.iod, csv_dir=self.output_dir, redis_storage_manager=True,
                           storage_path=self.db, cache=self.cache)

                rsm.delete_storage()

            else:

                print("test skipped: 'test_storage_management_no_testing' because redis db 2 is not empty")

        # Post-clean
        # CLEAN: output, _bad, decompressioni e db
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)
        bad_dir = os.path.join(self.output_dir, '_bad')
        if os.path.exists(bad_dir):
            shutil.rmtree(bad_dir)
        if os.path.exists(self.db):
            os.remove(self.db)
        if os.path.exists(self.cache):
            os.remove(self.cache)

    def test_cache(self):
        'Nothing should be produced in output, since the cache file reports that all the files in input were completed'

        # Pre-clean
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)

        citations_output_path = self.output_dir + "_citations"
        if os.path.exists(citations_output_path):
            shutil.rmtree(citations_output_path)

        # assicura corretto funzionamento di _bad
        bad_dir = os.path.join(self.output_dir, '_bad')
        if os.path.exists(bad_dir):
            shutil.rmtree(bad_dir)

        if os.path.exists(self.db):
            os.remove(self.db)
        if os.path.exists(self.cache):
            os.remove(self.cache)

        with open(self.cache_test, "w", encoding="utf-8") as write_cache:
            processed_files_dict = {'first_iteration': ['jSonFile_1', 'jSonFile_2'],
                                    'second_iteration': ['jSonFile_1', 'jSonFile_2']}
            json.dump(processed_files_dict, write_cache)

        preprocess(datacite_json_dir=self.json_dir, publishers_filepath=self.publisher_mapping,
                   orcid_doi_filepath=self.iod, csv_dir=self.output_dir, redis_storage_manager=False,
                   storage_path=self.db, cache=self.cache_test)

        citations_in_output = 0
        encountered_ids = set()
        unique_entities = 0

        for file in os.listdir(citations_output_path):
            with open(os.path.join(citations_output_path, file), 'r', encoding='utf-8') as f:
                cits_rows = list(csv.DictReader(f))
                citations_in_output += len(cits_rows)
                for x in cits_rows:
                    citing_ids = x["citing"].split(" ")
                    citied_ids = x["cited"].split(" ")
                    if all(id not in encountered_ids for id in citing_ids):
                        unique_entities += 1
                        encountered_ids.update(citing_ids)

                    if all(id not in encountered_ids for id in citied_ids):
                        unique_entities += 1
                        encountered_ids.update(citied_ids)

        expected_citations_in_output = 0

        expected_entities_in_output = 0

        self.assertEqual(expected_entities_in_output, unique_entities)
        self.assertEqual(expected_citations_in_output, citations_in_output)

        # Post-clean
        # CLEAN: output, _bad, decompressioni e db
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)
        bad_dir = os.path.join(self.output_dir, '_bad')
        if os.path.exists(bad_dir):
            shutil.rmtree(bad_dir)
        if os.path.exists(self.db):
            os.remove(self.db)
        if os.path.exists(self.cache):
            os.remove(self.cache)

if __name__ == '__main__':
    unittest.main()
