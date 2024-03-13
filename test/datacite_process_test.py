import unittest
import os
from os.path import join
import shutil
from oc_ds_converter.run.datacite_process import preprocess
from oc_ds_converter.oc_idmanager.oc_data_storage.redis_manager import \
    RedisStorageManager
import csv
import json

class DataciteProcessTest(unittest.TestCase):
    def setUp(self) -> None:
        self.test_dir = os.path.join('test', 'datacite_process')
        self.zst_input_folder = os.path.join(self.test_dir, 'sample_dc')
        self.output_dir = os.path.join(self.test_dir, 'output_dir')
        self.processing_test_dir = os.path.join('test', 'datacite_processing')
        self.publisher_mapping = os.path.join(self.processing_test_dir, 'publishers.csv')
        self.wanted_dois = os.path.join(self.processing_test_dir, 'wanted_dois')
        self.iod = os.path.join(self.processing_test_dir, 'iod')
        self.cache = os.path.join(self.test_dir, 'cache.json')
        self.cache_test = os.path.join(self.test_dir, 'cache_test.json')
        self.db = os.path.join(self.test_dir, 'anydb.db')

    def test_preprocess_base_decompress_and_read(self):
        """Test base functionalities of the Datacite processor for producing META csv tables and INDEX tables:
        1) All the files in the zst in input are correctly processed
        2) The number of files in input corresponds to the number of files in output for citations
        3) The number of files in input are duplicated in the output folder for both citing and cited entities
        """

        for el in os.listdir(self.zst_input_folder):
            if el.endswith("decompr_zst_dir"):
                shutil.rmtree(os.path.join(self.zst_input_folder, el))

        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)

        citations_output_path = self.output_dir + "_citations"
        if os.path.exists(citations_output_path):
            shutil.rmtree(citations_output_path)

        preprocess(datacite_ndjson_dir=self.zst_input_folder, publishers_filepath=self.publisher_mapping,
                   orcid_doi_filepath=self.iod, csv_dir=self.output_dir, redis_storage_manager=False,
                   storage_path=self.db, cache=self.cache)

        citations_in_output = 0

        for file in os.listdir(citations_output_path):
            with open(os.path.join(citations_output_path, file), 'r', encoding='utf-8') as f:
                cits_rows = list(csv.DictReader(f))
                citations_in_output += len(cits_rows)

        expected_citations_in_output = 4

        expected_entities_in_output = 7

        self.assertEqual(expected_citations_in_output, citations_in_output)

        citations_files_n = len(list(os.listdir(citations_output_path)))

        shutil.rmtree(citations_output_path)

        entities_in_meta_output = 0
        for file in os.listdir(self.output_dir):
            with open(os.path.join(self.output_dir, file), 'r', encoding='utf-8') as f:
                entities_in_meta_output += len(list(csv.DictReader(f)))

        self.assertEqual(expected_entities_in_output, entities_in_meta_output)

        input_files_n = 1
        self.assertTrue(citations_files_n == input_files_n)

        shutil.rmtree(self.output_dir)

        for el in os.listdir(self.zst_input_folder):
            if el.endswith("decompr_zst_dir"):
                shutil.rmtree(os.path.join(self.zst_input_folder, el))
        if os.path.exists(self.db):
            os.remove(self.db)

    def test_preprocess_base_decompress_and_read_redis(self):
        """Test base functionalities of the Datacite processor for producing META csv tables and INDEX tables:
        1) All the files in the zst in input are correctly processed
        2) The number of files in input corresponds to the number of files in output for citations
        3) The number of files in input are duplicated in the output folder for both citing and cited entities
        """

        for el in os.listdir(self.zst_input_folder):
            if el.endswith("decompr_zst_dir"):
                shutil.rmtree(os.path.join(self.zst_input_folder, el))

        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)

        citations_output_path = self.output_dir + "_citations"
        if os.path.exists(citations_output_path):
            shutil.rmtree(citations_output_path)

        preprocess(datacite_ndjson_dir=self.zst_input_folder, publishers_filepath=self.publisher_mapping,
                   orcid_doi_filepath=self.iod, csv_dir=self.output_dir, redis_storage_manager=True,
                   storage_path=self.db, cache=self.cache)

        citations_in_output = 0

        for file in os.listdir(citations_output_path):
            with open(os.path.join(citations_output_path, file), 'r', encoding='utf-8') as f:
                cits_rows = list(csv.DictReader(f))
                citations_in_output += len(cits_rows)

        expected_citations_in_output = 4

        expected_entities_in_output = 7

        self.assertEqual(expected_citations_in_output, citations_in_output)

        citations_files_n = len(list(os.listdir(citations_output_path)))

        shutil.rmtree(citations_output_path)

        entities_in_meta_output = 0
        for file in os.listdir(self.output_dir):
            with open(os.path.join(self.output_dir, file), 'r', encoding='utf-8') as f:
                entities_in_meta_output += len(list(csv.DictReader(f)))

        self.assertEqual(expected_entities_in_output, entities_in_meta_output)

        input_files_n = 1
        self.assertTrue(citations_files_n == input_files_n)

        shutil.rmtree(self.output_dir)

        for el in os.listdir(self.zst_input_folder):
            if el.endswith("decompr_zst_dir"):
                shutil.rmtree(os.path.join(self.zst_input_folder, el))
        if os.path.exists(self.db):
            os.remove(self.db)

    def test_any_db_creation_redis_no_testing(self):
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
                preprocess(datacite_ndjson_dir=self.zst_input_folder, publishers_filepath=self.publisher_mapping,
                           orcid_doi_filepath=self.iod, csv_dir=self.output_dir, redis_storage_manager=True,
                           storage_path=self.db, cache=self.cache)

                for el in os.listdir(self.zst_input_folder):
                    if el.endswith("decompr_zst_dir"):
                        shutil.rmtree(os.path.join(self.zst_input_folder, el))
                rsm.delete_storage()

            else:

                print("test skipped: 'test_storage_management_no_testing' because redis db 2 is not empty")
        if os.path.exists(self.db):
            os.remove(self.db)

    def test_cache(self):
        'Nothing should be produced in output, since the cache file reports that all the files in input were completed'

        for el in os.listdir(self.zst_input_folder):
            if el.endswith("decompr_zst_dir"):
                shutil.rmtree(os.path.join(self.zst_input_folder, el))

        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)

        citations_output_path = self.output_dir + "_citations"
        if os.path.exists(citations_output_path):
            shutil.rmtree(citations_output_path)
        with open(self.cache_test, "w") as write_cache:
            processed_files_dict = {'first_iteration': ['chunk_1'],
                                    'second_iteration': ['chunk_1']}
            json.dump(processed_files_dict, write_cache)

        preprocess(datacite_ndjson_dir=self.zst_input_folder, publishers_filepath=self.publisher_mapping,
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

        shutil.rmtree(citations_output_path)
        shutil.rmtree(self.output_dir)

        for el in os.listdir(self.zst_input_folder):
            if el.endswith("decompr_zst_dir"):
                shutil.rmtree(os.path.join(self.zst_input_folder, el))

        if os.path.exists(self.db):
            os.remove(self.db)

if __name__ == '__main__':
    unittest.main()