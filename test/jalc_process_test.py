
from oc_ds_converter.run.jalc_process import *

BASE = os.path.join('test', 'jalc_process')
OUTPUT1 = os.path.join(BASE, 'meta_input_without_citing')
OUTPUT2 = os.path.join(BASE, 'meta_input_with_citing')
MULTIPROCESS_OUTPUT = os.path.join(BASE, 'multi_process_test')
CITING_ENTITIES = os.path.join(BASE, 'cit_map_dir')
OUTPUT = os.path.join(BASE, 'output')
SUPPORT_MATERIAL = os.path.join(BASE, 'support_material')
IOD_SUPPORT = os.path.join(SUPPORT_MATERIAL, 'iod')
INPUT_SUPPORT = os.path.join(SUPPORT_MATERIAL, 'input')
PUBLISHERS_SUPPORT = os.path.join(SUPPORT_MATERIAL, 'publishers.csv')


import os.path
import shutil
import unittest
from os.path import join
from oc_ds_converter.run.jalc_process import *

class TestJalcProcess(unittest.TestCase):
    def setUp(self):
        self.test_dir = join("test", "jalc_process")
        self.sample_dump_dir= join(self.test_dir, "sample_dump")
        self.sample_fake_dump_dir = join(self.test_dir, "sample_fake_dump")
        self.output_dir = join(self.test_dir, "output_dir")
        self.support_mat = join(self.test_dir, "support_mat")
        self.cache_test = join(self.support_mat, "cache_1.json")
        self.any_db = join(self.test_dir, "anydb.db")
        self.any_db1 = join(self.test_dir, "anydb1.db")
        self.publishers_file = join(self.support_mat, "publishers.csv")
        self.orcid_doi = join(self.support_mat, "iod")
        self.sample_dupl = join(self.test_dir, "duplicates_sample")
        self.cache_test1 = join(self.support_mat, "cache_test1.json")

    def test_preprocess_base_decompress_and_read(self):
        """Test base functionalities of the JALC processor for producing META csv tables and INDEX tables:
        1) All the files in the ZIPs in input are correctly processed
        2) The number of files in input corresponds to the number of files in output for citations
        3) The number of files in input are duplicated in the output folder for both citing and cited entities
        """
        for el in os.listdir(self.sample_dump_dir):
            if el.endswith("decompr_zip_dir"):
                shutil.rmtree(os.path.join(self.sample_dump_dir, el))

        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)

        citations_output_path = self.output_dir + "_citations"
        if os.path.exists(citations_output_path):
            shutil.rmtree(citations_output_path)
        preprocess(jalc_json_dir=self.sample_dump_dir, publishers_filepath=self.publishers_file, orcid_doi_filepath=self.orcid_doi, csv_dir=self.output_dir, redis_storage_manager=False, storage_path=self.any_db, cache=self.cache_test)

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

        expected_citations_in_output = 8
        #first zip: {"citing":"10.11426/nagare1970.2.3_3", "cited":"10.1017/S0022112062000762"}, {"citing":"10.11426/nagare1970.2.4_1", "cited": "10.1295/kobunshi.16.842"},
        #{"citing":"10.11426/nagare1970.2.4_1", "cited":"10.1295/kobunshi.16.921"}, {"citing": "10.11426/nagare1970.3.3_13","cited": "10.1002/zamm.19210010401"}, {"citing": "10.11426/nagare1970.3.3_13","cited":"10.1002/zamm.19210010402"},
        #second zip: {"citing":"10.14825/kaseki.68.0_14", "cited":"10.1126/science.235.4793.1156"}, {"citing":"10.14825/kaseki.68.0_14", "cited":"10.1098/rstb.1989.0091"}, {"citing":"10.14825/kaseki.68.0_18","cited": "10.5575/geosoc.96.265"}

        expected_entities_in_output = 13

        self.assertEqual(expected_entities_in_output, unique_entities)
        self.assertEqual(expected_citations_in_output, citations_in_output)

        citations_files_n = len(list(os.listdir(citations_output_path)))

        #shutil.rmtree(citations_output_path)

        meta_files_n = len(list(os.listdir(self.output_dir)))

        # Make sure that a meta table row was created for each entity
        entities_in_meta_output = 0
        for file in os.listdir(self.output_dir):
            with open(os.path.join(self.output_dir, file), 'r', encoding='utf-8') as f:
                entities_in_meta_output += len(list(csv.DictReader(f)))

        self.assertEqual(expected_entities_in_output, entities_in_meta_output)
        self.assertEqual(unique_entities, entities_in_meta_output)

        input_files_n = 0
        for el in os.listdir(self.sample_dump_dir):
            if el.endswith("decompr_zip_dir"):
                input_files_n = len(list(os.listdir(os.path.join(self.sample_dump_dir, el))))

        # make sure that for each of the input files was created a citation file and two meta input file
        self.assertTrue(meta_files_n == 2*input_files_n == 4)
        self.assertTrue(citations_files_n == input_files_n)

       # shutil.rmtree(self.output_dir)

        for el in os.listdir(self.sample_dump_dir):
            if el.endswith("decompr_zip_dir"):
                shutil.rmtree(os.path.join(self.sample_dump_dir, el))
        os.remove(self.any_db)

    def test_preprocess_wrong_doi_cited(self):
        for el in os.listdir(self.sample_fake_dump_dir):
            if el.endswith("decompr_zip_dir"):
                shutil.rmtree(os.path.join(self.sample_fake_dump_dir, el))

        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)

        citations_output_path = self.output_dir + "_citations"
        if os.path.exists(citations_output_path):
            shutil.rmtree(citations_output_path)
        preprocess(jalc_json_dir=self.sample_fake_dump_dir, publishers_filepath=self.publishers_file,
                   orcid_doi_filepath=self.orcid_doi, csv_dir=self.output_dir, redis_storage_manager=True,
                   storage_path=self.any_db, cache=self.cache_test)

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

        expected_citations_in_output = 1

        expected_entities_in_output = 2
        ''''3 cited:
         - 10.5100/jje.30.40: doi not found,
         - 10.5100/jje.33.1: https://www.jstage.jst.go.jp/article/jje1965/33/1/33_1_1/_article/-char/ja/,
         - 10.1539/joh1959.5.56: doi not found'''
        
        self.assertEqual(expected_entities_in_output, unique_entities)
        self.assertEqual(expected_citations_in_output, citations_in_output)

        shutil.rmtree(self.output_dir)
        shutil.rmtree(citations_output_path)

        for el in os.listdir(self.sample_fake_dump_dir):
            if el.endswith("decompr_zip_dir"):
                shutil.rmtree(os.path.join(self.sample_fake_dump_dir, el))
        #os.remove(self.any_db)

    def test_preprocess_base_decompress_and_read_redis(self):
        """Test base functionalities of the JALC processor for producing META csv tables and INDEX tables:
        1) All the files in the ZIPs in input are correctly processed
        2) The number of files in input corresponds to the number of files in output for citations
        3) The number of files in input are duplicated in the output folder for both citing and cited entities
        """
        for el in os.listdir(self.sample_dump_dir):
            if el.endswith("decompr_zip_dir"):
                shutil.rmtree(os.path.join(self.sample_dump_dir, el))

        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)

        citations_output_path = self.output_dir + "_citations"
        if os.path.exists(citations_output_path):
            shutil.rmtree(citations_output_path)
        preprocess(jalc_json_dir=self.sample_dump_dir, publishers_filepath=self.publishers_file, orcid_doi_filepath=self.orcid_doi, csv_dir=self.output_dir, max_workers=2, redis_storage_manager=True, storage_path=self.any_db1, cache=self.cache_test)

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

        expected_citations_in_output = 8
        #first zip: {"citing":"10.11426/nagare1970.2.3_3", "cited":"10.1017/S0022112062000762"}, {"citing":"10.11426/nagare1970.2.4_1", "cited": "10.1295/kobunshi.16.842"},
        #{"citing":"10.11426/nagare1970.2.4_1", "cited":"10.1295/kobunshi.16.921"}, {"citing": "10.11426/nagare1970.3.3_13","cited": "10.1002/zamm.19210010401"}, {"citing": "10.11426/nagare1970.3.3_13","cited":"10.1002/zamm.19210010402"},
        #second zip: {"citing":"10.14825/kaseki.68.0_14", "cited":"10.1126/science.235.4793.1156"}, {"citing":"10.14825/kaseki.68.0_14", "cited":"10.1098/rstb.1989.0091"}, {"citing":"10.14825/kaseki.68.0_18","cited": "10.5575/geosoc.96.265"}

        expected_entities_in_output = 13

        self.assertEqual(expected_entities_in_output, unique_entities)
        self.assertEqual(expected_citations_in_output, citations_in_output)

        citations_files_n = len(list(os.listdir(citations_output_path)))

        shutil.rmtree(citations_output_path)

        meta_files_n = len(list(os.listdir(self.output_dir)))

        # Make sure that a meta table row was created for each entity
        entities_in_meta_output = 0
        for file in os.listdir(self.output_dir):
            with open(os.path.join(self.output_dir, file), 'r', encoding='utf-8') as f:
                entities_in_meta_output += len(list(csv.DictReader(f)))

        self.assertEqual(expected_entities_in_output, entities_in_meta_output)
        self.assertEqual(unique_entities, entities_in_meta_output)

        input_files_n = 0
        for el in os.listdir(self.sample_dump_dir):
            if el.endswith("decompr_zip_dir"):
                input_files_n = len(list(os.listdir(os.path.join(self.sample_dump_dir, el))))

        # make sure that for each of the input files was created a citation file and two meta input file
        self.assertTrue(meta_files_n == 2*input_files_n == 4)
        self.assertTrue(citations_files_n == input_files_n)

        shutil.rmtree(self.output_dir)

        for el in os.listdir(self.sample_dump_dir):
            if el.endswith("decompr_zip_dir"):
                shutil.rmtree(os.path.join(self.sample_dump_dir, el))
        #os.remove(self.any_db1)


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
                preprocess(jalc_json_dir=self.sample_dump_dir, publishers_filepath=self.publishers_file,
                           orcid_doi_filepath=self.orcid_doi, csv_dir=self.output_dir, redis_storage_manager=True,
                           storage_path=self.any_db, cache=self.cache_test)

                for el in os.listdir(self.sample_dump_dir):
                    if el.endswith("decompr_zip_dir"):
                        shutil.rmtree(os.path.join(self.sample_dump_dir, el))
                rsm.delete_storage()

            else:
                #print("get_all_keys()", rsm.get_all_keys())
                #rsm.delete_storage()
                print("test skipped: 'test_storage_management_no_testing' because redis db 2 is not empty")

    def test_cache(self):
        'Nothing should be produced in output, since the cache file reports that all the files in input were completed'

        for el in os.listdir(self.sample_dump_dir):
            if el.endswith("decompr_zip_dir"):
                shutil.rmtree(os.path.join(self.sample_dump_dir, el))

        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)

        citations_output_path = self.output_dir + "_citations"
        if os.path.exists(citations_output_path):
            shutil.rmtree(citations_output_path)
        with open(self.cache_test1, "w") as write_cache:
            processed_files_dict = {'first_iteration': ['10.11426.zip', '10.14825.zip'],
                                    'second_iteration': ['10.11426.zip', '10.14825.zip']}
            json.dump(processed_files_dict, write_cache)

        preprocess(jalc_json_dir=self.sample_dump_dir, orcid_doi_filepath=self.orcid_doi, csv_dir=self.output_dir,
                   publishers_filepath=self.publishers_file,
                   redis_storage_manager=True, cache=self.cache_test1)

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

        for el in os.listdir(self.sample_dump_dir):
            if el.endswith("decompr_zip_dir"):
                shutil.rmtree(os.path.join(self.sample_dump_dir, el))


if __name__ == '__main__':
    unittest.main()
#python -m unittest discover -s test -p "jalc_process_test.py"