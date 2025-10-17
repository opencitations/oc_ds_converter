import os.path
import shutil
import unittest
from os.path import join
from oc_ds_converter.run.crossref_process import *
from pathlib import Path


class CrossrefProcessTest(unittest.TestCase):
    def setUp(self) -> None:
        self.test_dir = os.path.join('test', 'crossref_processing')
        self.targz_input_folder = os.path.join(self.test_dir, 'tar_gz_test')
        self.targz_input = os.path.join(self.targz_input_folder, '40228.tar.gz')
        self.output = os.path.join(self.test_dir, 'output_dir')
        self.publisher_mapping = os.path.join(self.test_dir, 'publishers.csv')
        self.wanted_dois = os.path.join(self.test_dir, 'wanted_dois')
        self.iod = os.path.join(self.test_dir, 'iod')
        self.cache = os.path.join(self.test_dir, 'cache.json')
        self.db = os.path.join(self.test_dir, 'anydb.db')
        self.targz_cited_folder = os.path.join(self.test_dir, 'tar_gz_cited_test')
        self.targz_cited_input = os.path.join(self.targz_cited_folder, '3.json.tar.gz')
        self.gzip_input = os.path.join(self.test_dir, 'gzip_test')
        self.sample_fake_dump_dir = os.path.join(self.test_dir, 'tar_gz_wrong_cited_doi')
        self.sample_fake_dump = os.path.join(self.sample_fake_dump_dir, '1.tar.gz')
        self.any_db1 = join(self.test_dir, "anydb1.db")

    def test_preprocess_base_decompress_and_read_without_cited(self):
        """CASE 1: compressed input without cited entities"""
        if os.path.exists(self.output):
            shutil.rmtree(self.output)

        citations_output_path = self.output + "_citations"
        if os.path.exists(citations_output_path):
            shutil.rmtree(citations_output_path)

        preprocess(self.targz_input, publishers_filepath=self.publisher_mapping, orcid_doi_filepath=self.iod, csv_dir=self.output, redis_storage_manager=False, storage_path=self.db, cache=self.cache)

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
        expected_entities_in_output = 0
        expected_citations_in_output = 0
        self.assertEqual(expected_entities_in_output, unique_entities)
        self.assertEqual(expected_citations_in_output, citations_in_output)

        shutil.rmtree(self.output)
        shutil.rmtree(citations_output_path)

    def test_preprocess_base_and_decompress_with_cited(self):
        """CASE2: compressed input with cited entities"""
        if os.path.exists(self.output):
            shutil.rmtree(self.output)

        citations_output_path = self.output + "_citations"
        if os.path.exists(citations_output_path):
            shutil.rmtree(citations_output_path)

        preprocess(crossref_json_dir=self.targz_cited_input, publishers_filepath=self.publisher_mapping, orcid_doi_filepath=self.iod, csv_dir=self.output, redis_storage_manager=False, storage_path=self.db, cache=self.cache)
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
        expected_entities_in_output = 17
        expected_citations_in_output = 16
        self.assertEqual(expected_entities_in_output, unique_entities)
        self.assertEqual(expected_citations_in_output, citations_in_output)

        citations_files_n = len(list(os.listdir(citations_output_path)))

        shutil.rmtree(citations_output_path)

        meta_files_n = len(list(os.listdir(self.output)))

        # Make sure that a meta table row was created for each entity
        entities_in_meta_output = 0
        for file in os.listdir(self.output):
            with open(os.path.join(self.output, file), 'r', encoding='utf-8') as f:
                entities_in_meta_output += len(list(csv.DictReader(f)))

        self.assertEqual(expected_entities_in_output, entities_in_meta_output)
        self.assertEqual(unique_entities, entities_in_meta_output)

        # make sure that for each of the input files was created a citation file and two meta input file
        self.assertTrue(meta_files_n == 2)
        self.assertTrue(citations_files_n == 1)

        shutil.rmtree(self.output)
        if os.path.exists(self.db):
            os.remove(self.db)

    def test_preprocess_base_and_decompress_with_cited_redis(self):
        """CASE2: compressed input with cited entities"""
        if os.path.exists(self.output):
            shutil.rmtree(self.output)

        citations_output_path = self.output + "_citations"
        if os.path.exists(citations_output_path):
            shutil.rmtree(citations_output_path)

        preprocess(crossref_json_dir=self.targz_cited_input, publishers_filepath=self.publisher_mapping, orcid_doi_filepath=self.iod, csv_dir=self.output, redis_storage_manager=True, storage_path=self.any_db1, cache=self.cache)
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
        expected_entities_in_output = 17
        expected_citations_in_output = 16
        self.assertEqual(expected_entities_in_output, unique_entities)
        self.assertEqual(expected_citations_in_output, citations_in_output)

        citations_files_n = len(list(os.listdir(citations_output_path)))

        shutil.rmtree(citations_output_path)

        meta_files_n = len(list(os.listdir(self.output)))

        # Make sure that a meta table row was created for each entity
        entities_in_meta_output = 0
        for file in os.listdir(self.output):
            with open(os.path.join(self.output, file), 'r', encoding='utf-8') as f:
                entities_in_meta_output += len(list(csv.DictReader(f)))

        self.assertEqual(expected_entities_in_output, entities_in_meta_output)
        self.assertEqual(unique_entities, entities_in_meta_output)

        # make sure that for each of the input files was created a citation file and two meta input file
        self.assertTrue(meta_files_n == 2)
        self.assertTrue(citations_files_n == 1)

        shutil.rmtree(self.output)
        #os.remove(self.any_db1)

    def test_preprocess_wrong_doi_cited(self):

        if os.path.exists(self.output):
            shutil.rmtree(self.output)

        citations_output_path = self.output + "_citations"
        if os.path.exists(citations_output_path):
            shutil.rmtree(citations_output_path)

        preprocess(self.sample_fake_dump, publishers_filepath=self.publisher_mapping, orcid_doi_filepath=self.iod, csv_dir=self.output, redis_storage_manager=False, storage_path=self.db, cache=self.cache)

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

        expected_citations_in_output = 15

        expected_entities_in_output = 16

        self.assertEqual(expected_entities_in_output, unique_entities)
        self.assertEqual(expected_citations_in_output, citations_in_output)

        shutil.rmtree(self.output)
        shutil.rmtree(citations_output_path)

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
                preprocess(crossref_json_dir=self.targz_cited_input, publishers_filepath=self.publisher_mapping,
                           orcid_doi_filepath=self.iod, csv_dir=self.output, redis_storage_manager=True,
                           storage_path=self.db, cache=self.cache, verbose=True)

                rsm.delete_storage()

            else:
                #print("get_all_keys()", rsm.get_all_keys())
                #rsm.delete_storage()
                print("test skipped: 'test_storage_management_no_testing' because redis db 2 is not empty")

    def test_cache(self):
        'Nothing should be produced in output, since the cache file reports that all the files in input were completed'

        if os.path.exists(self.output):
            shutil.rmtree(self.output)

        citations_output_path = self.output + "_citations"
        if os.path.exists(citations_output_path):
            shutil.rmtree(citations_output_path)
        cache_dict = {'first_iteration': [], 'second_iteration': []}
        targz_fd = tarfile.open(self.targz_cited_input, "r:gz", encoding="utf-8")
        for cur_file in targz_fd:
            if cur_file.name.endswith('.json') and not basename(cur_file.name).startswith("."):
                cache_dict['first_iteration'].append(Path(cur_file.name).name)
                cache_dict['second_iteration'].append(Path(cur_file.name).name)

        with open(self.cache, "w") as write_cache:
            json.dump(cache_dict, write_cache)

        preprocess(crossref_json_dir=self.targz_cited_input, publishers_filepath=self.publisher_mapping,
                   orcid_doi_filepath=self.iod, csv_dir=self.output, redis_storage_manager=True,
                   storage_path=self.db, cache=self.cache)

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
        shutil.rmtree(self.output)

    def test_preprocess_orcid_api_disabled_no_index(self):
        """
        With the ORCID API disabled and without a DOI->ORCID index,
        ORCIDs must not appear in _citing.csv files.
        """
        if os.path.exists(self.output):
            shutil.rmtree(self.output)
        citations_output_path = self.output + "_citations"
        if os.path.exists(citations_output_path):
            shutil.rmtree(citations_output_path)

        preprocess(
            crossref_json_dir=self.targz_cited_input,
            publishers_filepath=self.publisher_mapping,
            orcid_doi_filepath=None,
            csv_dir=self.output,
            redis_storage_manager=False,
            storage_path=self.db,
            cache=self.cache,
            use_orcid_api=False
        )

        found_orcid = False
        for file in os.listdir(self.output):
            if file.endswith("_citing.csv"):
                with open(os.path.join(self.output, file), "r", encoding="utf-8") as f:
                    for row in csv.DictReader(f):
                        if "[orcid:" in (row.get("author", "") or ""):
                            found_orcid = True
                            break
            if found_orcid:
                break

        self.assertFalse(found_orcid)

        if os.path.exists(citations_output_path):
            shutil.rmtree(citations_output_path)
        if os.path.exists(self.output):
            shutil.rmtree(self.output)
        if os.path.exists(self.db):
            os.remove(self.db)

    def test_preprocess_orcid_api_disabled_no_leak(self):
        """With ORCID API disabled, authors should not contain [orcid:] unless the DOI is in the provided index."""
        if os.path.exists(self.output):
            shutil.rmtree(self.output)
        citations_output_path = self.output + "_citations"
        if os.path.exists(citations_output_path):
            shutil.rmtree(citations_output_path)

        preprocess(
            crossref_json_dir=self.targz_cited_input,
            publishers_filepath=self.publisher_mapping,
            orcid_doi_filepath=self.iod,
            csv_dir=self.output,
            redis_storage_manager=False,
            storage_path=self.db,
            cache=self.cache,
            use_orcid_api=False
        )

        subject_rows = 0
        orcid_mentions = 0
        for fname in os.listdir(self.output):
            if fname.endswith("_citing.csv"):
                with open(os.path.join(self.output, fname), encoding="utf-8") as f:
                    rdr = csv.DictReader(f)
                    for row in rdr:
                        subject_rows += 1
                        if "[orcid:" in row.get("author", ""):
                            orcid_mentions += 1

        self.assertGreater(subject_rows, 0)
        self.assertEqual(orcid_mentions, 0)

        shutil.rmtree(citations_output_path)
        shutil.rmtree(self.output)
        if os.path.exists(self.db):
            os.remove(self.db)


if __name__ == '__main__':
    unittest.main()
