import os.path
import shutil
import unittest
from os.path import join
from oc_ds_converter.run.crossref_process import *

BASE = os.path.join('test', 'crossref_processing')
TARGZ_INPUT_FOLDER = os.path.join(BASE, 'tar_gz_test')
TARGZ_INPUT = os.path.join(TARGZ_INPUT_FOLDER, '40228.tar.gz')
OUTPUT = os.path.join(BASE, 'output_dir')
PUBLISHERS_MAPPING = os.path.join(BASE, 'publishers.csv')
WANTED_DOIS_FOLDER = os.path.join(BASE, 'wanted_dois')
IOD = os.path.join(BASE, 'iod')
CACHE = os.path.join(BASE, 'cache.json')
DB = os.path.join(BASE, 'anydb.db')
TARGZ_CITED_INPUT_FOLDER = os.path.join(BASE, 'tar_gz_cited_test')
TARGZ_CITED_INPUT = os.path.join(TARGZ_CITED_INPUT_FOLDER, '3.tar.gz')

class CrossrefProcessTest(unittest.TestCase):
    def test_preprocess_base_decompress_and_read_without_cited(self):
        """CASE 1: compressed input without cited entities"""
        if os.path.exists(OUTPUT):
            shutil.rmtree(OUTPUT)

        citations_output_path = OUTPUT + "_citations"
        if os.path.exists(citations_output_path):
            shutil.rmtree(citations_output_path)

        preprocess(TARGZ_INPUT, PUBLISHERS_MAPPING, IOD, OUTPUT, redis_storage_manager=False, storage_path=DB, cache = CACHE)
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
        expected_citations_in_output=0
        self.assertEqual(expected_entities_in_output, unique_entities)
        self.assertEqual(expected_citations_in_output, citations_in_output)

        shutil.rmtree(OUTPUT)
        shutil.rmtree(citations_output_path)

    def test_preprocess_base_and_decompress_with_cited(self):
        """CASE2: compressed input with cited entities"""
        if os.path.exists(OUTPUT):
            shutil.rmtree(OUTPUT)

        citations_output_path = OUTPUT + "_citations"
        if os.path.exists(citations_output_path):
            shutil.rmtree(citations_output_path)

        preprocess(crossref_json_dir=TARGZ_CITED_INPUT, publishers_filepath=PUBLISHERS_MAPPING, orcid_doi_filepath=IOD, csv_dir=OUTPUT, redis_storage_manager=False, storage_path=DB, cache = CACHE)
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

        '''citations_files_n = len(list(os.listdir(citations_output_path)))

        #shutil.rmtree(citations_output_path)

        meta_files_n = len(list(os.listdir(OUTPUT)))

        # Make sure that a meta table row was created for each entity
        entities_in_meta_output = 0
        for file in os.listdir(OUTPUT):
            with open(os.path.join(OUTPUT, file), 'r', encoding='utf-8') as f:
                entities_in_meta_output += len(list(csv.DictReader(f)))

        self.assertEqual(expected_entities_in_output, entities_in_meta_output)
        self.assertEqual(unique_entities, entities_in_meta_output)


        # make sure that for each of the input files was created a citation file and two meta input file
        self.assertTrue(meta_files_n == 2)
        self.assertTrue(citations_files_n == 1)

        #shutil.rmtree(OUTPUT)'''
        '''os.remove(DB)'''