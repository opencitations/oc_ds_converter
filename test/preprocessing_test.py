import glob
import gzip
import json
import math
import os.path
import shutil
import tarfile
import unittest
from os import makedirs, listdir
from os.path import exists, join

import pandas as pd

from oc_ds_converter.preprocessing.datacite import DatacitePreProcessing
from oc_ds_converter.preprocessing.nih import NIHPreProcessing

BASE_DIR = os.path.join("test","preprocess")
class PreprocessingTest(unittest.TestCase):
        def setUp(self):
            self._input_dir_nih = os.path.join(BASE_DIR, "data_nih")
            self._input_dir_dc = os.path.join(BASE_DIR, "data_datacite")
            self._output_dir_preprocessing = os.path.join(BASE_DIR, "all_outputs")
            makedirs(self._output_dir_preprocessing, exist_ok=True)
            self._output_dir_nih = os.path.join(self._output_dir_preprocessing, "tmp_data_nih")
            self._input_tar_dc = os.path.join(self._input_dir_dc, "test_datastructure.tar")
            self._input_tar_dc_self_cit = os.path.join(self._input_dir_dc, "test_datastructure_self_citation.tar")
            self._output_dir_dc= os.path.join(self._output_dir_preprocessing,"tmp_data_datacite")
            self._interval = 78
            self._interval_dc=2
            self._relation_type_datacite = ["references", "isreferencedby", "cites", "iscitedby"]


        def tearDown(self):
            if exists(self._output_dir_preprocessing):
                shutil.rmtree(self._output_dir_preprocessing, ignore_errors=True)

        def test_nih_preprocessing(self):
            self._nih_pp = NIHPreProcessing(self._input_dir_nih, self._output_dir_nih, self._interval)
            self._nih_pp.split_input()
            len_lines = 0
            for file in (self._nih_pp.get_all_files(self._input_dir_nih, self._nih_pp._req_type))[0]:
                len_lines += len(pd.read_csv(file))
            number_of_files_produced = len_lines // self._interval
            if len_lines % self._interval != 0:
                number_of_files_produced += 1
            self.assertTrue(len(self._nih_pp.get_all_files(self._output_dir_nih, self._nih_pp._req_type)[0]) > 0)
            self.assertEqual(len(self._nih_pp.get_all_files(self._output_dir_nih, self._nih_pp._req_type)[0]), number_of_files_produced)

        def test_dc_preprocessing(self):
            self._dc_pp = DatacitePreProcessing(self._input_tar_dc, self._output_dir_dc, self._interval_dc)
            self._dc_pp.split_input()
            list_of_files = self._dc_pp.get_all_files(self._output_dir_dc,'.json')[0]
            out_entities_files = [file for file in list_of_files if os.path.basename(file).startswith('jSonFile')]
            if out_entities_files:
                all_processed_entities=0
                for file in out_entities_files:
                    with open(file, encoding="utf8") as f:
                        recover_dict = json.load(f)
                        data_list = recover_dict["data"]
                        all_processed_entities += len(data_list)
                self.assertEqual(all_processed_entities, 3)
                self.assertEqual(len(out_entities_files),2)
            else:
                self.fail('No entities files found')

        def test_dc_preprocessing_self_citation(self):
            self._dc_pp = DatacitePreProcessing(self._input_tar_dc_self_cit, self._output_dir_dc, self._interval_dc)
            self._dc_pp.split_input()
            list_of_files = self._dc_pp.get_all_files(self._output_dir_dc, '.json')[0]
            out_entities_files = [file for file in list_of_files if os.path.basename(file).startswith('jSonFile')]
            if out_entities_files:
                all_processed_entities = 0
                for file in out_entities_files:
                    with open(file, encoding="utf8") as f:
                        recover_dict = json.load(f)
                        data_list = recover_dict["data"]
                        all_processed_entities += len(data_list)
                self.assertEqual(all_processed_entities, 2)
                self.assertEqual(len(out_entities_files), 1)
            else:
                self.fail('No entities files found')

        def test_dc_preprocessing_interrupt_resume(self):
            # Verify partial: checkpoint exists, minimal/no output files

            state_file = os.path.join(self._output_dir_preprocessing, "processing_state.json")
            with open(state_file, 'w') as f:
                json.dump({
                    "processed_files": ["dois/updated_2023_07/part_0079.jsonl.gz"],
                    "count": 2
                }, f)

            self.assertTrue(os.path.exists(state_file))
            with open(state_file) as f:
                state = json.load(f)
            self.assertEqual(state['count'], 2)

            # 2. Resume & finish
            resume_pp = DatacitePreProcessing(self._input_tar_dc, self._output_dir_dc, self._interval_dc, state_file)
            resume_pp.split_input()

            # 3. Full check
            files_final = [f for f in resume_pp.get_all_files(self._output_dir_dc, '.json')[0]
                           if 'jSonFile' in os.path.basename(f)]

            all_entities = sum(len(json.load(open(f))['data']) for f in files_final)
            self.assertEqual(len(files_final), 1)
            self.assertEqual(all_entities, 1)


if __name__ == '__main__':
    unittest.main()