#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2023 Arianna Moretti <arianna.moretti4@unibo.it>
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED 'AS IS' AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.

import os.path
import shutil
import unittest
from os.path import join
from oc_ds_converter.run.openaire_process import *


class OpenAireProcessTest(unittest.TestCase):
    maxDiff = None
    def setUp(self):
        self.test_dir = join("test", "openaire_process")
        self.sample_1tar = join(self.test_dir, "1_tar_sample")
        self.sample_dupl = join(self.test_dir, "duplicates_sample")
        self.sample_2tar = join(self.test_dir, "2_tar_sample")
        self.sample_1tar_alt = join(self.test_dir, "1alt_tar_sample")
        self.output_dir = join(self.test_dir, "tmp")
        self.support_mat = join(self.test_dir, "support_mat")
        self.doi_orcid = join("test", "openaire_processing", "iod")

        self.any_db = join("test", "openaire_process", "anydb.db")

        self.publishers_file = join(self.support_mat, "publishers.json")
        self.journals_file = join(self.support_mat, "journals.json")

        self.publishers_dir_todel = join(self.support_mat, "publishers")
        self.publishers_file_todel = join(self.publishers_dir_todel, "publishers.json")

        self.journals_dir_todel = join(self.support_mat, "journals")
        self.journals_file_todel = join(self.journals_dir_todel, "journals.json")

        self.madeup_data_dir = join(self.support_mat, "made_up_mat")
        self.madeup_publishers = join(self.madeup_data_dir, "publishers.json")
        self.madeup_journals = join(self.madeup_data_dir,"journals.json")
        self.madeup_input = join(self.madeup_data_dir,"input")
        self.madeup_iod = join(self.madeup_data_dir,"iod")

        self.input_dirt_short= join(self.test_dir,"csv_files_short")
        self.input_dirt_iod= join(self.test_dir,"csv_file_iod")
        self.input_dirt_sample= join(self.test_dir,"csv_files_sample")
        self.input_dirt_compr= join(self.test_dir,"CSV_iCiteMD_zipped.zip")

        self.processing_csv_row_base = os.path.join('test', 'openaire_processing')
        self._id_orcid_data = os.path.join(self.processing_csv_row_base, 'iod')

    def test_preprocess_base_decompress_and_read(self):
        """Test base functionalities of the OROCI processor for producing META csv tables and INDEX tables:
        1) All the files in the TARs in input are correctly processed
        2) The number of files in input corresponds to the number of files in output (both for citation and for meta tables)
        3) The number of bibliographic entities corresponds to the number of citations in input *2 (citing + cited)
        """
        for el in os.listdir(self.sample_2tar):
            if el.endswith("decompr_zip_dir"):
                shutil.rmtree(os.path.join(self.sample_2tar, el))

        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)

        citations_output_path = self.output_dir + "_citations"
        if os.path.exists(citations_output_path):
            shutil.rmtree(citations_output_path)
        preprocess(openaire_json_dir=self.sample_2tar, csv_dir=self.output_dir, publishers_filepath=self.publishers_file, orcid_doi_filepath=self.doi_orcid, storage_manager=SqliteStorageManager,storage_path=self.any_db)

        citations_in_output = 0
        encountered_ids = set()
        unique_entities = 0

        citations_files_n = len(list(os.listdir(citations_output_path)))
        for file in os.listdir(citations_output_path):
            with open(os.path.join(citations_output_path, file), 'r', encoding='utf-8') as f:
                cits_rows = list(csv.DictReader(f))
                citations_in_output += len(cits_rows)
                for x in cits_rows:
                    citing_ids = x["citing"].split(" ")
                    citied_ids = x["referenced"].split(" ")
                    if all(id not in encountered_ids for id in citing_ids):
                        unique_entities += 1
                        encountered_ids.update(citing_ids)

                    if all(id not in encountered_ids for id in citied_ids):
                        unique_entities += 1
                        encountered_ids.update(citied_ids)

        expected_citations_in_output= 2*3*4
        # 2 tar * 3 files * 4 citations

        expected_entities_in_output= 2*3*4*2
        # 2 tar * 3 files * 4 citations * 2 entities

        self.assertEqual(expected_entities_in_output, unique_entities)
        self.assertEqual(expected_citations_in_output, citations_in_output)

        shutil.rmtree(citations_output_path)

        meta_files_n = len(list(os.listdir(self.output_dir)))

        # Make sure that a meta table row was created for each entity and, thus, that the number of
        entities_in_meta_output = 0
        for file in os.listdir(self.output_dir):
            with open(os.path.join(self.output_dir, file), 'r', encoding='utf-8') as f:
                entities_in_meta_output += len(list(csv.DictReader(f)))

        self.assertEqual(expected_entities_in_output, entities_in_meta_output)
        self.assertEqual(unique_entities, entities_in_meta_output)

        # make sure that for each of the input files was created a citation file and a meta input file
        self.assertTrue(meta_files_n == citations_files_n == 6)

        shutil.rmtree(self.output_dir)

        for el in os.listdir(self.sample_2tar):
            if el.endswith("decompr_zip_dir"):
                shutil.rmtree(os.path.join(self.sample_2tar, el))

        os.remove(self.any_db)


    def test_preprocess_duplicates_management(self):
        """Test functionalities of the OROCI processor for producing META csv tables and INDEX tables, when multiple
        citations with a common id involved are processed. Expected output, given two citations with the same citing
        entity: three rows in meta table, two rows in citations tables
        1) All the files in the TARs in input are correctly processed
        2) The number of files in input corresponds to the number of files in output (both for citation and for meta tables)
        3) The number of bibliographic entities corresponds to the number of citations in input *2 (citing + cited)
        """
        for el in os.listdir(self.sample_dupl):
            if el.endswith("decompr_zip_dir"):
                shutil.rmtree(os.path.join(self.sample_dupl, el))

        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)

        citations_output_path = self.output_dir + "_citations"
        if os.path.exists(citations_output_path):
            shutil.rmtree(citations_output_path)
        preprocess(openaire_json_dir=self.sample_dupl, csv_dir=self.output_dir, publishers_filepath=self.publishers_file, orcid_doi_filepath=self.doi_orcid, storage_manager=SqliteStorageManager, storage_path=self.any_db)

        citations_in_output = 0
        encountered_ids = set()
        unique_entities = 0

        citations_files_n = len(list(os.listdir(citations_output_path)))
        for file in os.listdir(citations_output_path):
            with open(os.path.join(citations_output_path, file), 'r', encoding='utf-8') as f:
                cits_rows = list(csv.DictReader(f))
                citations_in_output += len(cits_rows)
                for x in cits_rows:
                    citing_ids = x["citing"].split(" ")
                    citied_ids = x["referenced"].split(" ")
                    if all(id not in encountered_ids for id in citing_ids):
                        unique_entities += 1
                        encountered_ids.update(citing_ids)

                    if all(id not in encountered_ids for id in citied_ids):
                        unique_entities += 1
                        encountered_ids.update(citied_ids)

        expected_citations_in_output= 2
        # since the citing entity is the same for both citations

        expected_entities_in_output= 3
        # since the citing entity is the same for both citations

        self.assertEqual(expected_entities_in_output, unique_entities)
        self.assertEqual(expected_citations_in_output, citations_in_output)

        shutil.rmtree(citations_output_path)
        shutil.rmtree(self.output_dir)

        for el in os.listdir(self.sample_dupl):
            if el.endswith("decompr_zip_dir"):
                shutil.rmtree(os.path.join(self.sample_dupl, el))
        os.remove(self.any_db)

    def test_any_db_creation(self):
        preprocess(openaire_json_dir=self.sample_dupl, csv_dir=self.output_dir, publishers_filepath=self.publishers_file, orcid_doi_filepath=self.doi_orcid, storage_manager=SqliteStorageManager, storage_path=self.any_db)
        self.assertTrue(os.path.exists(self.any_db))
        for el in os.listdir(self.sample_dupl):
            if el.endswith("decompr_zip_dir"):
                shutil.rmtree(os.path.join(self.sample_dupl, el))
        os.remove(self.any_db)



if __name__ == '__main__':
    unittest.main()
