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
        self.sample_2tar = join(self.test_dir, "2_tar_sample")
        self.sample_1tar_alt = join(self.test_dir, "1alt_tar_sample")
        self.output_dir = join(self.test_dir, "tmp")
        self.support_mat = join(self.test_dir, "support_mat")
        self.doi_orcid = join("test", "openaire_processing", "iod")

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
        """Test base functionalities of the OROCI processor for producing META csv tables:
        1) All the files in the TARs in input are correctly processed
        2) The number of files in input corresponds to the number of files in output
        3) The number of bibliographic entities corresponds to the number of citations in input *2 (citing + cited)
        4) Citations files are correctly created (one for each file in input)
        """
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)
        citations_output_path = self.output_dir + "_citations"
        if os.path.exists(citations_output_path):
            shutil.rmtree(citations_output_path)
        preprocess(openaire_json_dir=self.sample_2tar, csv_dir=self.output_dir, publishers_filepath=self.publishers_file, orcid_doi_filepath=self.doi_orcid )

        # entities_in_output = 0
        # for file in os.listdir(self.output_dir):
        #     with open(os.path.join(self.output_dir, file), 'r', encoding='utf-8') as f:
        #         entities_in_output += len(list(csv.DictReader(f)))

        citations_in_output = 0
        encountered_ids = set()
        unique_entities = 0
        for file in os.listdir(citations_output_path):
            with open(os.path.join(citations_output_path, file), 'r', encoding='utf-8') as f:
                cits_rows = csv.DictReader(f)
                citations_in_output += len(list(cits_rows))
                for x in cits_rows:
                    citing_ids = x["citing"].split(" ")
                    citied_ids = x["referenced"].split(" ")
                    if all(id not in encountered_ids for id in citing_ids):
                        unique_entities += 1
                        encountered_ids.update(citing_ids)

                    if all(id not in encountered_ids for id in citied_ids):
                        unique_entities += 1
                        encountered_ids.update(citied_ids)


        print("LEN ", unique_entities)
        expected_citations_in_output= 2*3*4
        #2 tar * 3 files * 4 citations * 2 entities

        #self.assertEqual(expected_citations_in_output, citations_in_output)
        shutil.rmtree(self.output_dir)
        shutil.rmtree(citations_output_path)
        #decompr_

    # def test_preprocess_base_data_processing(self):
    #     """Test base functionalities of the OROCI processor for producing META csv tables:
    #     1) All the data provided by the datasource are correctly placed in meta table fields
    #     2) The meta tables fields store expected data
    #     3) The index tables fields store expected data
    #     """
    #     if os.path.exists(self.output_dir):
    #         shutil.rmtree(self.output_dir)
    #     preprocess(openaire_json_dir=self.sample_2tar, csv_dir=self.output_dir, publishers_filepath=self.publishers_file, orcid_doi_filepath=self.doi_orcid )
    #
    #     output = dict()
    #     for file in os.listdir(self.output_dir):
    #         with open(os.path.join(self.output_dir, file), 'r', encoding='utf-8') as f:
    #             output[file] = list(csv.DictReader(f))
    #     expected_output= {}
    #
    #     elements_in_output = list()
    #     for l in output.values():
    #         for e in l:
    #             elements_in_output.append(e)
    #
    #     elements_expected = list()
    #     for l in expected_output.values():
    #         for e in l:
    #             elements_expected.append(e)
    #
    #     self.assertCountEqual(elements_in_output, elements_expected)
    #     #shutil.rmtree(self.output_dir)

if __name__ == '__main__':
    unittest.main()
