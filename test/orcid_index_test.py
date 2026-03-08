#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
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

import os
import shutil
import tempfile
import unittest

from oc_ds_converter.datasource.orcid_index import OrcidIndexRedis, load_orcid_index_to_redis


class TestOrcidIndexRedis(unittest.TestCase):
    def setUp(self) -> None:
        self.orcid_index = OrcidIndexRedis(testing=True)

    def test_get_value_empty(self) -> None:
        result = self.orcid_index.get_value("doi:10.1234/nonexistent")
        self.assertIsNone(result)

    def test_has_data_empty(self) -> None:
        self.assertFalse(self.orcid_index.has_data())

    def test_add_values_batch_and_get_value(self) -> None:
        data = {
            "doi:10.1234/test1": {"Author One orcid:0000-0001-2345-6789", "Author Two orcid:0000-0002-3456-7890"},
            "doi:10.1234/test2": {"Author Three orcid:0000-0003-4567-8901"},
        }
        self.orcid_index.add_values_batch(data)

        result1 = self.orcid_index.get_value("doi:10.1234/test1")
        self.assertEqual(result1, {"Author One orcid:0000-0001-2345-6789", "Author Two orcid:0000-0002-3456-7890"})

        result2 = self.orcid_index.get_value("doi:10.1234/test2")
        self.assertEqual(result2, {"Author Three orcid:0000-0003-4567-8901"})

    def test_has_data_after_insert(self) -> None:
        self.orcid_index.add_values_batch({"doi:10.1234/test": {"value"}})
        self.assertTrue(self.orcid_index.has_data())

    def test_clear(self) -> None:
        self.orcid_index.add_values_batch({"doi:10.1234/test": {"value"}})
        self.assertTrue(self.orcid_index.has_data())
        self.orcid_index.clear()
        self.assertFalse(self.orcid_index.has_data())


class TestLoadOrcidIndexToRedis(unittest.TestCase):
    def setUp(self) -> None:
        self.orcid_index = OrcidIndexRedis(testing=True)
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self) -> None:
        shutil.rmtree(self.temp_dir)

    def test_load_from_directory(self) -> None:
        csv_content = '''"id","value"
"10.1234/article1","Smith, John orcid:0000-0001-2345-6789"
"10.1234/article1","Doe, Jane orcid:0000-0002-3456-7890"
"10.1234/article2","Brown, Alice orcid:0000-0003-4567-8901"
'''
        csv_path = os.path.join(self.temp_dir, "orcid_index.csv")
        with open(csv_path, "w", encoding="utf-8") as f:
            f.write(csv_content)

        load_orcid_index_to_redis(self.temp_dir, self.orcid_index)

        result1 = self.orcid_index.get_value("10.1234/article1")
        self.assertEqual(result1, {"Smith, John orcid:0000-0001-2345-6789", "Doe, Jane orcid:0000-0002-3456-7890"})

        result2 = self.orcid_index.get_value("10.1234/article2")
        self.assertEqual(result2, {"Brown, Alice orcid:0000-0003-4567-8901"})

    def test_load_nonexistent_directory(self) -> None:
        load_orcid_index_to_redis("/nonexistent/path", self.orcid_index)
        self.assertFalse(self.orcid_index.has_data())


if __name__ == "__main__":
    unittest.main()
