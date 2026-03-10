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

from oc_ds_converter.datasource.orcid_index import (
    OrcidIndexRedis,
    PublishersRedis,
    load_orcid_index_to_redis,
    load_publishers_to_redis,
)


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

        result1 = self.orcid_index.get_value("doi:10.1234/article1")
        self.assertEqual(result1, {"Smith, John orcid:0000-0001-2345-6789", "Doe, Jane orcid:0000-0002-3456-7890"})

        result2 = self.orcid_index.get_value("doi:10.1234/article2")
        self.assertEqual(result2, {"Brown, Alice orcid:0000-0003-4567-8901"})

    def test_load_nonexistent_directory(self) -> None:
        load_orcid_index_to_redis("/nonexistent/path", self.orcid_index)
        self.assertFalse(self.orcid_index.has_data())


class TestPublishersRedis(unittest.TestCase):
    def setUp(self) -> None:
        self.publishers_redis = PublishersRedis(testing=True)

    def test_get_by_member_empty(self) -> None:
        result = self.publishers_redis.get_by_member("999")
        self.assertIsNone(result)

    def test_set_and_get_by_member(self) -> None:
        self.publishers_redis.set_publisher("123", "Test Publisher", {"10.1234", "10.5678"})
        result = self.publishers_redis.get_by_member("123")
        assert result is not None
        self.assertEqual(result["name"], "Test Publisher")
        self.assertEqual(result["prefixes"], {"10.1234", "10.5678"})

    def test_get_by_prefix(self) -> None:
        self.publishers_redis.set_publisher("123", "Test Publisher", {"10.1234"})
        result = self.publishers_redis.get_by_prefix("10.1234")
        assert result is not None
        self.assertEqual(result["name"], "Test Publisher")

    def test_get_by_prefix_not_found(self) -> None:
        result = self.publishers_redis.get_by_prefix("10.9999")
        self.assertIsNone(result)

    def test_has_data_empty(self) -> None:
        self.assertFalse(self.publishers_redis.has_data())

    def test_has_data_after_insert(self) -> None:
        self.publishers_redis.set_publisher("1", "Publisher", {"10.1000"})
        self.assertTrue(self.publishers_redis.has_data())

    def test_clear(self) -> None:
        self.publishers_redis.set_publisher("1", "Publisher", {"10.1000"})
        self.assertTrue(self.publishers_redis.has_data())
        self.publishers_redis.clear()
        self.assertFalse(self.publishers_redis.has_data())

    def test_set_publishers_batch(self) -> None:
        publishers = {
            "1": {"name": "Publisher A", "prefixes": {"10.1111"}},
            "2": {"name": "Publisher B", "prefixes": {"10.2222", "10.3333"}},
        }
        self.publishers_redis.set_publishers_batch(publishers)

        result_a = self.publishers_redis.get_by_member("1")
        assert result_a is not None
        self.assertEqual(result_a["name"], "Publisher A")
        self.assertEqual(result_a["prefixes"], {"10.1111"})

        result_b = self.publishers_redis.get_by_prefix("10.2222")
        assert result_b is not None
        self.assertEqual(result_b["name"], "Publisher B")

        result_c = self.publishers_redis.get_by_prefix("10.3333")
        assert result_c is not None
        self.assertEqual(result_c["name"], "Publisher B")


class TestLoadPublishersToRedis(unittest.TestCase):
    def setUp(self) -> None:
        self.publishers_redis = PublishersRedis(testing=True)
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self) -> None:
        shutil.rmtree(self.temp_dir)

    def test_load_from_csv(self) -> None:
        csv_content = '''"id","name","prefix"
"100","Test Publisher 1","10.100"
"100","Test Publisher 1","10.101"
"200","Test Publisher 2","10.200"
'''
        csv_path = os.path.join(self.temp_dir, "publishers.csv")
        with open(csv_path, "w", encoding="utf-8") as f:
            f.write(csv_content)

        load_publishers_to_redis(csv_path, self.publishers_redis)

        result1 = self.publishers_redis.get_by_member("100")
        assert result1 is not None
        self.assertEqual(result1["name"], "Test Publisher 1")
        self.assertEqual(result1["prefixes"], {"10.100", "10.101"})

        result2 = self.publishers_redis.get_by_prefix("10.200")
        assert result2 is not None
        self.assertEqual(result2["name"], "Test Publisher 2")

    def test_load_nonexistent_file(self) -> None:
        load_publishers_to_redis("/nonexistent/file.csv", self.publishers_redis)
        self.assertFalse(self.publishers_redis.has_data())


if __name__ == "__main__":
    unittest.main()
