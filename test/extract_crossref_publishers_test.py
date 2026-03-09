import os
import tempfile
import time
import unittest
from unittest.mock import MagicMock, patch

from oc_ds_converter.crossref.extract_crossref_publishers import (
    get_publishers,
    get_via_requests,
    is_stale,
    process,
    store_csv_on_file,
)


class TestIsStale(unittest.TestCase):
    def test_nonexistent_file_is_stale(self) -> None:
        result = is_stale('/nonexistent/file.csv', 30)
        self.assertTrue(result)

    def test_recent_file_is_not_stale(self) -> None:
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write('test')
            temp_path = f.name
        try:
            result = is_stale(temp_path, 30)
            self.assertFalse(result)
        finally:
            os.unlink(temp_path)

    def test_old_file_is_stale(self) -> None:
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write('test')
            temp_path = f.name
        try:
            old_time = time.time() - (40 * 24 * 60 * 60)
            os.utime(temp_path, (old_time, old_time))
            result = is_stale(temp_path, 30)
            self.assertTrue(result)
        finally:
            os.unlink(temp_path)


class TestGetViaRequests(unittest.TestCase):
    @patch('oc_ds_converter.crossref.extract_crossref_publishers.get')
    def test_successful_request(self, mock_get: MagicMock) -> None:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '{"message": "success"}'
        mock_get.return_value = mock_response

        result = get_via_requests("https://api.crossref.org/members")

        self.assertEqual(result, {"message": "success"})

    @patch('oc_ds_converter.crossref.extract_crossref_publishers.get')
    def test_404_response(self, mock_get: MagicMock) -> None:
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response

        result = get_via_requests("https://api.crossref.org/members")

        self.assertIsNone(result)

    @patch('oc_ds_converter.crossref.extract_crossref_publishers.sleep')
    @patch('oc_ds_converter.crossref.extract_crossref_publishers.get')
    def test_retry_on_server_error(self, mock_get: MagicMock, mock_sleep: MagicMock) -> None:
        mock_response_500 = MagicMock()
        mock_response_500.status_code = 500
        mock_response_200 = MagicMock()
        mock_response_200.status_code = 200
        mock_response_200.text = '{"data": "ok"}'
        mock_get.side_effect = [mock_response_500, mock_response_200]

        result = get_via_requests("https://api.crossref.org/members")

        self.assertEqual(result, {"data": "ok"})
        self.assertEqual(mock_get.call_count, 2)
        mock_sleep.assert_called_once_with(5)

    @patch('oc_ds_converter.crossref.extract_crossref_publishers.sleep')
    @patch('oc_ds_converter.crossref.extract_crossref_publishers.get')
    def test_retry_on_exception(self, mock_get: MagicMock, mock_sleep: MagicMock) -> None:
        mock_response_200 = MagicMock()
        mock_response_200.status_code = 200
        mock_response_200.text = '{"data": "ok"}'
        mock_get.side_effect = [ConnectionError("Connection refused"), mock_response_200]

        result = get_via_requests("https://api.crossref.org/members")

        self.assertEqual(result, {"data": "ok"})
        self.assertEqual(mock_get.call_count, 2)
        mock_sleep.assert_called_once_with(5)

    @patch('oc_ds_converter.crossref.extract_crossref_publishers.sleep')
    @patch('oc_ds_converter.crossref.extract_crossref_publishers.get')
    def test_max_retries_exceeded(self, mock_get: MagicMock, mock_sleep: MagicMock) -> None:
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_get.return_value = mock_response

        with self.assertRaises(ConnectionError):
            get_via_requests("https://api.crossref.org/members")

        self.assertEqual(mock_get.call_count, 5)
        self.assertEqual(mock_sleep.call_count, 5)


class TestGetPublishers(unittest.TestCase):
    @patch('oc_ds_converter.crossref.extract_crossref_publishers.get_via_requests')
    def test_successful_response(self, mock_get_via_requests: MagicMock) -> None:
        mock_get_via_requests.return_value = {
            "message": {
                "total-results": 15000,
                "items": [
                    {"id": 1, "primary-name": "Publisher A", "prefix": [{"value": "10.1000"}]},
                    {"id": 2, "primary-name": "Publisher B", "prefix": [{"value": "10.2000"}]},
                ]
            }
        }

        result = get_publishers(0)

        self.assertIsNotNone(result)
        items, new_offset, total = result  # type: ignore[misc]
        self.assertEqual(len(items), 2)
        self.assertEqual(new_offset, 1000)
        self.assertEqual(total, 15000)

    @patch('oc_ds_converter.crossref.extract_crossref_publishers.get_via_requests')
    def test_none_response(self, mock_get_via_requests: MagicMock) -> None:
        mock_get_via_requests.return_value = None

        result = get_publishers(0)

        self.assertIsNone(result)


class TestStoreCSVOnFile(unittest.TestCase):
    def test_create_new_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = os.path.join(tmpdir, "publishers.csv")
            store_csv_on_file(filepath, ("id", "name", "prefix"), {"id": "1", "name": "Test Publisher", "prefix": "10.1234"})

            with open(filepath, "r", encoding="utf8") as f:
                content = f.read()

            self.assertIn('"id","name","prefix"', content)
            self.assertIn('"1","Test Publisher","10.1234"', content)

    def test_append_to_existing_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = os.path.join(tmpdir, "publishers.csv")
            store_csv_on_file(filepath, ("id", "name", "prefix"), {"id": "1", "name": "Publisher A", "prefix": "10.1000"})
            store_csv_on_file(filepath, ("id", "name", "prefix"), {"id": "2", "name": "Publisher B", "prefix": "10.2000"})

            with open(filepath, "r", encoding="utf8") as f:
                lines = f.readlines()

            self.assertEqual(len(lines), 3)
            self.assertIn('"id","name","prefix"', lines[0])
            self.assertIn('"1","Publisher A","10.1000"', lines[1])
            self.assertIn('"2","Publisher B","10.2000"', lines[2])


class TestProcess(unittest.TestCase):
    @patch('oc_ds_converter.crossref.extract_crossref_publishers.get_publishers')
    def test_process_new_file(self, mock_get_publishers: MagicMock) -> None:
        mock_get_publishers.return_value = (
            [
                {"id": 1, "primary-name": "Publisher A", "prefix": [{"value": "10.1000"}]},
                {"id": 2, "primary-name": "Publisher B", "prefix": [{"value": "10.2000"}, {"value": "10.2001"}]},
            ],
            1000,
            2
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = os.path.join(tmpdir, "publishers.csv")
            result = process(filepath)

            self.assertTrue(result)
            with open(filepath, "r", encoding="utf8") as f:
                lines = f.readlines()

            self.assertEqual(len(lines), 4)
            self.assertIn('"id","name","prefix"', lines[0])
            self.assertIn('"1","Publisher A","10.1000"', lines[1])
            self.assertIn('"2","Publisher B","10.2000"', lines[2])
            self.assertIn('"2","Publisher B","10.2001"', lines[3])

    @patch('oc_ds_converter.crossref.extract_crossref_publishers.get_publishers')
    def test_process_with_existing_data_deduplication(self, mock_get_publishers: MagicMock) -> None:
        mock_get_publishers.return_value = (
            [
                {"id": 1, "primary-name": "Publisher A", "prefix": [{"value": "10.1000"}]},
                {"id": 2, "primary-name": "Publisher B", "prefix": [{"value": "10.2000"}]},
            ],
            1000,
            2
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = os.path.join(tmpdir, "publishers.csv")
            with open(filepath, "w", encoding="utf8") as f:
                f.write('"id","name","prefix"\n')
                f.write('"1","Publisher A","10.1000"\n')

            process(filepath, force=True)

            with open(filepath, "r", encoding="utf8") as f:
                lines = f.readlines()

            self.assertEqual(len(lines), 3)
            id_occurrences = sum(1 for line in lines if '"1"' in line and "Publisher A" in line)
            self.assertEqual(id_occurrences, 1)

    @patch('oc_ds_converter.crossref.extract_crossref_publishers.get_publishers')
    def test_process_html_unescape(self, mock_get_publishers: MagicMock) -> None:
        mock_get_publishers.return_value = (
            [{"id": 1, "primary-name": "Publisher &amp; Co", "prefix": [{"value": "10.1000"}]}],
            1000,
            1
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = os.path.join(tmpdir, "publishers.csv")
            process(filepath)

            with open(filepath, "r", encoding="utf8") as f:
                content = f.read()

            self.assertIn("Publisher & Co", content)
            self.assertNotIn("&amp;", content)

    @patch('oc_ds_converter.crossref.extract_crossref_publishers.get_publishers')
    def test_process_api_failure_breaks_loop(self, mock_get_publishers: MagicMock) -> None:
        mock_get_publishers.return_value = None

        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = os.path.join(tmpdir, "publishers.csv")
            process(filepath)

            self.assertFalse(os.path.exists(filepath))

    @patch('oc_ds_converter.crossref.extract_crossref_publishers.get_publishers')
    def test_process_multiple_pages(self, mock_get_publishers: MagicMock) -> None:
        mock_get_publishers.side_effect = [
            ([{"id": 1, "primary-name": "Publisher A", "prefix": [{"value": "10.1000"}]}], 1000, 2000),
            ([{"id": 2, "primary-name": "Publisher B", "prefix": [{"value": "10.2000"}]}], 2000, 2000),
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = os.path.join(tmpdir, "publishers.csv")
            process(filepath)

            with open(filepath, "r", encoding="utf8") as f:
                lines = f.readlines()

            self.assertEqual(len(lines), 3)
            self.assertEqual(mock_get_publishers.call_count, 2)

    @patch('oc_ds_converter.crossref.extract_crossref_publishers.get_publishers')
    def test_process_duplicate_prefix_same_publisher(self, mock_get_publishers: MagicMock) -> None:
        mock_get_publishers.return_value = (
            [{"id": 1, "primary-name": "Publisher A", "prefix": [{"value": "10.1000"}, {"value": "10.1000"}]}],
            1000,
            1
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = os.path.join(tmpdir, "publishers.csv")
            process(filepath)

            with open(filepath, "r", encoding="utf8") as f:
                lines = f.readlines()

            self.assertEqual(len(lines), 2)

    @patch('oc_ds_converter.crossref.extract_crossref_publishers.get_publishers')
    def test_process_skips_if_file_recent(self, mock_get_publishers: MagicMock) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = os.path.join(tmpdir, "publishers.csv")
            with open(filepath, "w", encoding="utf8") as f:
                f.write('"id","name","prefix"\n')
                f.write('"1","Existing Publisher","10.1000"\n')

            result = process(filepath, max_age_days=30, force=False)

            self.assertFalse(result)
            mock_get_publishers.assert_not_called()

    @patch('oc_ds_converter.crossref.extract_crossref_publishers.get_publishers')
    def test_process_force_updates_recent_file(self, mock_get_publishers: MagicMock) -> None:
        mock_get_publishers.return_value = (
            [{"id": 2, "primary-name": "New Publisher", "prefix": [{"value": "10.2000"}]}],
            1000,
            1
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = os.path.join(tmpdir, "publishers.csv")
            with open(filepath, "w", encoding="utf8") as f:
                f.write('"id","name","prefix"\n')
                f.write('"1","Existing Publisher","10.1000"\n')

            result = process(filepath, max_age_days=30, force=True)

            self.assertTrue(result)
            mock_get_publishers.assert_called_once()

    @patch('oc_ds_converter.crossref.extract_crossref_publishers.get_publishers')
    def test_process_updates_stale_file(self, mock_get_publishers: MagicMock) -> None:
        mock_get_publishers.return_value = (
            [{"id": 2, "primary-name": "New Publisher", "prefix": [{"value": "10.2000"}]}],
            1000,
            1
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = os.path.join(tmpdir, "publishers.csv")
            with open(filepath, "w", encoding="utf8") as f:
                f.write('"id","name","prefix"\n')
                f.write('"1","Existing Publisher","10.1000"\n')

            old_time = time.time() - (40 * 24 * 60 * 60)
            os.utime(filepath, (old_time, old_time))

            result = process(filepath, max_age_days=30, force=False)

            self.assertTrue(result)
            mock_get_publishers.assert_called_once()


if __name__ == '__main__':
    unittest.main()
