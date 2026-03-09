import json
import os
import tempfile

from oc_ds_converter.lib.file_manager import init_cache


class TestInitCache:
    def test_none_filepath(self) -> None:
        result = init_cache(None)
        assert result == set()

    def test_nonexistent_file(self) -> None:
        result = init_cache("/nonexistent/path/cache.json")
        assert result == set()

    def test_empty_cache_file(self) -> None:
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump({}, f)
            f.flush()
            result = init_cache(f.name)
        os.unlink(f.name)
        assert result == set()

    def test_cache_with_data(self) -> None:
        cache_data = {
            "citing": ["file1.json", "file2.json", "file3.json"],
            "cited": ["file2.json", "file3.json", "file4.json"]
        }
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(cache_data, f)
            f.flush()
            result = init_cache(f.name)
        os.unlink(f.name)
        assert result == {"file2.json", "file3.json"}

    def test_cache_no_intersection(self) -> None:
        cache_data = {
            "citing": ["file1.json", "file2.json"],
            "cited": ["file3.json", "file4.json"]
        }
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(cache_data, f)
            f.flush()
            result = init_cache(f.name)
        os.unlink(f.name)
        assert result == set()

    def test_cache_empty_lists(self) -> None:
        cache_data = {"citing": [], "cited": []}
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(cache_data, f)
            f.flush()
            result = init_cache(f.name)
        os.unlink(f.name)
        assert result == set()

    def test_cache_only_citing_key(self) -> None:
        cache_data = {"citing": ["file1.json", "file2.json"]}
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(cache_data, f)
            f.flush()
            result = init_cache(f.name)
        os.unlink(f.name)
        assert result == set()

    def test_cache_only_cited_key(self) -> None:
        cache_data = {"cited": ["file1.json", "file2.json"]}
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(cache_data, f)
            f.flush()
            result = init_cache(f.name)
        os.unlink(f.name)
        assert result == set()
