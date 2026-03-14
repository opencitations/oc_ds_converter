# -*- coding: utf-8 -*-
# Copyright 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.

import csv
import json
import os
from pathlib import Path

from filelock import BaseFileLock

from oc_ds_converter.oc_idmanager.oc_data_storage.in_memory_manager import InMemoryStorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.redis_manager import RedisStorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.sqlite_manager import SqliteStorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.storage_manager import StorageManager


def get_storage_manager(storage_path: str | None, testing: bool) -> StorageManager:
    if storage_path:
        if not os.path.exists(storage_path):
            parent_dir = os.path.abspath(os.path.join(storage_path, os.pardir))
            if not os.path.exists(parent_dir):
                Path(parent_dir).mkdir(parents=True, exist_ok=True)
        if storage_path.endswith(".db"):
            return SqliteStorageManager(storage_path)
        if storage_path.endswith(".json"):
            return InMemoryStorageManager(storage_path)
        raise ValueError(f"Storage path must end with .db or .json, got: {storage_path}")
    return RedisStorageManager(testing=testing)


def normalize_cache_path(cache: str | None) -> str:
    if cache:
        if not cache.endswith(".json"):
            return os.path.join(os.getcwd(), "cache.json")
        parent_dir = os.path.abspath(os.path.join(cache, os.pardir))
        if not os.path.exists(parent_dir):
            Path(parent_dir).mkdir(parents=True, exist_ok=True)
        return cache
    return os.path.join(os.getcwd(), "cache.json")


def init_process_cache(cache_path: str, lock: BaseFileLock) -> dict[str, list[str]]:
    cache_dict: dict[str, list[str]] = {"citing": [], "cited": []}
    write_new = False
    if os.path.exists(cache_path):
        with lock:
            with open(cache_path, "r", encoding="utf-8") as c:
                try:
                    cache_dict = json.load(c)
                except json.JSONDecodeError:
                    write_new = True
    else:
        write_new = True
    if write_new:
        with lock:
            with open(cache_path, "w", encoding="utf-8") as c:
                json.dump(cache_dict, c)
    return cache_dict


def mark_file_completed(
    cache_path: str,
    lock: BaseFileLock,
    filename: str,
    processing_citing: bool,
) -> None:
    iteration_key = "citing" if processing_citing else "cited"
    with lock:
        cache_dict: dict[str, list[str]] = {}
        if os.path.exists(cache_path):
            with open(cache_path, "r", encoding="utf-8") as f:
                cache_dict = json.load(f)
        if iteration_key not in cache_dict:
            cache_dict[iteration_key] = []
        file_basename = Path(filename).name
        if file_basename not in cache_dict[iteration_key]:
            cache_dict[iteration_key].append(file_basename)
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(cache_dict, f)


def delete_cache_files(cache_path: str) -> None:
    if os.path.exists(cache_path):
        os.remove(cache_path)
    lock_file = cache_path + ".lock"
    if os.path.exists(lock_file):
        os.remove(lock_file)


def create_output_dirs(csv_dir: str) -> str:
    if not os.path.exists(csv_dir):
        os.makedirs(csv_dir)
    citations_dir = csv_dir + "_citations"
    if not os.path.exists(citations_dir):
        os.makedirs(citations_dir)
    return citations_dir


def write_csv_output(filepath: str, rows: list[dict[str, str]]) -> None:
    if not rows:
        return
    with open(filepath, 'w', newline='', encoding='utf-8') as output_file:
        dict_writer = csv.DictWriter(
            output_file, rows[0].keys(), delimiter=',', quotechar='"',
            quoting=csv.QUOTE_NONNUMERIC, escapechar='\\'
        )
        dict_writer.writeheader()
        dict_writer.writerows(rows)


def cleanup_testing_storage(testing: bool) -> None:
    if testing:
        storage_manager = RedisStorageManager(testing=testing)
        storage_manager.delete_storage()


def is_file_in_cache(
    cache_dict: dict[str, list[str]],
    filename: str,
    processing_citing: bool,
) -> bool:
    file_basename = Path(filename).name
    if processing_citing:
        return bool(cache_dict.get("citing") and file_basename in cache_dict["citing"])
    return bool(cache_dict.get("cited") and file_basename in cache_dict["cited"])
