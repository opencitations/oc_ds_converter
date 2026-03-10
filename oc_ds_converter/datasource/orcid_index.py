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

from __future__ import annotations

import json
from csv import DictReader
from os import sep, walk
from os.path import exists
from typing import Protocol, cast

import fakeredis

from oc_ds_converter.datasource.redis import RedisDataSource
from oc_ds_converter.lib.console import create_progress
from oc_ds_converter.oc_idmanager import DOIManager


class OrcidIndexInterface(Protocol):
    def get_value(self, id_string: str, /) -> set[str] | None: ...
    def get_values_batch(self, ids: list[str], /) -> dict[str, set[str]]: ...


class OrcidIndexRedis:
    def __init__(self, testing: bool = False) -> None:
        if testing:
            self._r = fakeredis.FakeStrictRedis(decode_responses=True)
        else:
            self._redis = RedisDataSource("DOI-ORCID-INDEX")
            self._r = self._redis._r

    def get_value(self, doi: str) -> set[str] | None:
        result = cast(set[str], self._r.smembers(doi))
        if result:
            return result
        return None

    def get_values_batch(self, dois: list[str]) -> dict[str, set[str]]:
        if not dois:
            return {}
        pipe = self._r.pipeline()
        for doi in dois:
            pipe.smembers(doi)
        results = pipe.execute()
        return {
            doi: cast(set[str], members)
            for doi, members in zip(dois, results)
            if members
        }

    def add_values_batch(self, data: dict[str, set[str]]) -> None:
        pipe = self._r.pipeline()
        for doi, values in data.items():
            if values:
                pipe.sadd(doi, *values)
        pipe.execute()

    def has_data(self) -> bool:
        return cast(int, self._r.dbsize()) > 0

    def clear(self) -> None:
        self._r.flushdb()


def load_orcid_index_to_redis(
    orcid_index_dir: str,
    orcid_index_redis: OrcidIndexRedis,
    batch_size: int = 50000,
) -> None:
    if not exists(orcid_index_dir):
        return

    files_to_process: list[str] = []
    for cur_dir, _, cur_files in walk(orcid_index_dir):
        for cur_file in cur_files:
            if cur_file.endswith('.csv'):
                files_to_process.append(cur_dir + sep + cur_file)

    doi_manager = DOIManager()
    with create_progress() as progress:
        task = progress.add_task("[green]Loading DOI-ORCID index files", total=len(files_to_process))
        batch: dict[str, set[str]] = {}
        count = 0

        for csv_path in files_to_process:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = DictReader(f)
                for row in reader:
                    raw_doi = row['id']
                    doi = doi_manager.normalise(raw_doi, include_prefix=True)
                    if not doi:
                        continue
                    value = row['value']
                    if doi not in batch:
                        batch[doi] = set()
                    batch[doi].add(value)
                    count += 1

                    if count >= batch_size:
                        orcid_index_redis.add_values_batch(batch)
                        batch = {}
                        count = 0
            progress.update(task, advance=1)

        if batch:
            orcid_index_redis.add_values_batch(batch)


class PublishersRedis:
    MEMBER_PREFIX = "member:"
    DOI_PREFIX_KEY = "prefix:"

    def __init__(self, testing: bool = False) -> None:
        if testing:
            self._r = fakeredis.FakeStrictRedis(decode_responses=True)
        else:
            self._redis = RedisDataSource("PUBLISHERS-INDEX")
            self._r = self._redis._r

    def get_by_member(self, member_id: str) -> dict[str, str | set[str]] | None:
        key = f"{self.MEMBER_PREFIX}{member_id}"
        data = self._r.get(key)
        if data:
            result = json.loads(str(data))
            result["prefixes"] = set(result["prefixes"])
            return result
        return None

    def get_by_prefix(self, prefix: str) -> dict[str, str | set[str]] | None:
        key = f"{self.DOI_PREFIX_KEY}{prefix}"
        member_id = self._r.get(key)
        if member_id:
            return self.get_by_member(str(member_id))
        return None

    def set_publisher(self, member_id: str, name: str, prefixes: set[str]) -> None:
        member_key = f"{self.MEMBER_PREFIX}{member_id}"
        data = {"name": name, "prefixes": list(prefixes)}
        self._r.set(member_key, json.dumps(data))
        for prefix in prefixes:
            prefix_key = f"{self.DOI_PREFIX_KEY}{prefix}"
            self._r.set(prefix_key, member_id)

    def set_publishers_batch(self, publishers: dict[str, dict[str, str | set[str]]]) -> None:
        pipe = self._r.pipeline()
        for member_id, data in publishers.items():
            member_key = f"{self.MEMBER_PREFIX}{member_id}"
            prefixes_list = list(data["prefixes"])
            pipe.set(member_key, json.dumps({"name": data["name"], "prefixes": prefixes_list}))
            for prefix in prefixes_list:
                prefix_key = f"{self.DOI_PREFIX_KEY}{prefix}"
                pipe.set(prefix_key, member_id)
        pipe.execute()

    def has_data(self) -> bool:
        return cast(int, self._r.dbsize()) > 0

    def clear(self) -> None:
        self._r.flushdb()


def load_publishers_to_redis(
    publishers_filepath: str,
    publishers_redis: PublishersRedis,
    batch_size: int = 5000,
) -> None:
    if not exists(publishers_filepath):
        return

    batch: dict[str, dict[str, str | set[str]]] = {}
    count = 0

    with open(publishers_filepath, 'r', encoding='utf-8') as f:
        reader = DictReader(f)
        for row in reader:
            pub_id = row['id']
            if pub_id not in batch:
                batch[pub_id] = {'name': row['name'], 'prefixes': set()}
            prefixes = batch[pub_id]['prefixes']
            prefixes.add(row['prefix'])  # type: ignore[union-attr]
            count += 1

            if count >= batch_size:
                publishers_redis.set_publishers_batch(batch)
                batch = {}
                count = 0

    if batch:
        publishers_redis.set_publishers_batch(batch)
