#!python
# Copyright (c) 2022 The OpenCitations Index Authors.
from __future__ import annotations

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

import configparser
import json
import os
from os.path import join
from typing import cast

import fakeredis
import redis

from oc_ds_converter.datasource.datasource import DataSource


class FakeRedisWrapper:
    def __init__(self) -> None:
        self._r = fakeredis.FakeStrictRedis()

    def get(self, resource_id: str) -> bytes | None:
        return cast(bytes | None, self._r.get(resource_id))

    def mget(self, resources_id: list[str]) -> list[bytes | None]:
        if not resources_id:
            return []
        return cast(list[bytes | None], self._r.mget(resources_id))

    def set(self, resource_id: str, value: str | bytes) -> bool | None:
        return cast(bool | None, self._r.set(resource_id, value))

    def sadd(self, resource_id: str, *values: str) -> int:
        return cast(int, self._r.sadd(resource_id, *values))

    def smembers(self, resource_id: str) -> set[bytes]:
        return cast(set[bytes], self._r.smembers(resource_id))

    def delete(self, resource_id: str) -> None:
        self._r.delete(resource_id)

    def flushdb(self) -> None:
        self._r.flushdb()

    def exists_as_set(self, resource_id: str) -> bool:
        return cast(int, self._r.scard(resource_id)) > 0

    def mexists_as_set(self, resources_id: list[str]) -> list[bool]:
        if not resources_id:
            return []
        pipe = self._r.pipeline()
        for rid in resources_id:
            pipe.scard(rid)
        results: list[int] = cast(list[int], pipe.execute())
        return [count > 0 for count in results]


class RedisDataSource(DataSource):
    _SERVICE_TO_DB_SECTION: dict[str, str] = {
        "DB-META-RA": "database 0",
        "DB-META-BR": "database 1",
        "PROCESS-DB": "database 2",
        "DOI-ORCID-INDEX": "database 3",
    }

    def __init__(self, service: str, config_filepath: str = 'config.ini') -> None:
        super().__init__(service)
        if service not in self._SERVICE_TO_DB_SECTION:
            raise ValueError(f"Unknown service: {service}")

        config = configparser.ConfigParser(allow_no_value=True)
        cur_path = os.path.dirname(os.path.abspath(__file__))
        conf_file = config_filepath if config_filepath != 'config.ini' else join(cur_path, config_filepath)
        config.read(conf_file)

        host = config.get('redis', 'host')
        port = int(config.get('redis', 'port'))
        password = config.get('redis', 'password') or None
        db_section = self._SERVICE_TO_DB_SECTION[service]
        db = int(config.get(db_section, 'db'))

        self._r = redis.Redis(
            host=host,
            port=port,
            db=db,
            password=password,
            decode_responses=True
        )

    def get(self, resource_id: str) -> str | int | object | None:
        redis_data = self._r.get(resource_id)
        if redis_data is not None:
            if isinstance(redis_data, (str, int)):
                return redis_data
            return json.loads(str(redis_data))
        return None

    def mget(self, resources_id: list[str]) -> list[object | None]:
        if not resources_id:
            return []
        result: list[object | None] = []
        raw_results = cast(list[str | None], self._r.mget(resources_id))
        for x in raw_results:
            if x and isinstance(x, (int, str, bool)):
                result.append(x)
            elif x and isinstance(x, bytes):
                result.append(json.loads(x))
            else:
                result.append(None)
        return result

    def flushdb(self) -> None:
        batch_size = 1000
        keys: list[str] = cast(list[str], self._r.keys('*'))
        for i in range(0, len(keys), batch_size):
            self._r.delete(*keys[i:i+batch_size])

    def delete(self, resource_id: str) -> None:
        self._r.delete(resource_id)

    def scan_iter(self, match: str = "*") -> object:
        return self._r.scan_iter(match=match)

    def set(self, resource_id: str, value: object) -> bool | None:
        return cast(bool | None, self._r.set(resource_id, json.dumps(value)))

    def mset(self, resources: dict[str, object]) -> bool | None:
        if resources:
            return cast(bool | None, self._r.mset({k: json.dumps(v) for k, v in resources.items()}))
        return None

    def sadd(self, resource_id: str, *values: str) -> int:
        return cast(int, self._r.sadd(resource_id, *values))

    def smembers(self, resource_id: str) -> set[str]:
        return cast(set[str], self._r.smembers(resource_id))

    def exists_as_set(self, resource_id: str) -> bool:
        return cast(int, self._r.scard(resource_id)) > 0

    def mexists_as_set(self, resources_id: list[str]) -> list[bool]:
        if not resources_id:
            return []
        pipe = self._r.pipeline()
        for rid in resources_id:
            pipe.scard(rid)
        results: list[int] = cast(list[int], pipe.execute())
        return [count > 0 for count in results]
