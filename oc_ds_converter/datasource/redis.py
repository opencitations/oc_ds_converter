#!python
# Copyright (c) 2022 The OpenCitations Index Authors.
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

import redis

from oc_ds_converter.datasource.datasource import DataSource


class RedisDataSource(DataSource):
    def __init__(self, service, config_filepath: str = 'config.ini'):
        super().__init__(service)
        config = configparser.ConfigParser(allow_no_value=True)
        cur_path = os.path.dirname(os.path.abspath(__file__))
        conf_file = config_filepath if config_filepath != 'config.ini' else join(cur_path, config_filepath)
        config.read(conf_file)
        if service == "DB-META-RA":
            self._r = redis.Redis(
                host='127.0.0.1',
                port=int(config.get('redis', 'port')),
                db=int(config.get('database 0', 'db')),
                password=None,
                decode_responses=True
            )
        elif service == "DB-META-BR":
            self._r = redis.Redis(
                host='127.0.0.1',
                port=int(config.get('redis', 'port')),
                db=int(config.get('database 1', 'db')),
                password=None,
                decode_responses=True
            )
        elif service == "PROCESS-DB":
            self._r = redis.Redis(
                host='127.0.0.1',
                port=int(config.get('redis', 'port')),
                db=int(config.get('database 2', 'db')),
                password=None,
                decode_responses=True
            )

        else:
            raise ValueError

    def get(self, resource_id: str) -> object:
        redis_data = self._r.get(resource_id)
        if redis_data is not None:
            if isinstance(redis_data, (str, int)):
                return redis_data
            return json.loads(redis_data)
        return None

    def mget(self, resources_id: list[str]) -> list[object]:
        if resources_id:
            result: list[object] = []
            for x in self._r.mget(resources_id):
                if x and isinstance(x, (int, str, bool)):
                    result.append(x)
                elif x and isinstance(x, bytes):
                    result.append(json.loads(x))
                else:
                    result.append(None)
            return result
        return []

    def flushdb(self) -> None:
        batch_size = 1000
        keys = list(self._r.keys('*'))
        for i in range(0, len(keys), batch_size):
            self._r.delete(*keys[i:i+batch_size])

    def delete(self, resource_id: str) -> None:
        self._r.delete(resource_id)

    def scan_iter(self, match: str = "*") -> object:
        return self._r.scan_iter(match=match)

    def set(self, resource_id: str, value: object) -> object:
        return self._r.set(resource_id, json.dumps(value))

    def mset(self, resources: dict[str, object]) -> object:
        if resources:
            return self._r.mset({k: v for k, v in resources.items()})
        return None