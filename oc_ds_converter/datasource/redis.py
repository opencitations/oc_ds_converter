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
            self._r =  redis.Redis(
                            host='127.0.0.1',
                            port=int(config.get('redis', 'port')),
                            db=(config.get('database 0', 'db')),
                            password=None,
                            decode_responses=True
                        )
        elif service == "DB-META-BR":
            self._r = redis.Redis(
                    host='127.0.0.1',
                    port=int(config.get('redis', 'port')),
                    db=(config.get('database 1', 'db')),
                    password=None,
                    decode_responses=True
                )
        elif service == "PROCESS-DB":
            self._r =  redis.Redis(
                            host='127.0.0.1',
                            port=int(config.get('redis', 'port')),
                            db=(config.get('database 2', 'db')),
                            password=None,
                            decode_responses=True
                        )

        else:
            raise ValueError

    def get(self, resource_id):
        redis_data = self._r.get(resource_id)
        if redis_data != None:
            if isinstance(redis_data, str) or isinstance(redis_data, int):
                return redis_data
            else:
                return json.loads(redis_data)
        else:
            return None

    def mget(self, resources_id):
        if resources_id:
            return [x if x and isinstance(x, (int,str,bool)) else json.loads(x) if x and isinstance(x, bytes) else None for x in self._r.mget(resources_id)]
        else:
            return[]
        # return {
        #     resources_id[i]: json.loads(v) if not v is None else None
        #     for i, v in enumerate(self._r.mget(resources_id))
        # }

    def flushall(self):
        self._r.flushall()

    def delete(self, resource_id):
        self._r.delete(resource_id)

    def scan_iter(self, match="*"):
        return self._r.scan_iter(match=match)

    def set(self, resource_id, value):
        return self._r.set(resource_id, json.dumps(value))

    def mset(self, resources):
        if resources:
            return self._r.mset({k: v for k, v in resources.items()})