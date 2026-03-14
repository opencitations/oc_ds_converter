# -*- coding: utf-8 -*-
# Copyright 2022-2026 Arcangelo Massari <arcangelo.massari@unibo.it>
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

from __future__ import annotations

import html
import json
import os
import re
from abc import abstractmethod
from pathlib import Path

from bs4 import BeautifulSoup

from oc_ds_converter.datasource.orcid_index import OrcidIndexRedis, PublishersRedis
from oc_ds_converter.datasource.redis import FakeRedisWrapper, RedisDataSource
from oc_ds_converter.oc_idmanager import ORCIDManager
from oc_ds_converter.oc_idmanager.doi import DOIManager
from oc_ds_converter.oc_idmanager.issn import ISSNManager
from oc_ds_converter.oc_idmanager.oc_data_storage.batch_manager import BatchManager
from oc_ds_converter.oc_idmanager.oc_data_storage.redis_manager import RedisStorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.storage_manager import StorageManager
from oc_ds_converter.ra_processor import RaProcessor


class CrossrefStyleProcessing(RaProcessor):
    """Base class for processors that follow the Crossref-style pattern.

    This includes common infrastructure for:
    - Redis-based storage and validation
    - ORCID index prefetching
    - Two-pass processing (citing/cited entities)
    """

    @staticmethod
    def clean_markup(text: str) -> str:
        if '<' in text:
            soup = BeautifulSoup(text, 'html.parser')
            text = soup.get_text()
        return html.unescape(text).replace('\n', '')

    def __init__(
        self,
        orcid_index: str | None = None,
        publishers_filepath: str | None = None,
        storage_manager: StorageManager | None = None,
        testing: bool = True,
        citing: bool = True,
        use_redis_orcid_index: bool = False,
        use_orcid_api: bool = True,
        use_redis_publishers: bool = False,
    ):
        orcid_index_obj = (
            OrcidIndexRedis(testing=testing)
            if use_redis_orcid_index and orcid_index is None
            else orcid_index
        )
        super().__init__(orcid_index_obj, publishers_filepath)
        self.citing = citing
        self._testing = testing
        self.use_orcid_api = use_orcid_api
        self.use_redis_publishers = use_redis_publishers
        self._publishers_redis: PublishersRedis | None = None
        if use_redis_publishers:
            self._publishers_redis = PublishersRedis(testing=testing)

        if storage_manager is None:
            self.storage_manager = RedisStorageManager(testing=testing)
        else:
            self.storage_manager = storage_manager

        self.temporary_manager = BatchManager()

        self.doi_m = DOIManager(storage_manager=self.storage_manager, testing=testing)
        self.issn_m = ISSNManager()

        self.venue_id_man_dict: dict[str, object] = {"issn": self.issn_m}

        self.tmp_doi_m = DOIManager(storage_manager=self.temporary_manager, testing=testing)

        self.venue_tmp_id_man_dict: dict[str, object] = {"issn": self.issn_m}

        self.orcid_m = ORCIDManager(
            storage_manager=self.storage_manager, testing=testing, use_api_service=use_orcid_api
        )
        self.tmp_orcid_m = ORCIDManager(
            storage_manager=self.temporary_manager, testing=testing, use_api_service=use_orcid_api
        )

        if testing:
            self.BR_redis: FakeRedisWrapper | RedisDataSource = FakeRedisWrapper()
            self.RA_redis: FakeRedisWrapper | RedisDataSource = FakeRedisWrapper()
        else:
            self.BR_redis = RedisDataSource("DB-META-BR")
            self.RA_redis = RedisDataSource("DB-META-RA")

        self._redis_values_br: list[str] = []
        self._redis_values_ra: list[str] = []
        self._doi_orcid_cache: dict[str, set[str]] = {}

    def update_redis_values(self, br: list[str], ra: list[str] | None = None) -> None:
        self._redis_values_br = [
            x for x in (
                self.doi_m.normalise(b, include_prefix=True) for b in (br or [])
            ) if x
        ]
        self._redis_values_ra = [
            x for x in (
                self.orcid_m.normalise(r, include_prefix=True) for r in (ra or [])
            ) if x
        ]

    def prefetch_doi_orcid_index(self, dois: list[str]) -> None:
        keys = [
            norm for doi in dois
            if (norm := self.doi_m.normalise(doi, include_prefix=True))
        ]
        self._doi_orcid_cache = self.orcid_index.get_values_batch(keys)

    def orcid_finder(self, doi: str) -> dict[str, str]:
        norm_doi = self.doi_m.normalise(doi, include_prefix=True)
        if not norm_doi:
            return {}
        people = self._doi_orcid_cache.get(norm_doi)
        if not people:
            return {}
        found: dict[str, str] = {}
        for person in people:
            match = re.search(r'\d{4}-\d{4}-\d{4}-\d{3}[\dX]', person)
            if match:
                orcid = match.group(0)
                name = person[:person.find(orcid) - 1]
                found[orcid] = name.strip().lower()
        return found

    def memory_to_storage(self) -> None:
        kv_in_memory = self.temporary_manager.get_validity_list_of_tuples()
        if kv_in_memory:
            self.storage_manager.set_multi_value(kv_in_memory)
        self.temporary_manager.delete_storage()

    def get_id_manager(
        self, schema_or_id: str, id_man_dict: dict[str, object]
    ) -> object | None:
        if ":" in schema_or_id:
            schema = schema_or_id.split(":")[0]
        else:
            schema = schema_or_id
        return id_man_dict.get(schema)

    def dict_to_cache(self, dict_to_be_saved: dict[str, list[str]], path: str) -> None:
        path_obj = Path(path)
        parent_dir_path = path_obj.parent.absolute()
        if not os.path.exists(parent_dir_path):
            Path(parent_dir_path).mkdir(parents=True, exist_ok=True)
        with open(path_obj, "w", encoding="utf-8") as fd:
            json.dump(dict_to_be_saved, fd, ensure_ascii=False, indent=4)

    def get_redis_validity_list(self, id_list: list[str], redis_db: str) -> list[str]:
        ids = list(id_list)
        if redis_db == "ra":
            validity = self.RA_redis.mexists_as_set(ids)
            return [ids[i] for i, v in enumerate(validity) if v]
        if redis_db == "br":
            validity = self.BR_redis.mexists_as_set(ids)
            return [ids[i] for i, v in enumerate(validity) if v]
        raise ValueError("redis_db must be either 'ra' or 'br'")

    def validated_as(self, id_dict: dict[str, str]) -> bool | None:
        schema = id_dict["schema"].strip().lower()
        identifier = id_dict["identifier"]

        if schema == "orcid":
            validity_value = self.tmp_orcid_m.validated_as_id(identifier)
            if validity_value is None:
                validity_value = self.orcid_m.validated_as_id(identifier)
            return validity_value

        if schema == "doi":
            validity_value = self.tmp_doi_m.validated_as_id(identifier)
            if validity_value is None:
                validity_value = self.doi_m.validated_as_id(identifier)
            return validity_value
        return None

    def to_validated_id_list(self, norm_id_dict: dict[str, str]) -> list[str]:
        valid_id_list: list[str] = []
        norm_id = norm_id_dict["id"]
        schema = norm_id_dict["schema"]

        if schema == "doi":
            if norm_id in self._redis_values_br:
                self.tmp_doi_m.storage_manager.set_value(norm_id, True)
                valid_id_list.append(norm_id)
            elif self.tmp_doi_m.is_valid(norm_id):
                valid_id_list.append(norm_id)

        elif schema == "orcid":
            if norm_id in self._redis_values_ra:
                self.tmp_orcid_m.storage_manager.set_value(norm_id, True)
                valid_id_list.append(norm_id)
            elif not self.use_orcid_api:
                pass
            elif self.tmp_orcid_m.is_valid(norm_id):
                valid_id_list.append(norm_id)

        return valid_id_list

    def get_publisher_by_prefix(self, prefix: str) -> tuple[str, str] | None:
        """Look up publisher by DOI prefix. Returns (name, member_id) or None."""
        if self.use_redis_publishers and self._publishers_redis:
            pub_data = self._publishers_redis.get_by_prefix(prefix)
            if pub_data:
                member_id = self._publishers_redis._r.get(
                    f"{self._publishers_redis.DOI_PREFIX_KEY}{prefix}"
                )
                return str(pub_data['name']), str(member_id)
            return None
        if self.publishers_mapping:
            for member, data in self.publishers_mapping.items():
                if prefix in data['prefixes']:
                    return str(data['name']), member
        return None

    def _extract_volume(self, item: dict) -> str:
        return item.get('volume', '')

    def _extract_issue(self, item: dict) -> str:
        return item.get('issue', '')

    def csv_creator(self, item: dict) -> dict:
        doi = self._extract_doi(item)
        if not doi:
            return {}
        norm_id = self.doi_m.normalise(doi, include_prefix=True)
        if not norm_id:
            return {}

        authors_list = self._extract_agents(item)
        authors_string_list, editors_string_list = self.get_agents_strings_list(doi, authors_list)

        metadata = {
            'id': norm_id,
            'title': self._extract_title(item),
            'author': '; '.join(authors_string_list),
            'issue': self._extract_issue(item),
            'volume': self._extract_volume(item),
            'venue': self._extract_venue(item),
            'pub_date': self._extract_pub_date(item),
            'page': self._extract_pages(item),
            'type': self._extract_type(item),
            'publisher': self._extract_publisher(item),
            'editor': '; '.join(editors_string_list)
        }
        return self.normalise_unicode(metadata)

    @abstractmethod
    def _extract_doi(self, item: dict) -> str:
        """Extract DOI from item dict."""
        ...

    @abstractmethod
    def _extract_title(self, item: dict) -> str:
        """Extract title from item dict."""
        ...

    @abstractmethod
    def _extract_agents(self, item: dict) -> list[dict]:
        """Extract list of agents (authors/editors) from item dict."""
        ...

    @abstractmethod
    def _extract_venue(self, item: dict) -> str:
        """Extract venue name and IDs from item dict."""
        ...

    @abstractmethod
    def _extract_pub_date(self, item: dict) -> str:
        """Extract publication date from item dict."""
        ...

    @abstractmethod
    def _extract_pages(self, item: dict) -> str:
        """Extract page range from item dict."""
        ...

    @abstractmethod
    def _extract_type(self, item: dict) -> str:
        """Extract publication type from item dict."""
        ...

    @abstractmethod
    def _extract_publisher(self, item: dict) -> str:
        """Extract publisher name from item dict."""
        ...

    @abstractmethod
    def extract_all_ids(self, entity_dict: dict, is_citing: bool) -> tuple[list[str], list[str]]:
        """Extract all IDs from entity dict for validation."""
        ...
