#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright 2022 Arcangelo Massari <arcangelo.massari@unibo.it>
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

import csv
import pathlib
from os.path import exists
from oc_ds_converter.pubmed.get_publishers import ExtractPublisherDOI

from oc_ds_converter.oc_idmanager.doi import DOIManager
from oc_ds_converter.oc_idmanager.issn import ISSNManager
from oc_ds_converter.oc_idmanager.jid import JIDManager

import os
import os.path
import json
import re
import warnings
from pathlib import Path
from typing import Optional

import fakeredis
from oc_ds_converter.datasource.redis import RedisDataSource
from oc_ds_converter.ra_processor import RaProcessor
from oc_ds_converter.oc_idmanager.oc_data_storage.storage_manager import StorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.in_memory_manager import InMemoryStorageManager
#from oc_ds_converter.oc_idmanager.oc_data_storage.sqlite_manager import SqliteStorageManager

warnings.filterwarnings("ignore", category=UserWarning, module='bs4')


class JalcProcessing(RaProcessor):

    def __init__(self, orcid_index: str = None, doi_csv: str = None, publishers_filepath_jalc: str = None, testing: bool = True, storage_manager: Optional[StorageManager] = None, citing=True):
        """This class is responsible for producing CSV tables to be used as input for the META process
        aimed at ingesting data from the sources."""
        super(JalcProcessing, self).__init__(orcid_index, doi_csv)
        self.citing = citing
        #if storage_manager is None:
        #    self.storage_manager = SqliteStorageManager()
        #else:
        self.storage_manager = storage_manager

        self.temporary_manager = InMemoryStorageManager('../memory.json')

        self.doi_m = DOIManager(storage_manager=self.storage_manager)
        self.issn_m = ISSNManager()
        self.jid_m = JIDManager(storage_manager=self.storage_manager)

        self.venue_id_man_dict = {"issn":self.issn_m, "jid":self.jid_m}

        '''Temporary storage managers : all data must be stored in tmp storage manager and passed all together to the
        main storage_manager only once a full file is processed. Checks must be done both on tmp and in
        storage_manager, so that in case the process breaks while processing a file which does not complete (so
        without writing the final file) all the data concerning the ids are not stored. Otherwise, the ids saved in
        a storage_manager db would be considered to have been processed and thus would be ignored by the process
        and lost.'''

        self.tmp_doi_m = DOIManager(storage_manager=self.temporary_manager)
        self.tmp_jid_m = JIDManager(storage_manager=self.temporary_manager)

        self.venue_tmp_id_man_dict = {"issn":self.issn_m, "jid":self.tmp_jid_m}


        if testing:
            self.BR_redis = fakeredis.FakeStrictRedis()

        else:
            self.BR_redis = RedisDataSource("DB-META-BR")

        self._redis_values_br = []

        if not publishers_filepath_jalc:
            self.publishers_filepath = None
        else:
            self.publishers_filepath = publishers_filepath_jalc

            if os.path.exists(self.publishers_filepath):
                pfp = dict()
                csv_headers = ("id", "name", "prefix")
                if self.publishers_filepath.endswith(".csv"):
                    with open(self.publishers_filepath, encoding="utf8") as f:
                        csv_reader = csv.DictReader(f, csv_headers)
                        for row in csv_reader:
                            pfp[row["prefix"]] = {"name": row["name"], "crossref_member": row["id"]}
                    self.publishers_filepath = self.publishers_filepath.replace(".csv", ".json")
                elif self.publishers_filepath.endswith(".json"):
                    with open(self.publishers_filepath, encoding="utf8") as f:
                        pfp = json.load(f)
                self.publishers_mapping = pfp

    def update_redis_values(self, br):
        self._redis_values_br = br

    def validated_as(self, id):
        """Check if the validity was already retrieved and thus
        a) if it is now saved either in the in-memory database, which only concerns data validated
         during the current file processing;
        b) or if it is now saved in the storage_manager database, which only concerns data validated
        during the previous files processing.
        In memory db is checked first because the dimension is smaller and the check is faster and
        because we assume that it is more likely to find the same ids in close positions, e.g.: same
        citing id in several citations with different cited ids.
        In conclusion, if the id is found with this method, it means that this has been found in the dump we are processing"""
        validity_value = self.tmp_doi_m.validated_as_id(id)
        if validity_value is None:
            validity_value = self.doi_m.validated_as_id(id)
        return validity_value


    def get_id_manager(self, schema_or_id, id_man_dict):
        """Given as input the string of a schema (e.g.:'jid') and a dictionary mapping strings of
        the schemas to their id managers, the method returns the correct id manager. Note that each
        instance of the Preprocessing class needs its own instances of the id managers, in order to
        avoid conflicts while validating data"""
        if ":" in schema_or_id:
            split_id_prefix = schema_or_id.split(":")
            schema = split_id_prefix[0]
        else:
            schema = schema_or_id
        id_man = id_man_dict.get(schema)
        return id_man

    def dict_to_cache(self, dict_to_be_saved, path):
        path = Path(path)
        parent_dir_path = path.parent.absolute()
        if not os.path.exists(parent_dir_path):
            Path(parent_dir_path).mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as fd:
            json.dump(dict_to_be_saved, fd, ensure_ascii=False, indent=4)

    def csv_creator(self, item:dict) -> dict:
        """This is the method that actually creates the csv table for Meta process given an entity dictionary"""
        doi = item["doi"]
        if (doi and self.doi_set and doi in self.doi_set) or (doi and not self.doi_set):
            norm_id = self.doi_m.normalise(doi, include_prefix=True)
            title = self.get_ja(item['title_list'])[0]['title'] if 'title_list' in item else ''
            authors_list = self.get_authors(item)
            authors_string_list, editors_string_list = self.get_agents_strings_list(doi, authors_list)
            issue = item['issue'] if 'issue' in item else ''
            volume = item['volume'] if 'volume' in item else ''
            publisher = self.get_publisher_name(item)

            metadata = {
                'id': norm_id,
                'title': title,
                'author': '; '.join(authors_string_list),
                'issue': issue,
                'volume': volume,
                'venue': self.get_venue(item),
                'pub_date': self.get_pub_date(item),
                'page': self.get_jalc_pages(item),
                'type': self.get_type(item),
                'publisher': publisher,
                'editor': ''
            }
            return self.normalise_unicode(metadata)


    @classmethod
    def get_ja(cls, field: list) -> list:
        """This method accepts as parameter a list containing dictionaries with the key "lang".
        If a metadata is originally furnished both in the original language and in the english translation,
        the method returns the japanese version, otherwise the english translation is returned."""
        if all('lang' in item for item in field):
            ja = [item for item in field if item['lang'] == 'ja']
            ja = list(filter(lambda x: x['type'] != 'before' if 'type' in x else x, ja))
            if ja:
                return ja
            en = [item for item in field if item['lang'] == 'en']
            en = list(filter(lambda x: x['type'] != 'before' if 'type' in x else x, en))
            if en:
                return en
        return field

    def get_jalc_pages(self, item: dict) -> str:
        first_page = item['first_page'] if 'first_page' in item else ''
        last_page = item['last_page'] if 'last_page' in item else ''
        page_list = list()
        if first_page:
            page_list.append(first_page)
        if last_page:
            page_list.append(last_page)
        return self.get_pages(page_list)


    def get_publisher_name(self, item: dict) -> str:
        """This method acts differently for citing and cited entities.
        If it processes a citing entity it simply returns a string with the name of the publisher if it has been provided in the input dictionary, giving priority to the japanese name.
        If there is no publisher, the output is an empty string.
        When it processes a cited entity, if a file containing a mapping of publishers' prefixes, names and crossref ids is provided, it extracts the prefix from the doi of the cited publication and checks if it is present in the mapping.
        If yes, it returns the linked publisher's name, otherwise an empty string. """
        if self.citing:
            publisher = self.get_ja(item['publisher_list'])[0]['publisher_name'] if 'publisher_list' in item else ''
        elif not self.citing and self.publishers_mapping:
            publisher=''
            doi = item['doi']
            prefix = doi.split('/')[0] if doi else ""
            if prefix:
                if prefix in self.publishers_mapping:
                    name = self.publishers_mapping[prefix]["name"]
                    member = self.publishers_mapping[prefix]["crossref_member"]
                    publisher = f'{name} [crossref:{member}]' if member else name
        else:
            publisher = ''
        return publisher


    def get_authors(self, data: dict) -> list:
        authors = list()
        if data.get("creator_list"):
            creators = data.get("creator_list")
            for c in creators:
                agent = {"role": "author"}
                names = c['names'] if 'names' in c else ''
                ja_name = self.get_ja(names)[0]
                last_name = ja_name['last_name'] if 'last_name' in ja_name else ''
                first_name = ja_name['first_name'] if 'first_name' in ja_name else ''
                full_name = ''
                if last_name:
                    full_name += last_name
                    if first_name:
                        full_name += f', {first_name}'
                agent["name"] = full_name
                agent["family"] = last_name
                agent["given"] = first_name
                authors.append(agent)
        return authors

    def get_venue(self, data: dict) -> str:
        venue_name = ''
        journal_ids = []
        if 'journal_title_name_list' in data:
            candidate_venues = self.get_ja(data['journal_title_name_list'])
            if candidate_venues:
                full_venue = [item for item in candidate_venues if 'type' in item if item['type'] == 'full']
                if full_venue:
                    venue_name = full_venue[0]['journal_title_name']
                elif candidate_venues:
                    venue_name = candidate_venues[0]['journal_title_name']
        if 'journal_id_list' in data:
            for v in data['journal_id_list']:
                if isinstance(v, dict):
                    if v.get("journal_id"):
                        if v.get("type").lower().strip() in ["issn", "jid"]:
                            schema = v.get("type").lower().strip()
                            venue_id = v.get("journal_id")
                            tmp_id_man = self.get_id_manager(schema, self.venue_tmp_id_man_dict)
                            if tmp_id_man:
                                norm_id = tmp_id_man.normalise(venue_id, include_prefix=True)
                                journal_ids.append(norm_id)
        return f"{venue_name} [{' '.join(journal_ids)}]" if journal_ids else venue_name


    @classmethod
    def get_type(cls, data:dict) -> str:
        if data.get('content_type'):
            content_type = data['content_type']
            if content_type == 'JA':
                br_type = 'journal article'
            elif content_type == 'BK':
                br_type = 'book'
            elif content_type == 'RD':
                br_type = 'dataset'
            elif content_type == 'EL':
                br_type = 'other'
            elif content_type == 'GD':
                br_type = 'other'
            return br_type
        else:
            return ''

    @classmethod
    def get_pub_date(cls, data) -> str:
        pub_date_dict = data['publication_date'] if 'publication_date' in data else ''
        pub_date_list = list()
        year = pub_date_dict['publication_year'] if 'publication_year' in pub_date_dict else ''
        if year:
            pub_date_list.append(year)
            month = pub_date_dict['publication_month'] if 'publication_month' in pub_date_dict else ''
            if month:
                pub_date_list.append(month)
                day = pub_date_dict['publication_day'] if 'publication_day' in pub_date_dict else ''
                if day:
                    pub_date_list.append(day)
        return '-'.join(pub_date_list)


    def to_validated_id_list(self, norm_id):
        """this method takes in input a normalized DOI identifier and the information of validity and returns a list valid and existent ids with prefixes.
        For each id, a first validation try is made by checking its presence in META db. If the id is not in META db yet,
        a second attempt is made by using the specific id-schema API"""

        valid_id_list = []
        if norm_id in self._redis_values_br:
            self.tmp_doi_m.storage_manager.set_value(norm_id, True)
            valid_id_list.append(norm_id)
        # if the id is not in redis db, validate it before appending
        elif self.tmp_doi_m.is_valid(norm_id):
            valid_id_list.append(norm_id)
        return valid_id_list

    def memory_to_storage(self):
        kv_in_memory = self.temporary_manager.get_validity_list_of_tuples()
        self.storage_manager.set_multi_value(kv_in_memory)
        self.temporary_manager.delete_storage()

    def extract_all_ids(self, citation, is_first_iteration: bool):
        """Given an entity dictionary, this method extracts all the DOIs.
        If the parameter "is_first_iteration" is True, just the DOI of the citing entity is retrieved, while
        if it is False, all the DOIs of cited entities are extracted."""
        '''if is_first_iteration:
            list_id_citing = list()
            d1_br = citation["data"]["doi"]
            norm_id = self.doi_m.normalise(d1_br, include_prefix=True)
            if norm_id:
                list_id_citing.append(norm_id)
                #for citing entities the validation is not necessary, so we add the normalized doi as valid to the temporary storage manager
                #self.tmp_doi_m.storage_manager.set_value(norm_id, True)
            return list_id_citing'''

        if not is_first_iteration:
            all_br = list()
            d2_br = [x["doi"] for x in citation["data"]["citation_list"] if x.get("doi")]
            for d in d2_br:
                norm_id = self.doi_m.normalise(d, include_prefix=True)
                if norm_id:
                    all_br.append(norm_id)
            return all_br

    def get_reids_validity_list(self, id_list):
        valid_br_ids = []
        validity_list_br = self.BR_redis.mget(id_list)
        for i, e in enumerate(id_list):
            if validity_list_br[i]:
                valid_br_ids.append(e)
        return valid_br_ids
