#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright 2019-2020 Fabio Mariani <fabio.mariani555@gmail.com>
# Copyright 2021-2022 Arcangelo Massari <arcangelo.massari@unibo.it>
# Copyright 2023-2025 Arianna Moretti <arianna.moretti4@unibo.it>
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


import html
import re
import warnings
from pathlib import Path
from typing import Optional
import os
import os.path
import json
import csv

from bs4 import BeautifulSoup
from oc_ds_converter.oc_idmanager import DOIManager
from oc_ds_converter.oc_idmanager import ORCIDManager
from oc_ds_converter.oc_idmanager import ISSNManager

from oc_ds_converter.lib.master_of_regex import *
import fakeredis
from oc_ds_converter.datasource.redis import RedisDataSource
from oc_ds_converter.ra_processor import RaProcessor
from oc_ds_converter.oc_idmanager.oc_data_storage.storage_manager import StorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.in_memory_manager import InMemoryStorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.sqlite_manager import SqliteStorageManager
from typing import Dict, List, Tuple
from oc_ds_converter.lib.cleaner import Cleaner



warnings.filterwarnings("ignore", category=UserWarning, module='bs4')

class CrossrefProcessing(RaProcessor):

    def __init__(self, orcid_index:str=None, doi_csv:str=None, publishers_filepath:str=None, testing: bool = True, storage_manager: Optional[StorageManager] = None, citing: bool = True, use_orcid_api: bool = True):
        super(CrossrefProcessing, self).__init__(orcid_index, doi_csv, publishers_filepath)
        self.citing = citing
        self.use_orcid_api = use_orcid_api

        if storage_manager is None:
            self.storage_manager = SqliteStorageManager()
        else:
            self.storage_manager = storage_manager

        self.temporary_manager = InMemoryStorageManager('../memory.json')

        self.doi_m = DOIManager(storage_manager=self.storage_manager)
        self.orcid_m = ORCIDManager(storage_manager=self.storage_manager, use_api_service=use_orcid_api)
        self.issn_m = ISSNManager()

        self.venue_id_man_dict = {"issn": self.issn_m}
        # Temporary storage managers
        self.tmp_doi_m = DOIManager(storage_manager=self.temporary_manager)
        self.tmp_orcid_m = ORCIDManager(storage_manager=self.temporary_manager, use_api_service=use_orcid_api)

        self.venue_tmp_id_man_dict = {"issn": self.issn_m}

        if testing:
            self.BR_redis = fakeredis.FakeStrictRedis()
            self.RA_redis= fakeredis.FakeStrictRedis()
        else:
            self.BR_redis = RedisDataSource("DB-META-BR")
            self.RA_redis = RedisDataSource("DB-META-RA")

        self._redis_values_br = []
        self._redis_values_ra = []


    def update_redis_values(self, br, ra):
        self._redis_values_br = br
        self._redis_values_ra = ra

    def to_validated_id_list(self, norm_id_dict):
        valid_id_list = []
        norm_id = norm_id_dict.get("id")
        schema = norm_id_dict.get("schema")

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
                # OFFLINE: non tentare la validazione via rete
                pass
            elif self.tmp_orcid_m.is_valid(norm_id):
                valid_id_list.append(norm_id)

        else:
            print("Schema not accepted:", schema, "in", norm_id_dict, ". Use 'orcid' or 'doi'.")

        return valid_id_list

    def memory_to_storage(self):
        kv_in_memory = self.temporary_manager.get_validity_list_of_tuples()
        if kv_in_memory:
            self.storage_manager.set_multi_value(kv_in_memory)
        # uniformità con Datacite: svuota sempre la memoria temporanea
        self.temporary_manager.delete_storage()


    def validated_as(self, id_dict):
        schema = id_dict["schema"].strip().lower()
        identifier = id_dict["identifier"]

        if schema == "orcid":
            validity_value = self.tmp_orcid_m.validated_as_id(identifier)
            if validity_value is None:
                validity_value = self.orcid_m.validated_as_id(identifier)
            return validity_value

        elif schema == "doi":
            validity_value = self.tmp_doi_m.validated_as_id(identifier)
            if validity_value is None:
                validity_value = self.doi_m.validated_as_id(identifier)
            return validity_value
        else:
            print("invalid schema in ", id_dict, ". schema should be either doi or orcid.")
            return None


    def get_id_manager(self, schema_or_id, id_man_dict):
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
        row = dict()
        if not 'DOI' in item:
            return row
        doi_manager = DOIManager(use_api_service=False)
        if isinstance(item['DOI'], list):
            doi = doi_manager.normalise(str(item['DOI'][0]), include_prefix=False)
        else:
            doi = doi_manager.normalise(str(item['DOI']), include_prefix=False)
        if (doi and self.doi_set and doi in self.doi_set) or (doi and not self.doi_set):
            # create empty row
            keys = ['id', 'title', 'author', 'pub_date', 'venue', 'volume', 'issue', 'page', 'type',
                    'publisher', 'editor']
            for k in keys:
                row[k] = ''

            if 'type' in item:
                if item['type']:
                    row['type'] = item['type'].replace('-', ' ')

            # row['id']
            ids_list = list()
            ids_list.append(str('doi:' + doi))

            if 'ISBN' in item:
                if row['type'] in {'book', 'dissertation', 'edited book', 'monograph', 'reference book', 'report', 'standard'}:
                    self.id_worker(item['ISBN'], ids_list, self.isbn_worker)

            if 'ISSN' in item:
                if row['type'] in {'book series', 'book set', 'journal', 'proceedings series', 'series', 'standard series'}:
                    self.id_worker(item['ISSN'], ids_list, self.issn_worker)
                elif row['type'] == 'report series':
                    br_id = True
                    if 'container-title' in item:
                        if item['container-title']:
                            br_id = False
                    if br_id:
                        self.id_worker(item['ISSN'], ids_list, self.issn_worker)
            row['id'] = ' '.join(ids_list)

            # row['title']
            if 'title' in item:
                if item['title']:
                    if isinstance(item['title'], list):
                        text_title = item['title'][0]
                    else:
                        text_title = item['title']
                    soup = BeautifulSoup(text_title, 'html.parser')
                    title_soup = soup.get_text().replace('\n', '')
                    title = html.unescape(title_soup)
                    row['title'] = title

            agents_list = []
            if 'author' in item:
                for author in item['author']:
                    author['role'] = 'author'
                agents_list.extend(item['author'])
            if 'editor' in item:
                for editor in item['editor']:
                    editor['role'] = 'editor'
                agents_list.extend(item['editor'])
            authors_strings_list, editors_string_list = self.get_agents_strings_list(doi, agents_list)

            # row['author']
            if 'author' in item:
                row['author'] = '; '.join(authors_strings_list)

            # row['pub_date']
            if 'issued' in item:
                if item['issued']['date-parts'][0][0]:
                    row['pub_date'] = '-'.join([str(y) for y in item['issued']['date-parts'][0]])
                else:
                    row['pub_date'] = ''

            # row['venue']
            row['venue'] = self.get_venue_name(item, row)

            if 'volume' in item:
                row['volume'] = item['volume']
            if 'issue' in item:
                row['issue'] = item['issue']
            if 'page' in item:
                row['page'] = self.get_crossref_pages(item)

            row['publisher'] = self.get_publisher_name(doi, item)

            if 'editor' in item:
                row['editor'] = '; '.join(editors_string_list)
        return self.normalise_unicode(row)

    def get_crossref_pages(self, item:dict) -> str:
        pages_list = re.split(pages_separator, item['page'])
        return self.get_pages(pages_list)

    def get_publisher_name(self, doi:str, item:dict) -> str:
        data = {
            'publisher': '',
            'member': None,
            'prefix': doi.split('/')[0]
        }
        for field in {'publisher', 'member', 'prefix'}:
            if field in item:
                if item[field]:
                    data[field] = item[field]
        publisher = data['publisher']
        member = data['member']
        prefix = data['prefix']
        relevant_member = False

        if self.publishers_mapping and member:
            if member in self.publishers_mapping:
                relevant_member = True
        if self.publishers_mapping:
            if relevant_member:
                name = self.publishers_mapping[member]['name']
                name_and_id = f'{name} [crossref:{member}]'
            else:
                member_dict = next(({member:data} for member, data in self.publishers_mapping.items() if prefix in data['prefixes']), None)
                if member_dict:
                    member = list(member_dict.keys())[0]
                    name_and_id = f"{member_dict[member]['name']} [crossref:{member}]"
                else:
                    name_and_id = publisher
        else:
            name_and_id = f'{publisher} [crossref:{member}]' if member else publisher
        return name_and_id


    def get_venue_name(self, item:dict, row:dict) -> str:
        name_and_id = ''
        if 'container-title' in item:
            if item['container-title']:
                if isinstance(item['container-title'], list):
                    ventit = str(item['container-title'][0]).replace('\n', '')
                else:
                    ventit = str(item['container-title']).replace('\n', '')
                ven_soup = BeautifulSoup(ventit, 'html.parser')
                ventit = html.unescape(ven_soup.get_text())
                ambiguous_brackets = re.search(ids_inside_square_brackets, ventit)
                if ambiguous_brackets:
                    match = ambiguous_brackets.group(1)
                    open_bracket = ventit.find(match) - 1
                    close_bracket = ventit.find(match) + len(match)
                    ventit = ventit[:open_bracket] + '(' + ventit[open_bracket + 1:]
                    ventit = ventit[:close_bracket] + ')' + ventit[close_bracket + 1:]
                venids_list = list()
                if 'ISBN' in item:
                    if row['type'] in {'book chapter', 'book part', 'book section', 'book track', 'reference entry'}:
                        self.id_worker(item['ISBN'], venids_list, self.isbn_worker)

                if 'ISSN' in item:
                    if row['type'] in {'book', 'data file', 'dataset', 'edited book', 'journal article', 'journal volume', 'journal issue', 'monograph', 'proceedings', 'peer review', 'reference book', 'reference entry', 'report'}:
                        self.id_worker(item['ISSN'], venids_list, self.issn_worker)
                    elif row['type'] == 'report series':
                        if 'container-title' in item:
                            if item['container-title']:
                                self.id_worker(item['ISSN'], venids_list, self.issn_worker)
                if venids_list:
                    name_and_id = ventit + ' [' + ' '.join(venids_list) + ']'
                else:
                    name_and_id = ventit
        return name_and_id

    # UPDATED FOR CROSSREF √
    def extract_all_ids(self, entity_dict, is_first_iteration: bool):
        all_br = set()
        all_ra = set()

        if is_first_iteration:
            # VALIDATE RESPONSIBLE AGENTS IDS FOR THE CITING ENTITY
            if entity_dict.get("author"):
                for author in entity_dict["author"]:
                    if "ORCID" in author:
                        orcid = self.orcid_m.normalise(author["ORCID"])
                        if orcid:
                            all_ra.add(orcid)

            if entity_dict.get("editor"):
                for author in entity_dict["editor"]:
                    if "ORCID" in author:
                        orcid = self.orcid_m.normalise(author["ORCID"])
                        if orcid:
                            all_ra.add(orcid)

        # RETRIEVE CITED IDS OF A CITING ENTITY
        else:
            citations = [x for x in entity_dict.get("reference", []) if x.get("DOI")]
            for cit in citations:
                norm_id = self.doi_m.normalise(cit["DOI"], include_prefix=True)
                if norm_id:
                    all_br.add(norm_id)

        all_br = list(all_br)
        all_ra = list(all_ra)
        return all_br, all_ra

    def get_reids_validity_list(self, id_list, redis_db):
        ids = list(id_list)  # garantisci ordine deterministico
        if redis_db == "ra":
            validity = self.RA_redis.mget(ids)
            return [ids[i] for i, v in enumerate(validity) if v]
        elif redis_db == "br":
            validity = self.BR_redis.mget(ids)
            return [ids[i] for i, v in enumerate(validity) if v]
        else:
            raise ValueError("redis_db must be either 'ra' or 'br'")

    def get_agents_strings_list(self, doi: str, agents_list: List[dict]) -> Tuple[list, list]:
        authors_strings_list = list()
        editors_string_list = list()
        dict_orcid = None
        if not all('orcid' in agent or 'ORCID' in agent for agent in agents_list):
            dict_orcid = self.orcid_finder(doi)
        agents_list = [
            {k: Cleaner(v).remove_unwanted_characters() if k in {'family', 'given', 'name'} and v is not None
            else v for k, v in agent_dict.items()} for agent_dict in agents_list]

        for agent in agents_list:
            cur_role = agent['role']
            f_name = None
            g_name = None
            agent_string = None
            if agent.get('family') and agent.get('given'):
                f_name = agent['family']
                g_name = agent['given']
                agent_string = f_name + ', ' + g_name
            elif agent.get('name'):
                agent_string = agent['name']
                f_name = agent_string.split(",")[0].strip() if "," in agent_string else None
                g_name = agent_string.split(",")[-1].strip() if "," in agent_string else None

                if f_name and g_name:
                    agent_string = f_name + ', ' + g_name
            if agent_string is None:
                if agent.get('family') and not agent.get('given'):
                    if g_name:
                        agent_string = agent['family'] + ', ' + g_name
                    else:
                        agent_string = agent['family'] + ', '
                elif agent.get('given') and not agent.get('family'):
                    if f_name:
                        agent_string = f_name + ', ' + agent['given']
                    else:
                        agent_string = ', ' + agent['given']
            orcid = None
            if 'orcid' in agent:
                if isinstance(agent['orcid'], list):
                    orcid = str(agent['orcid'][0])
                else:
                    orcid = str(agent['orcid'])
            elif 'ORCID' in agent:
                if isinstance(agent['ORCID'], list):
                    orcid = str(agent['ORCID'][0])
                else:
                    orcid = str(agent['ORCID'])
            if orcid:
                orcid = self.find_crossref_orcid(orcid, doi)
            elif dict_orcid and f_name:
                for ori in dict_orcid:
                    orc_n: List[str] = dict_orcid[ori].split(', ')
                    orc_f = orc_n[0].lower()
                    orc_g = orc_n[1] if len(orc_n) == 2 else None
                    if f_name.lower() in orc_f.lower() or orc_f.lower() in f_name.lower():
                        if g_name and orc_g:
                            if len([person for person in agents_list if 'family' in person if person['family'] if
                                    person['family'].lower() in orc_f.lower() or orc_f.lower() in person[
                                        'family'].lower()]) > 1:
                                if len([person for person in agents_list if 'given' in person if person['given'] if
                                        person['given'][0].lower() == orc_g[0].lower()]) > 1:
                                    homonyms_list = [person for person in agents_list if 'given' in person if
                                                     person['given'] if person['given'].lower() == orc_g.lower()]
                                    if len(homonyms_list) > 1:
                                        if [person for person in homonyms_list if person['role'] != cur_role]:
                                            if orc_g.lower() == g_name.lower():
                                                orcid = ori
                                    else:
                                        if orc_g.lower() == g_name.lower():
                                            orcid = ori
                                elif orc_g[0].lower() == g_name[0].lower():
                                    orcid = ori
                            elif any([person for person in agents_list if 'given' in person if person['given'] if
                                      person['given'].lower() == f_name.lower()]):
                                if orc_g.lower() == g_name.lower():
                                    orcid = ori
                            else:
                                orcid = ori
                        else:
                            orcid = ori

            if agent_string and orcid:
                if not orcid.startswith("orcid:"):
                    agent_string += ' [' + 'orcid:' + str(orcid) + ']'
                else:
                    agent_string += ' [' + str(orcid) + ']'
            if agent_string:
                if agent['role'] == 'author':
                    authors_strings_list.append(agent_string)
                elif agent['role'] == 'editor':
                    editors_string_list.append(agent_string)
        return authors_strings_list, editors_string_list

    def find_crossref_orcid(self, identifier, doi):
        orcid = ""
        if not isinstance(identifier, str):
            return orcid

        norm_orcid = self.orcid_m.normalise(identifier, include_prefix=True)
        if not norm_orcid:
            return orcid

        # 1) Already known in tmp/persistent storage?
        validity_value_orcid = self.validated_as({"schema": "orcid", "identifier": norm_orcid})
        if validity_value_orcid is True:
            return norm_orcid
        if validity_value_orcid is False:
            return ""

        # 2) DOI→ORCID index first (if DOI provided)
        found_orcids = self.orcid_finder(doi) if doi else None
        if found_orcids and norm_orcid.split(':')[1] in found_orcids:
            self.tmp_orcid_m.storage_manager.set_value(norm_orcid, True)
            return norm_orcid

        # 3) API OFF: only Redis snapshot
        if not self.use_orcid_api:
            if norm_orcid in self._redis_values_ra:
                self.tmp_orcid_m.storage_manager.set_value(norm_orcid, True)
                return norm_orcid
            return ""

        # 4) API ON: Redis snapshot + manager.is_valid()
        norm_id_dict = {"id": norm_orcid, "schema": "orcid"}
        if norm_orcid in self.to_validated_id_list(norm_id_dict):
            return norm_orcid

        return ""
