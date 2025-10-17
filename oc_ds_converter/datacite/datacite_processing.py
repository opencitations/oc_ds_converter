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
import os
import fakeredis
import csv
import json

from bs4 import BeautifulSoup
from oc_ds_converter.oc_idmanager.doi import DOIManager
from oc_ds_converter.oc_idmanager.orcid import ORCIDManager
from oc_ds_converter.lib.master_of_regex import *
from oc_ds_converter.oc_idmanager.oc_data_storage.storage_manager import StorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.in_memory_manager import InMemoryStorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.sqlite_manager import SqliteStorageManager
from oc_ds_converter.oc_idmanager.issn import ISSNManager
from oc_ds_converter.oc_idmanager.isbn import ISBNManager
from oc_ds_converter.datasource.redis import RedisDataSource
from oc_ds_converter.preprocessing.datacite import DatacitePreProcessing
from oc_ds_converter.ra_processor import RaProcessor
from typing import Dict, List, Tuple, Optional, Type, Callable
from pathlib import Path

warnings.filterwarnings("ignore", category=UserWarning, module='bs4')


class DataciteProcessing(RaProcessor):
    def __init__(self, orcid_index: str = None, doi_csv: str = None, publishers_filepath_dc: str = None, testing: bool = True, storage_manager: Optional[StorageManager] = None, citing=True, use_orcid_api: bool = True):
        super(DataciteProcessing, self).__init__(orcid_index, doi_csv)
        # self.preprocessor = DatacitePreProcessing(inp_dir, out_dir, interval, filter)
        if storage_manager is None:
            self.storage_manager = SqliteStorageManager()
        else:
            self.storage_manager = storage_manager

        self.temporary_manager = InMemoryStorageManager('../memory.json')

        self.needed_info = ["relationType", "relatedIdentifierType", "relatedIdentifier"]
        self.filter = ["references", "isreferencedby", "cites", "iscitedby"]

        self.RIS_types_map = {'abst': 'abstract',
  'news': 'newspaper article',
  'slide': 'presentation',
  'book': 'book',
  'data': 'dataset',
  'thes': 'dissertation',
  'jour': 'journal article',
  'mgzn': 'journal article',
  'gen': 'other',
  'advs': 'other',
  'video': 'other',
  'unpb': 'other',
  'ctlg': 'other',
  'art': 'other',
  'case': 'other',
  'icomm': 'other',
  'inpr': 'other',
  'map': 'other',
  'mpct': 'other',
  'music': 'other',
  'pamp': 'other',
  'pat': 'other',
  'pcomm': 'other',
  'catalog': 'other',
  'elec': 'other',
  'hear': 'other',
  'stat': 'other',
  'bill': 'other',
  'unbill': 'other',
  'cpaper': 'proceedings article',
  'rprt': 'report',
  'chap': 'book chapter',
  'ser': 'book series',
  'jfull': 'journal',
  'conf': 'proceedings',
  'comp': 'computer program',
  'sound': 'audio document'}
        self.BIBTEX_types_map = {'book': 'book',
  'mastersthesis': 'dissertation',
  'phdthesis': 'dissertation',
  'article': 'journal article',
  'misc': 'other',
  'unpublished': 'other',
  'manual': 'other',
  'booklet': 'other',
  'inproceedings': 'proceedings article',
  'techreport': 'report',
  'inbook': 'book chapter',
  'incollection': 'book part',
  'proceedings': 'proceedings'}
        self.CITEPROC_types_map = {'book': 'book',
  'dataset': 'dataset',
  'thesis': 'dissertation',
  'article-journal': 'journal article',
  'article': 'other',
  'graphic': 'other',
  'post-weblog': 'web content',
  'paper-conference': 'proceedings article',
  'report': 'report',
  'chapter': 'book chapter',
  'song': 'audio document'}
        self.SCHEMAORG_types_map = {'book': 'book',
  'dataset': 'dataset',
  'thesis': 'dissertation',
  'scholarlyarticle': 'journal article',
  'article': 'journal article',
  'creativework': 'other',
  'event': 'other',
  'service': 'other',
  'mediaobject': 'other',
  'review': 'other',
  'collection': 'other',
  'imageobject': 'other',
  'blogposting': 'web content',
  'report': 'report',
  'chapter': 'book chapter',
  'periodical': 'journal',
  'publicationissue': 'journal issue',
  'publicationvolume': 'journal volume',
  'softwaresourcecode': 'computer program',
  'audioobject': 'audio document'}
        self.RESOURCETYPEGENERAL_types_map = {'book': 'book',
  'dataset': 'dataset',
  'dissertation': 'dissertation',
  'journalarticle': 'journal article',
  'text': 'other',
  'other': 'other',
  'datapaper': 'other',
  'audiovisual': 'other',
  'interactiveresource': 'other',
  'physicalobject': 'other',
  'event': 'other',
  'service': 'other',
  'collection': 'other',
  'image': 'other',
  'model': 'other',
  'peerreview': 'peer review',
  'conferencepaper': 'proceedings article',
  'report': 'report',
  'bookchapter': 'book chapter',
  'journal': 'journal',
  'conferenceproceeding': 'proceedings',
  'standard': 'standard',
  'outputmanagementplan': 'data management plan',
  'preprint': 'preprint',
  'software': 'computer program',
  'sound': 'audio document',
  'workflow': 'workflow'}

    # def input_preprocessing(self):
    # self.preprocessor.split_input()

        self.doi_m = DOIManager(storage_manager=self.storage_manager)
        self.orcid_m = ORCIDManager(use_api_service=use_orcid_api, storage_manager=self.storage_manager)
        self.issn_m = ISSNManager()
        self.isbn_m = ISBNManager()
        self.use_orcid_api = use_orcid_api
        self.venue_id_man_dict = {"issn": self.issn_m, "isbn": self.isbn_m}
        # Temporary storage managers : all data must be stored in tmp storage manager and passed all together to the
        # main storage_manager  only once the full file is processed.
        self.tmp_doi_m = DOIManager(storage_manager=self.temporary_manager)
        self.tmp_orcid_m = ORCIDManager(use_api_service=use_orcid_api, storage_manager=self.temporary_manager)
        self.venue_tmp_id_man_dict = {"issn": self.issn_m, "isbn": self.isbn_m}

        if testing:
            self.BR_redis = fakeredis.FakeStrictRedis()
            self.RA_redis = fakeredis.FakeStrictRedis()
        else:
            self.BR_redis = RedisDataSource("DB-META-BR")
            self.RA_redis = RedisDataSource("DB-META-RA")

        self._redis_values_ra = []
        self._redis_values_br = []

        if not publishers_filepath_dc:
            self.publishers_filepath = None
        else:
            self.publishers_filepath = publishers_filepath_dc

            if os.path.exists(self.publishers_filepath):
                pfp = dict()
                csv_headers = ("id", "name", "prefix")
                if self.publishers_filepath.endswith(".csv"):
                    with open(self.publishers_filepath, encoding="utf8") as f:
                        csv_reader = csv.DictReader(f, csv_headers)
                        for row in csv_reader:
                            pfp[row["prefix"]] = {"name": row["name"], "datacite_member": row["id"]}
                    self.publishers_filepath = self.publishers_filepath.replace(".csv", ".json")
                elif self.publishers_filepath.endswith(".json"):
                    with open(self.publishers_filepath, encoding="utf8") as f:
                        pfp = json.load(f)
                self.publishers_mapping = pfp

    #added
    def update_redis_values(self, br, ra):
        self._redis_values_br = br
        self._redis_values_ra = ra

    #added
    def validated_as(self, id_dict):
        schema = id_dict["schema"].strip().lower()
        identifier = id_dict["identifier"]

        if schema != "orcid":
            validity_value = self.tmp_doi_m.validated_as_id(identifier)
            if validity_value is None:
                validity_value = self.doi_m.validated_as_id(identifier)
            return validity_value
        else:
            validity_value = self.tmp_orcid_m.validated_as_id(identifier)
            if validity_value is None:
                validity_value = self.orcid_m.validated_as_id(identifier)
            return validity_value

    #added(probably unuseful)
    def get_id_manager(self, schema_or_id, id_man_dict):
        if ":" in schema_or_id:
            split_id_prefix = schema_or_id.split(":")
            schema = split_id_prefix[0]
        else:
            schema = schema_or_id
        id_man = id_man_dict.get(schema)
        return id_man

    #added (probably unuseful)
    def normalise_any_id(self, id_with_prefix):
        id_man = self.doi_m
        id_no_pref = ":".join(id_with_prefix.split(":")[1:])
        norm_id_w_pref = id_man.normalise(id_no_pref, include_prefix=True)
        return norm_id_w_pref

    #added
    def dict_to_cache(self, dict_to_be_saved, path):
        path = Path(path)
        parent_dir_path = path.parent.absolute()
        if not os.path.exists(parent_dir_path):
            Path(parent_dir_path).mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as fd:
            json.dump(dict_to_be_saved, fd, ensure_ascii=False, indent=4)

    def csv_creator_objects(self, doi_object: str):
        row = dict()
        keys = ['id', 'title', 'author', 'pub_date', 'venue', 'volume', 'issue', 'page', 'type', 'publisher', 'editor']
        for k in keys:
            row[k] = ''
        row['id'] = doi_object
        try:
            return self.normalise_unicode(row)
        except TypeError:
            print(row)
            raise (TypeError)

    def csv_creator(self, item: dict) -> dict:
        row = dict()
        doi = str(item['id'])
        if (doi and self.doi_set and doi in self.doi_set) or (doi and not self.doi_set):
            norm_id = self.doi_m.normalise(doi, include_prefix=True)
            keys = ['id', 'title', 'author', 'pub_date', 'venue', 'volume', 'issue', 'page', 'type',
                    'publisher', 'editor']
            for k in keys:
                row[k] = ''

            attributes = item['attributes']

            # row['type']
            if attributes.get('types') is not None:
                types_dict = attributes['types']
                for k, v in types_dict.items():
                    if k.lower() == 'ris':
                        if type(v) is str:
                            norm_v = v.strip().lower()
                            if norm_v in self.RIS_types_map.keys():
                                row['type'] = self.RIS_types_map[norm_v]
                                break
                    if k.lower() == 'bibtex':
                        if type(v) is str:
                            norm_v = v.strip().lower()
                            if norm_v in self.BIBTEX_types_map.keys():
                                row['type'] = self.BIBTEX_types_map[norm_v]
                                break
                    if k.lower() == 'schemaorg':
                        if type(v) is str:
                            norm_v = v.strip().lower()
                            if norm_v in self.SCHEMAORG_types_map.keys():
                                row['type'] = self.SCHEMAORG_types_map[norm_v]
                                break
                    if k.lower() == 'citeproc':
                        if type(v) is str:
                            norm_v = v.strip().lower()
                            if norm_v in self.CITEPROC_types_map.keys():
                                row['type'] = self.CITEPROC_types_map[norm_v]
                                break
                    if k.lower() == 'resourcetypegeneral':
                        if type(v) is str:
                            norm_v = v.strip().lower()
                            if norm_v in self.RESOURCETYPEGENERAL_types_map.keys():
                                row['type'] = self.RESOURCETYPEGENERAL_types_map[norm_v]
                                break

            # row['id']
            ids_list = list()
            ids_list.append(norm_id)

            if attributes.get('identifiers'):
                for other_id in attributes.get('identifiers'):
                    if other_id.get('identifier') and other_id.get('identifierType'):
                        o_id_type = other_id.get('identifierType')
                        o_id = other_id.get('identifier')

                        if o_id_type == 'ISBN':
                            if row['type'] in {'book', 'dissertation', 'edited book', 'monograph', 'reference book', 'report',
                                               'standard'}:
                                self.id_worker(o_id, ids_list, self.isbn_worker)

                        elif o_id_type == 'ISSN':
                            if row['type'] in {'book series', 'book set', 'journal', 'proceedings series', 'series',
                                               'standard series', 'report series'}:
                                self.id_worker(o_id, ids_list, self.issn_worker)

            row['id'] = ' '.join(ids_list)

            # row['title']
            pub_title = ""
            if attributes.get("titles"):
                for title in attributes.get("titles"):
                    if title.get("title"):
                        p_title = title.get("title")
                        soup = BeautifulSoup(p_title, 'html.parser')
                        title_soup = soup.get_text().replace('\n', '')
                        title_soup_space_replaced = ' '.join(title_soup.split())
                        title_soup_strip = title_soup_space_replaced.strip()
                        clean_tit = html.unescape(title_soup_strip)
                        pub_title = clean_tit if clean_tit else p_title

            row['title'] = pub_title

            agent_list_authors_only = self.add_authors_to_agent_list(attributes, [])
            agents_list = self.add_editors_to_agent_list(attributes, agent_list_authors_only)

            authors_strings_list, editors_string_list = self.get_agents_strings_list(doi, agents_list)

            # row['author']
            if 'creators' in attributes:
                row['author'] = '; '.join(authors_strings_list)

            # row['pub_date']
            cur_date = ""
            dates = attributes.get("dates")
            if dates:
                for date in dates:
                    if date.get("dateType") == "Issued":
                        cur_date = date.get("date")
                        break
            if cur_date == "":
                if attributes.get("publicationYear"):
                    cur_date = str(attributes.get("publicationYear"))
            row['pub_date'] = cur_date

            # row['venue']
            row['venue'] = self.get_venue_name(attributes, row)

            issue = ""
            volume = ""

            if attributes.get("container"):
                container = attributes["container"]
                if container and (container.get("identifierType") in ("ISSN", "ISBN")):  # fix precedenza and/or
                    if container.get("issue"):
                        issue = container.get("issue")
                    if container.get("volume"):
                        volume = container.get("volume")

            if not issue or not volume:
                relatedIdentifiers = attributes.get("relatedIdentifiers")
                if relatedIdentifiers:
                    for related in relatedIdentifiers:
                        if related.get("relationType"):
                            if related.get("relationType").lower() == "ispartof":
                                if related.get("relatedIdentifierType") == "ISSN" or related.get("relatedIdentifierType") == "ISBN":
                                    if not issue and related.get("issue"):
                                        issue = related.get("issue")
                                    if not volume and related.get("volume"):
                                        volume = related.get("volume")
            row['volume'] = volume
            row['issue'] = issue
            row['page'] = self.get_datacite_pages(attributes)
            row['publisher'] = self.get_publisher_name(doi, attributes)

            if attributes.get("contributors"):
                editors = [contributor for contributor in attributes.get("contributors") if
                           contributor.get("contributorType") == "Editor"]
                if editors:
                    row['editor'] = '; '.join(editors_string_list)

            try:
                return self.normalise_unicode(row)
            except TypeError:
                print(row)
                raise(TypeError)

    #added
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
                # OFFLINE: non chiamare l'API per ORCID
                pass
            elif self.tmp_orcid_m.is_valid(norm_id):
                valid_id_list.append(norm_id)
        else:
            print("Schema not accepted:", norm_id_dict.get("schema"), "in ", norm_id_dict, ". Use 'orcid' or 'doi'.")
        return valid_id_list

    #no modified
    def get_datacite_pages(self, item: dict) -> str:
        container_pages_list = list()
        related_pages_list = list()
        container = item.get("container")
        if container:
            if container.get("identifierType") == "ISSN" or container.get("identifierType") == "ISBN":
                if container.get("firstPage"):
                    container_pages_list.append(container.get("firstPage"))
                if container.get("lastPage"):
                    container_pages_list.append(container.get("lastPage"))

        relatedIdentifiers = item.get("relatedIdentifiers")
        if relatedIdentifiers:
            for related in relatedIdentifiers:
                if related.get("relationType"):
                    if related.get("relationType").lower() == "ispartof":
                        if related.get("relatedIdentifierType") == "ISSN" or related.get("relatedIdentifierType") == "ISBN":
                            if related.get("firstPage"):
                                related_pages_list.append(related.get("firstPage"))
                            if related.get("lastPage"):
                                related_pages_list.append(related.get("lastPage"))

        page_list = related_pages_list if len(related_pages_list)> len(container_pages_list) else container_pages_list
        return self.get_pages(page_list)

    #modified
    def get_publisher_name(self, doi: str, item: dict) -> str:
        # (regex invariate)
        publisher = item.get("publisher")
        if publisher:
            txt = publisher.lower().strip()
            if re.match("\(?:unav\)?", txt):
                publisher = ""
            elif re.match("\(?:unkn\)?", txt):
                publisher = ""
            elif re.match(".*publ?isher not identified.*", txt):
                publisher = ""
            elif re.match("^\[?unknown]?(:*\[?unknown]?)*$", txt.replace(' ', '')):
                publisher = ""
            elif re.match("^not yet(?: published)?$", txt):
                publisher = ""
            elif re.match("[\[({]*s\.*[ln]\.*[)}\]]*([,:][\[({]*s\.*n\.*[)}\]]*)*", txt.replace(' ', '')):
                publisher = ""
            elif re.match("^(publisher )*not(?: specified\.*)|^(publisher )*not(?: provided\.*)$", txt):
                publisher = ""
            elif re.match("^not known$", txt):
                publisher = ""
            elif re.match("^(information )?not available.*", txt):
                publisher = ""
        else:
            publisher = ""

        data = {
            'publisher': publisher,
            'prefix': doi.split('/')[0]
        }

        publisher = data['publisher']
        prefix = data['prefix']

        if self.publishers_mapping:
            if prefix:
                if prefix in self.publishers_mapping:
                    name = self.publishers_mapping[prefix]["name"]
                    member = self.publishers_mapping[prefix]["datacite_member"]
                    name_and_id = f'{name} [datacite:{member}]' if member else name
                else:
                    name_and_id = publisher
        else:
            name_and_id = publisher

        return name_and_id

    #no modified
    def get_venue_name(self, item: dict, row: dict) -> str:
        cont_title = ""
        venids_list = list()

        container = item.get("container")
        if container:
            if container.get("title"):
                cont_title = (container["title"].lower()).replace('\n', '')
                ven_soup = BeautifulSoup(cont_title, 'html.parser')
                ventit = html.unescape(ven_soup.get_text())
                ambiguous_brackets = re.search('\[\s*((?:[^\s]+:[^\s]+)?(?:\s+[^\s]+:[^\s]+)*)\s*\]', ventit)
                if ambiguous_brackets:
                    match = ambiguous_brackets.group(1)
                    open_bracket = ventit.find(match) - 1
                    close_bracket = ventit.find(match) + len(match)
                    ventit = ventit[:open_bracket] + '(' + ventit[open_bracket + 1:]
                    ventit = ventit[:close_bracket] + ')' + ventit[close_bracket + 1:]
                    cont_title = ventit

            if container.get("identifierType") == "ISBN":
                if row['type'] in {'book chapter', 'book part', 'book section', 'book track', 'reference entry'}:
                    try:
                        self.id_worker(container.get("identifier"), venids_list, self.isbn_worker)
                    except ValueError:
                        print(f'''{container.get("identifier")} raised a value error''')

            if container.get("identifierType") == "ISSN":
                if row['type'] in {'book', 'data file', 'dataset', 'edited book', 'journal article', 'journal volume',
                                   'journal issue', 'monograph', 'proceedings', 'peer review', 'reference book',
                                   'reference entry', 'report'}:
                    try:
                        self.id_worker(container.get("identifier"), venids_list, self.issn_worker)
                    except ValueError:
                        print(f'''{container.get("identifier")} raised a value error''')
                elif row['type'] == 'report series':
                    if container.get("title"):
                        if container.get("title"):
                            try:
                                self.id_worker(container.get("identifier"), venids_list, self.issn_worker)
                            except ValueError:
                                print(f'''{container.get("identifier")} raised a value error''')

        if not venids_list:
            relatedIdentifiers = item.get("relatedIdentifiers")
            if relatedIdentifiers:
                for related in relatedIdentifiers:
                    if related.get("relationType"):
                        if related.get("relationType").lower() == "ispartof":
                            if related.get("relatedIdentifierType") == "ISBN":
                                if row['type'] in {'book chapter', 'book part', 'book section', 'book track',
                                                   'reference entry'}:
                                    self.id_worker(related.get("relatedIdentifier"), venids_list, self.isbn_worker)
                            if related.get("relatedIdentifierType") == "ISSN":
                                if row['type'] in {'book', 'data file', 'dataset', 'edited book', 'journal article',
                                                   'journal volume',
                                                   'journal issue', 'monograph', 'proceedings', 'peer review',
                                                   'reference book',
                                                   'reference entry', 'report'}:
                                    self.id_worker(related.get("relatedIdentifier"), venids_list, self.issn_worker)
                                elif row['type'] == 'report series':
                                    if related.get("title"):
                                        if related.get("title"):
                                            self.id_worker(related.get("relatedIdentifier"), venids_list, self.issn_worker)

        if venids_list:
            name_and_id = cont_title + ' [' + ' '.join(venids_list) + ']' if cont_title else '[' + ' '.join(venids_list) + ']'
        else:
            name_and_id = cont_title

        return name_and_id

    #added the call to find_datacite_orcid
    def add_editors_to_agent_list(self, item: dict, ag_list: list) -> list:
        agent_list = ag_list
        if item.get("contributors"):
            editors = [contributor for contributor in item.get("contributors") if
                       contributor.get("contributorType") == "Editor"]
            for ed in editors:
                agent = {}
                agent["role"] = "editor"
                if ed.get('name'):
                    agent["name"] = ed.get("name")
                if ed.get("nameType") == "Personal" or ("familyName" in ed or "givenName" in ed):
                    agent["family"] = ed.get("familyName")
                    agent["given"] = ed.get("givenName")
                    if ed.get("nameIdentifiers"):
                        orcid_ids = [x.get("nameIdentifier") for x in ed.get("nameIdentifiers") if
                                     x.get("nameIdentifierScheme") == "ORCID"]
                        if orcid_ids:
                            orcid_id = self.find_datacite_orcid(orcid_ids, item.get("doi"))
                            if orcid_id:
                                agent["orcid"] = orcid_id

                missing_names = [x for x in ["family", "given", "name"] if x not in agent]
                for mn in missing_names:
                    agent[mn] = ""
                agent_list.append(agent)
        return agent_list

    # added the call to find_datacite_orcid
    def add_authors_to_agent_list(self, item: dict, ag_list: list) -> list:
        agent_list = ag_list
        if item.get("creators"):
            creators = item.get("creators")
            for c in creators:
                agent = {}
                agent["role"] = "author"
                if c.get("name"):
                    agent["name"] = c.get("name")
                if c.get("nameType") == "Personal" or ("familyName" in c or "givenName" in c):
                    agent["family"] = c.get("familyName")
                    agent["given"] = c.get("givenName")
                    if c.get("nameIdentifiers"):
                        orcid_ids = [x.get("nameIdentifier") for x in c.get("nameIdentifiers") if
                                     x.get("nameIdentifierScheme") == "ORCID"]
                        if orcid_ids:
                            orcid_id = self.find_datacite_orcid(orcid_ids, item.get("doi"))
                            if orcid_id:
                                agent["orcid"] = orcid_id
                missing_names = [x for x in ["family", "given", "name"] if x not in agent]
                for mn in missing_names:
                    agent[mn] = ""
                agent_list.append(agent)
        return agent_list

    #added
    def find_datacite_orcid(self, all_author_ids, doi=None):
        if not all_author_ids:
            return ""

        for identifier in all_author_ids:
            norm_orcid = self.orcid_m.normalise(identifier, include_prefix=True)
            if not norm_orcid:
                continue

            validity = self.validated_as({"identifier": norm_orcid, "schema": "orcid"})
            if validity is True:
                return norm_orcid
            if validity is False:
                continue

            if doi:
                found_orcids = self.orcid_finder(doi)
                if found_orcids and norm_orcid.split(':')[1] in found_orcids:
                    self.tmp_orcid_m.storage_manager.set_value(norm_orcid, True)
                    return norm_orcid

            if not getattr(self, "use_orcid_api", True):
                if norm_orcid in self._redis_values_ra:
                    self.tmp_orcid_m.storage_manager.set_value(norm_orcid, True)
                    return norm_orcid
                continue

            norm_id_dict = {"id": norm_orcid, "schema": "orcid"}
            if norm_orcid in self.to_validated_id_list(norm_id_dict):
                return norm_orcid

        return ""

    # added
    def memory_to_storage(self):
        kv_in_memory = self.temporary_manager.get_validity_list_of_tuples()
        if kv_in_memory:
            self.storage_manager.set_multi_value(kv_in_memory)
        self.temporary_manager.delete_storage()

    # added (division in first and second iteration)
    def extract_all_ids(self, citation, is_first_iteration: bool):

        if is_first_iteration:
            all_br = set()
            all_ra = set()

            attributes = citation.get("attributes")
            if attributes:
                creators = attributes.get("creators")
                if creators:
                    for c in creators:
                        c_ids = c.get("nameIdentifiers")
                        if c_ids:
                            norm_c_orcids = {self.orcid_m.normalise(x.get("nameIdentifier"), include_prefix=True) for x in c.get("nameIdentifiers") if
                                         x.get("nameIdentifierScheme") == "ORCID"}
                            if norm_c_orcids:
                                all_ra.update(norm_c_orcids)

                if attributes.get("contributors"):
                    editors = [contributor for contributor in attributes.get("contributors") if
                               contributor.get("contributorType") == "Editor"]
                    for ed in editors:
                        if ed.get("nameIdentifiers"):
                            norm_ed_orcids = {self.orcid_m.normalise(x.get("nameIdentifier"), include_prefix=True) for x in ed.get("nameIdentifiers") if
                                                x.get("nameIdentifierScheme") == "ORCID"}
                            if norm_ed_orcids:
                                all_ra.update(norm_ed_orcids)

            all_br = [x for x in all_br if x is not None]
            all_ra = [y for y in all_ra if y is not None]
            return all_br, all_ra

        else:
            all_br = set()
            all_ra = set()
            attributes = citation.get("attributes", {})  # evita KeyError
            rel_ids = attributes.get("relatedIdentifiers")
            if rel_ids:
                for ref in rel_ids:
                    if all(elem in ref for elem in self.needed_info):
                        relatedIdentifierType = (str(ref["relatedIdentifierType"])).lower()
                        relationType = str(ref["relationType"]).lower()
                        if relatedIdentifierType == "doi":
                            if relationType in self.filter:
                                rel_id = self.doi_m.normalise(ref["relatedIdentifier"], include_prefix=True)
                                if rel_id:
                                    all_br.add(rel_id)
            all_br = [x for x in all_br if x is not None]
            all_ra = [y for y in all_ra if y is not None]
            return all_br, all_ra

    #added
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
