#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright 2019-2020 Fabio Mariani <fabio.mariani555@gmail.com>
# Copyright 2021-2022 Arcangelo Massari <arcangelo.massari@unibo.it>
# Copyright 2024 Arianna Moretti <arianna.moretti4@unibo.it>
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
from oc_ds_converter.oc_idmanager import ISBNManager
from oc_ds_converter.oc_idmanager import ISSNManager
from oc_ds_converter.oc_idmanager import ORCIDManager

from oc_ds_converter.lib.master_of_regex import *
import fakeredis
from oc_ds_converter.datasource.redis import RedisDataSource
from oc_ds_converter.ra_processor import RaProcessor
from oc_ds_converter.oc_idmanager.oc_data_storage.storage_manager import StorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.in_memory_manager import InMemoryStorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.sqlite_manager import SqliteStorageManager
from oc_ds_converter.crossref.crossref_processing import CrossrefProcessing
from typing import Dict, List, Tuple
from oc_ds_converter.lib.cleaner import Cleaner

warnings.filterwarnings("ignore", category=UserWarning, module='bs4')


class ZoteroProcessing(RaProcessor):

    def __init__(self, orcid_index: str = None, doi_csv: str = None, publishers_filepath: str = None,
                 testing: bool = True, storage_manager: Optional[StorageManager] = None, citing=True):
        super(ZoteroProcessing, self).__init__(orcid_index, doi_csv, publishers_filepath)
        self.citing = citing

        self.mapping_types_to_ocdm = {
            "chapter": "book chapter",
            "paper-conference": "proceedings article",
            "article-journal": "journal article",
            "book": "book"
        }

        self.accepted_ids = {
            "book chapter": [
                "doi",
                "pmid",
                "wikidata",
                "url",
                "openalex"
            ],
            "proceedings article": [
                "doi",
                "pmid",
                "pmcid",
                "wikidata",
                "url",
                "openalex"
            ],
            "journal article": [
                "doi",
                "pmid",
                "pmcid",
                "wikidata",
                "url",
                "openalex"
            ],
            "book": [
                "doi",
                "isbn",
                "pmid",
                "wikidata",
                "url",
                "openalex"
            ]
        }



        if storage_manager is None:
            self.storage_manager = SqliteStorageManager()
        else:
            self.storage_manager = storage_manager

        self.temporary_manager = InMemoryStorageManager('../memory.json')

        self.doi_m = DOIManager(storage_manager=self.storage_manager)
        self.issn_m = ISSNManager()
        self.isbn_m = ISBNManager()
        self.orcid_m = ORCIDManager(storage_manager=self.storage_manager)


        self.venue_id_man_dict = {"issn": self.issn_m}
        # Temporary storage managers : all data must be stored in tmp storage manager and passed all together to the
        # main storage_manager  only once the full file is processed. Checks must be done both on tmp and in
        # storage_manager, so that in case the process breaks while processing a file which does not complete (so
        # without writing the final file) all the data concerning the ids are not stored. Otherwise, the ids saved in
        # a storage_manager db would be considered to have been processed and thus would be ignored by the process
        # and lost.

        self.tmp_doi_m = DOIManager(storage_manager=self.temporary_manager)
        self.tmp_issn_m = ISSNManager()
        self.tmp_isbn_m = ISBNManager()

        self.venue_tmp_id_man_dict = {"issn": self.issn_m}

        if testing:
            self.BR_redis = fakeredis.FakeStrictRedis()
            self.RA_redis = fakeredis.FakeStrictRedis()


        else:
            self.BR_redis = RedisDataSource("DB-META-BR")
            self.RA_redis = RedisDataSource("DB-META-RA")

        self._redis_values_br = []
        self._redis_values_ra = []
        self.crossref_processor = CrossrefProcessing()

    def update_redis_values(self, br, ra):
        self._redis_values_br = br
        self._redis_values_ra = ra

    def to_validated_id_list(self, norm_id_dict):
        """returns a list containing the validated id if it is valid, and an empty list otheriwse.
        A first validation try is made by checking its presence in META db. If the id is not in META db yet,
        a second attempt is made by using the specific id-schema API"""
        # if self.BR_redis.get(norm_id):

        doi = norm_id_dict.get('DOI')
        issn = norm_id_dict.get('ISSN')
        isbn = norm_id_dict.get('ISBN')


        entity_ids = set()
        venue_ids = set()

        type = self.mapping_types_to_ocdm.get(norm_id_dict["type"])
        ids_per_type = self.accepted_ids[type]

        if doi and "doi" in ids_per_type:

            if doi in self._redis_values_br:
                self.tmp_doi_m.storage_manager.set_value(doi,
                                                         True)  # In questo modo l'id presente in redis viene inserito anche nello storage e risulta già
                # preso in considerazione negli step successivi
                entity_ids.add(self.tmp_doi_m.normalise(doi, include_prefix=True))

            # if the id is not in redis db, validate it before appending
            elif self.tmp_doi_m.is_valid(
                    doi):  # In questo modo l'id presente in redis viene inserito anche nello storage e risulta già
                # preso in considerazione negli step successivi
                entity_ids.add(self.tmp_doi_m.normalise(doi, include_prefix=True))

        if issn:
            issn = self.tmp_issn_m.normalise(issn, include_prefix=True)
            if issn and self.tmp_issn_m.is_valid(issn):
                if "issn" in ids_per_type:
                    entity_ids.add(issn)
                else:
                    venue_ids.add(issn)

        if isbn:
            isbn = self.tmp_isbn_m.normalise(isbn, include_prefix=True)
            if isbn and self.tmp_isbn_m.is_valid(isbn):
                if "isbn" in ids_per_type:
                    entity_ids.add(isbn)
                else:
                    venue_ids.add(isbn)

        return entity_ids, venue_ids


# RIPRENDERE DA QUI
    def memory_to_storage(self):
        kv_in_memory = self.temporary_manager.get_validity_list_of_tuples()
        if kv_in_memory:
            self.storage_manager.set_multi_value(kv_in_memory)
            self.temporary_manager.delete_storage()

    def validated_as(self, id_dict):
        # Check if the validity was already retrieved and thus
        # a) if it is now saved either in the in-memory database, which only concerns data validated
        # during the current file processing;
        # b) or if it is now saved in the storage_manager database, which only concerns data validated
        # during the previous files processing.
        # In memory db is checked first because the dimension is smaller and the check is faster and
        # Because we assume that it is more likely to find the same ids in close positions, e.g.: same
        # citing id in several citations with different cited ids.

        schema = id_dict["schema"].strip().lower()
        id = id_dict["identifier"]

        if schema == "orcid":
            tmp_id_m = self.tmp_orcid_m
            validity_value = tmp_id_m.validated_as_id(id)

            if validity_value is None:
                id_m = self.orcid_m
                validity_value = id_m.validated_as_id(id)
            return validity_value

        elif schema == "doi":
            validity_value = self.tmp_doi_m.validated_as_id(id)
            if validity_value is None:
                validity_value = self.doi_m.validated_as_id(id)
            return validity_value
        else:
            print("invalid schema in ", id_dict, ". schema should be either doi or orcid.")
            return None

    def get_id_manager(self, schema_or_id, id_man_dict):
        """Given as input the string of a schema (e.g.:'pmid') and a dictionary mapping strings of
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

    def csv_creator(self, item: dict) -> dict:

        row = dict()

        doi_manager = DOIManager(use_api_service=False)
        isbn_manager = ISBNManager()

        if item.get("DOI"):
            if isinstance(item['DOI'], list):
                doi = doi_manager.normalise(str(item['DOI'][0]), include_prefix=False)
            else:
                doi = doi_manager.normalise(str(item['DOI']), include_prefix=False)
            if not ((doi and self.doi_set and doi in self.doi_set) or (doi and not self.doi_set)):
                return row
        else:
            doi = ""
            # if isinstance(item['ISBN'], list):
            #     isbn = isbn_manager.normalise(str(item['ISBN'][0]), include_prefix=False)
            # else:
            #     isbn = isbn_manager.normalise(str(item['ISBN']), include_prefix=False)
            # if not isbn:
            #     return row


        keys = ['id', 'title', 'author', 'pub_date', 'venue', 'volume', 'issue', 'page', 'type',
                'publisher', 'editor']
        for k in keys:
            row[k] = ''

        # row['type']
        if item.get("type"):
            row['type'] = self.mapping_types_to_ocdm.get(str(item['type']).lower())


        # row['id']
        entity_ids, venue_ids = self.to_validated_id_list(item)
        entity_ids = list(entity_ids)
        row['id'] = ' '.join(entity_ids)

        # row['title']
        if item.get('title'):
            if isinstance(item['title'], list):
                text_title = item['title'][0]
            else:
                text_title = item['title']
            soup = BeautifulSoup(text_title, 'html.parser')
            title_soup = soup.get_text().replace('\n', '')
            title = html.unescape(title_soup)
            row['title'] = title

        # retrieving agents
        agents_list = []
        if item.get('author'):
            for author in item['author']:
                author['role'] = 'author'
            agents_list.extend(item['author'])
        if item.get('editor'):
            for editor in item['editor']:
                editor['role'] = 'editor'
            agents_list.extend(item['editor'])
        authors_strings_list, editors_string_list = self.get_agents_strings_list(doi, agents_list)

        # row['author']
        if item.get('author'):
            row['author'] = '; '.join(authors_strings_list)

        # row['pub_date']
        if item.get('issued'):
            if item['issued']['date-parts'][0][0]:
                row['pub_date'] = '-'.join([str(y) for y in item['issued']['date-parts'][0]])
            else:
                row['pub_date'] = ''

        # row['venue']
        row['venue'] = self.get_venue_name(item, row)

        # row['volume']
        if 'volume' in item:
            row['volume'] = item['volume']

        # row['issue']
        if 'issue' in item:
            row['issue'] = item['issue']

        # row['page']
        if 'page' in item:
            row['page'] = self.get_crossref_pages(item)

        # row['publisher']
        row['publisher'] = self.get_publisher_name(doi, item)

        if 'editor' in item:
            row['editor'] = '; '.join(editors_string_list)


        return self.normalise_unicode(row)


    def get_crossref_pages(self, item: dict) -> str:
        '''
        This function returns the pages interval.

        :params item: the item's dictionary
        :type item: dict
        :returns: str -- The output is a string in the format 'START-END', for example, '583-584'. If there are no pages, the output is an empty string.
        '''
        pages_list = re.split(pages_separator, item['page'])
        return self.get_pages(pages_list)

    def get_publisher_name(self, doi: str, item: dict) -> str:
        '''
        This function aims to return a publisher's name and id. If a mapping was provided,
        it is used to find the publisher's standardized name from its id or DOI prefix.

        :params doi: the item's DOI
        :type doi: str
        :params item: the item's dictionary
        :type item: dict
        :returns: str -- The output is a string in the format 'NAME [SCHEMA:ID]', for example, 'American Medical Association (AMA) [crossref:10]'. If the id does not exist, the output is only the name. Finally, if there is no publisher, the output is an empty string.
        '''
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
                member_dict = next(
                    ({member: data} for member, data in self.publishers_mapping.items() if prefix in data['prefixes']),
                    None)
                if member_dict:
                    member = list(member_dict.keys())[0]
                    name_and_id = f"{member_dict[member]['name']} [crossref:{member}]"
                else:
                    name_and_id = publisher
        else:
            name_and_id = f'{publisher} [crossref:{member}]' if member else publisher
        return name_and_id

    def get_venue_name(self, item: dict, row: dict) -> str:
        '''
        This method deals with generating the venue's name, followed by id in square brackets, separated by spaces.
        HTML tags are deleted and HTML entities escaped. In addition, any ISBN and ISSN are validated.
        Finally, the square brackets in the venue name are replaced by round brackets to avoid conflicts with the ids enclosures.

        :params item: the item's dictionary
        :type item: dict
        :params row: a CSV row
        :type row: dict
        :returns: str -- The output is a string in the format 'NAME [SCHEMA:ID]', for example, 'Nutrition & Food Science [issn:0034-6659]'. If the id does not exist, the output is only the name. Finally, if there is no venue, the output is an empty string.
        '''
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
                if item.get('ISBN'):
                    if row['type'] in {'book chapter', 'book part', 'book section', 'book track', 'reference entry'}:
                        self.id_worker(item['ISBN'], venids_list, self.isbn_worker)

                if item.get('ISSN'):
                    if row['type'] in {'book', 'data file', 'dataset', 'edited book', 'journal article',
                                       'journal volume', 'journal issue', 'monograph', 'proceedings', 'peer review',
                                       'reference book', 'reference entry', 'report'}:
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
            # VALIDATE RESPONSIBLE AGENTS IDS FOR THE CITING ENTITY (THE CITING ENTITY DOI IS VALID BY
            # DEFAULT SINCE IT WAS ASSIGNED BY CROSSREF, WHICH IS ALSO ITS DOI REGISTRATION AGENCY.

            # EXTRA: THE CITING DOI IS VALID BY DEFAULT
            # d1_br = entity_dict.get("DOI")
            # if d1_br:
            #  norm_id = self.doi_m.normalise(d1_br, include_prefix=True)
            #  if norm_id:
            #     all_br.add(norm_id)

            if entity_dict.get("author"):
                for author in entity_dict["author"]:
                    if "ORCID" in author:
                        orcid = self.orcid_m.normalise(
                            author["ORCID"]
                        )
                        if orcid:
                            all_ra.add(orcid)

            if entity_dict.get("editor"):
                for author in entity_dict["editor"]:
                    if "ORCID" in author:
                        orcid = self.orcid_m.normalise(
                            author["ORCID"]
                        )
                        if orcid:
                            all_ra.add(orcid)

        # RETRIEVE CITED IDS OF A CITING ENTITY
        else:
            citations = [x for x in entity_dict["reference"] if x.get("DOI")]
            for cit in citations:
                norm_id = self.doi_m.normalise(cit["DOI"], include_prefix=True)
                if norm_id:
                    all_br.add(norm_id)

        all_br = list(all_br)
        all_ra = list(all_ra)
        return all_br, all_ra

    def get_reids_validity_list(self, id_list, redis_db):
        if redis_db == "ra":
            valid_ra_ids = []
            # DO NOT UPDATED (REDIS RETRIEVAL METHOD HERE)
            validity_list_ra = self.RA_redis.mget(id_list)
            for i, e in enumerate(id_list):
                if validity_list_ra[i]:
                    valid_ra_ids.append(e)
            return valid_ra_ids

        elif redis_db == "br":
            valid_br_ids = []
            # DO NOT UPDATED (REDIS RETRIEVAL METHOD HERE)
            validity_list_br = self.BR_redis.mget(id_list)
            for i, e in enumerate(id_list):
                if validity_list_br[i]:
                    valid_br_ids.append(e)
            return valid_br_ids
        else:
            raise ValueError("redis_db must be either 'ra' for responsible agents ids "
                             "or 'br' for bibliographic resources ids")

    # done
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

                # VALIDATE ORCID HERE (with same procedure used for br identifiers)
                orcid = self.crossref_processor.find_crossref_orcid(orcid)
                # END: VALIDATE ORCID HERE

            elif dict_orcid and f_name:
                for ori in dict_orcid:
                    orc_n: List[str] = dict_orcid[ori].split(', ')
                    orc_f = orc_n[0].lower()
                    orc_g = orc_n[1] if len(orc_n) == 2 else None
                    if f_name.lower() in orc_f.lower() or orc_f.lower() in f_name.lower():
                        if g_name and orc_g:
                            # If there are several authors with the same surname
                            if len([person for person in agents_list if 'family' in person if person['family'] if
                                    person['family'].lower() in orc_f.lower() or orc_f.lower() in person[
                                        'family'].lower()]) > 1:
                                # If there are several authors with the same surname and the same given names' initials
                                if len([person for person in agents_list if 'given' in person if person['given'] if
                                        person['given'][0].lower() == orc_g[0].lower()]) > 1:
                                    homonyms_list = [person for person in agents_list if 'given' in person if
                                                     person['given'] if person['given'].lower() == orc_g.lower()]
                                    # If there are homonyms
                                    if len(homonyms_list) > 1:
                                        # If such homonyms have different roles from the current role
                                        if [person for person in homonyms_list if person['role'] != cur_role]:
                                            if orc_g.lower() == g_name.lower():
                                                orcid = ori

                                    else:
                                        if orc_g.lower() == g_name.lower():
                                            orcid = ori
                                elif orc_g[0].lower() == g_name[0].lower():
                                    orcid = ori

                            # If there is a person whose given name is equal to the family name of the current person (a common situation for cjk names)
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



