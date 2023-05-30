import gzip
import csv
import json
from os import makedirs
import os
import os.path
from oc_ds_converter.oc_idmanager.arxiv import ArXivManager
from oc_ds_converter.oc_idmanager.doi import DOIManager
from oc_ds_converter.oc_idmanager.pmid import PMIDManager
from oc_ds_converter.oc_idmanager.pmcid import PMCIDManager
from oc_ds_converter.oc_idmanager.orcid import ORCIDManager

from datetime import datetime
from argparse import ArgumentParser
import html
import json
import os
import pathlib
import re
import warnings
from os.path import exists
from pathlib import Path
from typing import Dict, List, Tuple

import fakeredis
from bs4 import BeautifulSoup
from oc_ds_converter.datasource.redis import RedisDataSource
from re import search, match, sub
from oc_ds_converter.lib.cleaner import Cleaner
from oc_ds_converter.lib.master_of_regex import *
from oc_ds_converter.pubmed.get_publishers import ExtractPublisherDOI
from oc_ds_converter.ra_processor import RaProcessor
from oc_ds_converter.oc_idmanager.oc_data_storage.storage_manager import StorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.in_memory_manager import InMemoryStorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.sqlite_manager import SqliteStorageManager

warnings.filterwarnings("ignore", category=UserWarning, module='bs4')


class OpenaireProcessing(RaProcessor):
    def __init__(self, orcid_index: str = None, doi_csv: str = None, publishers_filepath_openaire: str = None, testing:bool = True, storage_manager:StorageManager=None):
        super(OpenaireProcessing, self).__init__(orcid_index, doi_csv)
        if storage_manager is None:
            self.storage_manager = SqliteStorageManager()
        else:
            self.storage_manager = storage_manager

        self.types_dict = {
            "Article": "journal article",
            "Part of book or chapter of book": "book chapter",
            "Preprint": "other",
            "Other literature type": "other",
            "Conference object": "proceedings",
            "Doctoral thesis": "dissertation",
            "Book": "book",
            "Thesis": "dissertation",
            "Research": "other",
            "Master thesis": "dissertation",
            "Report": "report",
            "Review": "other",
            "Contribution for newspaper or weekly magazine": "other",
            "Journal": "journal",
            "Presentation": "other",
            "Software Paper": "other",
            "External research report": "report",
            "Data Paper": "other",
            "Project deliverable": "other",
            "Bachelor thesis": "dissertation",
            "Project proposal": "other",
            "Newsletter": "other",
            "Data Management Plan": "data management plan",
            "Software": "computer program",
            "Dataset": "dataset",
            "Audiovisual": "dataset",
            "Image": "dataset",
            "Other dataset type": "dataset",
            "Film": "dataset",
            "UNKNOWN": "other",
            "Other ORP type": "other",
            "InteractiveResource": "other",
            "PhysicalObject": "other",
            "Collection": "other",
            "Patent": "other",
            "Project milestone": "other",
            "Clinical Trial": "other",
            "Bioentity": "other",
            "Sound": "other",
        }
        self.doi_m = DOIManager(storage_manager=self.storage_manager)
        self.pmid_m = PMIDManager(storage_manager=self.storage_manager)
        self.pmc_m = PMCIDManager(storage_manager=self.storage_manager)
        self.arxiv_m = ArXivManager(storage_manager=self.storage_manager)

        self.orcid_m = ORCIDManager(storage_manager=self.storage_manager)

        self._id_man_dict = {"doi":self.doi_m, "pmid": self.pmid_m, "pmcid": self.pmc_m, "arxiv":self.arxiv_m}

        self._doi_prefixes_publishers_dict = {
        "10.48550":{"publisher":"arxiv", "priority":1},
        "doi:10.48550":{"publisher":"arxiv", "priority":1},
        "10.6084":{"publisher":"figshare","priority":1},
        "doi:10.6084":{"publisher":"figshare","priority":1},
        "10.1184":{"publisher": "Carnegie Mellon University", "priority":2},
        "doi:10.1184":{"publisher": "Carnegie Mellon University", "priority":2},
        "10.25384":{"publisher":"sage", "priority":2},
        "doi:10.25384":{"publisher":"sage", "priority":2},
        "10.5281":{"publisher":"zenodo", "priority":3},
        "doi:10.5281":{"publisher":"zenodo", "priority":3},
        "10.5061":{"publisher":"dryad", "priority":4},
        "doi:10.5061":{"publisher":"dryad", "priority":4},
        "10.17605":{"publisher":"psyarxiv", "priority":5},
        "doi:10.17605":{"publisher":"psyarxiv", "priority":5},
        "10.31234": {"publisher":"psyarxiv", "priority":6},
        "doi:10.31234": {"publisher":"psyarxiv", "priority":6},
        }

        if testing:
            self.BR_redis= fakeredis.FakeStrictRedis()
            self.RA_redis= fakeredis.FakeStrictRedis()

        else:
            self.BR_redis = RedisDataSource("DB-META-BR")
            self.RA_redis = RedisDataSource("DB-META-RA")


        if not publishers_filepath_openaire:

            if not exists(os.path.join(pathlib.Path(__file__).parent.resolve(), "support_files")):
                os.makedirs(os.path.join(pathlib.Path(__file__).parent.resolve(), "support_files"))
            self.publishers_filepath = os.path.join(pathlib.Path(__file__).parent.resolve(), "support_files",
                                                  "prefix_publishers.json")
        else:
            self.publishers_filepath = publishers_filepath_openaire

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

            if pfp:
                self.publisher_manager = ExtractPublisherDOI(pfp)
            else:
                self.publisher_manager = ExtractPublisherDOI({})
        else:
            self.publisher_manager = ExtractPublisherDOI({})
            with open(self.publishers_filepath, "w", encoding="utf8") as fdp:
                json.dump({}, fdp, ensure_ascii=False, indent=4)


    def validated_as(self, id_dict):
        schema = id_dict["schema"]
        id = id_dict["identifier"]
        id_m = self.get_id_manager(schema, self._id_man_dict)
        return id_m.validated_as_id(id)


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

    def normalise_any_id(self, id_with_prefix):
        id_man = self.get_id_manager(id_with_prefix, self._id_man_dict)
        id_no_pref = ":".join(id_with_prefix.split(":")[1:])
        norm_id_w_pref = id_man.normalise(id_no_pref, include_prefix=True)
        return norm_id_w_pref

    def get_norm_ids(self, entity):
        norm_ids = []
        for e in entity:
            e_schema = e.get("schema").strip().lower()
            if e_schema in self._id_man_dict:
                e_id = self._id_man_dict[e_schema].normalise(e["identifier"], include_prefix=True)
                if e_id:
                    dict_to_append = {"schema": e_schema, "identifier": e_id}
                    if dict_to_append not in norm_ids:
                        norm_ids.append(dict_to_append)
        return norm_ids

    def dict_to_cache(self, dict_to_be_saved, path):
        path = Path(path)
        parent_dir_path = path.parent.absolute()
        if not os.path.exists(parent_dir_path):
            Path(parent_dir_path).mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as fd:
            json.dump(dict_to_be_saved, fd, ensure_ascii=False, indent=4)

    def csv_creator(self, item: dict) -> dict:
        row = dict()
        
        doi = []
        
        keys = ['id', 'title', 'author', 'pub_date', 'venue', 'volume', 'issue', 'page', 'type',
                'publisher', 'editor']
        for k in keys:
            row[k] = ''

        attributes = item
        # row['type'] √
        att_type = attributes.get("objectSubType")
        if att_type:
            map_type = self.types_dict.get(att_type)
            if not map_type:
                map_type = "other"
        else:
            map_type = "other"
        row['type'] = map_type

        # row['id']
        att_identifier_dict_of_lists = attributes.get("identifier")
        valid_ids_list = self.to_validated_id_list(att_identifier_dict_of_lists)

        # Keep a doi for retrieving information related to its prefix (i.e.: publisher, RA..) only in the cases
        # where there is only one doi to refer to or where all the dois have the same prefix.
        if valid_ids_list:
            for id in valid_ids_list:
                if id.startswith("doi:"):
                    doi.append(id[len("doi:"):])
            row['id'] = ' '.join(valid_ids_list)
        else:
            return {}


        # row['title'] √
        pub_title = ""
        att_title = attributes.get("title")
        if att_title:
            p_title = att_title
            soup = BeautifulSoup(p_title, 'html.parser')
            title_soup = soup.get_text().replace('\n', '')
            title_soup_space_replaced = ' '.join(title_soup.split())
            title_soup_strip = title_soup_space_replaced.strip()
            clean_tit = html.unescape(title_soup_strip)
            pub_title = clean_tit if clean_tit else p_title
        
        row['title'] = pub_title

        # row['author'] √
        agents_list = self.add_authors_to_agent_list(attributes, [])
        # EVITARE API !!!!!!!
        pref_dois = [x for x in doi if x.split("/")[0] not in self._doi_prefixes_publishers_dict]
        if doi:
            best_doi = pref_dois[0] if pref_dois else doi[0]
        else:
            best_doi = ""
        authors_strings_list, editors_string_list = self.get_agents_strings_list(best_doi, agents_list)
        row['author'] = '; '.join(authors_strings_list)

        # row['pub_date'] √
        dates = attributes.get("publicationDate")
        row['pub_date'] = str(dates) if dates else ""

        # row['venue']
        row['venue'] = ""

        # row['volume']
        row['volume'] = ""

        # row['issue']
        row['issue'] = ""

        # row['page']
        row['page'] = ""

        # row['publisher']  √
        att_publ = attributes.get("publisher")
        publ = ""
        if att_publ:
            publ = att_publ[0]
        publishers = self.get_publisher_name(doi, publ)
                    
        row['publisher'] = publishers

        # row['editor']
        row['editor'] = ""

        try:
            return self.normalise_unicode(row)

        except TypeError:
            print(row)
            raise(TypeError)

    def get_publisher_name(self, doi_list: list, item: dict) -> str:
        '''
        This function aims to return a publisher's name and id. If a mapping was provided,
        it is used to find the publisher's standardized name from its id or DOI prefix.

        :params doi: the item's DOI
        :type doi_list: list
        :params item: the item's dictionary
        :type item: dict
        :returns: str -- The output is a string in the format 'NAME [SCHEMA:ID]', for example, 'American Medical Association (AMA) [crossref:10]'. If the id does not exist, the output is only the name. Finally, if there is no publisher, the output is an empty string.
        '''
        if not item or not isinstance(item, dict):
            return ""
        elif "name" not in item:
            return ""

        publisher = item.get("name") if item.get("name") else ""

        if publisher and doi_list:
            for doi in doi_list:
                prefix = doi.split('/')[0] if doi else ""
                if prefix:
                    if prefix in self.publisher_manager._prefix_to_data_dict:
                        prefix_data = self.publisher_manager.extract_publishers_v(doi, enable_extraagencies=False,get_all_prefix_data=True, skip_update=True)
                        if prefix_data:
                            member = prefix_data.get("crossref_member") if prefix_data.get("crossref_member") not in {"not found", None} else ""
                            retrieved_publisher_name = prefix_data.get("name") if prefix_data.get("name") not in {"unidentified", None} else ""
                            if isinstance(retrieved_publisher_name, str):
                                if publisher.lower().strip() == retrieved_publisher_name.lower().strip():
                                    return f'{publisher} [crossref:{member}]' if member else publisher

        return publisher

    def manage_arxiv_single_id(self, id_dict_list):
        result_dict_list = []
        arxiv_id = ""
        is_arxiv = False
        ent = id_dict_list[0]
        schema = ent.get("schema")
        if isinstance(schema, str):
            schema = schema.strip().lower()
            if schema == "doi":
                id = ent.get("identifier")
                splitted_pref = id.split('/')[0]
                pref = re.findall("(10.\d{4,9})", splitted_pref)[0]
                if pref == "10.48550":
                    if id.startswith("doi:"):
                        id = id[len("doi:"):]
                    id_no_pref = id.replace(pref,"")

                    arxiv_id = self._id_man_dict["arxiv"].normalise(id_no_pref, include_prefix=True)
                    if not arxiv_id:
                        return None
                    else:
                        is_arxiv = True
            elif schema == "arxiv":
                id = ent.get("identifier")
                arxiv_id = self._id_man_dict["arxiv"].normalise(id, include_prefix=True)
                if not arxiv_id:
                    return None
                else:
                    is_arxiv = True
            else:
                return id_dict_list
        if is_arxiv:
            result_dict_list = [{"schema": "arxiv", "identifier": arxiv_id}]

        if not result_dict_list:
            return id_dict_list
        return result_dict_list

    def manage_doi_prefixes_priorities(self, id_dict_list):
        result_id_dict_list= []
        priority_prefixes = [k for k,v in self._doi_prefixes_publishers_dict.items() if v.get("priority")==1]
        arxiv_or_figshare_dois = [x for x in id_dict_list if x.get("identifier").split("/")[0] in priority_prefixes]
        if len(arxiv_or_figshare_dois) == 1:
            id_dict = arxiv_or_figshare_dois[0]
            is_arxiv = self._doi_prefixes_publishers_dict[id_dict.get("identifier").split("/")[0]].get("publisher") == "arxiv"
            has_version = search("v\d+", id_dict.get("identifier"))
            if has_version: # It is necessarily a figshare doi (ARXIV have version only in arxiv id and not in arxiv dois)
                #√
                return arxiv_or_figshare_dois
            else:
                if not is_arxiv:
                    upd_id = id_dict.get("identifier") + "v1"
                    upd_dict = {k:v for k,v in id_dict.items() if k!= "identifier"}
                    upd_dict["identifier"] = upd_id
                    result_id_dict_list.append(upd_dict)
                    # √
                    return result_id_dict_list
                else:
                    # √
                    return self.manage_arxiv_single_id([id_dict])

        elif len(arxiv_or_figshare_dois) > 1:
            versioned_arxiv_or_figshare_dois = [x for x in arxiv_or_figshare_dois if search("v\d+", x.get("identifier"))]
            if versioned_arxiv_or_figshare_dois:
                # √
                return versioned_arxiv_or_figshare_dois
            else:
                for id_dict in arxiv_or_figshare_dois:
                    if self._doi_prefixes_publishers_dict[id_dict.get("identifier").split("/")[0]].get("publisher") == "arxiv":
                        # in order to avoid multiple ids of the same schema for the same entity without a reasonable expl.
                        # √
                        return self.manage_arxiv_single_id([id_dict])

                for id_dict in arxiv_or_figshare_dois:
                    if self._doi_prefixes_publishers_dict[id_dict.get("identifier").split("/")[0]].get("publisher") == "figshare":
                        version = "v1"
                        upd_dict = {k:v for k,v in id_dict.items() if k != "identifier"}
                        upd_id = id_dict.get("identifier") + version
                        upd_dict["identifier"] = upd_id
                        result_id_dict_list.append(upd_dict)
                        # √
                        return result_id_dict_list
        else:
            zenodo_ids_list = [x for x in id_dict_list if self._doi_prefixes_publishers_dict[x.get("identifier").split("/")[0]].get("publisher") == "zenodo"]
            if len(zenodo_ids_list) >= 2:
                list_of_id_n_str = [x["identifier"].replace("doi:", "").replace("10.5281/zenodo.", "") for x in zenodo_ids_list]
                list_of_id_n_int = []
                for n in list_of_id_n_str:
                    try:
                        int_n = int(n)
                        list_of_id_n_int.append(int_n)
                    except:
                        pass
                if list_of_id_n_int:
                    last_assigned_id = str(max(list_of_id_n_int))
                    for id_dict in zenodo_ids_list:
                        if id_dict.get("identifier").replace("doi:", "").replace("10.5281/zenodo.", "") == last_assigned_id:
                            result_id_dict_list.append(id_dict)
                            # √
                            return result_id_dict_list
            else:
                prefix_set = {x.get("identifier").split("/")[0] for x in id_dict_list}
                priorities = [self._doi_prefixes_publishers_dict[p]["priority"] for p in prefix_set]
                max_priority = min(priorities)

                prefixes_w_max_priority = {k for k,v in self._doi_prefixes_publishers_dict.items() if v["priority"] == max_priority}

                for id_dict in id_dict_list:
                    if id_dict.get("identifier").split("/")[0] in prefixes_w_max_priority:
                        norm_id = self.doi_m.normalise(id_dict["identifier"], include_prefix=True)

                        if self.BR_redis.get(norm_id):
                            result_id_dict_list.append(id_dict)
                            return result_id_dict_list
                        # if the id is not in redis db, validate it before appending
                        elif self.doi_m.is_valid(norm_id):
                            result_id_dict_list.append(id_dict)
                            return result_id_dict_list

                if not result_id_dict_list:

                    while id_dict_list and max_priority < 7:

                        id_dict_list = [x for x in id_dict_list if x["identifier"].split("/")[0] not in prefixes_w_max_priority]
                        max_priority += 1
                        prefixes_w_max_priority = {k for k, v in self._doi_prefixes_publishers_dict.items() if
                                                   v["priority"] == max_priority}

                        for id_dict in id_dict_list:
                            if id_dict.get("identifier").split("/")[0] in prefixes_w_max_priority:
                                norm_id = self.doi_m.normalise(id_dict["identifier"], include_prefix=True)

                                if self.BR_redis.get(norm_id):
                                    result_id_dict_list.append(id_dict)
                                    return result_id_dict_list
                                # if the id is not in redis db, validate it before appending
                                elif self.doi_m.is_valid(norm_id):
                                    result_id_dict_list.append(id_dict)
                                    return result_id_dict_list



        return result_id_dict_list

    def to_validated_id_list(self, id_dict_of_list):
        """this method takes in input a list of id dictionaries and returns a list valid and existent ids with prefixes.
        For each id, a first validation try is made by checking its presence in META db. If the id is not in META db yet,
        a second attempt is made by using the specific id-schema API"""
        valid_id_set = set([x["identifier"] for x in id_dict_of_list["valid"]])
        to_be_processed_input = id_dict_of_list["to_be_val"]
        to_be_processed_id_dict_list = []
        # If there is only an id, check whether it is either an arxiv id or an arxiv doi. In this cases, if there is a
        # versioned arxiv id, it is kept as such. Otherwise both the arxiv doi and the not versioned arxiv id are replaced
        # with the v1 version of the arxiv id. If it is not possible to retrieve an arxiv id from the only id which is
        # either declared as an arxiv id or starts with the arxiv doi prefix, return None and interrupt the process
        if len(valid_id_set) == 0 and len(to_be_processed_input)==1 :
            single_id_dict_list = self.manage_arxiv_single_id(to_be_processed_input)
            if single_id_dict_list:
                to_be_processed_id_dict_list = single_id_dict_list
            else:
                return
        elif len(valid_id_set) == 0 and len(to_be_processed_input)> 1:
            second_selection_list = [x for x in to_be_processed_input if x.get("schema") == "pmid" or (x.get("schema") =="doi" and x.get("identifier").split('/')[0] not in self._doi_prefixes_publishers_dict)]
            if second_selection_list:
                to_be_processed_id_dict_list = second_selection_list
            else:
                third_selection = [x for x in to_be_processed_input if x.get("schema") == "pmc"]
                if third_selection:
                    to_be_processed_id_dict_list = third_selection
                else:
                    fourth_selection = [x for x in to_be_processed_input if x.get("schema") == "arxiv"]
                    if fourth_selection:
                        to_be_processed_id_dict_list = fourth_selection
                    else:
                        fifth_selection =  [x for x in to_be_processed_input if x.get("schema") == "doi" and x.get("identifier").split('/')[0] in self._doi_prefixes_publishers_dict]
                        if fifth_selection:
                            to_be_processed_id_dict_list = self.manage_doi_prefixes_priorities(fifth_selection)

        else:
            return None

        if to_be_processed_id_dict_list:
            for ent in to_be_processed_id_dict_list:
                schema = ent.get("schema")
                norm_id = ent.get("identifier")
                id_man = self.get_id_manager(schema, self._id_man_dict)
                if schema == "pmid" or (schema =="doi" and norm_id.split('/')[0] not in self._doi_prefixes_publishers_dict):
                    if self.BR_redis.get(norm_id):
                        id_man.storage_manager.set_value(norm_id, True) #In questo modo l'id presente in redis viene inserito anche nello storage e risulta già
                        # preso in considerazione negli step successivi
                        valid_id_set.add(norm_id)
                    # if the id is not in redis db, validate it before appending
                    elif id_man.is_valid(norm_id):#In questo modo l'id presente in redis viene inserito anche nello storage e risulta già
                        # preso in considerazione negli step successivi
                        valid_id_set.add(norm_id)

        valid_id_list = list(valid_id_set)
        return valid_id_list

    def add_authors_to_agent_list(self, item: dict, ag_list: list) -> list:
        '''
        This function returns the the agents list updated with the authors dictionaries, in the correct format.

        :params item: the item's dictionary (attributes), ag_list: the agent list
        :type item: dict, ag_list: list

        :returns: list the agents list updated with the authors dictionaries, in the correct format.
        '''
        agent_list = ag_list
        if item.get("creator"):
            for author in item.get("creator"):
                agent = {}
                agent["role"] = "author"
                agent["name"] = author.get("name") if author.get("name") else ""
                missing_names = [x for x in ["family", "given"] if x not in agent]
                for mn in missing_names:
                    agent[mn] = ""
                all_ids = author.get("identifiers")
                orcid_id = self.find_openaire_orcid(all_ids)
                if orcid_id:
                    agent["orcid"] = orcid_id
                agent_list.append(agent)
        
        return agent_list

    def find_openaire_orcid(self, all_author_ids):
        orcid = ""
        if all_author_ids:
            for id in all_author_ids:
                schema = id.get("schema")
                identifier = id.get("identifier")
                if isinstance(schema, str):
                    if schema.lower().strip() == "orcid":
                        if isinstance(identifier, str):
                            norm_orcid = self.orcid_m.normalise(identifier, include_prefix =False)
                            if self.RA_redis.get(norm_orcid):
                                orcid = norm_orcid
                            # if the id is not in redis db, validate it before appending
                            elif self.orcid_m.is_valid(norm_orcid):
                                orcid = norm_orcid
        return orcid