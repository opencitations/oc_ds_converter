import gzip
import csv
import json
from os import makedirs
import os
from tqdm import tqdm
import os.path
from os.path import exists
from oc_idmanager.doi import DOIManager
from oc_idmanager.pmid import PMIDManager
from oc_idmanager.pmcid import PMCIDManager
from oc_idmanager.orcid import ORCIDManager
from oc_idmanager.arxiv import ArXivManager

from datetime import datetime
from argparse import ArgumentParser
import html
import json
import os
import pathlib
import re
import warnings
from os.path import exists
from typing import Dict, List, Tuple

import fakeredis
from bs4 import BeautifulSoup
from oc_ds_converter.datasource.redis import RedisDataSource
from re import search, match, sub
from oc_ds_converter.lib.cleaner import Cleaner
from oc_ds_converter.lib.master_of_regex import *
from oc_ds_converter.pubmed.get_publishers import ExtractPublisherDOI
from oc_ds_converter.ra_processor import RaProcessor

warnings.filterwarnings("ignore", category=UserWarning, module='bs4')


class OpenaireProcessing(RaProcessor):
    def __init__(self, orcid_index: str = None, doi_csv: str = None, publishers_filepath_openaire: str = None, testing:bool = True):
        super(OpenaireProcessing, self).__init__(orcid_index, doi_csv)
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
        self.doi_m = DOIManager()
        self.pmid_m = PMIDManager()
        self.pmc_m = PMCIDManager()
        self.orcid_m = ORCIDManager()
        self.arxiv_m = ArXivManager()
        self._id_man_dict = {"doi":self.doi_m, "pmid": self.pmid_m, "pmc": self.pmc_m, "arxiv":self.arxiv_m}
        self._doi_prefixes_publishers_dict = {
        "10.48550":{"publisher":"arxiv", "priority":1},
        "10.6084":{"publisher":"figshare","priority":1},
        "10.1184":{"publisher": "Carnegie Mellon University", "priority":2},
        "10.25384":{"publisher":"sage", "priority":2},
        "10.5281":{"publisher":"zenodo", "priority":3},
        "10.5061":{"publisher":"dryad", "priority":4},
        "10.17605":{"publisher":"psyarxiv", "priority":5},
        "10.31234": {"publisher":"psyarxiv", "priority":6},
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
                                                  "prefix_publishers.csv")
        else:
            self.publishers_filepath = publishers_filepath_openaire

        if os.path.exists(self.publishers_filepath):
            pfp = dict()
            csv_headers = (
                "id", "name", "prefix"
            )
            with open(self.publishers_filepath, encoding="utf8") as f:
                csv_reader = csv.DictReader(f, csv_headers)
                for row in csv_reader:
                    pfp[row["prefix"]] = {"name": row["name"], "crossref_member": row["id"]}

            if pfp:
                self.publisher_manager = ExtractPublisherDOI(pfp)
            else:
                self.publisher_manager = ExtractPublisherDOI({})
        else:
            self.publisher_manager = ExtractPublisherDOI({})
            with open(self.publishers_filepath, "w", encoding="utf8") as fdp:
                json.dump({}, fdp, ensure_ascii=False, indent=4)

    def get_id_manager(self, schema, id_man_dict):
        """Given as input the string of a schema (e.g.:'pmid') and a dictionary mapping strings of
        the schemas to their id managers, the method returns the correct id manager. Note that each
        instance of the Preprocessing class needs its own instances of the id managers, in order to
        avoid conflicts while validating data"""
        id_man = id_man_dict.get(schema)
        return id_man

    def dict_to_cache(self, dict_to_be_saved, path):
        with open(path, "w", encoding="utf-8") as fd:
            json.dump(dict_to_be_saved, fd, ensure_ascii=False, indent=4)

    def csv_creator(self, item: dict) -> dict:
        any_id = ""
        row = dict()
        
        doi = ""
        
        keys = ['id', 'title', 'author', 'pub_date', 'venue', 'volume', 'issue', 'page', 'type',
                'publisher', 'editor']
        for k in keys:
            row[k] = ''

        attributes = item
        #print("IIIII",  str(attributes).replace("'", '"'))

        # row['type'] √
        att_type = attributes.get("objectSubType")
        if att_type:
            map_type = self.types_dict.get(att_type)
            if not map_type:
                map_type = "other"
        else:
            map_type = "other"
        row['type'] = map_type

        # row['id'] DA RIVEDERE
        att_identifier_list = attributes.get("identifier")
        proc_ids_list, norm_doi = self.to_validated_id_list(att_identifier_list)

        # Keep a doi for retrieving information related to its prefix (i.e.: publisher, RA..) only in the cases
        # where there is only one doi to refer to or where all the dois have the same prefix.

        if len(norm_doi) == 1:
            doi = norm_doi[0]
        elif len(norm_doi) >= 1:
            pref_fist_doi = norm_doi[0].split('/')[0]
            if all(id.split('/')[0] == pref_fist_doi for id in norm_doi):
                doi = norm_doi[0]
        any_id = proc_ids_list[0] if proc_ids_list else ""
        row['id'] = ' '.join(proc_ids_list)

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
        authors_strings_list, editors_string_list = self.get_agents_strings_list(doi, agents_list)
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
            return self.normalise_unicode(row), any_id

        except TypeError:
            print(row)
            raise(TypeError)

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
        if not doi and not item:
            return ""

        data = {
            "publisher": item.get('name') if item else "",
            "member": None,
            "prefix": doi.split('/')[0] if doi else ""
        }

        publisher = data['publisher']
        member = data['member']
        prefix = data['prefix']

        retr_prefix_data = False

        if prefix:
            if prefix in self.publisher_manager._prefix_to_data_dict:
                retr_prefix_data = True


        if retr_prefix_data:
            prefix_data = self.publisher_manager.extract_publishers_v(doi, enable_extraagencies=False,get_all_prefix_data=False, skip_update=True)
            cr_m = ""
            retrieved_publisher_name = ""
            if prefix_data:
                cr_m = prefix_data.get("crossref_member") if prefix_data.get("crossref_member") != "not found" else ""
                retrieved_publisher_name = prefix_data.get("name") if prefix_data.get("name") != "unidentified" else ""

            if retrieved_publisher_name:
                if publisher:
                    if publisher.lower().strip() == retrieved_publisher_name.lower().strip() and cr_m:
                        member = cr_m
                else:
                    publisher = retrieved_publisher_name if retrieved_publisher_name else ""
                    member = cr_m if cr_m else ""

        name_and_id = f'{publisher} [crossref:{member}]' if member else publisher


        return name_and_id
    
    
    def to_validated_id_list(self, id_dict_list):
        """this method takes in input a list of id dictionaries and returns a list valid and existent ids with prefixes.
        For each id, a first validation try is made by checking its presence in META db. If the id is not in META db yet,
        a second attempt is made by using the specific id-schema API"""        
        valid_id_set = set()
        norm_doi = set()


        for ent in id_dict_list:
            schema = ent.get("schema")
            if isinstance(schema, str):
                schema = schema.strip().lower()
            id = ent.get("identifier")
            id_man = self.get_id_manager(schema, self._id_man_dict)
            if id_man:
                norm_id = id_man.normalise(id, include_prefix=True)
                # check if the id is in redis db
                if norm_id:
                    if schema == "doi":
                        norm_doi.add(":".join(norm_id.split(":")[1:]))
                    if self.BR_redis.get(norm_id):
                        valid_id_set.add(norm_id)
                    # if the id is not in redis db, validate it before appending
                    elif id_man.is_valid(norm_id):
                        valid_id_set.add(norm_id)

        valid_id_list = list(valid_id_set)
        norm_doi_list = list(norm_doi)
        return valid_id_list, norm_doi_list

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

    def normalise_arxiv_id(self, id):
        search_id_w = search("(\d{4}.\d{4,5}|[a-z\-]+(\.[A-Z]{2})?\/\d{7})(v\d+)?", id)
        if search_id_w:
            id_w_ver = search_id_w.group(0)
            if id_w_ver:
                return id_w_ver
        version = "v1"
        search_id = search("(\d{4}.\d{4,5}|[a-z\-]+(\.[A-Z]{2})?\/\d{7})", id)
        if search_id:
            id_no_ver = search_id.group(0)
            if id_no_ver:
                id_w_ver = id_no_ver + version
                return id_w_ver
        return None


        version = ""
        arxiv_string_match = search("(v\d+)$", id)
        if arxiv_string_match:
            version = arxiv_string_match[1]

        if version:
            pass




