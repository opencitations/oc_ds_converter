import gzip
import json
from os import makedirs
import os
from tqdm import tqdm
import os.path
from os.path import exists
from datetime import datetime
from argparse import ArgumentParser
import csv
import os
import sys
from argparse import ArgumentParser
from tarfile import TarInfo

import yaml
from tqdm import tqdm

from oc_ds_converter.lib.file_manager import normalize_path
from oc_ds_converter.lib.jsonmanager import *
from oc_ds_converter.openaire.openaire_processing import *
from oc_idmanager.doi import DOIManager
from oc_idmanager.pmid import PMIDManager
from oc_idmanager.pmcid import PMCIDManager
from oc_ds_converter.oc_idmanager.arxiv import ArXivManager
from oc_ds_converter.oc_idmanager.oc_data_storage.storage_manager import StorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.in_memory_manager import InMemoryStorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.sqlite_manager import SqliteStorageManager
import sqlite3


def preprocess(openaire_json_dir:str, publishers_filepath:str, orcid_doi_filepath:str, csv_dir:str, wanted_doi_filepath:str=None, cache:str=None, verbose:bool=False, storage_manager:StorageManager=None, storage_path:str = None) -> None:

    storage_manager = storage_manager if storage_manager else SqliteStorageManager()

    if not storage_path or not os.path.exists(storage_path):
        new_path_dir = os.path.join(os.getcwd(), "storage")
        if not os.path.exists(new_path_dir):
            os.makedirs(new_path_dir)
        if storage_manager == SqliteStorageManager():
            storage_manager = SqliteStorageManager(os.path.join(new_path_dir, "id_valid_dict.db"))
        else:
            storage_manager = InMemoryStorageManager(os.path.join(new_path_dir, "id_valid_dict.json"))

    else:
        if storage_manager == SqliteStorageManager():
            storage_manager = SqliteStorageManager(storage_path)
        else:
            storage_manager = InMemoryStorageManager(storage_path)


    req_type = ".gz"
    preprocessed_citations_dir = csv_dir + "_citations"
    if not os.path.exists(preprocessed_citations_dir):
        makedirs(preprocessed_citations_dir)
    if verbose:
        if publishers_filepath or orcid_doi_filepath or wanted_doi_filepath:
            what = list()
            if publishers_filepath:
                what.append('publishers mapping')
            if orcid_doi_filepath:
                what.append('DOI-ORCID index')
            if wanted_doi_filepath:
                what.append('wanted DOIs CSV')
            log = '[INFO: openaire_process] Processing: ' + '; '.join(what)
            print(log)

    openaire_csv = OpenaireProcessing(orcid_index=orcid_doi_filepath, doi_csv=wanted_doi_filepath, publishers_filepath_openaire=publishers_filepath, storage_manager=storage_manager)
    if verbose:
        print(f'[INFO: openaire_process] Getting all files from {openaire_json_dir}')



    for tar in tqdm(os.listdir(openaire_json_dir)):
        all_files, targz_fd = get_all_files_by_type(os.path.join(openaire_json_dir, tar), req_type, cache)

        if verbose:
            pbar = tqdm(total=len(all_files))

        for filename in all_files:
            index_citations_to_csv = []
            f = gzip.open(filename, 'rb')
            source_data = f.readlines()
            filename = filename.name if isinstance(filename, TarInfo) else filename
            filename_without_ext = filename.replace('.json', '').replace('.tar', '').replace('.gz', '')
            filepath = os.path.join(csv_dir, f'{os.path.basename(filename_without_ext)}.csv')
            filepath_citations = os.path.join(preprocessed_citations_dir, f'{os.path.basename(filename_without_ext)}.csv')
            pathoo(filepath)
            data = list()

            for entity in tqdm(source_data):
                if entity:
                    d = json.loads(entity.decode('utf-8'))
                    if d.get("relationship"):
                        if d.get("relationship").get("name") == "Cites":
                            skip_source_process = False
                            skip_target_process = False

                            norm_source_ids = []
                            norm_target_ids = []

                            any_source_id = ""
                            any_target_id = ""

                            valid_citation_ids_s = []
                            valid_citation_ids_t = []

                            source_entity = d.get("source")
                            if source_entity:
                                norm_source_ids = openaire_csv.get_norm_ids(source_entity)
                                new_ids_s = False
                                valid_citation_ids_s = []
                                if norm_source_ids:
                                    for nsi in norm_source_ids:
                                        stored_validity = openaire_csv.validated_as(nsi)
                                        if not isinstance(stored_validity, bool):
                                            new_ids_s = True
                                        elif stored_validity is True:
                                            valid_citation_ids_s.append(nsi["identifier"])

                                skip_source_process = True if not new_ids_s else False
                                any_source_id = valid_citation_ids_s[0] if valid_citation_ids_s else ""



                            target_entity = d.get("target")
                            if target_entity:
                                norm_target_ids = openaire_csv.get_norm_ids(target_entity)
                                new_ids_t = False
                                valid_citation_ids_t = []
                                if norm_target_ids:
                                    for nti in norm_target_ids:
                                        stored_validity_t = openaire_csv.validated_as(nti)
                                        if not isinstance(stored_validity_t, bool):
                                            new_ids_t = True
                                        elif stored_validity_t is True:
                                            valid_citation_ids_t.append(nti["identifier"])

                                skip_target_process = True if not new_ids_t else False
                                any_target_id = valid_citation_ids_t[0] if valid_citation_ids_t else ""


                            # check that there is a citation we can handle (i.e.: expressed with ids we actually manage)
                            if norm_source_ids and norm_target_ids:


                                if not skip_source_process:
                                    source_tab_data = openaire_csv.csv_creator(source_entity) #valid_citation_ids_s --> evitare rivalidazione ?
                                    if source_tab_data:
                                        all_citing = source_tab_data["id"].split(" ") # VERIFICARE PER SINGOLO ID
                                        if not any_source_id:
                                            any_source_id = all_citing[0]
                                        data.append(source_tab_data)

                                if not skip_target_process:
                                    target_tab_data = openaire_csv.csv_creator(target_entity) # valid_citation_ids_t  --> evitare rivalidazione ?
                                    if target_tab_data:
                                        all_cited = target_tab_data["id"].split(" ") # VERIFICARE PER SINGOLO ID
                                        if not any_target_id:
                                            any_target_id = all_cited[0]

                                        data.append(target_tab_data)


                            if any_source_id and any_target_id:
                                citation = dict()
                                citation["citing"] = any_source_id
                                citation["cited"] = any_target_id
                                index_citations_to_csv.append(citation)

            if data:
                with open(filepath, 'w', newline='', encoding='utf-8') as output_file:
                    dict_writer = csv.DictWriter(output_file, data[0].keys(), delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC, escapechar='\\')
                    dict_writer.writeheader()
                    dict_writer.writerows(data)

            if index_citations_to_csv:
                with open(filepath_citations, 'w', newline='', encoding='utf-8') as output_file_citations:
                    dict_writer = csv.DictWriter(output_file_citations, index_citations_to_csv[0].keys(), delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC, escapechar='\\')
                    dict_writer.writeheader()
                    dict_writer.writerows(index_citations_to_csv)

            if cache:
                with open(cache, 'a', encoding='utf-8') as aux_file:
                    aux_file.write(os.path.basename(filename) + '\n')

            pbar.update() if verbose else None
        pbar.close() if verbose else None

    if cache:
        if os.path.exists(cache):
            os.remove(cache)

def pathoo(path:str) -> None:
    if not os.path.exists(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path))

if __name__ == '__main__':
    arg_parser = ArgumentParser('openaire_process.py', description='This script creates CSV files from Openaire JSON files, enriching them through of a DOI-ORCID index')
    arg_parser.add_argument('-c', '--config', dest='config', required=False,
                            help='Configuration file path')
    required = not any(arg in sys.argv for arg in {'--config', '-c'})
    arg_parser.add_argument('-cf', '--openaire', dest='openaire_json_dir', required=required,
                            help='Openaire json files directory')
    arg_parser.add_argument('-out', '--output', dest='csv_dir', required=required,
                            help='Directory where CSV will be stored')
    arg_parser.add_argument('-p', '--publishers', dest='publishers_filepath', required=False,
                            help='CSV file path containing information about publishers (id, name, prefix)')
    arg_parser.add_argument('-ep', '--ext_prefixes', dest='ext_prefixes_filepath', required=False,
                            help='json file path containing prefixes external to crossref')
    arg_parser.add_argument('-o', '--orcid', dest='orcid_doi_filepath', required=False,
                            help='DOI-ORCID index filepath, to enrich data')
    arg_parser.add_argument('-w', '--wanted', dest='wanted_doi_filepath', required=False,
                            help='A CSV filepath containing what DOI to process, not mandatory')
    arg_parser.add_argument('-ca', '--cache', dest='cache', required=False,
                        help='The cache file path. This file will be deleted at the end of the process')
    arg_parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', required=False,
                            help='Show a loading bar, elapsed time and estimated time')
    args = arg_parser.parse_args()
    config = args.config
    settings = None
    if config:
        with open(config, encoding='utf-8') as f:
            settings = yaml.full_load(f)
    openaire_json_dir = settings['openaire_json_dir'] if settings else args.openaire_json_dir
    openaire_json_dir = normalize_path(openaire_json_dir)
    csv_dir = settings['output'] if settings else args.csv_dir
    csv_dir = normalize_path(csv_dir)
    publishers_filepath = settings['publishers_filepath'] if settings else args.publishers_filepath
    publishers_filepath = normalize_path(publishers_filepath) if publishers_filepath else None
    orcid_doi_filepath = settings['orcid_doi_filepath'] if settings else args.orcid_doi_filepath
    orcid_doi_filepath = normalize_path(orcid_doi_filepath) if orcid_doi_filepath else None
    wanted_doi_filepath = settings['wanted_doi_filepath'] if settings else args.wanted_doi_filepath
    wanted_doi_filepath = normalize_path(wanted_doi_filepath) if wanted_doi_filepath else None
    cache = settings['cache_filepath'] if settings else args.cache
    cache = normalize_path(cache) if cache else None
    verbose = settings['verbose'] if settings else args.verbose
    preprocess(openaire_json_dir=openaire_json_dir, publishers_filepath=publishers_filepath,
               orcid_doi_filepath=orcid_doi_filepath, csv_dir=csv_dir, wanted_doi_filepath=wanted_doi_filepath, cache=cache, verbose=verbose)