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


def preprocess(openaire_json_dir:str, publishers_filepath:str, orcid_doi_filepath:str, csv_dir:str, wanted_doi_filepath:str=None, cache:str=None, verbose:bool=False) -> None:
    processed_ids = set()
    accepted_ids_br = {"doi", "pmid", "pmc", "arxiv"}
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
    openaire_csv = OpenaireProcessing(orcid_index=orcid_doi_filepath, doi_csv=wanted_doi_filepath, publishers_filepath_openaire=publishers_filepath)
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
                            source_ids = []
                            target_ids = []

                            source_entity = d.get("source")
                            # normalizza
                            if source_entity:
                                source_ids = [x.get("schema").strip().lower()+":"+x.get("identifier").strip().lower() for x in source_entity.get("identifier") if x.get("schema").strip().lower() in accepted_ids_br ]

                            target_entity = d.get("target")
                            # normalizza

                            if target_entity:
                                target_ids = [x.get("schema").strip().lower()+":"+x.get("identifier").strip().lower() for x in source_entity.get("identifier") if
                                              x.get("schema").strip().lower() in accepted_ids_br]

                            # check that there is a citation we can handle (i.e.: expressed with ids we actually manage)
                            if source_ids and target_ids:

                                citation = dict()
                                any_citing = ""
                                any_referenced = ""
                                
                                if not all(elem in processed_ids for elem in source_ids):
                                    source_tab_data = openaire_csv.csv_creator(source_entity)
                                    any_citing = source_tab_data["id"].split(" ")[0]
                                    citation["citing"] = any_citing
                                    if source_tab_data:
                                        data.append(source_tab_data)
                                    new_ids_s = {x for x in source_ids if x not in processed_ids}
                                    processed_ids.update(new_ids_s)
                                else:
                                    pass
                                    # RECUPERA UN ID ANCHE PER LE ENTITà GIA' PROCESSATE
                                
                                if not all(elem in processed_ids for elem in target_ids):
                                    target_tab_data = openaire_csv.csv_creator(target_entity)
                                    any_referenced = target_tab_data["id"].split(" ")[0]
                                    citation["referenced"] = any_referenced
                                    if target_tab_data:
                                        data.append(target_tab_data)
                                    new_ids_t = {x for x in target_ids if x not in processed_ids}
                                    processed_ids.update(new_ids_t)
                                    if any_citing and any_referenced:
                                        index_citations_to_csv.append(citation)
                                else:
                                    pass # RECUPERA UN ID ANCHE PER LE ENTITà GIA' PROCESSATE


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