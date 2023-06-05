import os.path
import sys
from tarfile import TarInfo

import yaml
from tqdm import tqdm

from oc_ds_converter.lib.file_manager import normalize_path
from oc_ds_converter.lib.jsonmanager import *
from oc_ds_converter.openaire.openaire_processing import *
from oc_ds_converter.oc_idmanager.oc_data_storage.storage_manager import StorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.in_memory_manager import InMemoryStorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.sqlite_manager import SqliteStorageManager


def preprocess(openaire_json_dir:str, publishers_filepath:str, orcid_doi_filepath:str, csv_dir:str, wanted_doi_filepath:str=None, cache:str=None, verbose:bool=False, storage_manager:StorageManager=None, storage_path:str = None) -> None:

    storage_manager = storage_manager if storage_manager else SqliteStorageManager()

    if not os.path.exists(csv_dir):
        os.makedirs(csv_dir)

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

    all_input_tar = os.listdir(openaire_json_dir)
    pbar_tar = tqdm(all_input_tar)
    for tar in pbar_tar:
        pbar_tar.set_description("Processing %s" % tar)
        all_files, targz_fd = get_all_files_by_type(os.path.join(openaire_json_dir, tar), req_type, cache)

        pbar_all_files = tqdm(all_files)
        for filename in pbar_all_files:
            pbar_all_files.set_description(f'Processing File: {filename.split("/")[-1]} in tar {tar}')
            index_citations_to_csv = []
            f = gzip.open(filename, 'rb')
            source_data = f.readlines()
            f.close()
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

                            norm_source_ids = []
                            norm_target_ids = []

                            any_source_id = ""
                            any_target_id = ""

                            source_entity = d.get("source")
                            if source_entity:
                                norm_source_ids = openaire_csv.get_norm_ids(source_entity['identifier'])
                                if norm_source_ids:
                                    for e, nsi in enumerate(norm_source_ids):
                                        stored_validity = openaire_csv.validated_as(nsi)
                                        norm_source_ids[e]["valid"] = stored_validity


                            target_entity = d.get("target")
                            if target_entity:
                                norm_target_ids = openaire_csv.get_norm_ids(target_entity['identifier'])
                                if norm_target_ids:
                                    for i, nti in enumerate(norm_target_ids):
                                        stored_validity_t = openaire_csv.validated_as(nti)
                                        norm_target_ids[i]["valid"] = stored_validity_t

                            # check that there is a citation we can handle (i.e.: expressed with ids we actually manage)
                            if norm_source_ids and norm_target_ids:

                                source_entity_upd_ids = {k:v for k,v in source_entity.items() if k != "identifier"}
                                source_valid_ids = [x for x in norm_source_ids if x["valid"] is True]
                                source_invalid_ids = [x for x in norm_source_ids if x["valid"] is False]
                                source_to_be_val_ids = [x for x in norm_source_ids if x["valid"] is None]
                                source_identifier = {}
                                source_identifier["valid"] = source_valid_ids
                                source_identifier["not_valid"] = source_invalid_ids
                                source_identifier["to_be_val"] = source_to_be_val_ids
                                source_entity_upd_ids["identifier"] = source_identifier

                                target_entity_upd_ids = {k:v for k,v in target_entity.items() if k != "identifier"}
                                target_valid_ids = [x for x in norm_target_ids if x["valid"] is True]
                                target_invalid_ids = [x for x in norm_target_ids if x["valid"] is False]
                                target_to_be_val_ids = [x for x in norm_target_ids if x["valid"] is None]
                                target_identifier = {}
                                target_identifier["valid"] = target_valid_ids
                                target_identifier["not_valid"] = target_invalid_ids
                                target_identifier["to_be_val"] = target_to_be_val_ids
                                target_entity_upd_ids["identifier"] = target_identifier

                                # creation of a new row in meta table because there are new ids to be validated.
                                # "any_source_id" will be chosen among the valid source entity ids, if any
                                if source_identifier["to_be_val"]:
                                    source_tab_data = openaire_csv.csv_creator(source_entity_upd_ids) #valid_citation_ids_s --> evitare rivalidazione ?
                                    if source_tab_data:
                                        processed_source_ids = source_tab_data["id"].split(" ")
                                        all_citing_valid = processed_source_ids
                                        if all_citing_valid: # It meanst that there is at least one valid id for the citing entity
                                            any_source_id = all_citing_valid[0]
                                            data.append(source_tab_data) # Otherwise the row should not be included in meta tables


                                # skip creation of a new row in meta table because there is no new id to be validated
                                # "any_source_id" will be chosen among the valid source entity ids, if any
                                elif source_identifier["valid"]:
                                    all_citing_valid = source_identifier["valid"]
                                    any_source_id = all_citing_valid[0]["identifier"]

                                # creation of a new row in meta table because there are new ids to be validated.
                                # "any_target_id" will be chosen among the valid target entity ids, if any
                                if target_identifier["to_be_val"]:
                                    target_tab_data = openaire_csv.csv_creator(target_entity_upd_ids) # valid_citation_ids_t  --> evitare rivalidazione ?
                                    if target_tab_data:
                                        processed_target_ids = target_tab_data["id"].split(" ")
                                        all_cited_valid = processed_target_ids
                                        if all_cited_valid:
                                            any_target_id = all_cited_valid[0]
                                            data.append(target_tab_data) # Otherwise the row should not be included in meta tables

                                # skip creation of a new row in meta table because there is no new id to be validated
                                # "any_target_id" will be chosen among the valid source entity ids, if any
                                elif target_identifier["valid"]:
                                    all_cited_valid = target_identifier["valid"]
                                    any_target_id = all_cited_valid[0]["identifier"]


                            if any_source_id and any_target_id:
                                citation = dict()
                                citation["citing"] = any_source_id
                                citation["referenced"] = any_target_id
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