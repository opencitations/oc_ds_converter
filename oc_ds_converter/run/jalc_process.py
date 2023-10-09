import csv
import json
import os.path
from pathlib import Path
from zipfile import ZipInfo

from filelock import FileLock
import re
import sys
from argparse import ArgumentParser
from tarfile import TarInfo

import ndjson
import yaml
from pebble import ProcessFuture, ProcessPool
from tqdm import tqdm

from oc_ds_converter.oc_idmanager.oc_data_storage.redis_manager import \
    RedisStorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.sqlite_manager import \
    SqliteStorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.in_memory_manager import \
    InMemoryStorageManager

from oc_ds_converter.jalc.jalc_processing import JalcProcessing
from oc_ds_converter.lib.file_manager import normalize_path
from oc_ds_converter.lib.jsonmanager import *


def preprocess(jalc_json_dir:str, publishers_filepath:str, orcid_doi_filepath:str,
               csv_dir:str, wanted_doi_filepath:str=None, cache:str=None, verbose:bool=False, storage_path:str = None,
               testing: bool = True, redis_storage_manager: bool = False, max_workers: int = 1) -> None:

    els_to_be_skipped=[]
    #check if in the input folder the zipped folder has already been decompressed
    if not testing: # NON CANCELLARE FILES MA PRENDI SOLO IN CONSIDERAZIONE
        input_dir_cont = os.listdir(jalc_json_dir)
        # for element in the list of elements in jalc_json_dir (input)
        for el in input_dir_cont: #should be one (the input dir contains 1 zip)
            if el.startswith("._"):
                # skip elements starting with ._
                els_to_be_skipped.append(os.path.join(jalc_json_dir, el))
            else:
                if el.endswith(".zip"):
                    base_name = el.replace('.zip', '')
                    if [x for x in os.listdir(jalc_json_dir) if x.startswith(base_name) and x.endswith("decompr_zip_dir")]:
                        els_to_be_skipped.append(os.path.join(jalc_json_dir, el))
        # remember to skip files in els_to_be_skipped during the process

    if not os.path.exists(csv_dir):
        os.makedirs(csv_dir)

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
            log = '[INFO: jalc_process] Processing: ' + '; '.join(what)
            print(log)

    if verbose:
        print(f'[INFO: jalc_process] Getting all files from {jalc_json_dir}')

    req_type = ".zip"
    all_input_zip = []
    if not testing:
        els_to_be_skipped_cont = [x for x in els_to_be_skipped if x.endswith(".zip")]

        if els_to_be_skipped_cont:
            for el_to_skip in els_to_be_skipped_cont:
                if el_to_skip.startswith("._"):
                    continue
                base_name_el_to_skip = el_to_skip.replace('.zip', '')
                for el in os.listdir(jalc_json_dir):
                    if el == base_name_el_to_skip + "decompr_zip_dir":
                    #if el.startswith(base_name_el_to_skip) and el.endswith("decompr_zip_dir"):
                        all_input_zip = [os.path.join(jalc_json_dir, el, file) for file in os.listdir(os.path.join(jalc_json_dir, el)) if not file.endswith(".json") and not file.startswith("._")]


        if len(all_input_zip) == 0:

            for zip_lev0 in os.listdir(jalc_json_dir):
                all_input_zip, targz_fd = get_all_files_by_type(os.path.join(jalc_json_dir, zip_lev0), req_type, cache)

    else:
        all_input_zip = os.listdir(jalc_json_dir)
        for zip in all_input_zip:
            all_input_zip, targz_fd = get_all_files_by_type(os.path.join(jalc_json_dir, zip), req_type, cache)

    if not redis_storage_manager or max_workers == 1:
        for zip_file in all_input_zip:
            get_citations_and_metadata(zip_file, preprocessed_citations_dir, csv_dir, orcid_doi_filepath,
                                       wanted_doi_filepath, publishers_filepath, storage_path,
                                       redis_storage_manager,
                                       testing, cache, is_first_iteration=True)
        for zip_file in all_input_zip:
            get_citations_and_metadata(zip_file, preprocessed_citations_dir, csv_dir, orcid_doi_filepath,
                                       wanted_doi_filepath, publishers_filepath, storage_path,
                                       redis_storage_manager,
                                       testing, cache, is_first_iteration=False)


    elif redis_storage_manager or max_workers > 1:

        with ProcessPool(max_workers=max_workers, max_tasks=1) as executor:
            for zip_file in all_input_zip:
                print(zip_file)

                future: ProcessFuture = executor.schedule(
                    function=get_citations_and_metadata,
                    args=(
                    zip_file, preprocessed_citations_dir, csv_dir, orcid_doi_filepath, wanted_doi_filepath,
                    publishers_filepath, storage_path, redis_storage_manager, testing, cache, True))

        with ProcessPool(max_workers=max_workers, max_tasks=1) as executor:
            for zip_file in all_input_zip:
                print(zip_file)
                future: ProcessFuture = executor.schedule(
                    function=get_citations_and_metadata,
                    args=(
                    zip_file, preprocessed_citations_dir, csv_dir, orcid_doi_filepath, wanted_doi_filepath,
                    publishers_filepath, storage_path, redis_storage_manager, testing, cache, False))

    if cache:
        if os.path.exists(cache):
            os.remove(cache)
    lock_file = cache + ".lock"
    if os.path.exists(lock_file):
        os.remove(lock_file)

def get_citations_and_metadata(zip_file: str, preprocessed_citations_dir: str, csv_dir: str,
                               orcid_index: str,
                               doi_csv: str, publishers_filepath_jalc: str, storage_path: str,
                               redis_storage_manager: bool,
                               testing: bool, cache: str, is_first_iteration:bool):
    storage_manager = get_storage_manager(storage_path, redis_storage_manager, testing=testing)
    if cache:
        if not cache.endswith(".json"):
            cache = os.path.join(os.getcwd(), "cache.json")
        else:
            if not os.path.exists(os.path.abspath(os.path.join(cache, os.pardir))):
                Path(os.path.abspath(os.path.join(cache, os.pardir))).mkdir(parents=True, exist_ok=True)
    else:
        cache = os.path.join(os.getcwd(), "cache.json")

    lock = FileLock(cache + ".lock")
    cache_dict = dict()
    write_new = False
    if os.path.exists(cache):
        with lock:
            with open(cache, "r", encoding="utf-8") as c:
                try:
                    cache_dict = json.load(c)
                except:
                    write_new = True
    else:
        write_new = True
    if write_new:
        with lock:
            with open(cache, "w", encoding="utf-8") as c:
                json.dump(cache_dict, c)

    # skip if in cache
    filename = Path(zip_file).name
    if cache_dict.get("first_iteration"):
        if is_first_iteration and filename in cache_dict["first_iteration"]:
            return
    if cache_dict.get("second_iteration"):
        if not is_first_iteration and filename in cache_dict["second_iteration"]:
            return

    if is_first_iteration:
        jalc_csv = JalcProcessing(orcid_index=orcid_index, doi_csv=doi_csv,
                                      publishers_filepath_jalc=publishers_filepath_jalc,
                                      storage_manager=storage_manager, testing=testing, citing=True)
    elif not is_first_iteration:
        jalc_csv = JalcProcessing(orcid_index=orcid_index, doi_csv=doi_csv,
                                  publishers_filepath_jalc=publishers_filepath_jalc,
                                  storage_manager=storage_manager, testing=testing, citing=False)
    index_citations_to_csv = []
    data_citing = []
    data_cited = []
    zip_f = zipfile.ZipFile(zip_file)
    source_data = [x for x in zip_f.namelist() if not x.startswith("doiList")]
    source_dict = []
    #here I create a list containing all the json in the zip folder as dictionaries
    for json_file in tqdm(source_data):
        f = zip_f.open(json_file, 'r')
        my_dict = json.load(f)
        source_dict.append(my_dict)

    #pbar = tqdm(total=len(source_dict))

    filename_without_ext = filename.replace('.zip', '')
    filepath_ne = os.path.join(csv_dir, f'{os.path.basename(filename_without_ext)}')
    filepath_citations_ne = os.path.join(preprocessed_citations_dir, f'{os.path.basename(filename_without_ext)}')

    filepath = os.path.join(csv_dir, f'{os.path.basename(filename_without_ext)}.csv')
    filepath_citations = os.path.join(preprocessed_citations_dir, f'{os.path.basename(filename_without_ext)}.csv')
    pathoo(filepath)
    pathoo(filepath_citations)
    def get_all_redis_ids_and_save_updates(sli_da, is_first_iteration_par:bool):
        all_br = []
        for entity in sli_da:
            if entity:
                d = entity.get("data")
                # filtering out entities without citations
                if "citation_list" in d:
                    cit_list = d["citation_list"]
                    cit_list_doi = [x for x in cit_list if x.get("doi")]
                    # filtering out entities with citations without dois
                    if cit_list_doi:
                        if is_first_iteration_par:
                            ent_all_br = jalc_csv.extract_all_ids(entity, True)
                        else:
                            ent_all_br = jalc_csv.extract_all_ids(entity, False)
                        all_br.extend(ent_all_br)

        redis_validity_values_br = jalc_csv.get_reids_validity_list(all_br)
        jalc_csv.update_redis_values(redis_validity_values_br)

    def save_files(ent_list, citation_list, is_first_iteration_par: bool):
        if ent_list:
            # qua il filename sarà quello della cartella zippata, tipo “105834_citing” o "105834_cited"
            if is_first_iteration_par:
                filename_str = filepath_ne+"_citing.csv"
            else:
                filename_str = filepath_ne+"_cited.csv"
            with open(filename_str, 'w', newline='', encoding='utf-8') as output_file:
                dict_writer = csv.DictWriter(output_file, ent_list[0].keys(), delimiter=',', quotechar='"',
                                             quoting=csv.QUOTE_NONNUMERIC, escapechar='\\')
                dict_writer.writeheader()
                dict_writer.writerows(ent_list)
            ent_list = []
        if not is_first_iteration_par:
            if citation_list:
                filename_cit_str = filepath_citations_ne + ".csv"
                with open(filename_cit_str, 'w', newline='', encoding='utf-8') as output_file_citations:
                    dict_writer = csv.DictWriter(output_file_citations, citation_list[0].keys(), delimiter=',',
                                                 quotechar='"', quoting=csv.QUOTE_NONNUMERIC, escapechar='\\')
                    dict_writer.writeheader()
                    dict_writer.writerows(citation_list)
                citation_list = []

        jalc_csv.memory_to_storage()
        if is_first_iteration_par:
            task_done(is_first_iteration_par=True)
        else:
            task_done(is_first_iteration_par=False)
        return ent_list, citation_list

    def task_done(is_first_iteration_par: bool) -> None:
        try:

            if is_first_iteration_par and "first_iteration" not in cache_dict.keys():
                cache_dict["first_iteration"] = set()

            if not is_first_iteration_par and "second_iteration" not in cache_dict.keys():
                cache_dict["second_iteration"] = set()

            for k,v in cache_dict.items():
                cache_dict[k] = set(v)

            if is_first_iteration_par:
                cache_dict["first_iteration"].add(Path(zip_file).name)

            if not is_first_iteration_par:
                cache_dict["second_iteration"].add(Path(zip_file).name)


            with lock:
                with open(cache, 'r', encoding='utf-8') as aux_file:
                    cur_cache_dict = json.load(aux_file)

                    for k,v in cur_cache_dict.items():
                        cur_cache_dict[k] = set(v)
                        if not cache_dict.get(k) and cur_cache_dict.get(k):
                            cache_dict[k] = v
                        elif cache_dict[k] != v:
                            zip_files_processed_values_list = cache_dict[k]
                            cur_zip_files_processed_values_list = cur_cache_dict[k]

                            #unione set e poi lista
                            list_updated = list(cur_zip_files_processed_values_list.union(zip_files_processed_values_list))
                            cache_dict[k] = list_updated

                    for k,v in cache_dict.items():
                        if k not in cur_cache_dict:
                            cur_cache_dict[k] = v

                for k,v in cache_dict.items():
                    if isinstance(v, set):
                        cache_dict[k] = list(v)

                with open(cache, 'w', encoding='utf-8') as aux_file:
                    json.dump(cache_dict, aux_file)

        except Exception as e:
            print(e)

    if is_first_iteration:
        get_all_redis_ids_and_save_updates(source_dict, is_first_iteration_par=True)
        # prima l'ultimo file va processato
        for entity in tqdm(source_dict):
            #pbar.update()
            if entity:
                d = entity.get("data")
                #per i citanti la validazione non serve, se è normalizzabile va direttamente alla crezione tabelle Meta
                norm_source_id = jalc_csv.doi_m.normalise(d['doi'], include_prefix=True)
                if norm_source_id:
                    source_tab_data = jalc_csv.csv_creator(d)
                    if source_tab_data:
                        processed_source_id = source_tab_data["id"]
                        if processed_source_id:
                            data_citing.append(source_tab_data)

        save_files(data_citing, index_citations_to_csv, True)
        #pbar.close()

    '''cited entities:
    - look for the DOI in the temporary manager and in the storage manager:
        - if found as valid -> do not create the Meta table, but include the cited entity in the citations' tables;
        - if not found -> look for the doi in Redis server and later call the API if needed -> if the DOI is valid create the
        table for Meta and include the cited entity in the citations' tables
        - if found as not valid -> next entity'''

    if not is_first_iteration:
        get_all_redis_ids_and_save_updates(source_dict, is_first_iteration_par=False)
        for entity in tqdm(source_dict):
            if entity:
                d = entity.get("data")
                norm_source_id = jalc_csv.doi_m.normalise(d['doi'], include_prefix=True)
                if norm_source_id:
                    cit_list_entities = [x for x in d["citation_list"] if x.get("doi")]
                    # filtering out entities with citations without dois
                    if cit_list_entities:
                        valid_target_ids = []
                        for cited_entity in cit_list_entities:
                            norm_id = jalc_csv.doi_m.normalise(cited_entity["doi"], include_prefix=True)
                            if norm_id:
                                stored_validity = jalc_csv.validated_as(norm_id)
                                if stored_validity is None:
                                    if norm_id in jalc_csv.to_validated_id_list(norm_id):
                                        target_tab_data = jalc_csv.csv_creator(cited_entity)
                                        if target_tab_data:
                                            processed_target_id = target_tab_data.get("id")
                                            if processed_target_id:
                                                data_cited.append(target_tab_data)
                                                valid_target_ids.append(norm_id)
                                elif stored_validity is True:
                                    valid_target_ids.append(norm_id)

                        for target_id in valid_target_ids:
                            citation = dict()
                            citation["citing"] = norm_source_id
                            citation["cited"] = target_id
                            index_citations_to_csv.append(citation)
        save_files(data_cited, index_citations_to_csv, False)
def get_storage_manager(storage_path: str, redis_storage_manager: bool, testing: bool):
    if not redis_storage_manager:
        if storage_path:
            if not os.path.exists(storage_path):
            # if parent dir does not exist, it is created
                if not os.path.exists(os.path.abspath(os.path.join(storage_path, os.pardir))):
                    Path(os.path.abspath(os.path.join(storage_path, os.pardir))).mkdir(parents=True, exist_ok=True)
            if storage_path.endswith(".db"):
                storage_manager = SqliteStorageManager(storage_path)
            elif storage_path.endswith(".json"):
                storage_manager = InMemoryStorageManager(storage_path)

        if not storage_path and not redis_storage_manager:
            new_path_dir = os.path.join(os.getcwd(), "storage")
            if not os.path.exists(new_path_dir):
                os.makedirs(new_path_dir)
            storage_manager = SqliteStorageManager(os.path.join(new_path_dir, "id_valid_dict.db"))
    elif redis_storage_manager:
        if testing:
            storage_manager = RedisStorageManager(testing=True)
        else:
            storage_manager = RedisStorageManager(testing=False)
    return storage_manager

def pathoo(path:str) -> None:
    if not os.path.exists(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path))


if __name__ == '__main__':
    arg_parser = ArgumentParser('jalc_process.py', description='This script creates CSV files from JALC original dump, enriching data through of a DOI-ORCID index')
    arg_parser.add_argument('-c', '--config', dest='config', required=False,
                            help='Configuration file path')
    required = not any(arg in sys.argv for arg in {'--config', '-c'})
    arg_parser.add_argument('-ja', '--jalc', dest='jalc_json_dir', required=required,
                            help='Jalc json files directory')
    arg_parser.add_argument('-out', '--output', dest='csv_dir', required=required,
                            help='Directory where CSV will be stored')
    arg_parser.add_argument('-p', '--publishers', dest='publishers_filepath', required=False,
                            help='CSV file path containing information about publishers (id, name, prefix)')
    arg_parser.add_argument('-o', '--orcid', dest='orcid_doi_filepath', required=False,
                            help='DOI-ORCID index filepath, to enrich data')
    arg_parser.add_argument('-w', '--wanted', dest='wanted_doi_filepath', required=False,
                            help='A CSV filepath containing what DOI to process, not mandatory')
    arg_parser.add_argument('-ca', '--cache', dest='cache', required=False,
                            help='The cache file path. This file will be deleted at the end of the process')
    arg_parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', required=False,
                            help='Show a loading bar, elapsed time and estimated time')
    arg_parser.add_argument('-sp', '--storage_path', dest='storage_path', required=False,
                            help='path of the file where to store data concerning validated pids information.'
                                 'Pay attention to specify a ".db" file in case you chose the SqliteStorageManager'
                                 'and a ".json" file if you chose InMemoryStorageManager')
    arg_parser.add_argument('-t', '--testing', dest='testing', action='store_true', required=False,
                            help='parameter to define if the script is to be run in testing mode. Pay attention:'
                                 'by default the script is run in test modality and thus the data managed by redis, '
                                 'stored in a specific redis db, are not retrieved nor permanently saved, since an '
                                 'instance of a FakeRedis class is created and deleted by the end of the process.')
    arg_parser.add_argument('-r', '--redis_storage_manager', dest='redis_storage_manager', action='store_true',
                            required=False,
                            help='parameter to define whether or not to use redis as storage manager. Note that by default the parameter '
                                 'value is set to false, which means that -unless it is differently stated- the storage manager used is'
                                 'the one chosen as value of the parameter --storage_manager. The redis db used by the storage manager is the n.2')
    arg_parser.add_argument('-m', '--max_workers', dest='max_workers', required=False, default=1, type=int,
                            help='Workers number')
    args = arg_parser.parse_args()
    config = args.config
    settings = None
    if config:
        with open(config, encoding='utf-8') as f:
            settings = yaml.full_load(f)
    jalc_json_dir = settings['jalc_json_dir'] if settings else args.jalc_json_dir
    jalc_json_dir = normalize_path(jalc_json_dir)
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
    storage_path = settings['storage_path'] if settings else args.storage_path
    storage_path = normalize_path(storage_path) if storage_path else None
    testing = settings['testing'] if settings else args.testing
    redis_storage_manager = settings['redis_storage_manager'] if settings else args.redis_storage_manager
    max_workers = settings['max_workers'] if settings else args.max_workers
    preprocess(jalc_json_dir=jalc_json_dir, publishers_filepath=publishers_filepath, orcid_doi_filepath=orcid_doi_filepath, csv_dir=csv_dir, wanted_doi_filepath=wanted_doi_filepath, cache=cache, verbose=verbose, storage_path=storage_path, testing=testing,
               redis_storage_manager=redis_storage_manager, max_workers=max_workers)

