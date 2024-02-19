from pathlib import Path
import yaml
from oc_ds_converter.lib.file_manager import normalize_path
from oc_ds_converter.lib.jsonmanager import *
from pebble import ProcessFuture, ProcessPool
from oc_ds_converter.oc_idmanager.oc_data_storage.redis_manager import \
    RedisStorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.sqlite_manager import \
    SqliteStorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.in_memory_manager import \
    InMemoryStorageManager
from oc_ds_converter.datacite.datacite_processing import DataciteProcessing
import json
import csv
from filelock import Timeout, FileLock
from tqdm import tqdm
from argparse import ArgumentParser
import sys

def preprocess(datacite_ndjson_dir:str, publishers_filepath:str, orcid_doi_filepath:str,
        csv_dir:str, wanted_doi_filepath:str=None, cache:str=None, verbose:bool=False, storage_path:str = None,
        testing: bool = True, redis_storage_manager: bool = False, max_workers: int = 1, target=50000) -> None:


    els_to_be_skipped = []
    if not testing:
        input_dir_cont = os.listdir(datacite_ndjson_dir)
        for el in input_dir_cont:
            if el.startswith("._"):
                els_to_be_skipped.append(os.path.join(datacite_ndjson_dir, el))
            else:
                if el.endswith(".zst"):
                    base_name = el.replace('.zst', '')
                    if [x for x in os.listdir(datacite_ndjson_dir) if x.startswith(base_name) and x.endswith("decompr_zst_dir")]:
                        els_to_be_skipped.append(os.path.join(datacite_ndjson_dir, el))

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
        print(f'[INFO: datacite_process] Getting all files from {datacite_ndjson_dir}')

    req_type = ".ndjson"
    all_input_ndjson = []
    if not testing:
        els_to_be_skipped_cont = [x for x in els_to_be_skipped if x.endswith(".zst")]

        if els_to_be_skipped_cont:
            for el_to_skip in els_to_be_skipped_cont:
                if el_to_skip.startswith("._"):
                    continue
                base_name_el_to_skip = el_to_skip.replace('.zst', '')
                for el in os.listdir(datacite_ndjson_dir):
                    if el == base_name_el_to_skip + "decompr_zst_dir":
                    # if el.startswith(base_name_el_to_skip) and el.endswith("decompr_zst_dir"):
                    #CHECK
                        all_input_ndjson = [os.path.join(datacite_ndjson_dir, el, file) for file in os.listdir(os.path.join(datacite_ndjson_dir, el)) if not file.endswith(".json") and not file.startswith("._")]

        if len(all_input_ndjson) == 0:

            for lev_zst in os.listdir(datacite_ndjson_dir):
                all_input_ndjson, targz_fd = get_all_files_by_type(os.path.join(datacite_ndjson_dir, lev_zst), req_type, cache)

    # in test files the decompressed directory, at the end of each execution of the process, is always deleted
    else:
        for lev_zst in os.listdir(datacite_ndjson_dir):
            all_input_ndjson, targz_fd = get_all_files_by_type(os.path.join(datacite_ndjson_dir, lev_zst), req_type, cache)


    # We need to understand how often (how many processed files) we should send the call to Redis
    if not redis_storage_manager or max_workers == 1:
        for ndjson_file in all_input_ndjson:# it should be one
            for idx, chunk in enumerate(read_ndjson_chunk(ndjson_file, target), start=1):
                chunk_to_save = f'chunk_{idx}'
                get_citations_and_metadata(ndjson_file, chunk, preprocessed_citations_dir, csv_dir, chunk_to_save, orcid_doi_filepath,
                                           wanted_doi_filepath, publishers_filepath, storage_path,
                                           redis_storage_manager,
                                           testing, cache, is_first_iteration=True)
        for ndjson_file in all_input_ndjson:
            for idx, chunk in enumerate(read_ndjson_chunk(ndjson_file, target), start=1):
                chunk_to_save = f'chunk_{idx}'
                get_citations_and_metadata(ndjson_file, chunk, preprocessed_citations_dir, csv_dir, chunk_to_save, orcid_doi_filepath,
                                           wanted_doi_filepath, publishers_filepath, storage_path,
                                           redis_storage_manager,
                                           testing, cache, is_first_iteration=False)

    elif redis_storage_manager or max_workers > 1:

        with ProcessPool(max_workers=max_workers, max_tasks=1) as executor:
            for ndjson_file in all_input_ndjson:
                for idx, chunk in enumerate(read_ndjson_chunk(ndjson_file, target), start=1):
                    chunk_to_save = f'chunk_{idx}'
                    future: ProcessFuture = executor.schedule(
                        function=get_citations_and_metadata,
                        args=(
                        ndjson_file, chunk, preprocessed_citations_dir, csv_dir, chunk_to_save, orcid_doi_filepath, wanted_doi_filepath,
                        publishers_filepath, storage_path, redis_storage_manager, testing, cache, True))

        with ProcessPool(max_workers=max_workers, max_tasks=1) as executor:
            for ndjson_file in all_input_ndjson:
                for idx, chunk in enumerate(read_ndjson_chunk(ndjson_file, target), start=1):
                    chunk_to_save = f'chunk_{idx}'
                    future: ProcessFuture = executor.schedule(
                        function=get_citations_and_metadata,
                        args=(
                        ndjson_file, chunk, preprocessed_citations_dir, csv_dir, chunk_to_save, orcid_doi_filepath, wanted_doi_filepath,
                        publishers_filepath, storage_path, redis_storage_manager, testing, cache, False))

    if cache:
        if os.path.exists(cache):
            os.remove(cache)
    lock_file = cache + ".lock"
    if os.path.exists(lock_file):
        os.remove(lock_file)


def get_citations_and_metadata(ndjson_file:str, chunk: list, preprocessed_citations_dir: str, csv_dir: str, chunk_to_save:str,
                               orcid_index: str,
                               doi_csv: str, publishers_filepath: str, storage_path: str,
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

    if cache_dict.get("first_iteration"):
        if is_first_iteration and chunk_to_save in cache_dict["first_iteration"]:
            return

    if cache_dict.get("second_iteration"):
        if not is_first_iteration and chunk_to_save in cache_dict["second_iteration"]:
            return

    if is_first_iteration:
        dc_csv = DataciteProcessing(orcid_index=orcid_index, doi_csv=doi_csv,
                                      publishers_filepath_dc=publishers_filepath,
                                      storage_manager=storage_manager, testing=testing, citing=True)
    elif not is_first_iteration:
        dc_csv = DataciteProcessing(orcid_index=orcid_index, doi_csv=doi_csv,
                                  publishers_filepath_dc=publishers_filepath,
                                  storage_manager=storage_manager, testing=testing, citing=False)

    index_citations_to_csv = []
    data_subject = []
    data_object = []

    filename_without_ext = ndjson_file.replace('.ndjson', '')+'_'+chunk_to_save
    filepath_ne = os.path.join(csv_dir, f'{os.path.basename(filename_without_ext)}')
    filepath_citations_ne = os.path.join(preprocessed_citations_dir, f'{os.path.basename(filename_without_ext)}')

    filepath = os.path.join(csv_dir, f'{os.path.basename(filename_without_ext)}.csv')
    filepath_citations = os.path.join(preprocessed_citations_dir, f'{os.path.basename(filename_without_ext)}.csv')
    pathoo(filepath)
    pathoo(filepath_citations)

    def get_all_redis_ids_and_save_updates(sli_da, is_first_iteration_par: bool):
        all_br = []
        all_ra = []
        # RETRIEVE ALL THE IDENTIFIERS TO BE VALIDATED THAT MAY BE IN REDIS
        # DOI, ORCID
        for entity in sli_da:  # for each bibliographical entity in the list
            if entity and "attributes" in entity:
                attributes = entity["attributes"]
                rel_ids = attributes.get("relatedIdentifiers")
                if rel_ids:
                    at_least_one_valid_object_id = False
                    for ref in rel_ids:
                        if all(elem in ref for elem in dc_csv.needed_info):
                            relatedIdentifierType = (str(ref["relatedIdentifierType"])).lower()
                            relationType = str(ref["relationType"]).lower()
                            if relatedIdentifierType == "doi":
                                if relationType in dc_csv.filter:
                                    at_least_one_valid_object_id = True
                    if at_least_one_valid_object_id:
                        if is_first_iteration_par:
                            ent_all_br, ent_all_ra = dc_csv.extract_all_ids(entity, True)
                        else:
                            ent_all_br, ent_all_ra = dc_csv.extract_all_ids(entity, False)
                        all_br.extend(ent_all_br)
                        all_ra.extend(ent_all_ra)
        redis_validity_values_br = dc_csv.get_reids_validity_list(all_br, "br")
        redis_validity_values_ra = dc_csv.get_reids_validity_list(all_br, "ra")
        dc_csv.update_redis_values(redis_validity_values_br, redis_validity_values_ra)

    def save_files(ent_list, citation_list, is_first_iteration_par:bool):
        if ent_list:
            # Filename of the source json, At first iteration, we will generate a CSV file containing all the
            # citing entities metadata, at the second iteration we will generate a cited entities metadata file
            # and the citations csv file
            if is_first_iteration_par:
                filename_str = filepath_ne+"_subject.csv"
            else:
                filename_str = filepath_ne+"_object.csv"
            with open(filename_str, 'w', newline='', encoding='utf-8') as output_file:
                dict_writer = csv.DictWriter(output_file, ent_list[0].keys(), delimiter=',', quotechar='"',
                                             quoting=csv.QUOTE_NONNUMERIC, escapechar='\\')
                dict_writer.writeheader()
                dict_writer.writerows(ent_list)
            ent_list = []
        dc_csv.memory_to_storage()

        if not is_first_iteration_par:
            if citation_list:
                filename_cit_str = filepath_citations_ne + ".csv"
                with open(filename_cit_str, 'w', newline='', encoding='utf-8') as output_file_citations:
                    dict_writer = csv.DictWriter(output_file_citations, citation_list[0].keys(), delimiter=',',
                                                 quotechar='"', quoting=csv.QUOTE_NONNUMERIC, escapechar='\\')
                    dict_writer.writeheader()
                    dict_writer.writerows(citation_list)
                citation_list = []

        dc_csv.memory_to_storage()
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
                cache_dict["first_iteration"].add(chunk_to_save)

            if not is_first_iteration_par:
                cache_dict["second_iteration"].add(chunk_to_save)


            with lock:
                with open(cache, 'r', encoding='utf-8') as aux_file:
                    cur_cache_dict = json.load(aux_file)

                    for k,v in cur_cache_dict.items():
                        cur_cache_dict[k] = set(v)
                        if not cache_dict.get(k) and cur_cache_dict.get(k):
                            cache_dict[k] = v
                        elif cache_dict[k] != v:
                            chunk_processed_values_list = cache_dict[k]
                            cur_chunk_processed_values_list = cur_cache_dict[k]

                            list_updated = list(cur_chunk_processed_values_list.union(chunk_processed_values_list))
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
        get_all_redis_ids_and_save_updates(chunk, is_first_iteration_par=True)
        for entity in tqdm(chunk):
            if entity:
                # for the subject the validation of the DOI is not necessary, if the id is normalizable go to the creation of the tables for Meta
                attributes = entity.get("attributes")
                subject_id = attributes.get("doi")
                rel_ids = attributes.get("relatedIdentifiers")
                #filter entities just with relatedIdentifiers, without validating them
                if subject_id and rel_ids:
                    at_least_one_valid_object_id = False
                    for ref in rel_ids:
                        if all(elem in ref for elem in dc_csv.needed_info):
                            relatedIdentifierType = (str(ref["relatedIdentifierType"])).lower()
                            relationType = str(ref["relationType"]).lower()
                            if relatedIdentifierType == "doi":
                                if relationType in dc_csv.filter:
                                    at_least_one_valid_object_id = True
                    if at_least_one_valid_object_id:
                        norm_subject_id = dc_csv.doi_m.normalise(subject_id, include_prefix=True)

                        if not dc_csv.doi_m.storage_manager.get_value(norm_subject_id):
                            dc_csv.tmp_doi_m.storage_manager.set_value(norm_subject_id, True)

                            if norm_subject_id:
                                source_tab_data = dc_csv.csv_creator(entity)
                                if source_tab_data:
                                    processed_source_id = source_tab_data["id"]
                                    if processed_source_id:
                                        data_subject.append(source_tab_data)
        save_files(data_subject, index_citations_to_csv, True)

    '''object entities:
       - look for the DOI in the temporary manager and in the storage manager:
           - if found as valid -> do not create the Meta table, but include the cited entity in the citations' tables;
           - if not found -> look for the doi in Redis server and later call the API if needed -> if the DOI is valid create the
           table for Meta and include the cited entity in the citations' tables
           - if found as not valid -> next entity'''
    if not is_first_iteration:
        get_all_redis_ids_and_save_updates(chunk, is_first_iteration_par=False)
        for entity in tqdm(chunk):
            if entity:
                attributes = entity.get("attributes")
                rel_ids = attributes.get("relatedIdentifiers")
                if attributes.get("doi"):
                    norm_subject_id = dc_csv.doi_m.normalise(attributes["doi"], include_prefix=True)
                    if norm_subject_id and rel_ids:
                        valid_target_ids = []
                        for ref in rel_ids:
                            if all(elem in ref for elem in dc_csv.needed_info):
                                relatedIdentifierType = (str(ref["relatedIdentifierType"])).lower()
                                relationType = str(ref["relationType"]).lower()
                                if relatedIdentifierType == "doi":
                                    if relationType in dc_csv.filter:
                                        norm_object_id = dc_csv.doi_m.normalise(ref["relatedIdentifier"], include_prefix=True)
                                        if norm_object_id:
                                            norm_id_dict_to_val = {"schema": "doi"}
                                            norm_id_dict_to_val["identifier"] = norm_object_id
                                            stored_validity = dc_csv.validated_as(norm_id_dict_to_val)
                                            if stored_validity is None:
                                                norm_id_dict = {"id": norm_object_id, "schema": "doi"}
                                                if norm_object_id in dc_csv.to_validated_id_list(norm_id_dict):
                                                    target_tab_data = dc_csv.csv_creator({"id": norm_object_id, "type": "dois", "attributes": {"doi": norm_object_id}})
                                                    if target_tab_data:
                                                        processed_target_id = target_tab_data.get("id")
                                                        if processed_target_id:
                                                            data_object.append(target_tab_data)
                                                            if relationType in ["cites", "references"]:
                                                                rel_dict = {"rel_type": "cites", "object_id": norm_object_id}
                                                            elif relationType in ["iscitedby", "isreferencedby"]:
                                                                rel_dict = {"rel_type": "iscitedby", "object_id": norm_object_id}
                                                            valid_target_ids.append(rel_dict)
                                            elif stored_validity is True:
                                                if relationType in ["cites", "references"]:
                                                    rel_dict = {"rel_type": "cites", "object_id": norm_object_id}
                                                elif relationType in ["iscitedby", "isreferencedby"]:
                                                    rel_dict = {"rel_type": "iscitedby", "object_id": norm_object_id}
                                                valid_target_ids.append(rel_dict)

                        unique_dicts = [dict(t) for t in {tuple(sorted(d.items())) for d in valid_target_ids}]
                        for rel_type_dict in unique_dicts:
                            citation = dict()
                            if rel_type_dict["rel_type"] == "cites":
                                citation["citing"] = norm_subject_id
                                citation["cited"] = rel_type_dict["object_id"]
                            elif rel_type_dict["rel_type"] == "iscitedby":
                                citation["citing"] = rel_type_dict["object_id"]
                                citation["cited"] = norm_subject_id
                            index_citations_to_csv.append(citation)
        save_files(data_object, index_citations_to_csv, False)

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

def read_ndjson_chunk(file_path, chunk_size):
    with open(file_path, 'r', encoding='utf-8') as file:
        while True:
            chunk = []
            for _ in range(chunk_size):
                line = file.readline()
                if not line:
                    break
                try:
                    data = json.loads(line)
                    chunk.append(data)
                except json.JSONDecodeError as e:
                    # Handle JSON decoding errors if necessary
                    print(f"Error decoding JSON: {e}")
            if not chunk:
                break
            yield chunk

if __name__ == '__main__':
    arg_parser = ArgumentParser('datacite_process.py', description='This script creates CSV files from Datacite original dump, enriching data through of a DOI-ORCID index')
    arg_parser.add_argument('-c', '--config', dest='config', required=False,
                            help='Configuration file path')
    required = not any(arg in sys.argv for arg in {'--config', '-c'})
    arg_parser.add_argument('-dc', '--datacite', dest='datacite_ndjson_dir', required=required,
                            help='Datacite ndjson files directory')
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
    datacite_ndjson_dir = settings['datacite_ndjson_dir'] if settings else args.datacite_ndjson_dir
    datacite_ndjson_dir = normalize_path(datacite_ndjson_dir)
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

    preprocess(datacite_ndjson_dir=datacite_ndjson_dir, publishers_filepath=publishers_filepath, orcid_doi_filepath=orcid_doi_filepath, csv_dir=csv_dir, wanted_doi_filepath=wanted_doi_filepath, cache=cache, verbose=verbose, storage_path=storage_path, testing=testing,
               redis_storage_manager=redis_storage_manager, max_workers=max_workers)
#preprocess(datacite_ndjson_dir="D:\DATACITE\sample_dc",publishers_filepath=r"C:\Users\marta\Desktop\oc_ds_converter\test\datacite_processing\publishers.csv", orcid_doi_filepath=r"C:\Users\marta\Desktop\oc_ds_converter\test\datacite_processing\iod", csv_dir="D:\DATACITE\out_process_prova", cache="D:\DATACITE\cache.json", storage_path=r"D:\DATACITE\any_db.db")
