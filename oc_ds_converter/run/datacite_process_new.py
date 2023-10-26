from pathlib import Path
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
from filelock import Timeout, FileLock


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
        for ndjson_file in all_input_ndjson:#it should be one
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


def get_citations_and_metadata(ndjson_file: str, chunk: list, preprocessed_citations_dir: str, csv_dir: str, chunk_to_save:str,
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

    last_part_processed = 0
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


    ndjson_filename = Path(ndjson_file).name
    if cache_dict.get("first_iteration"):
        if is_first_iteration and chunk_to_save in cache_dict["first_iteration"][ndjson_filename]:
            return

    if cache_dict.get("second_iteration"):
        if not is_first_iteration and chunk_to_save in cache_dict["second_iteration"][ndjson_filename]:
            return

    if is_first_iteration:
        dc_csv = DataciteProcessing(orcid_index=orcid_index, doi_csv=doi_csv,
                                      publishers_filepath_jalc=publishers_filepath_jalc,
                                      storage_manager=storage_manager, testing=testing, citing=True)
    elif not is_first_iteration:
        dc_csv = DataciteProcessing(orcid_index=orcid_index, doi_csv=doi_csv,
                                  publishers_filepath_jalc=publishers_filepath_jalc,
                                  storage_manager=storage_manager, testing=testing, citing=False)

    filename_without_ext = ndjson_filename.replace('.ndjson', '')+'_'+chunk_to_save
    filepath_ne = os.path.join(csv_dir, f'{os.path.basename(filename_without_ext)}')
    filepath_citations_ne = os.path.join(preprocessed_citations_dir, f'{os.path.basename(filename_without_ext)}')

    filepath = os.path.join(csv_dir, f'{os.path.basename(filename_without_ext)}.csv')
    filepath_citations = os.path.join(preprocessed_citations_dir, f'{os.path.basename(filename_without_ext)}.csv')
    pathoo(filepath)
    pathoo(filepath_citations)








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