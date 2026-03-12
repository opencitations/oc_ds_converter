import csv
import json
import os
import sys
from argparse import ArgumentParser
from pathlib import Path

import yaml
from concurrent.futures import ProcessPoolExecutor
from filelock import FileLock
from multiprocessing import get_context
from tqdm import tqdm

from oc_ds_converter.datacite.datacite_processing import DataciteProcessing
from oc_ds_converter.lib.file_manager import normalize_path
from oc_ds_converter.lib.jsonmanager import get_all_files_by_type
from oc_ds_converter.oc_idmanager.oc_data_storage.in_memory_manager import InMemoryStorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.redis_manager import RedisStorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.sqlite_manager import SqliteStorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.storage_manager import StorageManager


def get_storage_manager(storage_path: str | None, testing: bool) -> StorageManager:
    if storage_path:
        if not os.path.exists(storage_path):
            if not os.path.exists(os.path.abspath(os.path.join(storage_path, os.pardir))):
                Path(os.path.abspath(os.path.join(storage_path, os.pardir))).mkdir(parents=True, exist_ok=True)
        if storage_path.endswith(".db"):
            return SqliteStorageManager(storage_path)
        if storage_path.endswith(".json"):
            return InMemoryStorageManager(storage_path)
        raise ValueError(f"Storage path must end with .db or .json, got: {storage_path}")
    return RedisStorageManager(testing=testing)


def preprocess(datacite_ndjson_dir: str, publishers_filepath: str | None, orcid_doi_filepath: str | None,
        csv_dir: str, cache: str | None = None, verbose: bool = False,
        testing: bool = True, max_workers: int = 1, target: int = 50000, use_orcid_api: bool = True,
        exclude_existing: bool = False, storage_path: str | None = None) -> None:

    els_to_be_skipped = []
    if not testing and os.path.isdir(datacite_ndjson_dir):
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
        os.makedirs(preprocessed_citations_dir)

    bad_dir = os.path.join(csv_dir, "_bad")  # creato solo on-demand in read_ndjson_chunk

    if verbose:
        if publishers_filepath or orcid_doi_filepath:
            what = list()
            if publishers_filepath:
                what.append('publishers mapping')
            if orcid_doi_filepath:
                what.append('DOI-ORCID index')
            log = '[INFO: datacite_process] Processing: ' + '; '.join(what)
            print(log)

    if verbose:
        print(f'[INFO: datacite_process] Getting all files from {datacite_ndjson_dir}')

    req_type = ".ndjson"
    all_input_ndjson = []

    # Supporto a percorso file singolo .ndjson
    if os.path.isfile(datacite_ndjson_dir) and datacite_ndjson_dir.endswith(".ndjson"):
        all_input_ndjson = [datacite_ndjson_dir]
    else:
        # Directory: aggiungi eventuali .ndjson "piatti" nella cartella
        if os.path.isdir(datacite_ndjson_dir):
            for entry in os.listdir(datacite_ndjson_dir):
                fp = os.path.join(datacite_ndjson_dir, entry)
                if os.path.isfile(fp) and fp.endswith(".ndjson") and not entry.startswith("._"):
                    all_input_ndjson.append(fp)

        if not testing and os.path.isdir(datacite_ndjson_dir):
            els_to_be_skipped_cont = [x for x in els_to_be_skipped if x.endswith(".zst")]

            if els_to_be_skipped_cont:
                for el_to_skip in els_to_be_skipped_cont:
                    if el_to_skip.startswith("._"):
                        continue
                    base_name_el_to_skip = el_to_skip.replace('.zst', '')
                    for el in os.listdir(datacite_ndjson_dir):
                        if el == base_name_el_to_skip + "decompr_zst_dir":
                            all_input_ndjson.extend(
                                os.path.join(datacite_ndjson_dir, el, file)
                                for file in os.listdir(os.path.join(datacite_ndjson_dir, el))
                                if not file.endswith(".json") and not file.startswith("._")
                            )

            if len(all_input_ndjson) == 0:
                for lev_zst in os.listdir(datacite_ndjson_dir):
                    got, targz_fd = get_all_files_by_type(os.path.join(datacite_ndjson_dir, lev_zst), req_type, cache)
                    all_input_ndjson.extend(got)

        elif testing and os.path.isdir(datacite_ndjson_dir):
            for lev_zst in os.listdir(datacite_ndjson_dir):
                got, targz_fd = get_all_files_by_type(os.path.join(datacite_ndjson_dir, lev_zst), req_type, cache)
                all_input_ndjson.extend(got)

    # dedup e ordine stabile
    all_input_ndjson = sorted(list(dict.fromkeys(all_input_ndjson)))

    if max_workers == 1:
        for ndjson_file in all_input_ndjson:
            for idx, chunk in enumerate(read_ndjson_chunk(ndjson_file, target, bad_dir=bad_dir), start=1):
                chunk_to_save = f'chunk_{idx}'
                get_citations_and_metadata(ndjson_file, chunk, preprocessed_citations_dir, csv_dir, chunk_to_save, orcid_doi_filepath,
                                           publishers_filepath,
                                           testing, cache, is_citing=True, use_orcid_api=use_orcid_api, exclude_existing=exclude_existing,
                                           storage_path=storage_path)
        for ndjson_file in all_input_ndjson:
            for idx, chunk in enumerate(read_ndjson_chunk(ndjson_file, target, bad_dir=bad_dir), start=1):
                chunk_to_save = f'chunk_{idx}'
                get_citations_and_metadata(ndjson_file, chunk, preprocessed_citations_dir, csv_dir, chunk_to_save, orcid_doi_filepath,
                                           publishers_filepath,
                                           testing, cache, is_citing=False, use_orcid_api=use_orcid_api, exclude_existing=exclude_existing,
                                           storage_path=storage_path)

    elif max_workers > 1:
        with ProcessPoolExecutor(max_workers=max_workers, mp_context=get_context('spawn')) as executor:
            for ndjson_file in all_input_ndjson:
                for idx, chunk in enumerate(read_ndjson_chunk(ndjson_file, target, bad_dir=bad_dir), start=1):
                    chunk_to_save = f'chunk_{idx}'
                    executor.submit(
                        get_citations_and_metadata,
                        ndjson_file, chunk, preprocessed_citations_dir, csv_dir, chunk_to_save, orcid_doi_filepath,
                        publishers_filepath, testing, cache, True, use_orcid_api, exclude_existing, storage_path)

        with ProcessPoolExecutor(max_workers=max_workers, mp_context=get_context('spawn')) as executor:
            for ndjson_file in all_input_ndjson:
                for idx, chunk in enumerate(read_ndjson_chunk(ndjson_file, target, bad_dir=bad_dir), start=1):
                    chunk_to_save = f'chunk_{idx}'
                    executor.submit(
                        get_citations_and_metadata,
                        ndjson_file, chunk, preprocessed_citations_dir, csv_dir, chunk_to_save, orcid_doi_filepath,
                        publishers_filepath, testing, cache, False, use_orcid_api, exclude_existing, storage_path)

    if cache:
        if os.path.exists(cache):
            os.remove(cache)
        lock_file = cache + ".lock"
        if os.path.exists(lock_file):
            os.remove(lock_file)

    elif not cache:
        fallback = os.path.join(os.getcwd(), "cache.json")
        if os.path.exists(fallback):
            os.remove(fallback)
        lock_file = fallback + ".lock"
        if os.path.exists(lock_file):
            os.remove(lock_file)

    if testing:
        storage_manager = RedisStorageManager(testing=testing)
        storage_manager.delete_storage()


def get_citations_and_metadata(ndjson_file: str, chunk: list, preprocessed_citations_dir: str, csv_dir: str, chunk_to_save: str,
                               orcid_index: str | None,
                               publishers_filepath: str | None,
                               testing: bool, cache: str | None, is_citing: bool, use_orcid_api: bool,
                               exclude_existing: bool = False, storage_path: str | None = None):

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
                except json.JSONDecodeError:
                    write_new = True
    else:
        write_new = True

    if write_new:
        with lock:
            with open(cache, "w", encoding="utf-8") as c:
                json.dump(cache_dict, c)

    if cache_dict.get("citing"):
        if is_citing and chunk_to_save in cache_dict["citing"]:
            return

    if cache_dict.get("cited"):
        if not is_citing and chunk_to_save in cache_dict["cited"]:
            return

    storage_manager = get_storage_manager(storage_path, testing)
    dc_csv = DataciteProcessing(orcid_index=orcid_index,
                                publishers_filepath_dc=publishers_filepath,
                                storage_manager=storage_manager,
                                testing=testing, citing=is_citing, use_orcid_api=use_orcid_api, exclude_existing=exclude_existing)

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

    def get_all_redis_ids_and_save_updates(sli_da, is_citing_par: bool):
        all_br = []
        all_ra = []
        for entity in sli_da:
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
                        if is_citing_par:
                            ent_all_br, ent_all_ra = dc_csv.extract_all_ids(entity, True)
                        else:
                            ent_all_br, ent_all_ra = dc_csv.extract_all_ids(entity, False)
                        all_br.extend(ent_all_br)
                        all_ra.extend(ent_all_ra)
        redis_validity_values_br = dc_csv.get_redis_validity_list(all_br, "br")
        redis_validity_values_ra = dc_csv.get_redis_validity_list(all_ra, "ra")
        dc_csv.update_redis_values(redis_validity_values_br, redis_validity_values_ra)

    def save_files(ent_list, citation_list, is_citing_par:bool):
        if ent_list:
            if is_citing_par:
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

        if not is_citing_par:
            if citation_list:
                filename_cit_str = filepath_citations_ne + ".csv"
                with open(filename_cit_str, 'w', newline='', encoding='utf-8') as output_file_citations:
                    dict_writer = csv.DictWriter(output_file_citations, citation_list[0].keys(), delimiter=',',
                                                 quotechar='"', quoting=csv.QUOTE_NONNUMERIC, escapechar='\\')
                    dict_writer.writeheader()
                    dict_writer.writerows(citation_list)
                citation_list = []

        dc_csv.memory_to_storage()
        if is_citing_par:
            task_done(is_citing_par=True)
        else:
            task_done(is_citing_par=False)
        return ent_list, citation_list


    def task_done(is_citing_par: bool) -> None:

        try:
            if is_citing_par and "citing" not in cache_dict.keys():
                cache_dict["citing"] = set()

            if not is_citing_par and "cited" not in cache_dict.keys():
                cache_dict["cited"] = set()

            for k,v in cache_dict.items():
                cache_dict[k] = set(v)

            if is_citing_par:
                cache_dict["citing"].add(chunk_to_save)

            if not is_citing_par:
                cache_dict["cited"].add(chunk_to_save)

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

    if is_citing:
        get_all_redis_ids_and_save_updates(chunk, is_citing_par=True)
        for entity in tqdm(chunk):
            try:
                if entity:
                    attributes = entity.get("attributes")
                    subject_id = attributes.get("doi")
                    rel_ids = attributes.get("relatedIdentifiers")
                    if subject_id and rel_ids:
                        at_least_one_valid_object_id = False
                        for ref in rel_ids:
                            if all(elem in ref for elem in dc_csv.needed_info):
                                relatedIdentifierType = (str(ref["relatedIdentifierType"])).lower()
                                relationType = (str(ref["relationType"])).lower()
                                if relatedIdentifierType == "doi":
                                    if relationType in dc_csv.filter:
                                        at_least_one_valid_object_id = True
                        if at_least_one_valid_object_id:
                            norm_subject_id = dc_csv.doi_m.normalise(subject_id, include_prefix=True)

                            if norm_subject_id and not dc_csv.doi_m.storage_manager.get_value(norm_subject_id):
                                if dc_csv.exclude_existing and dc_csv.BR_redis.exists_as_set(norm_subject_id):
                                    dc_csv.tmp_doi_m.storage_manager.set_value(norm_subject_id, True)
                                    continue
                                dc_csv.tmp_doi_m.storage_manager.set_value(norm_subject_id, True)

                                source_tab_data = dc_csv.csv_creator(entity)
                                if source_tab_data:
                                    processed_source_id = source_tab_data["id"]
                                    if processed_source_id:
                                        data_subject.append(source_tab_data)
            except Exception as e:
                print("[PROCESS ERROR] during subject processing. Entity preview:")
                try:
                    print(json.dumps(entity, ensure_ascii=False)[:500] + "...")
                except Exception:
                    print(str(entity)[:500] + "...")
                print(f"Details: {e}")
                continue
        save_files(data_subject, index_citations_to_csv, True)

    if not is_citing:
        get_all_redis_ids_and_save_updates(chunk, is_citing_par=False)
        for entity in tqdm(chunk):
            try:
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
                                    relationType = (str(ref["relationType"])).lower()
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
                                                        if dc_csv.exclude_existing and dc_csv.BR_redis.exists_as_set(norm_object_id):
                                                            if relationType in ["cites", "references"]:
                                                                rel_dict = {"rel_type": "cites", "object_id": norm_object_id}
                                                                valid_target_ids.append(rel_dict)
                                                            elif relationType in ["iscitedby", "isreferencedby"]:
                                                                rel_dict = {"rel_type": "iscitedby", "object_id": norm_object_id}
                                                                valid_target_ids.append(rel_dict)
                                                            continue
                                                        target_tab_data = dc_csv.csv_creator({"id": norm_object_id, "type": "dois", "attributes": {"doi": norm_object_id}})
                                                        if target_tab_data:
                                                            processed_target_id = target_tab_data.get("id")
                                                            if processed_target_id:
                                                                data_object.append(target_tab_data)
                                                                if relationType in ["cites", "references"]:
                                                                    rel_dict = {"rel_type": "cites", "object_id": norm_object_id}
                                                                    valid_target_ids.append(rel_dict)
                                                                elif relationType in ["iscitedby", "isreferencedby"]:
                                                                    rel_dict = {"rel_type": "iscitedby", "object_id": norm_object_id}
                                                                    valid_target_ids.append(rel_dict)
                                                elif stored_validity is True:
                                                    if relationType in ["cites", "references"]:
                                                        rel_dict = {"rel_type": "cites", "object_id": norm_object_id}
                                                        valid_target_ids.append(rel_dict)
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
            except Exception as e:
                print("[PROCESS ERROR] during object processing. Entity preview:")
                try:
                    print(json.dumps(entity, ensure_ascii=False)[:500] + "...")
                except Exception:
                    print(str(entity)[:500] + "...")
                print(f"Details: {e}")
                continue
        save_files(data_object, index_citations_to_csv, False)

def pathoo(path:str) -> None:
    if not os.path.exists(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path))

def read_ndjson_chunk(file_path, chunk_size, bad_dir=None, echo_bad_preview=300):
    bad_fp = None

    with open(file_path, 'r', encoding='utf-8') as file:
        line_no = 0
        while True:
            chunk = []
            for _ in range(chunk_size):
                line = file.readline()
                if not line:
                    break
                line_no += 1
                try:
                    data = json.loads(line)
                    chunk.append(data)
                except json.JSONDecodeError as e:
                    preview = line[:echo_bad_preview].rstrip().replace('\n', '\\n')
                    print(
                        f"[JSON ERROR] file={file_path} line={line_no}: {e}\n"
                        f"  preview: {preview}{'...' if len(line) > echo_bad_preview else ''}"
                    )
                    if bad_dir:
                        if bad_fp is None:
                            os.makedirs(bad_dir, exist_ok=True)
                            bad_fp = os.path.join(bad_dir, Path(file_path).name + ".bad.ndjson")
                        with open(bad_fp, 'a', encoding='utf-8') as bf:
                            bf.write(line)
                    continue
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
    arg_parser.add_argument('-ca', '--cache', dest='cache', required=False,
                            help='The cache file path. This file will be deleted at the end of the process')
    arg_parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', required=False,
                            help='Show extra informational logs')
    arg_parser.add_argument('-t', '--testing', dest='testing', action='store_true', required=False,
                            help='parameter to define if the script is to be run in testing mode. Pay attention:'
                                 'by default the script is run in test modality and thus the data managed by redis, '
                                 'stored in a specific redis db, are not retrieved nor permanently saved, since an '
                                 'instance of a FakeRedis class is created and deleted by the end of the process.')
    arg_parser.add_argument('-m', '--max_workers', dest='max_workers', required=False, default=1, type=int,
                            help='Workers number')
    arg_parser.add_argument('--no-orcid-api', dest='no_orcid_api', action='store_true', required=False,
                            help='Disable ORCID API validation (use only DOI→ORCID index and caches)')
    arg_parser.add_argument('--exclude-existing', dest='exclude_existing', action='store_true', required=False,
                            help='Exclude entities that already exist in Meta from the output CSV')
    arg_parser.add_argument('-s', '--storage_path', dest='storage_path', required=False,
                            help='Path for ID validation storage. Use .db extension for SQLite or .json for '
                                 'in-memory JSON storage. If not specified, uses Redis (default). '
                                 'Note: SQLite and JSON storage are single-threaded (--max_workers is ignored).')

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
    cache = settings['cache_filepath'] if settings else args.cache
    cache = normalize_path(cache) if cache else None
    verbose = settings['verbose'] if settings else args.verbose
    testing = settings['testing'] if settings else args.testing
    max_workers = settings['max_workers'] if settings else args.max_workers
    no_orcid_api = settings.get('disable_orcid_api', False) if settings else args.no_orcid_api
    use_orcid_api = not no_orcid_api
    exclude_existing = settings.get('exclude_existing', False) if settings else args.exclude_existing
    storage_path = settings.get('storage_path', args.storage_path) if settings else args.storage_path
    storage_path = normalize_path(storage_path) if storage_path else None

    if storage_path and max_workers > 1:
        print('[Warning] SQLite/JSON storage requires single-threaded mode. Setting max_workers=1')
        max_workers = 1

    preprocess(datacite_ndjson_dir=datacite_ndjson_dir, publishers_filepath=publishers_filepath, orcid_doi_filepath=orcid_doi_filepath, csv_dir=csv_dir, cache=cache, verbose=verbose, testing=testing,
               max_workers=max_workers, use_orcid_api=use_orcid_api, exclude_existing=exclude_existing, storage_path=storage_path)
