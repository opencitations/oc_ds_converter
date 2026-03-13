import csv
import json
import os
import sys
from argparse import ArgumentParser
from concurrent.futures import ProcessPoolExecutor
from json import JSONDecodeError
from multiprocessing import get_context
from pathlib import Path

import yaml
from filelock import FileLock
from tqdm import tqdm

from oc_ds_converter.datacite.datacite_processing import DataciteProcessing
from oc_ds_converter.lib.file_manager import normalize_path
from oc_ds_converter.oc_idmanager.oc_data_storage.in_memory_manager import InMemoryStorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.redis_manager import RedisStorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.sqlite_manager import SqliteStorageManager


def preprocess(datacite_json_dir:str, publishers_filepath:str|None, orcid_doi_filepath:str|None,
        csv_dir:str, wanted_doi_filepath:str=None, cache:str=None, verbose:bool=False, storage_path:str = None,
        testing: bool = True, redis_storage_manager: bool = False, max_workers: int = 1, use_orcid_api: bool = True,
        use_ror_api: bool = True, use_viaf_api: bool = True, use_wikidata_api: bool = True) -> None:

    if not os.path.exists(csv_dir):
        os.makedirs(csv_dir)

    preprocessed_citations_dir = csv_dir + "_citations"
    if not os.path.exists(preprocessed_citations_dir):
        os.makedirs(preprocessed_citations_dir)

    bad_dir = os.path.join(csv_dir, "_bad")  # creato solo on-demand in read_json

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
        print(f'[INFO: datacite_process] Getting all files from {datacite_json_dir}')

    all_input_json = []
    for entry in os.listdir(datacite_json_dir):
        fp = os.path.join(datacite_json_dir, entry)
        if os.path.isfile(fp) and fp.endswith(".json") and os.path.basename(fp).startswith("jSonFile_") and not entry.startswith("._"):
            all_input_json.append(fp)

    # dedup e ordine stabile
    all_input_json = sorted(list(dict.fromkeys(all_input_json)))

    if not redis_storage_manager or max_workers == 1:
        for json_file in tqdm(all_input_json):
            chunk = read_json(json_file, bad_dir)
            if chunk:
                get_citations_and_metadata(json_file, chunk, preprocessed_citations_dir, csv_dir, orcid_doi_filepath,
                                       wanted_doi_filepath, publishers_filepath, storage_path,
                                       redis_storage_manager,
                                       testing, cache, is_first_iteration=True, use_orcid_api=use_orcid_api, use_ror_api=use_ror_api,
                                        use_viaf_api=use_viaf_api, use_wikidata_api=use_wikidata_api)
            else:
                continue

        for json_file in tqdm(all_input_json):
            chunk = read_json(json_file, bad_dir)
            if chunk:
                get_citations_and_metadata(json_file, chunk, preprocessed_citations_dir, csv_dir, orcid_doi_filepath,
                                           wanted_doi_filepath, publishers_filepath, storage_path,
                                           redis_storage_manager,
                                           testing, cache, is_first_iteration=False, use_orcid_api=use_orcid_api, use_ror_api=use_ror_api,
                                           use_viaf_api=use_viaf_api, use_wikidata_api=use_wikidata_api)
            else:
                continue

    elif redis_storage_manager or max_workers > 1:
        with ProcessPoolExecutor(max_workers=max_workers, mp_context=get_context('spawn')) as executor:
            # Pass 1: is_first_iteration=True
            futures_pass1 = []
            for json_file in tqdm(all_input_json):
                chunk = read_json(json_file, bad_dir)
                if chunk:
                    future = executor.submit(
                        get_citations_and_metadata,
                        json_file, chunk, preprocessed_citations_dir, csv_dir,
                        orcid_doi_filepath, wanted_doi_filepath,
                        publishers_filepath, storage_path, redis_storage_manager,
                        testing, cache, True, use_orcid_api, use_ror_api, use_viaf_api, use_wikidata_api)
                    futures_pass1.append(future)

            for future in futures_pass1:
                try:
                    future.result()
                except Exception as e:
                    print(f"Task failed: {e}")

        with ProcessPoolExecutor(max_workers=max_workers, mp_context=get_context('spawn')) as executor:
            # Pass 2: is_first_iteration=False
            futures_pass2 = []
            for json_file in tqdm(all_input_json):
                chunk = read_json(json_file, bad_dir)
                if chunk:
                    future = executor.submit(
                        get_citations_and_metadata,
                        json_file, chunk, preprocessed_citations_dir, csv_dir,
                        orcid_doi_filepath, wanted_doi_filepath,
                        publishers_filepath, storage_path, redis_storage_manager,
                        testing, cache, False, use_orcid_api, use_ror_api, use_viaf_api, use_wikidata_api)
                    futures_pass2.append(future)

            for future in futures_pass2:
                try:
                    future.result()
                except Exception as e:
                    print(f"Task failed: {e}")

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


def get_citations_and_metadata(json_file:str, chunk: list, preprocessed_citations_dir: str, csv_dir: str,
                               orcid_index: str,
                               doi_csv: str, publishers_filepath: str, storage_path: str,
                               redis_storage_manager: bool,
                               testing: bool, cache: str, is_first_iteration:bool, use_orcid_api: bool, use_ror_api: bool,
                               use_viaf_api: bool, use_wikidata_api: bool):
    if redis_storage_manager:
        storage_manager = RedisStorageManager(testing=testing)
    else:
        storage_manager = SqliteStorageManager(storage_path) if storage_path else InMemoryStorageManager()

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

    json_to_save = os.path.basename(json_file).replace(".json", "")

    if cache_dict.get("first_iteration"):
        if is_first_iteration and json_to_save in cache_dict["first_iteration"]:
            return

    if cache_dict.get("second_iteration"):
        if not is_first_iteration and json_to_save in cache_dict["second_iteration"]:
            return

    if is_first_iteration:
        dc_csv = DataciteProcessing(orcid_index=orcid_index, doi_csv=doi_csv,
                                      publishers_filepath_dc=publishers_filepath,
                                      storage_manager=storage_manager, testing=testing, use_orcid_api=use_orcid_api,
                                    use_ror_api=use_ror_api, use_viaf_api=use_viaf_api, use_wikidata_api=use_wikidata_api)
    elif not is_first_iteration:
        dc_csv = DataciteProcessing(orcid_index=orcid_index, doi_csv=doi_csv,
                                  publishers_filepath_dc=publishers_filepath,
                                  storage_manager=storage_manager, testing=testing, use_orcid_api=use_orcid_api, use_ror_api=use_ror_api,
                                  use_viaf_api=use_viaf_api, use_wikidata_api=use_wikidata_api)

    index_citations_to_csv = []
    data_subject = []
    data_object = []

    filename_without_ext = json_file.replace('.json', '')
    filepath_ne = os.path.join(csv_dir, f'{os.path.basename(filename_without_ext)}')
    filepath_citations_ne = os.path.join(preprocessed_citations_dir, f'{os.path.basename(filename_without_ext)}')

    filepath = os.path.join(csv_dir, f'{os.path.basename(filename_without_ext)}.csv')
    filepath_citations = os.path.join(preprocessed_citations_dir, f'{os.path.basename(filename_without_ext)}.csv')
    pathoo(filepath)
    pathoo(filepath_citations)

    def get_all_redis_ids_and_save_updates(sli_da, is_first_iteration_par: bool):
        """Questo metodo prende i valori degli identificativi validi per BR e RA che provengono da Redis e
        li usa per aggiornare le variabili locali in memoria self._redis_values_br e self._redis_values_ra di dc_csv
        (istanza della classe DataciteProcessing)"""
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
                                    break
                    
                    if at_least_one_valid_object_id:
                        if is_first_iteration_par:
                            ent_all_br, ent_all_ra = dc_csv.extract_all_ids(entity, True)
                        else:
                            ent_all_br, ent_all_ra = dc_csv.extract_all_ids(entity, False)
                        all_br.extend(ent_all_br)
                        all_ra.extend(ent_all_ra)

        redis_validity_values_br = dc_csv.get_reids_validity_list(all_br, "br")
        redis_validity_values_ra = dc_csv.get_reids_validity_list(all_ra, "ra")
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
            task_done(is_first_iteration_par=True)
        else:
            task_done(is_first_iteration_par=False)
        return ent_list, citation_list


    def task_done(is_first_iteration_par: bool) -> None:
        try:
            if is_first_iteration_par and "citing" not in cache_dict.keys():
                cache_dict["citing"] = set()

            if not is_first_iteration_par and "cited" not in cache_dict.keys():
                cache_dict["cited"] = set()

            for k,v in cache_dict.items():
                cache_dict[k] = set(v)

            if is_first_iteration_par:
                cache_dict["first_iteration"].add(json_to_save)

            if not is_first_iteration_par:
                cache_dict["second_iteration"].add(json_to_save)

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
            try:
                if entity:
                    attributes = entity.get("attributes")
                    subject_id = attributes.get("doi")

                    #identificativo della bibliographic resource primaria
                    #normalizzo l'ID ricevuto in input e verifico se è già stato elaborato in precedenza consultando lo storage principale.
                    #Se l'ID risulta nuovo (non presente), viene aggiunto a uno storage temporaneo per una successiva validazione o elaborazione batch.

                    norm_subject_id = dc_csv.doi_m.normalise(subject_id, include_prefix=True)

                    if not dc_csv.doi_m.storage_manager.get_value(norm_subject_id):
                        dc_csv.tmp_doi_m.storage_manager.set_value(norm_subject_id, True)

                        if norm_subject_id:
                            #creo la riga per meta
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

    if not is_first_iteration:
        get_all_redis_ids_and_save_updates(chunk, is_first_iteration_par=False)
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
                                            #controllo che l'identificativo dell'entità primaria sia diverso da quello related (non creo la citazione per self-citations)
                                            if norm_object_id and norm_object_id != norm_subject_id:
                                                norm_id_dict_to_val = {"schema": "doi"}
                                                norm_id_dict_to_val["identifier"] = norm_object_id

                                                stored_validity = dc_csv.validated_as(norm_id_dict_to_val)
                                                #se non ho informazioni di validità su questo identificativo
                                                if stored_validity is None:
                                                    norm_id_dict = {"id": norm_object_id, "schema": "doi"}
                                                    # valido l'identificativo
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

def read_json(json_path, bad_dir: str = None, preview_chars: int = 100):
    try:
        with open(json_path, 'r') as json_object:
            chunk = json.load(json_object)
            data = chunk.get('data')
            return data
    except JSONDecodeError as e:
        # File-level preview
        try:
            preview = Path(json_path).read_text(encoding='utf-8', errors='ignore')[:preview_chars]
            preview = preview.rstrip().replace('\n', '\\n')
            preview += '...' if len(preview) == preview_chars else ''
        except Exception:
            preview = 'Unable to preview'

        print(f"[JSON ERROR] file={json_path}: {e}\n  preview: {preview}")

        # Dump full bad file
        if bad_dir:
            os.makedirs(bad_dir, exist_ok=True)
            bad_fp = os.path.join(bad_dir, Path(json_path).name + '.bad.json')
            try:
                Path(json_path).replace(bad_fp)  # Atomic move if possible
            except Exception:
                with open(bad_fp, 'wb') as bf:
                    bf.write(Path(json_path).read_bytes())

        return None


if __name__ == '__main__':
    arg_parser = ArgumentParser('datacite_process.py', description='This script creates CSV files from Datacite original dump, enriching data through of a DOI-ORCID index')
    arg_parser.add_argument('-c', '--config', dest='config', required=False,
                            help='Configuration file path')
    required = not any(arg in sys.argv for arg in {'--config', '-c'})
    arg_parser.add_argument('-dc', '--datacite', dest='datacite_json_dir', required=required,
                            help='Datacite json files directory')
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
    arg_parser.add_argument('--no-ror-api', dest='no_ror_api', action='store_true', required=False,
                            help='Disable ROR API validation for publisher ids')
    arg_parser.add_argument('--no-viaf-api', dest='no_viaf_api', action='store_true', required=False,
                            help='Disable VIAF API validation for publisher ids')
    arg_parser.add_argument('--no-wikidata-api', dest='no_wikidata_api', action='store_true', required=False,
                            help='Disable Wikidata API validation for publisher ids')
    arg_parser.add_argument('--no-crossref-api', dest='no_crossref_api', action='store_true', required=False,
                            help='Disable Crossref API validation for publisher ids')

    args = arg_parser.parse_args()
    config = args.config
    settings = None
    if config:
        with open(config, encoding='utf-8') as f:
            settings = yaml.full_load(f)
    datacite_json_dir = settings['datacite_json_dir'] if settings else args.datacite_json_dir
    datacite_json_dir = normalize_path(datacite_json_dir)
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
    no_ror_api = settings.get('disable_ror_api', False) if settings else args.no_ror_api
    no_viaf_api = settings.get('disable_viaf_api', False) if settings else args.no_viaf_api
    no_wikidata_api = settings.get('disable_wikidata_api', False) if settings else args.no_wikidata_api
    use_orcid_api = not no_orcid_api
    use_ror_api = not no_ror_api
    use_viaf_api =  not no_viaf_api
    use_wikidata_api = not no_wikidata_api

    preprocess(datacite_json_dir=datacite_json_dir, publishers_filepath=publishers_filepath, orcid_doi_filepath=orcid_doi_filepath, csv_dir=csv_dir, wanted_doi_filepath=wanted_doi_filepath, cache=cache, verbose=verbose, storage_path=storage_path, testing=testing,
               redis_storage_manager=redis_storage_manager, max_workers=max_workers, use_orcid_api=use_orcid_api, use_ror_api=use_ror_api, use_viaf_api=use_viaf_api, use_wikidata_api=use_wikidata_api)
