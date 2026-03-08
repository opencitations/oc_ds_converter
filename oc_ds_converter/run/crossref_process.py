#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright 2021-2022 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED 'AS IS' AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.


import csv
import json
import os
import sys
import tarfile
from argparse import ArgumentParser
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import get_context
from pathlib import Path
from tarfile import TarInfo

import yaml
from filelock import FileLock

from oc_ds_converter.crossref.crossref_processing import CrossrefProcessing
from oc_ds_converter.crossref.extract_crossref_publishers import process as extract_publishers
from oc_ds_converter.lib.console import console, create_progress
from oc_ds_converter.lib.file_manager import normalize_path
from oc_ds_converter.lib.jsonmanager import get_all_files_by_type, load_json
from oc_ds_converter.oc_idmanager.oc_data_storage.in_memory_manager import InMemoryStorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.redis_manager import RedisStorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.sqlite_manager import SqliteStorageManager


def _run_iteration_sequential(
    all_files: list[str | TarInfo],
    targz_fd: tarfile.TarFile | None,
    preprocessed_citations_dir: str,
    csv_dir: str,
    orcid_doi_filepath: str | None,
    wanted_doi_filepath: str | None,
    publishers_filepath: str | None,
    storage_path: str | None,
    redis_storage_manager: bool,
    testing: bool,
    cache: str | None,
    is_first_iteration: bool,
    use_orcid_api: bool,
) -> None:
    iteration_label = "citing entities" if is_first_iteration else "cited entities"
    iteration_num = "First" if is_first_iteration else "Second"
    with create_progress() as progress:
        task = progress.add_task(f"[green]{iteration_num} iteration ({iteration_label})", total=len(all_files))
        for filename in all_files:
            get_citations_and_metadata(
                filename, targz_fd, preprocessed_citations_dir, csv_dir, orcid_doi_filepath,
                wanted_doi_filepath, publishers_filepath, storage_path, redis_storage_manager,
                testing, cache, is_first_iteration=is_first_iteration, use_orcid_api=use_orcid_api
            )
            progress.update(task, advance=1)


def _run_iteration_parallel(
    all_files: list[str | TarInfo],
    targz_fd: tarfile.TarFile | None,
    preprocessed_citations_dir: str,
    csv_dir: str,
    orcid_doi_filepath: str | None,
    wanted_doi_filepath: str | None,
    publishers_filepath: str | None,
    storage_path: str | None,
    redis_storage_manager: bool,
    testing: bool,
    cache: str | None,
    is_first_iteration: bool,
    use_orcid_api: bool,
    max_workers: int,
) -> None:
    iteration_label = "citing entities" if is_first_iteration else "cited entities"
    iteration_num = "First" if is_first_iteration else "Second"
    with create_progress() as progress:
        task = progress.add_task(f"[green]{iteration_num} iteration ({iteration_label})", total=len(all_files))
        with ProcessPoolExecutor(max_workers=max_workers, mp_context=get_context('spawn')) as executor:
            futures = []
            for filename in all_files:
                if isinstance(filename, str) and filename.startswith("._"):
                    progress.update(task, advance=1)
                    continue
                future = executor.submit(
                    get_citations_and_metadata,
                    filename, targz_fd, preprocessed_citations_dir, csv_dir, orcid_doi_filepath,
                    wanted_doi_filepath, publishers_filepath, storage_path, redis_storage_manager,
                    testing, cache, is_first_iteration, use_orcid_api
                )
                futures.append(future)
            for future in futures:
                future.result()
                progress.update(task, advance=1)
    console.print(f'[green]{iteration_num} iteration complete[/green]')


def _delete_cache_files(cache_path: str) -> None:
    if os.path.exists(cache_path):
        os.remove(cache_path)
    lock_file = cache_path + ".lock"
    if os.path.exists(lock_file):
        os.remove(lock_file)


def preprocess(crossref_json_dir: str, orcid_doi_filepath: str | None, csv_dir: str, wanted_doi_filepath: str | None = None, cache: str | None = None, storage_path: str | None = None,
               testing: bool = True, redis_storage_manager: bool = False, max_workers: int = 1, use_orcid_api: bool = True) -> None:

    # create output dir if does not exist
    if not os.path.exists(csv_dir):
        os.makedirs(csv_dir)

    publishers_filepath = os.path.join(os.path.dirname(__file__), '..', 'crossref', 'data', 'publishers.csv')
    publishers_filepath = os.path.normpath(publishers_filepath)
    if not testing:
        console.print('[cyan]Updating publishers data from Crossref API...[/cyan]')
        extract_publishers(publishers_filepath)
    if not os.path.exists(publishers_filepath):
        publishers_filepath = None

    if orcid_doi_filepath or wanted_doi_filepath:
        what = []
        if orcid_doi_filepath:
            what.append('DOI-ORCID index')
        if wanted_doi_filepath:
            what.append('wanted DOIs CSV')
        console.print(f'[cyan]Processing: {"; ".join(what)}[/cyan]')

    # create output dir for citation data
    preprocessed_citations_dir = csv_dir + "_citations"
    if not os.path.exists(preprocessed_citations_dir):
        os.makedirs(preprocessed_citations_dir)

    console.print(f'[cyan]Getting all files from {crossref_json_dir}[/cyan]')
    all_files, targz_fd = get_all_files_by_type(crossref_json_dir, ".json", cache)
    total_files = len(all_files)
    console.print(f'[cyan]Found {total_files} files to process[/cyan]')

    iteration_args = (
        all_files, targz_fd, preprocessed_citations_dir, csv_dir, orcid_doi_filepath,
        wanted_doi_filepath, publishers_filepath, storage_path, redis_storage_manager,
        testing, cache
    )

    if not redis_storage_manager or max_workers == 1:
        _run_iteration_sequential(*iteration_args, is_first_iteration=True, use_orcid_api=use_orcid_api)
        _run_iteration_sequential(*iteration_args, is_first_iteration=False, use_orcid_api=use_orcid_api)
    else:
        _run_iteration_parallel(*iteration_args, is_first_iteration=True, use_orcid_api=use_orcid_api, max_workers=max_workers)
        _run_iteration_parallel(*iteration_args, is_first_iteration=False, use_orcid_api=use_orcid_api, max_workers=max_workers)

    # DELETE CACHE AND .LOCK FILE
    cache_path = cache if cache else os.path.join(os.getcwd(), "cache.json")
    _delete_cache_files(cache_path)

    # added to avoid order-related issues in sequential tests runs
    if testing:
        storage_manager = get_storage_manager(storage_path, redis_storage_manager, testing=testing)
        storage_manager.delete_storage()


def get_citations_and_metadata(file_name, targz_fd, preprocessed_citations_dir: str, csv_dir: str,
                               orcid_index: str | None,
                               doi_csv: str | None, publishers_filepath: str | None, storage_path: str | None,
                               redis_storage_manager: bool,
                               testing: bool, cache: str | None, is_first_iteration: bool, use_orcid_api: bool):
    if isinstance(file_name, tarfile.TarInfo):
        file_name = file_name.name
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
                except json.JSONDecodeError:
                    write_new = True
    else:
        write_new = True
    if write_new:
        with lock:
            with open(cache, "w", encoding="utf-8") as c:
                json.dump(cache_dict, c)

    # skip if in cache
    filename = file_name
    if cache_dict.get("first_iteration"):
        if is_first_iteration and filename in cache_dict["first_iteration"]:
            return
    if cache_dict.get("second_iteration"):
        if not is_first_iteration and filename in cache_dict["second_iteration"]:
            return

    crossref_csv = CrossrefProcessing(
        orcid_index=orcid_index, doi_csv=doi_csv, publishers_filepath=publishers_filepath,
        storage_manager=storage_manager, testing=testing, citing=is_first_iteration, use_orcid_api=use_orcid_api
    )
    index_citations_to_csv = []
    data_citing = []
    data_cited = []

    source_data = load_json(filename, targz_fd)
    if source_data is None:
        return
    source_dict = source_data['items']


    filename = filename.name if isinstance(filename, TarInfo) else filename
    filename_without_ext = filename.replace('.json', '').replace('.tar', '').replace('.gz', '')
    filepath = os.path.join(csv_dir, f'{os.path.basename(filename_without_ext)}.csv')
    pathoo(filepath)

    filepath_ne = os.path.join(csv_dir, f'{os.path.basename(filename_without_ext)}')
    filepath_citations_ne = os.path.join(preprocessed_citations_dir, f'{os.path.basename(filename_without_ext)}')

    filepath_citations = os.path.join(preprocessed_citations_dir, f'{os.path.basename(filename_without_ext)}.csv')
    pathoo(filepath_citations)

    #  √ REDIS UPDATE
    def get_all_redis_ids_and_save_updates(sli_da, is_first_iteration_par:bool):
        all_br = []
        all_ra = []

        # RETRIEVE ALL THE IDENTIFIERS TO BE VALIDATED THAT MAY BE IN REDIS
        # DOI, ORCID,
        for entity in sli_da:  # for each bibliographical entity in the list
            if entity and "reference" in entity:
                # filtering out entities without citations
                has_doi_references = True if [x for x in entity["reference"] if x.get("DOI")] else False
                if has_doi_references:
                    ent_all_br, ent_all_ra = crossref_csv.extract_all_ids(entity, is_first_iteration_par)
                    all_br.extend(ent_all_br)
                    # ERRORE CORRETTO all_ra -> ent_all_ra
                    all_ra.extend(ent_all_ra)

        redis_validity_values_br = crossref_csv.get_reids_validity_list(all_br, "br")
        redis_validity_values_ra = crossref_csv.get_reids_validity_list(all_ra, "ra")
        crossref_csv.update_redis_values(redis_validity_values_br, redis_validity_values_ra)

    def save_files(ent_list, citation_list, is_first_iteration_par: bool):
        if ent_list:
            # Filename of the source json, At first iteration, we will generate a CSV file containing all the
            # citing entities metadata, at the second iteration we will generate a cited entities metadata file
            # and the citations csv file
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
        crossref_csv.memory_to_storage()

        if not is_first_iteration_par:
            if citation_list:
                filename_cit_str = filepath_citations_ne + ".csv"
                with open(filename_cit_str, 'w', newline='', encoding='utf-8') as output_file_citations:
                    dict_writer = csv.DictWriter(output_file_citations, citation_list[0].keys(), delimiter=',',
                                                 quotechar='"', quoting=csv.QUOTE_NONNUMERIC, escapechar='\\')
                    dict_writer.writeheader()
                    dict_writer.writerows(citation_list)
                citation_list = []

        crossref_csv.memory_to_storage()
        task_done(is_first_iteration_par)
        return ent_list, citation_list

    def task_done(is_first_iteration_par: bool) -> None:
        iteration_key = "first_iteration" if is_first_iteration_par else "second_iteration"
        if iteration_key not in cache_dict:
            cache_dict[iteration_key] = set()

        for k, v in cache_dict.items():
            cache_dict[k] = set(v)

        cache_dict[iteration_key].add(Path(file_name).name)

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

                        # unione set e poi lista
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

    if is_first_iteration:
        get_all_redis_ids_and_save_updates(source_dict, is_first_iteration_par=True)
        # prima l'ultimo file va processato
        for entity in source_dict:
            if entity:
                # per i citanti la validazione non serve, se è normalizzabile va direttamente alla crezione tabelle Meta
                norm_source_id = crossref_csv.tmp_doi_m.normalise(entity['DOI'], include_prefix=True)
                if norm_source_id is None:
                    continue

                # if the id is not in the redis database, it means that it was not processed and that it is not in the csv output tables yet.
                if not crossref_csv.doi_m.storage_manager.get_value(norm_source_id):
                    # add the id as valid to the temporary storage manager (whose values will be transferred to the redis storage manager at the
                    # time of the csv files creation process) and create a meta csv row for the entity in this case only
                    crossref_csv.tmp_doi_m.storage_manager.set_value(norm_source_id, True)
                    source_tab_data = crossref_csv.csv_creator(entity)
                    if source_tab_data:
                        processed_source_id = source_tab_data["id"]
                        if processed_source_id:
                            data_citing.append(source_tab_data)

        save_files(data_citing, index_citations_to_csv, True)

    '''cited entities:
    - look for the DOI in the temporary manager and in the storage manager:
        - if found as valid -> do not create the Meta table, but include the cited entity in the citations' tables;
        - if not found -> look for the doi in Redis server and later call the API if needed -> if the DOI is valid create the
        table for Meta and include the cited entity in the citations' tables
        - if found as not valid -> next entity'''

    if not is_first_iteration:
        get_all_redis_ids_and_save_updates(source_dict, is_first_iteration_par=False)
        for entity in source_dict:
            if entity and "reference" in entity:
                # filtering out entities without citations
                has_doi_references = [x for x in entity["reference"] if x.get("DOI")]
                if has_doi_references:
                    norm_source_id = crossref_csv.doi_m.normalise(entity['DOI'], include_prefix=True)

                    cit_list_entities = [x.get("DOI") for x in has_doi_references]
                    cit_list_entities_dois = [x for x in cit_list_entities if x]
                    # filtering out entities with citations without dois
                    if cit_list_entities_dois:

                        valid_target_ids = []
                        for cited_entity in cit_list_entities_dois:

                            # / START: BR ID VALIDATION
                            norm_id = crossref_csv.doi_m.normalise(cited_entity, include_prefix=True)
                            if norm_id:
                                norm_id_dict_to_val = {"schema":"doi"}
                                norm_id_dict_to_val["identifier"] = norm_id
                                stored_validity = crossref_csv.validated_as(norm_id_dict_to_val)
                                if stored_validity is None:
                                    norm_id_dict = {"id": norm_id, "schema":"doi"}
                                    if norm_id in crossref_csv.to_validated_id_list(norm_id_dict):
                                        cited_entity_dict = {"DOI": norm_id}
                                        target_tab_data = crossref_csv.csv_creator(cited_entity_dict)
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

def get_storage_manager(storage_path: str | None, redis_storage_manager: bool, testing: bool) -> SqliteStorageManager | InMemoryStorageManager | RedisStorageManager:
    if redis_storage_manager:
        return RedisStorageManager(testing=testing)
    if storage_path:
        if not os.path.exists(storage_path):
            if not os.path.exists(os.path.abspath(os.path.join(storage_path, os.pardir))):
                Path(os.path.abspath(os.path.join(storage_path, os.pardir))).mkdir(parents=True, exist_ok=True)
        if storage_path.endswith(".db"):
            return SqliteStorageManager(storage_path)
        if storage_path.endswith(".json"):
            return InMemoryStorageManager(storage_path)
        raise ValueError(f"Storage path must end with .db or .json, got: {storage_path}")
    new_path_dir = os.path.join(os.getcwd(), "storage")
    if not os.path.exists(new_path_dir):
        os.makedirs(new_path_dir)
    return SqliteStorageManager(os.path.join(new_path_dir, "id_valid_dict.db"))

def pathoo(path:str) -> None:
    if not os.path.exists(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path))


if __name__ == '__main__':  # pragma: no cover
    arg_parser = ArgumentParser('crossref_process.py', description='This script creates CSV files from Crossref JSON files, enriching them through of a DOI-ORCID index')
    arg_parser.add_argument('-c', '--config', dest='config', required=False,
                            help='Configuration file path')
    required = not any(arg in sys.argv for arg in {'--config', '-c'})
    arg_parser.add_argument('-cf', '--crossref', dest='crossref_json_dir', required=required,
                            help='Crossref json files directory')
    arg_parser.add_argument('-out', '--output', dest='csv_dir', required=required,
                            help='Directory where CSV will be stored')
    arg_parser.add_argument('-o', '--orcid', dest='orcid_doi_filepath', required=False,
                            help='DOI-ORCID index filepath, to enrich data')
    arg_parser.add_argument('-w', '--wanted', dest='wanted_doi_filepath', required=False,
                            help='A CSV filepath containing what DOI to process, not mandatory')
    arg_parser.add_argument('-ca', '--cache', dest='cache', required=False,
                        help='The cache file path. This file will be deleted at the end of the process')
    arg_parser.add_argument('-sp', '--storage_path', dest='storage_path', required=False,
                            help='path of the file where to store data concerning validated pids information.'
                                 'Pay attention to specify a ".db" file in case you chose the SqliteStorageManager'
                                 'and a ".json" file if you chose InMemoryStorageManager')
    arg_parser.add_argument('-t', '--testing', dest='testing', action='store_true', required=False,
                            help='Run in testing mode: uses in-memory FakeRedis instead of real Redis, '
                                 'skips publisher data update from Crossref API, and cleans up storage at the end. '
                                 'Use this flag for tests only, not for production runs.')
    arg_parser.add_argument('-r', '--redis_storage_manager', dest='redis_storage_manager', action='store_true',
                            required=False,
                            help='parameter to define whether or not to use redis as storage manager. Note that by default the parameter '
                                 'value is set to false, which means that -unless it is differently stated- the storage manager used is'
                                 'the one chosen as value of the parameter --storage_manager. The redis db used by the storage manager is the n.2')
    arg_parser.add_argument('-m', '--max_workers', dest='max_workers', required=False, default=1, type=int,
                            help='Workers number')
    arg_parser.add_argument('--no-orcid-api', dest='no_orcid_api', action='store_true', required=False,
                            help='Disable ORCID API validation (use only DOI→ORCID index and caches)')

    args = arg_parser.parse_args()
    config = args.config
    settings = None
    if config:
        with open(config, encoding='utf-8') as f:
            settings = yaml.full_load(f)
    crossref_json_dir = settings['crossref_json_dir'] if settings else args.crossref_json_dir
    crossref_json_dir = normalize_path(crossref_json_dir)
    csv_dir = settings['output'] if settings else args.csv_dir
    csv_dir = normalize_path(csv_dir)
    orcid_doi_filepath = settings['orcid_doi_filepath'] if settings else args.orcid_doi_filepath
    orcid_doi_filepath = normalize_path(orcid_doi_filepath) if orcid_doi_filepath else None
    wanted_doi_filepath = settings['wanted_doi_filepath'] if settings else args.wanted_doi_filepath
    wanted_doi_filepath = normalize_path(wanted_doi_filepath) if wanted_doi_filepath else None
    cache = settings['cache_filepath'] if settings else args.cache
    cache = normalize_path(cache) if cache else None
    storage_path = settings['storage_path'] if settings else args.storage_path
    storage_path = normalize_path(storage_path) if storage_path else None
    testing = settings['testing'] if settings else args.testing
    redis_storage_manager = settings['redis_storage_manager'] if settings else args.redis_storage_manager
    max_workers = settings['max_workers'] if settings else args.max_workers
    no_orcid_api = settings.get('disable_orcid_api', False) if settings else args.no_orcid_api
    use_orcid_api = not no_orcid_api

    if max_workers > 1 and crossref_json_dir.endswith('.tar.gz'):
        arg_parser.error(
            'Multiprocessing (--max_workers > 1) is incompatible with tar.gz input. '
            'Either extract the archive first or use --max_workers 1.'
        )

    preprocess(crossref_json_dir=crossref_json_dir, orcid_doi_filepath=orcid_doi_filepath, csv_dir=csv_dir, wanted_doi_filepath=wanted_doi_filepath, cache=cache, storage_path=storage_path, testing=testing,
               redis_storage_manager=redis_storage_manager, max_workers=max_workers, use_orcid_api=use_orcid_api)
