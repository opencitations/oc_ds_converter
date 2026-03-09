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

import yaml
from filelock import BaseFileLock, FileLock
from tarfile import TarInfo

from oc_ds_converter.crossref.crossref_processing import CrossrefProcessing
from oc_ds_converter.crossref.extract_crossref_publishers import is_stale as publishers_is_stale
from oc_ds_converter.crossref.extract_crossref_publishers import process as extract_publishers
from oc_ds_converter.datasource.orcid_index import (
    OrcidIndexRedis,
    PublishersRedis,
    load_orcid_index_to_redis,
    load_publishers_to_redis,
)
from oc_ds_converter.lib.console import console, create_progress
from oc_ds_converter.lib.file_manager import normalize_path, pathoo
from oc_ds_converter.lib.jsonmanager import get_all_files_by_type, load_json
from oc_ds_converter.oc_idmanager.oc_data_storage.redis_manager import RedisStorageManager


def _run_iteration(
    all_files: list[str | TarInfo],
    targz_fd: tarfile.TarFile | None,
    preprocessed_citations_dir: str,
    csv_dir: str,
    orcid_doi_filepath: str | None,
    wanted_doi_filepath: str | None,
    publishers_filepath: str | None,
    testing: bool,
    cache: str | None,
    processing_citing: bool,
    use_orcid_api: bool,
    max_workers: int = 1,
    use_redis_publishers: bool = False,
) -> None:
    iteration_label = "citing entities" if processing_citing else "cited entities"
    iteration_num = "First" if processing_citing else "Second"

    with create_progress() as progress:
        task = progress.add_task(f"[green]{iteration_num} iteration ({iteration_label})", total=len(all_files))

        if max_workers == 1:
            for filename in all_files:
                get_citations_and_metadata(
                    filename, targz_fd, preprocessed_citations_dir, csv_dir, orcid_doi_filepath,
                    wanted_doi_filepath, publishers_filepath,
                    testing, cache, processing_citing=processing_citing, use_orcid_api=use_orcid_api,
                    use_redis_publishers=use_redis_publishers
                )
                progress.update(task, advance=1)
        else:
            with ProcessPoolExecutor(max_workers=max_workers, mp_context=get_context('spawn')) as executor:
                futures = []
                for filename in all_files:
                    if isinstance(filename, str) and filename.startswith("._"):
                        progress.update(task, advance=1)
                        continue
                    future = executor.submit(
                        get_citations_and_metadata,
                        filename, targz_fd, preprocessed_citations_dir, csv_dir, orcid_doi_filepath,
                        wanted_doi_filepath, publishers_filepath,
                        testing, cache, processing_citing, use_orcid_api, use_redis_publishers
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


def _extract_redis_ids_and_update(
    processor: CrossrefProcessing,
    entity_list: list[dict],
    processing_citing: bool,
) -> None:
    all_br: list[str] = []
    all_ra: list[str] = []

    for entity in entity_list:
        if entity and "reference" in entity:
            has_doi_references = bool([x for x in entity["reference"] if x.get("DOI")])
            if has_doi_references:
                ent_all_br, ent_all_ra = processor.extract_all_ids(entity, processing_citing)
                all_br.extend(ent_all_br)
                all_ra.extend(ent_all_ra)

    redis_validity_values_br = processor.get_redis_validity_list(all_br, "br")
    redis_validity_values_ra = processor.get_redis_validity_list(all_ra, "ra")
    processor.update_redis_values(redis_validity_values_br, redis_validity_values_ra)


def _save_output_files(
    entity_rows: list[dict],
    citation_rows: list[dict],
    metadata_output_base: str,
    citation_links_output_base: str,
    processor: CrossrefProcessing,
    processing_citing: bool,
    cache_path: str,
    lock: BaseFileLock,
    filename: str,
) -> None:
    if entity_rows:
        suffix = "_citing.csv" if processing_citing else "_cited.csv"
        filepath = metadata_output_base + suffix
        with open(filepath, 'w', newline='', encoding='utf-8') as output_file:
            dict_writer = csv.DictWriter(
                output_file, entity_rows[0].keys(), delimiter=',', quotechar='"',
                quoting=csv.QUOTE_NONNUMERIC, escapechar='\\'
            )
            dict_writer.writeheader()
            dict_writer.writerows(entity_rows)
    processor.memory_to_storage()

    if not processing_citing and citation_rows:
        filepath = citation_links_output_base + ".csv"
        with open(filepath, 'w', newline='', encoding='utf-8') as output_file:
            dict_writer = csv.DictWriter(
                output_file, citation_rows[0].keys(), delimiter=',', quotechar='"',
                quoting=csv.QUOTE_NONNUMERIC, escapechar='\\'
            )
            dict_writer.writeheader()
            dict_writer.writerows(citation_rows)

    processor.memory_to_storage()
    _mark_file_completed(cache_path, lock, filename, processing_citing)


def _mark_file_completed(
    cache_path: str,
    lock: BaseFileLock,
    filename: str,
    processing_citing: bool,
) -> None:
    iteration_key = "citing" if processing_citing else "cited"
    with lock:
        cache_dict: dict[str, list[str]] = {}
        if os.path.exists(cache_path):
            with open(cache_path, "r", encoding="utf-8") as f:
                cache_dict = json.load(f)

        if iteration_key not in cache_dict:
            cache_dict[iteration_key] = []

        file_basename = Path(filename).name
        if file_basename not in cache_dict[iteration_key]:
            cache_dict[iteration_key].append(file_basename)

        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(cache_dict, f)


def _process_citing_entities(
    processor: CrossrefProcessing,
    source_dict: list[dict],
) -> list[dict]:
    citing_entity_rows: list[dict] = []
    for entity in source_dict:
        if entity:
            # For citing entities, validation is not needed; if normalizable, proceed directly to Meta table creation
            norm_source_id = processor.tmp_doi_m.normalise(entity['DOI'], include_prefix=True)
            if norm_source_id is None:
                continue

            # If the id is not in the storage, it means it was not processed and is not in the csv output tables yet
            if not processor.doi_m.storage_manager.get_value(norm_source_id):
                # Add the id as valid to the temporary storage manager and create a meta csv row
                processor.tmp_doi_m.storage_manager.set_value(norm_source_id, True)
                source_tab_data = processor.csv_creator(entity)
                if source_tab_data:
                    processed_source_id = source_tab_data["id"]
                    if processed_source_id:
                        citing_entity_rows.append(source_tab_data)
    return citing_entity_rows


def _process_cited_entities(
    processor: CrossrefProcessing,
    source_dict: list[dict],
) -> tuple[list[dict], list[dict]]:
    cited_entity_rows: list[dict] = []
    citation_rows: list[dict] = []

    for entity in source_dict:
        if entity and "reference" in entity:
            has_doi_references = [x for x in entity["reference"] if x.get("DOI")]
            if has_doi_references:
                norm_source_id = processor.doi_m.normalise(entity['DOI'], include_prefix=True)

                cit_list_entities = [x.get("DOI") for x in has_doi_references]
                cit_list_entities_dois = [x for x in cit_list_entities if x]
                if cit_list_entities_dois:
                    valid_target_ids: list[str] = []
                    for cited_entity in cit_list_entities_dois:
                        norm_id = processor.doi_m.normalise(cited_entity, include_prefix=True)
                        if norm_id:
                            norm_id_dict_to_val = {"schema": "doi", "identifier": norm_id}
                            stored_validity = processor.validated_as(norm_id_dict_to_val)
                            if stored_validity is None:
                                norm_id_dict = {"id": norm_id, "schema": "doi"}
                                if norm_id in processor.to_validated_id_list(norm_id_dict):
                                    cited_entity_dict = {"DOI": norm_id}
                                    target_tab_data = processor.csv_creator(cited_entity_dict)
                                    if target_tab_data:
                                        processed_target_id = target_tab_data.get("id")
                                        if processed_target_id:
                                            cited_entity_rows.append(target_tab_data)
                                            valid_target_ids.append(norm_id)
                            elif stored_validity is True:
                                valid_target_ids.append(norm_id)

                    for target_id in valid_target_ids:
                        citation_rows.append({"citing": norm_source_id, "cited": target_id})

    return cited_entity_rows, citation_rows


def preprocess(
    crossref_json_dir: str,
    orcid_doi_filepath: str | None,
    csv_dir: str,
    wanted_doi_filepath: str | None = None,
    cache: str | None = None,
    testing: bool = True,
    max_workers: int = 1,
    use_orcid_api: bool = True,
    update_publishers: bool = False,
    publishers_max_age: int = 30,
) -> None:

    # create output dir if does not exist
    if not os.path.exists(csv_dir):
        os.makedirs(csv_dir)

    publishers_filepath = os.path.join(os.path.dirname(__file__), '..', 'crossref', 'data', 'publishers.csv')
    publishers_filepath = os.path.normpath(publishers_filepath)

    if not testing:
        if update_publishers:
            console.print('[cyan]Force updating publishers data from Crossref API...[/cyan]')
            extract_publishers(publishers_filepath, force=True)
        elif publishers_is_stale(publishers_filepath, publishers_max_age):
            console.print(f'[cyan]Publishers data is older than {publishers_max_age} days, updating...[/cyan]')
            extract_publishers(publishers_filepath, max_age_days=publishers_max_age)
        else:
            console.print('[green]Publishers data is up to date[/green]')

    use_redis_publishers = max_workers > 1
    if use_redis_publishers and os.path.exists(publishers_filepath):
        publishers_redis = PublishersRedis(testing=testing)
        if not publishers_redis.has_data() or update_publishers:
            console.print('[cyan]Loading publishers to Redis...[/cyan]')
            publishers_redis.clear()
            load_publishers_to_redis(publishers_filepath, publishers_redis)
            console.print('[green]Publishers loaded to Redis[/green]')
        publishers_filepath = None

    if not os.path.exists(publishers_filepath) if publishers_filepath else False:
        publishers_filepath = None

    orcid_index_redis = OrcidIndexRedis(testing=testing)
    if orcid_doi_filepath:
        console.print('[cyan]Updating DOI-ORCID index in Redis...[/cyan]')
        orcid_index_redis.clear()
        load_orcid_index_to_redis(orcid_doi_filepath, orcid_index_redis)
        console.print('[green]DOI-ORCID index updated in Redis[/green]')
    else:
        console.print('[cyan]Using existing DOI-ORCID index from Redis[/cyan]')

    if wanted_doi_filepath:
        console.print('[cyan]Processing: wanted DOIs CSV[/cyan]')

    # create output dir for citation data
    preprocessed_citations_dir = csv_dir + "_citations"
    if not os.path.exists(preprocessed_citations_dir):
        os.makedirs(preprocessed_citations_dir)

    console.print(f'[cyan]Getting all files from {crossref_json_dir}[/cyan]')
    all_files, targz_fd = get_all_files_by_type(crossref_json_dir, ".json", cache)
    total_files = len(all_files)
    console.print(f'[cyan]Found {total_files} files to process[/cyan]')

    iteration_args = (
        all_files, targz_fd, preprocessed_citations_dir, csv_dir, None,
        wanted_doi_filepath, publishers_filepath,
        testing, cache
    )

    _run_iteration(*iteration_args, processing_citing=True, use_orcid_api=use_orcid_api, max_workers=max_workers, use_redis_publishers=use_redis_publishers)
    _run_iteration(*iteration_args, processing_citing=False, use_orcid_api=use_orcid_api, max_workers=max_workers, use_redis_publishers=use_redis_publishers)

    # DELETE CACHE AND .LOCK FILE
    cache_path = cache if cache else os.path.join(os.getcwd(), "cache.json")
    _delete_cache_files(cache_path)

    # added to avoid order-related issues in sequential tests runs
    if testing:
        storage_manager = RedisStorageManager(testing=testing)
        storage_manager.delete_storage()


def get_citations_and_metadata(file_name, targz_fd, preprocessed_citations_dir: str, csv_dir: str,
                               orcid_index: str | None,
                               doi_csv: str | None, publishers_filepath: str | None,
                               testing: bool, cache: str | None, processing_citing: bool, use_orcid_api: bool,
                               use_redis_publishers: bool = False):
    if isinstance(file_name, tarfile.TarInfo):
        file_name = file_name.name
    storage_manager = RedisStorageManager(testing=testing)
    if cache:
        if not cache.endswith(".json"):
            cache = os.path.join(os.getcwd(), "cache.json")
        else:
            if not os.path.exists(os.path.abspath(os.path.join(cache, os.pardir))):
                Path(os.path.abspath(os.path.join(cache, os.pardir))).mkdir(parents=True, exist_ok=True)
    else:
        cache = os.path.join(os.getcwd(), "cache.json")

    lock = FileLock(cache + ".lock")
    cache_dict: dict[str, list[str]] = {"citing": [], "cited": []}
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
    file_basename = Path(file_name).name
    if cache_dict.get("citing"):
        if processing_citing and file_basename in cache_dict["citing"]:
            return
    if cache_dict.get("cited"):
        if not processing_citing and file_basename in cache_dict["cited"]:
            return

    crossref_csv = CrossrefProcessing(
        orcid_index=orcid_index, doi_csv=doi_csv, publishers_filepath=publishers_filepath,
        testing=testing, citing=processing_citing, use_orcid_api=use_orcid_api,
        use_redis_orcid_index=True, use_redis_publishers=use_redis_publishers
    )

    source_data = load_json(file_name, targz_fd)
    if source_data is None:
        return
    source_dict = source_data['items']

    filename_without_ext = file_basename.replace('.json', '').replace('.tar', '').replace('.gz', '')
    filepath = os.path.join(csv_dir, f'{filename_without_ext}.csv')
    pathoo(filepath)

    metadata_output_base = os.path.join(csv_dir, filename_without_ext)
    citation_links_output_base = os.path.join(preprocessed_citations_dir, filename_without_ext)

    filepath_citations = os.path.join(preprocessed_citations_dir, f'{filename_without_ext}.csv')
    pathoo(filepath_citations)

    _extract_redis_ids_and_update(crossref_csv, source_dict, processing_citing)

    if processing_citing:
        citing_entity_rows = _process_citing_entities(crossref_csv, source_dict)
        _save_output_files(
            citing_entity_rows, [], metadata_output_base, citation_links_output_base,
            crossref_csv, True, cache, lock, file_basename
        )
    else:
        cited_entity_rows, citation_rows = _process_cited_entities(crossref_csv, source_dict)
        _save_output_files(
            cited_entity_rows, citation_rows, metadata_output_base, citation_links_output_base,
            crossref_csv, False, cache, lock, file_basename
        )


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
                            help='Directory containing DOI-ORCID index CSV files. If specified, updates the Redis '
                                 'DOI-ORCID index database. If not specified, uses the existing index in Redis.')
    arg_parser.add_argument('-w', '--wanted', dest='wanted_doi_filepath', required=False,
                            help='A CSV filepath containing what DOI to process, not mandatory')
    arg_parser.add_argument('-ca', '--cache', dest='cache', required=False,
                            help='Path to a JSON file for caching processed files. Tracks which files have been '
                                 'processed to allow resuming. Deleted at the end of successful processing.')
    arg_parser.add_argument('-t', '--testing', dest='testing', action='store_true', required=False,
                            help='Run in testing mode: uses in-memory FakeRedis instead of real Redis, '
                                 'skips publisher data update from Crossref API, and cleans up storage at the end. '
                                 'Use this flag for tests only, not for production runs.')
    arg_parser.add_argument('-m', '--max_workers', dest='max_workers', required=False, default=1, type=int,
                            help='Workers number')
    arg_parser.add_argument('--no-orcid-api', dest='no_orcid_api', action='store_true', required=False,
                            help='Disable ORCID API validation (use only DOI→ORCID index and caches)')
    arg_parser.add_argument('--update-publishers', dest='update_publishers', action='store_true', required=False,
                            help='Force update of publishers data from Crossref API, ignoring age check')
    arg_parser.add_argument('--publishers-max-age', dest='publishers_max_age', required=False, default=30, type=int,
                            help='Maximum age in days for publishers data before automatic update (default: 30)')

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
    testing = settings.get('testing', args.testing) if settings else args.testing
    max_workers = settings.get('max_workers', args.max_workers) if settings else args.max_workers
    no_orcid_api = settings.get('disable_orcid_api', args.no_orcid_api) if settings else args.no_orcid_api
    use_orcid_api = not no_orcid_api
    update_publishers = settings.get('update_publishers', args.update_publishers) if settings else args.update_publishers
    publishers_max_age = settings.get('publishers_max_age', args.publishers_max_age) if settings else args.publishers_max_age

    if max_workers > 1 and crossref_json_dir.endswith('.tar.gz'):
        arg_parser.error(
            'Multiprocessing (--max_workers > 1) is incompatible with tar.gz input. '
            'Either extract the archive first or use --max_workers 1.'
        )

    preprocess(
        crossref_json_dir=crossref_json_dir,
        orcid_doi_filepath=orcid_doi_filepath,
        csv_dir=csv_dir,
        wanted_doi_filepath=wanted_doi_filepath,
        cache=cache,
        testing=testing,
        max_workers=max_workers,
        use_orcid_api=use_orcid_api,
        update_publishers=update_publishers,
        publishers_max_age=publishers_max_age,
    )
