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
import os
import sys
import tarfile
from argparse import ArgumentParser
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import get_context
from pathlib import Path
from tarfile import TarInfo

import yaml
from filelock import BaseFileLock, FileLock

from oc_ds_converter.crossref.crossref_processing import CrossrefProcessing
from oc_ds_converter.crossref.extract_crossref_publishers import is_stale as publishers_is_stale
from oc_ds_converter.crossref.extract_crossref_publishers import process as extract_publishers
from oc_ds_converter.datasource.orcid_index import (
    OrcidIndexRedis,
    PublishersRedis,
    load_orcid_index_to_redis,
    load_publishers_to_redis,
)
from oc_ds_converter.lib.console import advance_progress, console, create_progress
from oc_ds_converter.lib.file_manager import normalize_path, pathoo
from oc_ds_converter.lib.jsonmanager import get_all_files_by_type, load_json
from oc_ds_converter.lib.process_utils import (
    cleanup_testing_storage,
    delete_cache_files,
    get_storage_manager,
    init_process_cache,
    is_file_in_cache,
    mark_file_completed,
    normalize_cache_path,
)



def _run_iteration(
    all_files: list[str | TarInfo],
    targz_fd: tarfile.TarFile | None,
    preprocessed_citations_dir: str,
    csv_dir: str,
    orcid_doi_filepath: str | None,
    publishers_filepath: str | None,
    testing: bool,
    cache: str | None,
    processing_citing: bool,
    use_orcid_api: bool,
    max_workers: int = 1,
    use_redis: bool = False,
    exclude_existing: bool = False,
    storage_path: str | None = None,
) -> None:
    iteration_label = "citing entities" if processing_citing else "cited entities"
    iteration_num = "First" if processing_citing else "Second"

    with create_progress() as progress:
        task = progress.add_task(f"[green]{iteration_num} iteration ({iteration_label})", total=len(all_files))

        if max_workers == 1:
            for filename in all_files:
                was_processed = get_citations_and_metadata(
                    filename, targz_fd, preprocessed_citations_dir, csv_dir, orcid_doi_filepath,
                    publishers_filepath,
                    testing, cache, processing_citing=processing_citing, use_orcid_api=use_orcid_api,
                    use_redis=use_redis, exclude_existing=exclude_existing,
                    storage_path=storage_path
                )
                advance_progress(progress, task, processed=was_processed)
        else:
            with ProcessPoolExecutor(max_workers=max_workers, mp_context=get_context('spawn')) as executor:
                futures = []
                for filename in all_files:
                    if isinstance(filename, str) and filename.startswith("._"):
                        advance_progress(progress, task, processed=False)
                        continue
                    future = executor.submit(
                        get_citations_and_metadata,
                        filename, targz_fd, preprocessed_citations_dir, csv_dir, orcid_doi_filepath,
                        publishers_filepath,
                        testing, cache, processing_citing, use_orcid_api, use_redis,
                        exclude_existing, storage_path
                    )
                    futures.append(future)
                for future in futures:
                    was_processed = future.result()
                    advance_progress(progress, task, processed=was_processed)
            console.print(f'[green]{iteration_num} iteration complete[/green]')


def _extract_redis_ids_and_update(
    processor: CrossrefProcessing,
    entity_list: list[dict],
    processing_citing: bool,
) -> None:
    all_br: list[str] = []
    all_ra: list[str] = []
    all_dois_for_orcid_index: list[str] = []

    for entity in entity_list:
        if entity:
            doi = entity.get("DOI")
            if doi:
                all_dois_for_orcid_index.append(doi)
            if "reference" in entity:
                has_doi_references = bool([x for x in entity["reference"] if x.get("DOI")])
                if has_doi_references:
                    ent_all_br, ent_all_ra = processor.extract_all_ids(entity, processing_citing)
                    all_br.extend(ent_all_br)
                    all_ra.extend(ent_all_ra)

    processor.prefetch_doi_orcid_index(all_dois_for_orcid_index)
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
    mark_file_completed(cache_path, lock, filename, processing_citing)


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
            in_storage = processor.doi_m.storage_manager.get_value(norm_source_id)

            if not in_storage:
                # If exclude_existing is enabled, skip entities that already exist in Meta
                if processor.exclude_existing:
                    exists_in_meta = processor.BR_redis.exists_as_set(norm_source_id)
                    if exists_in_meta:
                        processor.tmp_doi_m.storage_manager.set_value(norm_source_id, True)
                        continue

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
                                validated_list = processor.to_validated_id_list(norm_id_dict)

                                if norm_id in validated_list:
                                    valid_target_ids.append(norm_id)
                                    # If exclude_existing is enabled, skip metadata creation for entities already in Meta
                                    if processor.exclude_existing:
                                        exists_in_meta = processor.BR_redis.exists_as_set(norm_id)
                                        if exists_in_meta:
                                            continue

                                    cited_entity_dict = {"DOI": norm_id}
                                    target_tab_data = processor.csv_creator(cited_entity_dict)

                                    if target_tab_data:
                                        processed_target_id = target_tab_data.get("id")
                                        if processed_target_id:
                                            cited_entity_rows.append(target_tab_data)
                            elif stored_validity is True:
                                valid_target_ids.append(norm_id)

                    for target_id in valid_target_ids:
                        citation_rows.append({"citing": norm_source_id, "cited": target_id})

    return cited_entity_rows, citation_rows


def preprocess(
    crossref_json_dir: str,
    orcid_doi_filepath: str | None,
    csv_dir: str,
    cache: str | None = None,
    testing: bool = True,
    use_redis: bool = False,
    max_workers: int = 1,
    use_orcid_api: bool = True,
    update_publishers: bool = False,
    publishers_max_age: int = 30,
    exclude_existing: bool = False,
    storage_path: str | None = None,
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

    if use_redis and os.path.exists(publishers_filepath):
        publishers_redis = PublishersRedis(testing=testing)
        if not publishers_redis.has_data() or update_publishers:
            console.print('[cyan]Loading publishers to Redis...[/cyan]')
            publishers_redis.clear()
            load_publishers_to_redis(publishers_filepath, publishers_redis)
            console.print('[green]Publishers loaded to Redis[/green]')
        publishers_filepath = None

    if not os.path.exists(publishers_filepath) if publishers_filepath else False:
        publishers_filepath = None

    if use_redis:
        orcid_index_redis = OrcidIndexRedis(testing=testing)
        if orcid_doi_filepath:
            console.print('[cyan]Updating DOI-ORCID index in Redis...[/cyan]')
            orcid_index_redis.clear()
            load_orcid_index_to_redis(orcid_doi_filepath, orcid_index_redis)
            console.print('[green]DOI-ORCID index updated in Redis[/green]')
        else:
            console.print('[cyan]Using existing DOI-ORCID index from Redis[/cyan]')
        orcid_index_for_processor: str | None = None
    else:
        orcid_index_for_processor = orcid_doi_filepath

    # create output dir for citation data
    preprocessed_citations_dir = csv_dir + "_citations"
    if not os.path.exists(preprocessed_citations_dir):
        os.makedirs(preprocessed_citations_dir)

    console.print(f'[cyan]Getting all files from {crossref_json_dir}[/cyan]')
    all_files, targz_fd = get_all_files_by_type(crossref_json_dir, ".json", cache)
    total_files = len(all_files)
    console.print(f'[cyan]Found {total_files} files to process[/cyan]')

    iteration_args = (
        all_files, targz_fd, preprocessed_citations_dir, csv_dir, orcid_index_for_processor,
        publishers_filepath, testing, cache
    )

    _run_iteration(*iteration_args, processing_citing=True, use_orcid_api=use_orcid_api, max_workers=max_workers, use_redis=use_redis, exclude_existing=exclude_existing, storage_path=storage_path)
    _run_iteration(*iteration_args, processing_citing=False, use_orcid_api=use_orcid_api, max_workers=max_workers, use_redis=use_redis, exclude_existing=exclude_existing, storage_path=storage_path)

    cache_path = cache if cache else os.path.join(os.getcwd(), "cache.json")
    delete_cache_files(cache_path)
    cleanup_testing_storage(testing)


def get_citations_and_metadata(file_name, targz_fd, preprocessed_citations_dir: str, csv_dir: str,
                               orcid_index: str | None,
                               publishers_filepath: str | None,
                               testing: bool, cache: str | None, processing_citing: bool, use_orcid_api: bool,
                               use_redis: bool = False, exclude_existing: bool = False,
                               storage_path: str | None = None) -> bool:
    if isinstance(file_name, tarfile.TarInfo):
        file_name = file_name.name
    cache_path = normalize_cache_path(cache)
    lock = FileLock(cache_path + ".lock")
    cache_dict = init_process_cache(cache_path, lock)

    file_basename = Path(file_name).name
    if is_file_in_cache(cache_dict, file_basename, processing_citing):
        return False

    storage_manager = get_storage_manager(storage_path, testing)
    crossref_csv = CrossrefProcessing(
        orcid_index=orcid_index, publishers_filepath=publishers_filepath,
        storage_manager=storage_manager,
        testing=testing, citing=processing_citing, use_orcid_api=use_orcid_api,
        use_redis_orcid_index=use_redis, use_redis_publishers=use_redis,
        exclude_existing=exclude_existing
    )

    source_data = load_json(file_name, targz_fd)
    if source_data is None:
        return False
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
            crossref_csv, True, cache_path, lock, file_basename
        )
    else:
        cited_entity_rows, citation_rows = _process_cited_entities(crossref_csv, source_dict)
        _save_output_files(
            cited_entity_rows, citation_rows, metadata_output_base, citation_links_output_base,
            crossref_csv, False, cache_path, lock, file_basename
        )

    return True


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
    arg_parser.add_argument('--exclude-existing', dest='exclude_existing', action='store_true', required=False,
                            help='Exclude entities that already exist in Meta from the output CSV (metadata only, '
                                 'not citations). When enabled, checks DB-META-BR before creating metadata rows.')
    arg_parser.add_argument('-s', '--storage_path', dest='storage_path', required=False,
                            help='Path for ID validation storage. Use .db extension for SQLite or .json for '
                                 'in-memory JSON storage. If not specified, uses in-memory storage.')
    arg_parser.add_argument('-r', '--use-redis', dest='use_redis', action='store_true', required=False,
                            help='Use Redis for DOI-ORCID index and publishers lookup. Required for multiprocessing. '
                                 'By default, in-memory storage is used.')

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
    cache = settings['cache_filepath'] if settings else args.cache
    cache = normalize_path(cache) if cache else None
    testing = settings.get('testing', args.testing) if settings else args.testing
    max_workers = settings.get('max_workers', args.max_workers) if settings else args.max_workers
    no_orcid_api = settings.get('disable_orcid_api', args.no_orcid_api) if settings else args.no_orcid_api
    use_orcid_api = not no_orcid_api
    update_publishers = settings.get('update_publishers', args.update_publishers) if settings else args.update_publishers
    publishers_max_age = settings.get('publishers_max_age', args.publishers_max_age) if settings else args.publishers_max_age
    exclude_existing = settings.get('exclude_existing', args.exclude_existing) if settings else args.exclude_existing
    storage_path = settings.get('storage_path', args.storage_path) if settings else args.storage_path
    storage_path = normalize_path(storage_path) if storage_path else None
    use_redis = settings.get('use_redis', args.use_redis) if settings else args.use_redis

    # SQLite and InMemory don't support concurrent access
    if storage_path and max_workers > 1:
        console.print('[yellow]Warning: SQLite/JSON storage requires single-threaded mode. Setting max_workers=1[/yellow]')
        max_workers = 1

    if max_workers > 1 and not use_redis:
        console.print('[yellow]Warning: Multiprocessing requires Redis. Setting max_workers=1[/yellow]')
        max_workers = 1

    if max_workers > 1 and crossref_json_dir.endswith('.tar.gz'):
        arg_parser.error(
            'Multiprocessing (--max_workers > 1) is incompatible with tar.gz input. '
            'Either extract the archive first or use --max_workers 1.'
        )

    preprocess(
        crossref_json_dir=crossref_json_dir,
        orcid_doi_filepath=orcid_doi_filepath,
        csv_dir=csv_dir,
        cache=cache,
        testing=testing,
        use_redis=use_redis,
        max_workers=max_workers,
        use_orcid_api=use_orcid_api,
        update_publishers=update_publishers,
        publishers_max_age=publishers_max_age,
        exclude_existing=exclude_existing,
        storage_path=storage_path,
    )
