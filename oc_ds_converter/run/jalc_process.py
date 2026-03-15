#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
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

import json
import os
import sys
import threading
import time
import zipfile
from argparse import ArgumentParser
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Manager, get_context
from multiprocessing.managers import ValueProxy
from pathlib import Path
from typing import Callable

import yaml
from filelock import BaseFileLock, FileLock

from oc_ds_converter.datasource.orcid_index import (
    OrcidIndexRedis,
    load_orcid_index_to_redis,
)
from oc_ds_converter.jalc.jalc_processing import JalcProcessing
from oc_ds_converter.lib.console import advance_progress, console, create_progress
from oc_ds_converter.lib.file_manager import normalize_path, pathoo
from oc_ds_converter.lib.jsonmanager import get_all_files_by_type
from oc_ds_converter.lib.process_utils import (
    cleanup_testing_storage,
    create_output_dirs,
    delete_cache_files,
    get_storage_manager,
    init_process_cache,
    is_file_in_cache,
    mark_file_completed,
    normalize_cache_path,
    write_csv_output,
)


def _count_json_files(
    all_zip_files: list[str],
    cache_path: str | None = None,
    processing_citing: bool = True,
) -> int:
    cached_files: set[str] = set()
    if cache_path:
        normalized_cache = normalize_cache_path(cache_path)
        if os.path.exists(normalized_cache):
            lock = FileLock(normalized_cache + ".lock")
            cache_dict = init_process_cache(normalized_cache, lock)
            key = "citing" if processing_citing else "cited"
            cached_files = set(cache_dict.get(key, []))

    total = 0
    for zip_path in all_zip_files:
        filename = Path(zip_path).name
        if filename in cached_files:
            continue
        with zipfile.ZipFile(zip_path) as zf:
            total += sum(1 for x in zf.namelist() if "doiList" not in x and not x.endswith("/"))
    return total


def _run_iteration(
    all_files: list[str],
    preprocessed_citations_dir: str,
    csv_dir: str,
    orcid_index_filepath: str | None,
    testing: bool,
    cache: str | None,
    processing_citing: bool,
    max_workers: int = 1,
    exclude_existing: bool = False,
    storage_path: str | None = None,
    use_redis: bool = False,
) -> None:
    iteration_label = "citing entities" if processing_citing else "cited entities"
    iteration_num = "First" if processing_citing else "Second"

    if max_workers == 1:
        with create_progress() as progress:
            task = progress.add_task(f"[green]{iteration_num} iteration ({iteration_label})", total=len(all_files))
            for zip_file in all_files:
                was_processed = get_citations_and_metadata(
                    zip_file=zip_file,
                    preprocessed_citations_dir=preprocessed_citations_dir,
                    csv_dir=csv_dir,
                    orcid_index_filepath=orcid_index_filepath,
                    testing=testing,
                    cache=cache,
                    processing_citing=processing_citing,
                    exclude_existing=exclude_existing,
                    storage_path=storage_path,
                    use_redis=use_redis,
                )
                advance_progress(progress, task, processed=was_processed)
    else:
        console.print(f'[cyan]Counting JSON files for {iteration_label}...[/cyan]')
        total_json_files = _count_json_files(all_files, cache, processing_citing)
        console.print(f'[cyan]Found {total_json_files} JSON files to process[/cyan]')

        manager = Manager()
        entity_counter: ValueProxy[int] = manager.Value('i', 0)
        stop_event = threading.Event()

        with create_progress() as progress:
            entity_task = progress.add_task(
                f"[green]{iteration_num} iteration ({iteration_label}) - JSON files",
                total=total_json_files,
            )

            def update_progress() -> None:
                last_value = 0
                while not stop_event.is_set():
                    current = entity_counter.value
                    if current != last_value:
                        progress.update(entity_task, completed=current)
                        last_value = current
                    time.sleep(0.2)
                progress.update(entity_task, completed=entity_counter.value)

            updater_thread = threading.Thread(target=update_progress, daemon=True)
            updater_thread.start()

            with ProcessPoolExecutor(max_workers=max_workers, mp_context=get_context('spawn')) as executor:
                futures = []
                for zip_file in all_files:
                    future = executor.submit(
                        get_citations_and_metadata,
                        zip_file=zip_file,
                        preprocessed_citations_dir=preprocessed_citations_dir,
                        csv_dir=csv_dir,
                        orcid_index_filepath=orcid_index_filepath,
                        testing=testing,
                        cache=cache,
                        processing_citing=processing_citing,
                        exclude_existing=exclude_existing,
                        storage_path=storage_path,
                        use_redis=use_redis,
                        entity_counter=entity_counter,
                    )
                    futures.append(future)
                for future in futures:
                    try:
                        future.result()
                    except Exception as e:
                        console.print(f'[red]Error: {e}[/red]')
                        raise

            stop_event.set()
            updater_thread.join(timeout=1.0)

        console.print(f'[green]{iteration_num} iteration complete ({entity_counter.value} JSON files processed)[/green]')


def _extract_redis_ids_and_update(
    processor: JalcProcessing,
    entity_list: list[dict],
    processing_citing: bool,
) -> None:
    all_br: list[str] = []
    all_ra: list[str] = []
    all_dois_for_orcid_index: list[str] = []

    for entity in entity_list:
        if entity:
            d = entity["data"]
            doi = d.get("doi")
            if doi:
                all_dois_for_orcid_index.append(doi)
            if d.get("citation_list"):
                cit_list = d["citation_list"]
                cit_list_doi = [x for x in cit_list if x.get("doi")]
                if cit_list_doi:
                    ent_all_br, ent_all_ra = processor.extract_all_ids(entity, processing_citing)
                    all_br.extend(ent_all_br)
                    all_ra.extend(ent_all_ra)

    processor.prefetch_doi_orcid_index(all_dois_for_orcid_index)
    redis_validity_values_br = processor.get_redis_validity_list(all_br, "br")
    redis_validity_values_ra = processor.get_redis_validity_list(all_ra, "ra")
    processor.update_redis_values(redis_validity_values_br, redis_validity_values_ra)


def _save_output_files(
    entity_rows: list[dict[str, str]],
    citation_rows: list[dict[str, str]],
    metadata_output_base: str,
    citation_links_output_base: str,
    processor: JalcProcessing,
    processing_citing: bool,
    cache_path: str,
    lock: BaseFileLock,
    filename: str,
) -> None:
    if entity_rows:
        suffix = "_citing.csv" if processing_citing else "_cited.csv"
        filepath = metadata_output_base + suffix
        write_csv_output(filepath, entity_rows)
    processor.memory_to_storage()

    if not processing_citing and citation_rows:
        filepath = citation_links_output_base + ".csv"
        write_csv_output(filepath, citation_rows)

    processor.memory_to_storage()
    mark_file_completed(cache_path, lock, filename, processing_citing)


def _process_citing_entities(
    processor: JalcProcessing,
    source_dict: list[dict],
    on_entity_processed: Callable[[], None] | None = None,
) -> list[dict[str, str]]:
    citing_entity_rows: list[dict[str, str]] = []

    for entity in source_dict:
        if entity:
            d = entity.get("data")
            if not d:
                if on_entity_processed:
                    on_entity_processed()
                continue
            norm_source_id = processor.doi_m.normalise(d['doi'], include_prefix=True)

            if norm_source_id and not processor.doi_m.storage_manager.get_value(norm_source_id):
                if processor.exclude_existing and processor.BR_redis.exists_as_set(norm_source_id):
                    processor.tmp_doi_m.storage_manager.set_value(norm_source_id, True)
                else:
                    processor.tmp_doi_m.storage_manager.set_value(norm_source_id, True)
                    source_tab_data = processor.csv_creator(d)
                    if source_tab_data:
                        processed_source_id = source_tab_data["id"]
                        if processed_source_id:
                            citing_entity_rows.append(source_tab_data)
        if on_entity_processed:
            on_entity_processed()

    return citing_entity_rows


def _process_cited_entities(
    processor: JalcProcessing,
    source_dict: list[dict],
    on_entity_processed: Callable[[], None] | None = None,
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    cited_entity_rows: list[dict[str, str]] = []
    citation_rows: list[dict[str, str]] = []

    for entity in source_dict:
        if entity:
            d = entity.get("data")
            if not d or not d.get("citation_list"):
                if on_entity_processed:
                    on_entity_processed()
                continue
            norm_source_id = processor.doi_m.normalise(d['doi'], include_prefix=True)
            if not norm_source_id:
                if on_entity_processed:
                    on_entity_processed()
                continue

            cit_list_entities = [x for x in d["citation_list"] if x.get("doi")]
            if not cit_list_entities:
                if on_entity_processed:
                    on_entity_processed()
                continue

            valid_target_ids: list[str] = []
            for cited_entity in cit_list_entities:
                norm_id = processor.doi_m.normalise(cited_entity["doi"], include_prefix=True)
                if not norm_id:
                    continue

                stored_validity = processor.validated_as({"schema": "doi", "identifier": norm_id})

                if stored_validity is None:
                    if norm_id in processor.to_validated_id_list({"id": norm_id, "schema": "doi"}):
                        valid_target_ids.append(norm_id)
                        if processor.exclude_existing and processor.BR_redis.exists_as_set(norm_id):
                            continue
                        target_tab_data = processor.csv_creator(cited_entity)
                        if target_tab_data:
                            processed_target_id = target_tab_data.get("id")
                            if processed_target_id:
                                cited_entity_rows.append(target_tab_data)
                elif stored_validity is True:
                    valid_target_ids.append(norm_id)

            for target_id in valid_target_ids:
                citation_rows.append({"citing": norm_source_id, "cited": target_id})
        if on_entity_processed:
            on_entity_processed()

    return cited_entity_rows, citation_rows


def preprocess(
    jalc_json_dir: str,
    orcid_doi_filepath: str | None,
    csv_dir: str,
    cache: str | None = None,
    testing: bool = True,
    use_redis: bool = False,
    max_workers: int = 1,
    exclude_existing: bool = False,
    storage_path: str | None = None,
) -> None:
    preprocessed_citations_dir = create_output_dirs(csv_dir)

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

    console.print(f'[cyan]Getting all files from {jalc_json_dir}[/cyan]')
    all_input_zip_raw, _ = get_all_files_by_type(jalc_json_dir, ".zip", cache)
    all_input_zip: list[str] = [f for f in all_input_zip_raw if isinstance(f, str)]
    console.print(f'[cyan]Found {len(all_input_zip)} ZIP files to process[/cyan]')

    iteration_args = (
        all_input_zip, preprocessed_citations_dir, csv_dir,
        orcid_index_for_processor, testing, cache
    )

    _run_iteration(*iteration_args, processing_citing=True, max_workers=max_workers,
                   exclude_existing=exclude_existing, storage_path=storage_path,
                   use_redis=use_redis)
    _run_iteration(*iteration_args, processing_citing=False, max_workers=max_workers,
                   exclude_existing=exclude_existing, storage_path=storage_path,
                   use_redis=use_redis)

    cache_path = cache if cache else os.path.join(os.getcwd(), "cache.json")
    delete_cache_files(cache_path)
    cleanup_testing_storage(testing)


def get_citations_and_metadata(
    zip_file: str,
    preprocessed_citations_dir: str,
    csv_dir: str,
    orcid_index_filepath: str | None,
    testing: bool,
    cache: str | None,
    processing_citing: bool,
    exclude_existing: bool = False,
    storage_path: str | None = None,
    use_redis: bool = False,
    entity_counter: ValueProxy[int] | None = None,
) -> bool:
    cache_path = normalize_cache_path(cache)
    lock = FileLock(cache_path + ".lock")
    cache_dict = init_process_cache(cache_path, lock)

    filename = Path(zip_file).name
    if is_file_in_cache(cache_dict, filename, processing_citing):
        return False

    storage_manager = get_storage_manager(storage_path, testing)
    jalc_csv = JalcProcessing(
        orcid_index=orcid_index_filepath,
        storage_manager=storage_manager,
        testing=testing,
        citing=processing_citing,
        exclude_existing=exclude_existing,
        use_redis_orcid_index=use_redis,
    )

    zip_f = zipfile.ZipFile(zip_file)
    source_data = [x for x in zip_f.namelist() if "doiList" not in x and not x.endswith("/")]
    source_dict: list[dict] = []
    for json_file in source_data:
        f = zip_f.open(json_file, 'r')
        my_dict = json.load(f)
        source_dict.append(my_dict)

    filename_without_ext = filename.replace('.zip', '')
    filepath = os.path.join(csv_dir, f'{filename_without_ext}.csv')
    pathoo(filepath)

    metadata_output_base = os.path.join(csv_dir, filename_without_ext)
    citation_links_output_base = os.path.join(preprocessed_citations_dir, filename_without_ext)

    filepath_citations = os.path.join(preprocessed_citations_dir, f'{filename_without_ext}.csv')
    pathoo(filepath_citations)

    _extract_redis_ids_and_update(jalc_csv, source_dict, processing_citing)

    def increment_counter() -> None:
        if entity_counter is not None:
            entity_counter.value += 1

    if processing_citing:
        citing_entity_rows = _process_citing_entities(jalc_csv, source_dict, increment_counter)
        _save_output_files(
            citing_entity_rows, [], metadata_output_base, citation_links_output_base,
            jalc_csv, True, cache_path, lock, filename
        )
    else:
        cited_entity_rows, citation_rows = _process_cited_entities(jalc_csv, source_dict, increment_counter)
        _save_output_files(
            cited_entity_rows, citation_rows, metadata_output_base, citation_links_output_base,
            jalc_csv, False, cache_path, lock, filename
        )

    return True


if __name__ == '__main__':  # pragma: no cover
    arg_parser = ArgumentParser(
        'jalc_process.py',
        description='This script creates CSV files from JALC original dump, '
                    'enriching data through of a DOI-ORCID index'
    )
    arg_parser.add_argument('-c', '--config', dest='config', required=False,
                            help='Configuration file path')
    required = not any(arg in sys.argv for arg in {'--config', '-c'})
    arg_parser.add_argument('-ja', '--jalc', dest='jalc_json_dir', required=required,
                            help='Jalc json files directory')
    arg_parser.add_argument('-out', '--output', dest='csv_dir', required=required,
                            help='Directory where CSV will be stored')
    arg_parser.add_argument('-o', '--orcid', dest='orcid_doi_filepath', required=False,
                            help='DOI-ORCID index filepath, to enrich data')
    arg_parser.add_argument('-ca', '--cache', dest='cache', required=False,
                            help='Path to a JSON file for caching processed files. Tracks which files have been '
                                 'processed to allow resuming. Deleted at the end of successful processing.')
    arg_parser.add_argument('-t', '--testing', dest='testing', action='store_true', required=False,
                            help='Run in testing mode: uses in-memory FakeRedis instead of real Redis, '
                                 'and cleans up storage at the end. Use this flag for tests only.')
    arg_parser.add_argument('-m', '--max_workers', dest='max_workers', required=False, default=1, type=int,
                            help='Workers number')
    arg_parser.add_argument('--exclude-existing', dest='exclude_existing', action='store_true', required=False,
                            help='Exclude entities that already exist in Meta from the output CSV')
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
    jalc_json_dir = settings['jalc_json_dir'] if settings else args.jalc_json_dir
    jalc_json_dir = normalize_path(jalc_json_dir)
    csv_dir = settings['output'] if settings else args.csv_dir
    csv_dir = normalize_path(csv_dir)
    orcid_doi_filepath = settings['orcid_doi_filepath'] if settings else args.orcid_doi_filepath
    orcid_doi_filepath = normalize_path(orcid_doi_filepath) if orcid_doi_filepath else None
    cache = settings['cache_filepath'] if settings else args.cache
    cache = normalize_path(cache) if cache else None
    testing = settings.get('testing', args.testing) if settings else args.testing
    max_workers = settings.get('max_workers', args.max_workers) if settings else args.max_workers
    exclude_existing = settings.get('exclude_existing', args.exclude_existing) if settings else args.exclude_existing
    storage_path = settings.get('storage_path', args.storage_path) if settings else args.storage_path
    storage_path = normalize_path(storage_path) if storage_path else None
    use_redis = settings.get('use_redis', args.use_redis) if settings else args.use_redis

    if storage_path and max_workers > 1:
        console.print('[yellow]Warning: SQLite/JSON storage requires single-threaded mode. Setting max_workers=1[/yellow]')
        max_workers = 1

    if max_workers > 1 and not use_redis:
        console.print('[yellow]Warning: Multiprocessing requires Redis. Setting max_workers=1[/yellow]')
        max_workers = 1

    preprocess(
        jalc_json_dir=jalc_json_dir,
        orcid_doi_filepath=orcid_doi_filepath,
        csv_dir=csv_dir,
        cache=cache,
        testing=testing,
        use_redis=use_redis,
        max_workers=max_workers,
        exclude_existing=exclude_existing,
        storage_path=storage_path,
    )
