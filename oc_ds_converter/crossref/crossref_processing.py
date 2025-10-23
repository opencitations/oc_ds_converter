#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright 2019-2020 Fabio Mariani <fabio.mariani555@gmail.com>
# Copyright 2021-2022 Arcangelo Massari <arcangelo.massari@unibo.it>
# Copyright 2023-2025 Arianna Moretti <arianna.moretti4@unibo.it>
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


import html
import re
import warnings
from pathlib import Path
from typing import Optional
import os
import os.path
import json
import csv

from bs4 import BeautifulSoup
from oc_ds_converter.oc_idmanager import DOIManager
from oc_ds_converter.oc_idmanager import ORCIDManager
from oc_ds_converter.oc_idmanager import ISSNManager

from oc_ds_converter.lib.master_of_regex import *
import fakeredis
from oc_ds_converter.datasource.redis import RedisDataSource
from oc_ds_converter.ra_processor import RaProcessor
from oc_ds_converter.oc_idmanager.oc_data_storage.storage_manager import StorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.in_memory_manager import InMemoryStorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.sqlite_manager import SqliteStorageManager
from typing import Dict, List, Tuple
from oc_ds_converter.lib.cleaner import Cleaner
from collections import defaultdict



warnings.filterwarnings("ignore", category=UserWarning, module='bs4')

class CrossrefProcessing(RaProcessor):

    def __init__(self, orcid_index:str=None, doi_csv:str=None, publishers_filepath:str=None, testing: bool = True, storage_manager: Optional[StorageManager] = None, citing: bool = True, use_orcid_api: bool = True):
        super(CrossrefProcessing, self).__init__(orcid_index, doi_csv, publishers_filepath)
        self.citing = citing
        self.use_orcid_api = use_orcid_api

        if storage_manager is None:
            self.storage_manager = SqliteStorageManager()
        else:
            self.storage_manager = storage_manager

        self.temporary_manager = InMemoryStorageManager('../memory.json')

        self.doi_m = DOIManager(storage_manager=self.storage_manager)
        self.orcid_m = ORCIDManager(storage_manager=self.storage_manager, use_api_service=use_orcid_api)
        self.issn_m = ISSNManager()

        self.venue_id_man_dict = {"issn": self.issn_m}
        # Temporary storage managers
        self.tmp_doi_m = DOIManager(storage_manager=self.temporary_manager)
        self.tmp_orcid_m = ORCIDManager(storage_manager=self.temporary_manager, use_api_service=use_orcid_api)

        self.venue_tmp_id_man_dict = {"issn": self.issn_m}

        if testing:
            self.BR_redis = fakeredis.FakeStrictRedis()
            self.RA_redis= fakeredis.FakeStrictRedis()
        else:
            self.BR_redis = RedisDataSource("DB-META-BR")
            self.RA_redis = RedisDataSource("DB-META-RA")

        self._redis_values_br = []
        self._redis_values_ra = []

    def update_redis_values(self, br, ra):
        # normalizza e filtra valori validi CON prefisso
        self._redis_values_br = [
            x for x in (
                self.doi_m.normalise(b, include_prefix=True) for b in (br or [])
            ) if x
        ]
        self._redis_values_ra = [
            x for x in (
                self.orcid_m.normalise(r, include_prefix=True) for r in (ra or [])
            ) if x
        ]

    def to_validated_id_list(self, norm_id_dict):
        valid_id_list = []
        norm_id = norm_id_dict.get("id")
        schema = norm_id_dict.get("schema")

        if schema == "doi":
            if norm_id in self._redis_values_br:
                self.tmp_doi_m.storage_manager.set_value(norm_id, True)
                valid_id_list.append(norm_id)
            elif self.tmp_doi_m.is_valid(norm_id):
                valid_id_list.append(norm_id)

        elif schema == "orcid":
            if norm_id in self._redis_values_ra:
                self.tmp_orcid_m.storage_manager.set_value(norm_id, True)
                valid_id_list.append(norm_id)
            elif not self.use_orcid_api:
                # OFFLINE: non tenta validazione via rete
                pass
            elif self.tmp_orcid_m.is_valid(norm_id):
                valid_id_list.append(norm_id)

        else:
            print("Schema not accepted:", schema, "in", norm_id_dict, ". Use 'orcid' or 'doi'.")

        return valid_id_list

    def memory_to_storage(self):
        kv_in_memory = self.temporary_manager.get_validity_list_of_tuples()
        if kv_in_memory:
            self.storage_manager.set_multi_value(kv_in_memory)
        # come con Datacite svuota sempre la memoria temporanea
        self.temporary_manager.delete_storage()


    def validated_as(self, id_dict):
        schema = id_dict["schema"].strip().lower()
        identifier = id_dict["identifier"]

        if schema == "orcid":
            validity_value = self.tmp_orcid_m.validated_as_id(identifier)
            if validity_value is None:
                validity_value = self.orcid_m.validated_as_id(identifier)
            return validity_value

        elif schema == "doi":
            validity_value = self.tmp_doi_m.validated_as_id(identifier)
            if validity_value is None:
                validity_value = self.doi_m.validated_as_id(identifier)
            return validity_value
        else:
            print("invalid schema in ", id_dict, ". schema should be either doi or orcid.")
            return None


    def get_id_manager(self, schema_or_id, id_man_dict):
        if ":" in schema_or_id:
            split_id_prefix = schema_or_id.split(":")
            schema = split_id_prefix[0]
        else:
            schema = schema_or_id
        id_man = id_man_dict.get(schema)
        return id_man


    def dict_to_cache(self, dict_to_be_saved, path):
        path = Path(path)
        parent_dir_path = path.parent.absolute()
        if not os.path.exists(parent_dir_path):
            Path(parent_dir_path).mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as fd:
            json.dump(dict_to_be_saved, fd, ensure_ascii=False, indent=4)

    def csv_creator(self, item:dict) -> dict:
        row = dict()
        if not 'DOI' in item:
            return row
        doi_manager = DOIManager(use_api_service=False)
        if isinstance(item['DOI'], list):
            doi = doi_manager.normalise(str(item['DOI'][0]), include_prefix=False)
        else:
            doi = doi_manager.normalise(str(item['DOI']), include_prefix=False)
        if (doi and self.doi_set and doi in self.doi_set) or (doi and not self.doi_set):
            # create empty row
            keys = ['id', 'title', 'author', 'pub_date', 'venue', 'volume', 'issue', 'page', 'type',
                    'publisher', 'editor']
            for k in keys:
                row[k] = ''

            if 'type' in item:
                if item['type']:
                    row['type'] = item['type'].replace('-', ' ')

            # row['id']
            ids_list = list()
            ids_list.append(str('doi:' + doi))

            if 'ISBN' in item:
                if row['type'] in {'book', 'dissertation', 'edited book', 'monograph', 'reference book', 'report', 'standard'}:
                    self.id_worker(item['ISBN'], ids_list, self.isbn_worker)

            if 'ISSN' in item:
                if row['type'] in {'book series', 'book set', 'journal', 'proceedings series', 'series', 'standard series'}:
                    self.id_worker(item['ISSN'], ids_list, self.issn_worker)
                elif row['type'] == 'report series':
                    br_id = True
                    if 'container-title' in item:
                        if item['container-title']:
                            br_id = False
                    if br_id:
                        self.id_worker(item['ISSN'], ids_list, self.issn_worker)
            row['id'] = ' '.join(ids_list)

            # row['title']
            if 'title' in item:
                if item['title']:
                    if isinstance(item['title'], list):
                        text_title = item['title'][0]
                    else:
                        text_title = item['title']
                    soup = BeautifulSoup(text_title, 'html.parser')
                    title_soup = soup.get_text().replace('\n', '')
                    title = html.unescape(title_soup)
                    row['title'] = title

            agents_list = []
            if 'author' in item:
                for author in item['author']:
                    author['role'] = 'author'
                agents_list.extend(item['author'])
            if 'editor' in item:
                for editor in item['editor']:
                    editor['role'] = 'editor'
                agents_list.extend(item['editor'])
            authors_strings_list, editors_string_list = self.get_agents_strings_list(doi, agents_list)

            # row['author']
            if 'author' in item:
                row['author'] = '; '.join(authors_strings_list)

            # row['pub_date']
            if 'issued' in item:
                if item['issued']['date-parts'][0][0]:
                    row['pub_date'] = '-'.join([str(y) for y in item['issued']['date-parts'][0]])
                else:
                    row['pub_date'] = ''

            # row['venue']
            row['venue'] = self.get_venue_name(item, row)

            if 'volume' in item:
                row['volume'] = item['volume']
            if 'issue' in item:
                row['issue'] = item['issue']
            if 'page' in item:
                row['page'] = self.get_crossref_pages(item)

            row['publisher'] = self.get_publisher_name(doi, item)

            if 'editor' in item:
                row['editor'] = '; '.join(editors_string_list)
        return self.normalise_unicode(row)

    def get_crossref_pages(self, item:dict) -> str:
        pages_list = re.split(pages_separator, item['page'])
        return self.get_pages(pages_list)

    def get_publisher_name(self, doi:str, item:dict) -> str:
        data = {
            'publisher': '',
            'member': None,
            'prefix': doi.split('/')[0]
        }
        for field in {'publisher', 'member', 'prefix'}:
            if field in item:
                if item[field]:
                    data[field] = item[field]
        publisher = data['publisher']
        member = data['member']
        prefix = data['prefix']
        relevant_member = False

        if self.publishers_mapping and member:
            if member in self.publishers_mapping:
                relevant_member = True
        if self.publishers_mapping:
            if relevant_member:
                name = self.publishers_mapping[member]['name']
                name_and_id = f'{name} [crossref:{member}]'
            else:
                member_dict = next(({member:data} for member, data in self.publishers_mapping.items() if prefix in data['prefixes']), None)
                if member_dict:
                    member = list(member_dict.keys())[0]
                    name_and_id = f"{member_dict[member]['name']} [crossref:{member}]"
                else:
                    name_and_id = publisher
        else:
            name_and_id = f'{publisher} [crossref:{member}]' if member else publisher
        return name_and_id


    def get_venue_name(self, item:dict, row:dict) -> str:
        name_and_id = ''
        if 'container-title' in item:
            if item['container-title']:
                if isinstance(item['container-title'], list):
                    ventit = str(item['container-title'][0]).replace('\n', '')
                else:
                    ventit = str(item['container-title']).replace('\n', '')
                ven_soup = BeautifulSoup(ventit, 'html.parser')
                ventit = html.unescape(ven_soup.get_text())
                ambiguous_brackets = re.search(ids_inside_square_brackets, ventit)
                if ambiguous_brackets:
                    match = ambiguous_brackets.group(1)
                    open_bracket = ventit.find(match) - 1
                    close_bracket = ventit.find(match) + len(match)
                    ventit = ventit[:open_bracket] + '(' + ventit[open_bracket + 1:]
                    ventit = ventit[:close_bracket] + ')' + ventit[close_bracket + 1:]
                venids_list = list()
                if 'ISBN' in item:
                    if row['type'] in {'book chapter', 'book part', 'book section', 'book track', 'reference entry'}:
                        self.id_worker(item['ISBN'], venids_list, self.isbn_worker)

                if 'ISSN' in item:
                    if row['type'] in {'book', 'data file', 'dataset', 'edited book', 'journal article', 'journal volume', 'journal issue', 'monograph', 'proceedings', 'peer review', 'reference book', 'reference entry', 'report'}:
                        self.id_worker(item['ISSN'], venids_list, self.issn_worker)
                    elif row['type'] == 'report series':
                        if 'container-title' in item:
                            if item['container-title']:
                                self.id_worker(item['ISSN'], venids_list, self.issn_worker)
                if venids_list:
                    name_and_id = ventit + ' [' + ' '.join(venids_list) + ']'
                else:
                    name_and_id = ventit
        return name_and_id

    # UPDATED
    def extract_all_ids(self, entity_dict, is_first_iteration: bool):
        all_br = set()
        all_ra = set()

        if is_first_iteration:
            # VALIDATE RESPONSIBLE AGENTS IDS FOR THE CITING ENTITY
            if entity_dict.get("author"):
                for author in entity_dict["author"]:
                    if "ORCID" in author:
                        orcid = self.orcid_m.normalise(author["ORCID"], include_prefix=True)
                        if orcid:
                            all_ra.add(orcid)

            if entity_dict.get("editor"):
                for author in entity_dict["editor"]:
                    if "ORCID" in author:
                        orcid = self.orcid_m.normalise(author["ORCID"], include_prefix=True)
                        if orcid:
                            all_ra.add(orcid)

        # RETRIEVE CITED IDS OF A CITING ENTITY
        else:
            citations = [x for x in entity_dict.get("reference", []) if x.get("DOI")]
            for cit in citations:
                norm_id = self.doi_m.normalise(cit["DOI"], include_prefix=True)
                if norm_id:
                    all_br.add(norm_id)

        all_br = list(all_br)
        all_ra = list(all_ra)
        return all_br, all_ra

    def get_reids_validity_list(self, id_list, redis_db):
        ids = list(id_list)  # garantisci ordine deterministico
        if redis_db == "ra":
            validity = self.RA_redis.mget(ids)
            return [ids[i] for i, v in enumerate(validity) if v]
        elif redis_db == "br":
            validity = self.BR_redis.mget(ids)
            return [ids[i] for i, v in enumerate(validity) if v]
        else:
            raise ValueError("redis_db must be either 'ra' or 'br'")

    def get_agents_strings_list(self, doi: str, agents_list: List[dict]) -> Tuple[list, list]:
        authors_strings_list = []
        editors_string_list = []

        # --- 1) DOI → prova indice in cascata (prefissato, non prefissato, raw) ---
        norm_doi_pref = self.doi_m.normalise(doi, include_prefix=True) if doi else None
        norm_doi_nopref = self.doi_m.normalise(doi, include_prefix=False) if doi else None

        raw_index = None
        for key in (norm_doi_pref, norm_doi_nopref, doi):
            if not key:
                continue
            raw_index = self.orcid_finder(key)
            if raw_index:
                break  # trovato qualcosa nell'indice

        # --- 2) Parser indice → lista candidati (family, given, orcid normalizzato) ---
        def _extract_orcid(text: str) -> Optional[str]:
            if not isinstance(text, str):
                return None
            m = re.search(r'(\d{4}-\d{4}-\d{4}-[0-9]{3}[0-9X])', text)
            return f"orcid:{m.group(1)}" if m else None

        def _split_name(text: str) -> Tuple[str, Optional[str]]:
            if not isinstance(text, str):
                return "", None
            base = re.sub(r'\s*\[.*?\]\s*$', '', text).strip()
            if ',' in base:
                fam, giv = [p.strip() for p in base.split(',', 1)]
                return fam, (giv if giv else None)
            toks = base.split()
            if len(toks) >= 2:
                return toks[-1], ' '.join(toks[:-1])
            return base, None

        candidates: List[Tuple[str, Optional[str], str]] = []
        if isinstance(raw_index, dict):
            for k, v in raw_index.items():
                oc = k if str(k).lower().startswith("orcid:") else f"orcid:{k}"
                oc_norm = self.orcid_m.normalise(oc, include_prefix=True)
                if not oc_norm:
                    continue
                fam, giv = _split_name(v)
                candidates.append((fam.lower(), (giv or "").lower() or None, oc_norm))
        elif isinstance(raw_index, (list, set, tuple)):
            for v in raw_index:
                oc = _extract_orcid(str(v))
                if not oc:
                    continue
                oc_norm = self.orcid_m.normalise(oc, include_prefix=True)
                if not oc_norm:
                    continue
                fam, giv = _split_name(str(v))
                candidates.append((fam.lower(), (giv or "").lower() or None, oc_norm))
        elif isinstance(raw_index, str):
            oc = _extract_orcid(raw_index)
            if oc:
                oc_norm = self.orcid_m.normalise(oc, include_prefix=True)
                if oc_norm:
                    fam, giv = _split_name(raw_index)
                    candidates.append((fam.lower(), (giv or "").lower() or None, oc_norm))

        # --- 3) Pulizia agenti ---
        agents_list = [
            {
                k: Cleaner(v).remove_unwanted_characters()
                if k in {"family", "given", "name"} and v is not None else v
                for k, v in agent_dict.items()
            }
            for agent_dict in agents_list
        ]

        def _format_person(a: dict) -> Tuple[Optional[str], Optional[str], Optional[str]]:
            family = a.get("family")
            given = a.get("given")
            if family and given:
                return family, given, f"{family}, {given}"
            if a.get("name"):
                base = a["name"]
                if "," in base:
                    fam, giv = [p.strip() for p in base.split(",", 1)]
                    return fam, (giv if giv else None), (f"{fam}, {giv}" if giv else fam)
                return base, None, base
            if family:
                return family, given, f"{family}, {given or ''}"
            if given:
                return None, given, f", {given}"
            return None, None, None

        # --- helper locali per normalizzazione e iniziale ---
        def _norm(s: str) -> str:
            return re.sub(r"\s+", " ", (s or "").strip().lower())

        def _initial_from_given(given: Optional[str]) -> str:
            if not given:
                return ""
            first_token = re.split(r"[\s\-]+", given.strip())[0]
            m = re.search(r"[A-Za-z0-9]", first_token)
            return m.group(0).lower() if m else ""

        # --- contatori per disambiguazione *per ruolo* (author/editor) ---
        from collections import defaultdict
        name_counts = defaultdict(int)  # key: (role, fam_norm, given_norm)
        initial_counts = defaultdict(int)  # key: (role, fam_norm, initial)

        for _a in agents_list:
            role_n = (_a.get("role") or "").strip().lower()
            fam_n = _norm(_a.get("family") or "")
            giv_n = _norm(_a.get("given") or "")
            ini_n = _initial_from_given(_a.get("given"))
            name_counts[(role_n, fam_n, giv_n)] += 1
            if fam_n and ini_n:
                initial_counts[(role_n, fam_n, ini_n)] += 1

        # --- insieme di coppie (family_norm, given_norm) presenti nel batch ---
        name_pairs = set()
        for _a in agents_list:
            fam_n = _norm(_a.get("family") or "")
            giv_n = _norm(_a.get("given") or "")
            name_pairs.add((fam_n, giv_n))

        def _match_orcid(fam: Optional[str], giv: Optional[str], role: str) -> Optional[str]:
            if not fam:
                return None

            role_n = (role or "").strip().lower()
            fam_n = _norm(fam)
            giv_n = _norm(giv) if giv else ""
            init = _initial_from_given(giv)

            # filtra candidati indice per family (tollerando containment)
            def fam_ok(cf: str) -> bool:
                cf_n = _norm(cf)
                return cf_n == fam_n or cf_n in fam_n or fam_n in cf_n

            cands = [(cf, cg, oc) for (cf, cg, oc) in candidates if fam_ok(cf)]
            if not cands:
                return None

            # A) omonimi perfetti nello *stesso ruolo* → non apporre tag
            if name_counts.get((role_n, fam_n, giv_n), 0) > 1:
                return None

            # 1) MATCH FORTE: given pieno uguale
            strong = [(cf, cg, oc) for (cf, cg, oc) in cands if cg and giv_n and _norm(cg) == giv_n]
            if len(strong) == 1:
                return strong[0][2]
            elif len(strong) > 1:
                orcids = {oc for (_, _, oc) in strong if oc}
                return orcids.pop() if len(orcids) == 1 else None

            # --- inversion guard ---
            # Se esiste nel batch la coppia invertita (family=cg, given=fam) per un candidato dell'indice,
            # disabilita il fallback per iniziale (evita tagging del falso positivo).
            inversion_present = any(
                cg and (_norm(cg), fam_n) in name_pairs
                for (_cf, cg, _oc) in cands
            )
            if inversion_present:
                return None

            # 2) FALLBACK PER INIZIALE (solo se UNIVOCO *nel medesimo ruolo*)
            if init:
                if initial_counts.get((role_n, fam_n, init), 0) > 1:
                    return None
                cands_init = [(cf, cg, oc) for (cf, cg, oc) in cands if _initial_from_given(cg) == init]
                if len(cands_init) == 1:
                    return cands_init[0][2]
                elif len(cands_init) > 1:
                    orcids = {oc for (_, _, oc) in cands_init if oc}
                    return orcids.pop() if len(orcids) == 1 else None

            return None

        # --- 4) Costruzione liste ---
        for agent in agents_list:
            role = agent.get("role", "")
            fam, giv, display = _format_person(agent)
            if not display:
                continue

            oc = None
            # 1) ORCID nei metadati
            for key in ("orcid", "ORCID"):
                if key in agent and agent[key]:
                    raw = agent[key][0] if isinstance(agent[key], list) else agent[key]
                    oc = self.find_crossref_orcid(raw, doi)
                    break

            # 2) Indice DOI→ORCID
            if not oc and candidates:
                oc = _match_orcid(fam, giv, role)

            # 3) Fallback Redis snapshot (se presente e univoca o coerente con l'indice)
            if not oc and getattr(self, "_redis_values_ra", None):
                for oc_snap in self._redis_values_ra:
                    oc_norm = self.orcid_m.normalise(oc_snap, include_prefix=True)
                    if oc_norm:
                        if len(self._redis_values_ra) == 1 or (
                                raw_index and oc_norm.split(":", 1)[1] in str(raw_index)):
                            oc = oc_norm
                            break

            if oc:
                if not oc.startswith("orcid:"):
                    oc = f"orcid:{oc}"
                display = f"{display} [{oc}]"
                self.tmp_orcid_m.storage_manager.set_value(oc, True)

            if role == "author":
                authors_strings_list.append(display)
            elif role == "editor":
                editors_string_list.append(display)

        return authors_strings_list, editors_string_list

    def find_crossref_orcid(self, identifier, doi):
        if not isinstance(identifier, str):
            return ""

        norm_orcid = self.orcid_m.normalise(identifier, include_prefix=True)
        if not norm_orcid:
            return ""

        validity = self.validated_as({"schema": "orcid", "identifier": norm_orcid})
        if validity is True:
            return norm_orcid
        if validity is False:
            return ""

        norm_doi = self.doi_m.normalise(doi, include_prefix=True) if doi else None
        alt_doi = doi if (doi and not str(doi).lower().startswith("doi:")) else None
        found_orcids = set()

        # DOI→ORCID index (robusto a vari formati)
        for candidate in (norm_doi, alt_doi):
            if not candidate:
                continue
            raw = self.orcid_finder(candidate)
            if isinstance(raw, dict):
                found_orcids.update(k.replace("orcid:", "").strip() for k in raw.keys())
            elif isinstance(raw, (list, set, tuple)):
                for v in raw:
                    m = re.findall(r"(\d{4}-\d{4}-\d{4}-\d{3,4}[0-9X])", str(v))
                    found_orcids.update(m)
            elif isinstance(raw, str):
                m = re.findall(r"(\d{4}-\d{4}-\d{4}-\d{3,4}[0-9X])", raw)
                found_orcids.update(m)

        bare_orcid = norm_orcid.split(":", 1)[1]
        if bare_orcid in found_orcids:
            self.tmp_orcid_m.storage_manager.set_value(norm_orcid, True)
            return norm_orcid

        # API OFF → Redis snapshot fallback
        if not self.use_orcid_api:
            if norm_orcid in self._redis_values_ra:
                self.tmp_orcid_m.storage_manager.set_value(norm_orcid, True)
                return norm_orcid
            return ""

        # API ON → Redis + validazione manager
        norm_id_dict = {"id": norm_orcid, "schema": "orcid"}
        if norm_orcid in self.to_validated_id_list(norm_id_dict):
            return norm_orcid

        return ""
