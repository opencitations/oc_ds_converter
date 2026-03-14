#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright 2019-2020 Fabio Mariani <fabio.mariani555@gmail.com>
# Copyright 2021-2026 Arcangelo Massari <arcangelo.massari@unibo.it>
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

from __future__ import annotations

import re
from collections import defaultdict
from typing import List, Optional, Tuple

from oc_ds_converter.lib.cleaner import Cleaner
from oc_ds_converter.lib.crossref_style_processing import CrossrefStyleProcessing
from oc_ds_converter.lib.master_of_regex import ids_inside_square_brackets, pages_separator
from oc_ds_converter.oc_idmanager.oc_data_storage.storage_manager import StorageManager


class CrossrefProcessing(CrossrefStyleProcessing):

    def __init__(
        self,
        orcid_index: str | None = None,
        publishers_filepath: str | None = None,
        storage_manager: StorageManager | None = None,
        testing: bool = True,
        citing: bool = True,
        use_orcid_api: bool = True,
        use_redis_orcid_index: bool = False,
        use_redis_publishers: bool = False,
        exclude_existing: bool = False,
    ):
        super().__init__(
            orcid_index=orcid_index,
            publishers_filepath=publishers_filepath,
            storage_manager=storage_manager,
            testing=testing,
            citing=citing,
            use_redis_orcid_index=use_redis_orcid_index,
            use_orcid_api=use_orcid_api,
            use_redis_publishers=use_redis_publishers,
        )
        self.exclude_existing = exclude_existing

    def _extract_doi(self, item: dict) -> str:
        doi = item.get('DOI', '')
        if isinstance(doi, list):
            doi = doi[0] if doi else ''
        return str(doi)

    def _extract_title(self, item: dict) -> str:
        title = item.get('title')
        if not title:
            return ''
        if isinstance(title, list):
            title = title[0] if title else ''
        return self.clean_markup(str(title))

    def _extract_agents(self, item: dict) -> list[dict]:
        agents_list: list[dict] = []
        if 'author' in item:
            for author in item['author']:
                agents_list.append({**author, 'role': 'author'})
        if 'editor' in item:
            for editor in item['editor']:
                agents_list.append({**editor, 'role': 'editor'})
        return agents_list

    def _extract_venue(self, item: dict) -> str:
        item_type = self._extract_type(item)
        return self.get_venue_name(item, {'type': item_type})

    def _extract_pub_date(self, item: dict) -> str:
        if 'issued' not in item:
            return ''
        date_parts = item['issued'].get('date-parts', [[]])
        if date_parts and date_parts[0] and date_parts[0][0]:
            return '-'.join([str(y) for y in date_parts[0]])
        return ''

    def _extract_pages(self, item: dict) -> str:
        if 'page' not in item:
            return ''
        pages_list = re.split(pages_separator, item['page'])
        return self.get_pages(pages_list)

    def _extract_type(self, item: dict) -> str:
        item_type = item.get('type', '')
        if item_type:
            return item_type.replace('-', ' ')
        return ''

    def _extract_publisher(self, item: dict) -> str:
        doi = self._extract_doi(item)
        if not doi:
            return ''
        norm_doi = self.doi_m.normalise(doi, include_prefix=False)
        if not norm_doi:
            return ''
        return self.get_publisher_name(norm_doi, item)

    def csv_creator(self, item: dict) -> dict:
        doi = self._extract_doi(item)
        if not doi:
            return {}

        norm_doi = self.doi_m.normalise(doi, include_prefix=False)
        if not norm_doi:
            return {}

        item_type = self._extract_type(item)

        # Build ID (DOI + optional ISBN/ISSN based on type)
        ids_list = [f'doi:{norm_doi}']
        if 'ISBN' in item:
            if item_type in {'book', 'dissertation', 'edited book', 'monograph', 'reference book', 'report', 'standard'}:
                self.id_worker(item['ISBN'], ids_list, self.isbn_worker)
        if 'ISSN' in item:
            if item_type in {'book series', 'book set', 'journal', 'proceedings series', 'series', 'standard series'}:
                self.id_worker(item['ISSN'], ids_list, self.issn_worker)
            elif item_type == 'report series':
                if not item.get('container-title'):
                    self.id_worker(item['ISSN'], ids_list, self.issn_worker)

        agents_list = self._extract_agents(item)
        authors_strings_list, editors_string_list = self.get_agents_strings_list(norm_doi, agents_list)

        row = {
            'id': ' '.join(ids_list),
            'title': self._extract_title(item),
            'author': '; '.join(authors_strings_list),
            'issue': self._extract_issue(item),
            'volume': self._extract_volume(item),
            'venue': self._extract_venue(item),
            'pub_date': self._extract_pub_date(item),
            'page': self._extract_pages(item),
            'type': item_type,
            'publisher': self._extract_publisher(item),
            'editor': '; '.join(editors_string_list)
        }
        return self.normalise_unicode(row)

    def get_crossref_pages(self, item:dict) -> str:
        pages_list = re.split(pages_separator, item['page'])
        return self.get_pages(pages_list)

    def get_publisher_name(self, doi: str, item: dict) -> str:
        publisher = item.get('publisher', '')
        member = item.get('member')
        prefix = item.get('prefix') or doi.split('/')[0]

        if member:
            name = self._get_publisher_name_by_member(str(member))
            if name:
                return f'{name} [crossref:{member}]'

        result = self.get_publisher_by_prefix(prefix)
        if result:
            name, member_id = result
            return f'{name} [crossref:{member_id}]'

        return f'{publisher} [crossref:{member}]' if member else publisher

    def _get_publisher_name_by_member(self, member: str) -> str | None:
        if self.use_redis_publishers and self._publishers_redis:
            pub_data = self._publishers_redis.get_by_member(member)
            if pub_data:
                return str(pub_data['name'])
            return None
        if self.publishers_mapping and member in self.publishers_mapping:
            return str(self.publishers_mapping[member]['name'])
        return None


    def get_venue_name(self, item:dict, row:dict) -> str:
        name_and_id = ''
        if 'container-title' in item:
            if item['container-title']:
                if isinstance(item['container-title'], list):
                    ventit = str(item['container-title'][0])
                else:
                    ventit = str(item['container-title'])
                ventit = self.clean_markup(ventit)
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

    def extract_all_ids(
        self, entity_dict: dict, is_citing: bool
    ) -> tuple[list[str], list[str]]:
        all_br: set[str] = set()
        all_ra: set[str] = set()

        if is_citing:
            # VALIDATE RESPONSIBLE AGENTS IDS FOR THE CITING ENTITY
            if entity_dict.get("author"):
                for author in entity_dict["author"]:
                    if "ORCID" in author:
                        orcid = self.orcid_m.normalise(author["ORCID"], include_prefix=True)
                        if orcid:
                            all_ra.add(orcid)

            if entity_dict.get("editor"):
                for editor in entity_dict["editor"]:
                    if "ORCID" in editor:
                        orcid = self.orcid_m.normalise(editor["ORCID"], include_prefix=True)
                        if orcid:
                            all_ra.add(orcid)

        # RETRIEVE CITED IDS OF A CITING ENTITY
        else:
            citations = [x for x in entity_dict.get("reference", []) if x.get("DOI")]
            for cit in citations:
                norm_id = self.doi_m.normalise(cit["DOI"], include_prefix=True)
                if norm_id:
                    all_br.add(norm_id)

        return list(all_br), list(all_ra)

    def get_agents_strings_list(self, doi: str, agents_list: List[dict]) -> Tuple[list, list]:
        authors_strings_list = []
        editors_string_list = []

        # --- 1) DOI → lookup indice ---
        raw_index = self.orcid_finder(doi) if doi else None

        # --- 2) Parser indice → lista candidati (family, given, orcid normalizzato) ---
        def _split_name(text: str) -> Tuple[str, Optional[str]]:
            base = re.sub(r'\s*\[.*?\]\s*$', '', text).strip()
            if ',' in base:
                fam, giv = [p.strip() for p in base.split(',', 1)]
                return fam, (giv if giv else None)
            toks = base.split()
            if len(toks) >= 2:
                return toks[-1], ' '.join(toks[:-1])
            return base, None

        candidates: List[Tuple[str, Optional[str], str]] = []
        if raw_index:
            for k, v in raw_index.items():
                oc = k if str(k).lower().startswith("orcid:") else f"orcid:{k}"
                oc_norm = self.orcid_m.normalise(oc, include_prefix=True)
                if not oc_norm:
                    continue
                fam, giv = _split_name(v)
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
                for (_, cg, _) in cands
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

        # DOI→ORCID index
        for candidate in (norm_doi, alt_doi):
            if not candidate:
                continue
            raw = self.orcid_finder(candidate)
            if raw:
                found_orcids.update(k.replace("orcid:", "").strip() for k in raw.keys())

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
