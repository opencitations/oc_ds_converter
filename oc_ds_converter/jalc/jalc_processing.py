#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright 2022 Arcangelo Massari <arcangelo.massari@unibo.it>
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

from oc_ds_converter.lib.crossref_style_processing import CrossrefStyleProcessing
from oc_ds_converter.oc_idmanager.jid import JIDManager
from oc_ds_converter.oc_idmanager.oc_data_storage.storage_manager import StorageManager


class JalcProcessing(CrossrefStyleProcessing):
    # Publisher prefix mapping is disabled for JALC. Unlike Crossref, which provides
    # a /members API endpoint with authoritative publisher names and their DOI prefixes,
    # JALC has no equivalent endpoint. The JALC /prefixes API only returns prefix, ra,
    # siteId, and updated_date - no publisher names. Since 99.8% of JALC prefixes are
    # not in the Crossref mapping anyway (JALC is a separate DOI registration agency),
    # we use publisher names directly from the source data's publisher_list field.

    def __init__(
        self,
        orcid_index: str | None = None,
        storage_manager: StorageManager | None = None,
        testing: bool = True,
        citing: bool = True,
        exclude_existing: bool = False,
        use_redis_orcid_index: bool = False,
    ):
        super().__init__(
            orcid_index=orcid_index,
            publishers_filepath=None,
            storage_manager=storage_manager,
            testing=testing,
            citing=citing,
            use_redis_orcid_index=use_redis_orcid_index,
            use_redis_publishers=False,
        )
        self.exclude_existing = exclude_existing

        self.jid_m = JIDManager(storage_manager=self.storage_manager, testing=testing)
        self.tmp_jid_m = JIDManager(storage_manager=self.temporary_manager, testing=testing)

        self.venue_id_man_dict["jid"] = self.jid_m
        self.venue_tmp_id_man_dict["jid"] = self.tmp_jid_m

    @classmethod
    def get_ja(cls, field: list) -> list:
        """Select Japanese version of metadata, falling back to English."""
        if all('lang' in item for item in field):
            ja = [item for item in field if item['lang'] == 'ja']
            ja = list(filter(lambda x: x['type'] != 'before' if 'type' in x else x, ja))
            if ja:
                return ja
            en = [item for item in field if item['lang'] == 'en']
            en = list(filter(lambda x: x['type'] != 'before' if 'type' in x else x, en))
            if en:
                return en
        return field

    def _extract_doi(self, item: dict) -> str:
        return item.get("doi", "")

    def _extract_title(self, item: dict) -> str:
        title_list = item.get('title_list')
        if title_list:
            return self.get_ja(title_list)[0].get('title', '')
        return ''

    def _extract_agents(self, item: dict) -> list[dict]:
        authors: list[dict[str, str]] = []
        creator_list = item.get("creator_list")
        if creator_list:
            for creator in creator_list:
                agent: dict[str, str] = {"role": "author"}
                names = creator.get('names', [])
                if names:
                    ja_name = self.get_ja(names)[0]
                    last_name = ja_name.get('last_name', '')
                    first_name = ja_name.get('first_name', '')
                else:
                    last_name = ''
                    first_name = ''
                full_name = ''
                if last_name:
                    full_name += last_name
                    if first_name:
                        full_name += f', {first_name}'
                agent["name"] = full_name
                agent["family"] = last_name
                agent["given"] = first_name
                researcher_id_list = creator.get('researcher_id_list', [])
                for researcher_id in researcher_id_list:
                    if researcher_id.get('type') == 'ORCID' and researcher_id.get('id_code'):
                        agent['orcid'] = researcher_id['id_code']
                        break
                authors.append(agent)
        return authors

    def _extract_venue(self, item: dict) -> str:
        venue_name = ''
        journal_ids: list[str] = []
        if 'journal_title_name_list' in item:
            candidate_venues = self.get_ja(item['journal_title_name_list'])
            if candidate_venues:
                full_venue = [v for v in candidate_venues if v.get('type') == 'full']
                if full_venue:
                    venue_name = full_venue[0].get('journal_title_name', '')
                elif candidate_venues:
                    venue_name = candidate_venues[0].get('journal_title_name', '')
        if 'journal_id_list' in item:
            for v in item['journal_id_list']:
                if isinstance(v, dict):
                    journal_id = v.get("journal_id")
                    id_type = v.get("type")
                    if journal_id and id_type:
                        schema = id_type.lower().strip()
                        if schema in ["issn", "jid"]:
                            tmp_id_man = self.venue_tmp_id_man_dict.get(schema)
                            if tmp_id_man and hasattr(tmp_id_man, 'normalise'):
                                norm_id = getattr(tmp_id_man, 'normalise')(journal_id, include_prefix=True)
                                if norm_id:
                                    journal_ids.append(norm_id)
        return f"{venue_name} [{' '.join(journal_ids)}]" if journal_ids else venue_name

    def _extract_pub_date(self, item: dict) -> str:
        pub_date_dict = item.get('publication_date')
        if not pub_date_dict or not isinstance(pub_date_dict, dict):
            return ''
        pub_date_list: list[str] = []
        year = pub_date_dict.get('publication_year', '')
        if year:
            pub_date_list.append(str(year))
            month = pub_date_dict.get('publication_month', '')
            if month:
                pub_date_list.append(str(month))
                day = pub_date_dict.get('publication_day', '')
                if day:
                    pub_date_list.append(str(day))
        return '-'.join(pub_date_list)

    def _extract_pages(self, item: dict) -> str:
        first_page = item.get('first_page', '')
        last_page = item.get('last_page', '')
        page_list: list[str] = []
        if first_page:
            page_list.append(first_page)
        if last_page:
            page_list.append(last_page)
        return self.get_pages(page_list)

    def _extract_type(self, item: dict) -> str:
        content_type = item.get('content_type')
        if not content_type:
            return ''
        type_map = {
            'JA': 'journal article',
            'BK': 'book',
            'RD': 'dataset',
            'EL': 'other',
            'GD': 'other',
        }
        return type_map.get(content_type, '')

    def _extract_publisher(self, item: dict) -> str:
        if 'publisher_list' in item:
            return self.get_ja(item['publisher_list'])[0].get('publisher_name', '')
        return ''

    def extract_all_ids(self, entity_dict: dict, is_citing: bool) -> tuple[list[str], list[str]]:
        all_br: list[str] = []
        all_ra: list[str] = []

        if not is_citing:
            citation_list = entity_dict.get("data", {}).get("citation_list", [])
            for citation in citation_list:
                doi = citation.get("doi")
                if doi:
                    norm_id = self.doi_m.normalise(doi, include_prefix=True)
                    if norm_id:
                        all_br.append(norm_id)
        return all_br, all_ra
