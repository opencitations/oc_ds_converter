# SPDX-FileCopyrightText: 2022-2026 Arcangelo Massari <arcangelo.massari@unibo.it>
# SPDX-FileCopyrightText: 2023 Marta Soricetti <marta.soricetti@unibo.it>
#
# SPDX-License-Identifier: ISC

from __future__ import annotations

import html
import re

from oc_ds_converter.lib.crossref_style_processing import CrossrefStyleProcessing
from oc_ds_converter.oc_idmanager.jid import JIDManager
from oc_ds_converter.oc_idmanager.oc_data_storage.storage_manager import StorageManager


# Inline lang prefixes observed in malformed JaLC entries (e.g.
# "金大考古\nen: The Archaeological Journal of Kanazawa University"):
# the non-prefixed line is JaLC's primary language (ja), prefixed lines
# declare their own language. A full scan of the JaLC dump (9.85M records)
# shows only 'en:' appears as an inline prefix in packed-multilang entries;
# all other languages (de, ru, fr, ...) occur exclusively as explicit
# ``lang`` tags on well-formed entries and are handled by ``get_ja``.
_INLINE_LANGS = ('ja', 'en')
_LANG_PREFIX_RE = re.compile(
    rf'^\s*(?P<lang>{"|".join(_INLINE_LANGS)})\s*:\s*(?P<text>.+?)\s*$'
)
_WHITESPACE_RE = re.compile(r'\s+')


def _expand_multilang_entries(entries: list[dict], text_key: str) -> list[dict]:
    """Split lang-less entries whose text packs multiple languages separated
    by newlines (``"<ja>\\n en: <text>"``) into one entry per language. The
    non-prefixed line is tagged as Japanese (JaLC's primary language).
    Entries that already declare ``lang``, or that do not match the
    packed-lang pattern, are returned unchanged. Sibling keys (such as
    ``type``) are preserved on every derived entry.
    """
    out: list[dict] = []
    for entry in entries:
        if 'lang' in entry or text_key not in entry:
            out.append(entry)
            continue
        raw = entry[text_key]
        if not isinstance(raw, str):
            out.append(entry)
            continue
        # Resolve HTML entities up-front: JaLC sources sometimes encode
        # newlines as ``&#10;``, which would otherwise hide the packed-lang
        # structure from the \n split below.
        text = html.unescape(raw)
        if '\n' not in text:
            out.append(entry)
            continue
        lines = [stripped for stripped in (s.strip() for s in text.splitlines()) if stripped]
        if len(lines) < 2:
            out.append(entry)
            continue
        split: list[dict] = []
        for index, line in enumerate(lines):
            match = _LANG_PREFIX_RE.match(line)
            if match:
                split.append({'lang': match['lang'], 'text': _WHITESPACE_RE.sub(' ', match['text']).strip()})
            elif index == 0:
                split.append({'lang': 'ja', 'text': _WHITESPACE_RE.sub(' ', line).strip()})
            else:
                split = []
                break
        if not split:
            out.append(entry)
            continue
        extras = {k: v for k, v in entry.items() if k != text_key}
        for piece in split:
            out.append({**extras, text_key: piece['text'], 'lang': piece['lang']})
    return out


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
            expanded = _expand_multilang_entries(title_list, 'title')
            return self.sanitize_text(self.get_ja(expanded)[0].get('title', ''))
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
                    last_name = self.sanitize_text(ja_name.get('last_name', ''))
                    first_name = self.sanitize_text(ja_name.get('first_name', ''))
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
            expanded = _expand_multilang_entries(
                item['journal_title_name_list'], 'journal_title_name'
            )
            candidate_venues = self.get_ja(expanded)
            if candidate_venues:
                full_venue = [v for v in candidate_venues if v.get('type') == 'full']
                if full_venue:
                    venue_name = self.sanitize_text(full_venue[0].get('journal_title_name', ''))
                elif candidate_venues:
                    venue_name = self.sanitize_text(candidate_venues[0].get('journal_title_name', ''))
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
            return self.sanitize_text(self.get_ja(item['publisher_list'])[0].get('publisher_name', ''))
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
