#!python
# Copyright 2019, Silvio Peroni <essepuntato@gmail.com>
# Copyright 2022, Giuseppe Grieco <giuseppe.grieco3@unibo.it>, Arianna Moretti <arianna.moretti4@unibo.it>, Elia Rizzetto <elia.rizzetto@studio.unibo.it>, Arcangelo Massari <arcangelo.massari@unibo.it>
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.


from __future__ import annotations

import re
from re import match, sub
from urllib.parse import quote, unquote

from oc_ds_converter.oc_idmanager.base import IdentifierManager
from oc_ds_converter.oc_idmanager.isbn import ISBNManager
from oc_ds_converter.oc_idmanager.issn import ISSNManager
from oc_ds_converter.oc_idmanager.orcid import ORCIDManager
from oc_ds_converter.oc_idmanager.support import call_api

from oc_ds_converter.metadata_manager import MetadataManager
from oc_ds_converter.oc_idmanager.oc_data_storage.storage_manager import StorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.in_memory_manager import InMemoryStorageManager
# from oc_ds_converter.oc_idmanager.oc_data_storage.sqlite_manager import SqliteStorageManager
from typing import Optional, Type, Tuple


class DOIManager(IdentifierManager):
    """This class implements an identifier manager for doi identifier"""

    def __init__(self, use_api_service=True, storage_manager:Optional[StorageManager] = None):
        """DOI manager constructor."""
        super(DOIManager,self).__init__()
        if storage_manager is None:
            self.storage_manager = InMemoryStorageManager()
        else:
            self.storage_manager = storage_manager

        self._api = "https://doi.org/api/handles/"
        self._api_airiti = ""
        self._api_cnki = ""
        self._api_crossref = "https://api.crossref.org/works/"
        self._api_datacite = "https://api.datacite.org/dois/"
        self._api_istic = ""
        self._api_jalc = "https://api.japanlinkcenter.org/dois/"
        self._api_kisti = ""
        self._api_medra = "https://api.medra.org/metadata/"
        self._api_op = ""
        self._api_public = ""
        self._api_unknown = "https://doi.org/ra/"
        self._use_api_service = use_api_service
        self._p = "doi:"
        self._issnm = ISSNManager()
        self._isbnm = ISBNManager()
        self._om = ORCIDManager()

        # ISC License (ISC)
        # ==================================
        # Copyright 2021 Arcangelo Massari, Cristian Santini, Ricarda Boente, Deniz Tural

        # Permission to use, copy, modify, and/or distribute this software for any purpose with or
        # without fee is hereby granted, provided that the above copyright notice and this permission
        # notice appear in all copies.

        # THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS
        # SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL
        # THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
        # WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE
        # OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

        prefix_dx = r"HTTP:\/\/DX\.D[0|O]I\.[0|O]RG\/"
        prefix_doi = r"HTTPS:\/\/D[0|O]I\.[0|O]RG\/"
        suffix_dcsupplemental = r"\/-\/DCSUPPLEMENTAL"
        suffix_suppinfo = r"SUPPINF[0|O](\.)?"
        suffix_pmid1 = r"[\.|\(|,|;]?PMID:\d+.*?"
        suffix_pmid2 = r"[\.|\(|,|;]?PMCID:PMC\d+.*?"
        suffix_epub = r"[\(|\[]EPUBAHEADOFPRINT[\)\]]"
        suffix_published_online = r"[\.|\(|,|;]?ARTICLEPUBLISHEDONLINE.*?\d{4}"
        suffix_http = r"[\.|\(|,|;]*HTTP:\/\/.*?"
        suffix_subcontent = r"\/(META|ABSTRACT|FULL|EPDF|PDF|SUMMARY)([>|\)](LAST)?ACCESSED\d+)?"
        suffix_accessed = r"[>|\)](LAST)?ACCESSED\d+"
        suffix_sagepub = r"[\.|\(|,|;]?[A-Z]*\.?SAGEPUB.*?"
        suffix_dotted_line = r"\.{5}.*?"
        suffix_delimiters = r"[\.|,|<|&|\(|;]+"
        suffix_doi_mark = r"\[DOI\].*?"
        suffix_year = r"\(\d{4}\)?"
        suffix_query = r"\?.*?=.*?"
        suffix_hash = r"#.*?"

        self.suffix_regex_lst = [suffix_dcsupplemental, suffix_suppinfo, suffix_pmid1, suffix_pmid2, suffix_epub,
                            suffix_published_online, suffix_http, suffix_subcontent, suffix_accessed, suffix_sagepub,
                            suffix_dotted_line, suffix_delimiters, suffix_doi_mark, suffix_year,
                            suffix_query, suffix_hash]
        self.prefix_regex_lst = [prefix_dx, prefix_doi]
        self.prefix_regex = r"(.*?)(?:\.)?(?:" + "|".join(self.prefix_regex_lst) + r")(.*)"
        self.suffix_regex = r"(.*?)(?:" + "|".join(self.suffix_regex_lst) + r")$"

    def validated_as_id(self, id_string):
        doi_vaidation_value = self.storage_manager.get_value(id_string)
        if isinstance(doi_vaidation_value, bool):
            return doi_vaidation_value
        else:
            return None

    def is_valid(self, id_string, get_extra_info=False):
        doi = self.normalise(id_string, include_prefix=True)
        if doi is None:
            return False
        else:
            doi_vaidation_value = self.storage_manager.get_value(doi)
            if isinstance(doi_vaidation_value, bool):
                return doi_vaidation_value
            else:
                if get_extra_info:
                    info = self.exists(doi, get_extra_info=True)
                    self.storage_manager.set_full_value(doi,info[1])
                    return (info[0] and self.syntax_ok(doi)), info[1]
                validity_check = self.syntax_ok(doi) and self.exists(doi) 
                self.storage_manager.set_value(doi, validity_check)
                return validity_check

    def base_normalise(self, id_string):
        try:
            id_string = sub(
                r"\0+", "", sub(r"\s+", "", unquote(id_string[id_string.index("10.") :]))
            )
            return id_string.lower().strip() if id_string else None
        except:
            # Any error in processing the DOI will return None
            return None

    def normalise(self, id_string, include_prefix=False):
        try:
            id_string = self.clean_doi(id_string)[0]
            return "%s%s" % (
                self._p if include_prefix else "",
                id_string.lower().strip(),
            ) if id_string else None

        except:
            # Any error in processing the DOI will return None
            return None

    def clean_doi(self, doi:str) -> Tuple[str, dict]:
        # ISC License (ISC)
        # ==================================
        # Copyright 2021 Arcangelo Massari, Cristian Santini, Ricarda Boente, Deniz Tural

        # Permission to use, copy, modify, and/or distribute this software for any purpose with or
        # without fee is hereby granted, provided that the above copyright notice and this permission
        # notice appear in all copies.

        # THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS
        # SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL
        # THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
        # WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE
        # OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
        doi = self.base_normalise(doi)
        tmp_doi = doi.replace(" ", "")
        prefix_match = re.search(self.prefix_regex, tmp_doi, re.IGNORECASE)
        classes_of_errors = {
            "prefix": 0,
            "suffix": 0,
            "other-type": 0
        }
        if prefix_match:
            tmp_doi = prefix_match.group(1)
            classes_of_errors["prefix"] = 1
        suffix_match = re.search(self.suffix_regex, tmp_doi, re.IGNORECASE)
        if suffix_match:
            tmp_doi = suffix_match.group(1)
            classes_of_errors["suffix"] = 1
        new_doi = re.sub("\\\\", "", tmp_doi)
        new_doi = re.sub("__", "_", new_doi)
        new_doi = re.sub("\\.\\.", ".", new_doi)
        new_doi = re.sub("<.*?>.*?</.*?>", "", new_doi)
        new_doi = re.sub("<.*?/>", "", new_doi)
        if new_doi != tmp_doi:
            classes_of_errors["other-type"] = 1
        return new_doi, classes_of_errors

    def syntax_ok(self, id_string):
        if not id_string.startswith(self._p):
            id_string = self._p+id_string
        return True if match(r"^doi:10\.(\d{4,9}|[^\s/]+(\.[^\s/]+)*)/[^\s]+$", id_string, re.IGNORECASE) else False

    def exists(self, doi_full, get_extra_info=False, allow_extra_api=None):
        valid_bool = True
        doi = doi_full
        if self._use_api_service:
            doi = self.normalise(doi_full)
            if doi:
                json_res = call_api(url=self._api + quote(doi), headers=self._headers)
                if json_res:
                    valid_bool = json_res.get("responseCode") == 1
                    if get_extra_info:
                        extra_info = {'id': doi, 'valid': valid_bool, 'ra': 'unknown'}
                        if allow_extra_api is None:
                            return valid_bool, extra_info
                        elif valid_bool is True and allow_extra_api:
                            r_format = "xml" if allow_extra_api == "medra" else "json"
                            extra_api_result = call_api(url=getattr(self, f'_api_{allow_extra_api}') + quote(doi), headers=self._headers, r_format=r_format)
                            if extra_api_result:
                                metadata_manager = MetadataManager(allow_extra_api, json_res)
                                extra_info.update(metadata_manager.extract_metadata())
                                return valid_bool, extra_info
                            else:
                                return valid_bool, {'id': doi, 'valid': valid_bool, 'ra': 'unknown'}
                    return valid_bool
                valid_bool = False
            else:
                return (False, {'id': None, 'valid': False, 'ra': 'unknown'}) if get_extra_info else False
        return (valid_bool, {'id': doi, 'valid': valid_bool, 'ra': 'unknown'}) if get_extra_info else valid_bool
