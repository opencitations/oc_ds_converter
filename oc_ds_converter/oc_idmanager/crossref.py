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

from oc_ds_converter.oc_idmanager.base import IdentifierManager
from oc_ds_converter.oc_idmanager.oc_data_storage.storage_manager import StorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.in_memory_manager import InMemoryStorageManager
from oc_ds_converter.oc_idmanager.support import call_api
from re import sub, match
from typing import Optional


class CrossrefManager(IdentifierManager):
    """This class implements an identifier manager for Crossref member identifier"""

    def __init__(self, use_api_service=True, storage_manager: Optional[StorageManager] = None):
        """Crossref member ID manager constructor."""
        super(CrossrefManager, self).__init__()
        if storage_manager is None:
            self.storage_manager = InMemoryStorageManager()
        else:
            self.storage_manager = storage_manager
        self._api = "https://api.crossref.org/members/"
        self._api_works_route = r"https://api.openalex.org/works/"
        self._api_sources_route = r"https://api.openalex.org/sources/"
        self._use_api_service = use_api_service
        self._p = "crossref:"
        self._url_id_pref = "https://openalex.org/"

    def is_valid(self, cr_member_id, get_extra_info=False):
        cr_member_id = self.normalise(cr_member_id, include_prefix=True)

        if cr_member_id is None:
            return False
        else:
            id_validation_value = self.storage_manager.get_value(cr_member_id)
            if isinstance(id_validation_value, bool):
                return id_validation_value
            else:
                if get_extra_info:
                    info = self.exists(cr_member_id, get_extra_info=True)
                    self.storage_manager.set_full_value(cr_member_id, info[1])
                    return (info[0] and self.syntax_ok(cr_member_id)), info[1]
                validity_check = self.exists(cr_member_id) and self.syntax_ok(cr_member_id)
                self.storage_manager.set_value(cr_member_id, validity_check)

                return validity_check

    def normalise(self, id_string, include_prefix=False):
        try:
            if id_string.startswith(self._p):
                oal_string = id_string[len(self._p):]
            else:
                oal_string = id_string

            oal_string = sub(r"\D", "", oal_string)

            return "%s%s" % (
                self._p if include_prefix else "",
                oal_string.strip(),
            )
        except:
            # Any error in processing the OpenAlex ID will return None
            return None

    def syntax_ok(self, id_string):

        if not id_string.startswith("crossref:"):
            id_string = self._p + id_string
        return True if match(r"^crossref:\d+$", id_string) else False

    def exists(self, cr_member_id_full, get_extra_info=False, allow_extra_api=None):
        valid_bool = True
        cr_member_id_full = self._p + cr_member_id_full if not cr_member_id_full.startswith(self._p) else cr_member_id_full
        if self._use_api_service:
            cr_member_id = self.normalise(cr_member_id_full) # returns None or unprefixed ID (include_prefix is set to False)
            pref_cr_member_id = self._p + cr_member_id if cr_member_id else None
            if pref_cr_member_id is not None:
                json_res = call_api(url=self._api+cr_member_id, headers=self._headers)
                if json_res:
                    valid_bool = str(json_res['message']['id']) == cr_member_id
                    if get_extra_info:
                        extra_info_result = {'id': pref_cr_member_id, 'valid': valid_bool}
                        return valid_bool, extra_info_result
                    return valid_bool
                valid_bool = False
            else:
                return (False, {'id': None, 'valid': False}) if get_extra_info else False
        return (valid_bool, {'id': cr_member_id_full, 'valid': valid_bool}) if get_extra_info else valid_bool

    def extra_info(self, api_response, choose_api=None, info_dict={}):
        result = {}
        result["valid"] = True
        # to be implemented
        return result
