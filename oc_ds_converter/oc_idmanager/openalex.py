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
from re import sub, match
from requests import ReadTimeout, get
from requests.exceptions import ConnectionError
from json import loads
from time import sleep
from typing import Optional


class OpenAlexManager(IdentifierManager):
    """This class implements an identifier manager for openalex identifier"""

    def __init__(self, use_api_service=True, storage_manager: Optional[StorageManager] = None):
        """OpenAlex manager constructor."""
        super(OpenAlexManager, self).__init__()
        if storage_manager is None:
            self.storage_manager = InMemoryStorageManager()
        else:
            self.storage_manager = storage_manager
        self._api = "https://api.openalex.org/"
        self._api_works_route = r"https://api.openalex.org/works/"
        self._api_sources_route = r"https://api.openalex.org/sources/"
        self._use_api_service = use_api_service
        self._p = "openalex:"
        self._url_id_pref = "https://openalex.org/"
        self._headers = {
            "User-Agent": "Identifier Manager / OpenCitations Indexes "
                          "(http://opencitations.net; mailto:contact@opencitations.net)"
        }

    def is_valid(self, oal_id, get_extra_info=False):
        oal_id = self.normalise(oal_id, include_prefix=True)

        if oal_id is None:
            return False
        else:
            id_validation_value = self.storage_manager.get_value(oal_id)
            if isinstance(id_validation_value, bool):
                return id_validation_value
            else:
                if get_extra_info:
                    info = self.exists(oal_id, get_extra_info=True)
                    self.storage_manager.set_full_value(oal_id,info[1])
                    return (info[0] and self.syntax_ok(oal_id)), info[1]
                validity_check = self.syntax_ok(oal_id) and self.exists(oal_id) 
                self.storage_manager.set_value(oal_id, validity_check)

                return validity_check

    def normalise(self, id_string, include_prefix=False):
        try:
            if id_string.startswith(self._p):
                oal_string = id_string[len(self._p):]
            else:
                oal_string = id_string

            oal_string = sub(r"\0+", "", (sub(r"\s+", "", oal_string)))

            oal_string = oal_string.replace(self._api_works_route, '', 1)
            oal_string = oal_string.replace(self._api_sources_route, '', 1)
            oal_string = oal_string.replace(self._api, '', 1)
            oal_string = oal_string.replace(self._url_id_pref, '', 1)

            oal_string = oal_string.upper()
            return "%s%s" % (
                self._p if include_prefix else "",
                oal_string.strip(),
            )
        except:
            # Any error in processing the OpenAlex ID will return None
            return None

    def syntax_ok(self, id_string):

        if not id_string.startswith("openalex:"):
            id_string = self._p + id_string
        return True if match("^openalex:[WS][1-9]\\d*$", id_string) else False

    def exists(self, openalex_id_full, get_extra_info=False, allow_extra_api=None):
        valid_bool = True
        openalex_id_full = self._p + openalex_id_full if not openalex_id_full.startswith(self._p) else openalex_id_full
        if self._use_api_service:
            oal_id = self.normalise(openalex_id_full) # returns None or unprefixed ID (include_prefix is set to False)
            pref_oalid = self._p + oal_id if oal_id else None
            if pref_oalid is not None:
                tentative = 3
                while tentative:
                    tentative -= 1
                    try:
                        r = get(self._api + oal_id, headers=self._headers, timeout=30)
                        if r.status_code == 200:
                            r.encoding = "utf-8"
                            json_res = loads(r.text)
                            if get_extra_info:
                                extra_info_result = {'id': pref_oalid}
                                try:
                                    result = True if json_res['id'] == (self._url_id_pref + oal_id) else False
                                    extra_info_result['valid'] = result
                                    return result, extra_info_result
                                except KeyError:
                                    extra_info_result['valid'] = False
                                    return False, extra_info_result
                            try:
                                return True if json_res['id'] == (self._url_id_pref + oal_id) else False
                            except KeyError:
                                return False
                        if r.status_code == 429:
                            sleep(1)  # only handles per-second rate limits (not per-day rate limits)
                        elif 400 <= r.status_code < 500:
                            if get_extra_info:
                                return False, {'id': pref_oalid, 'valid': False}
                            return False
                    except ReadTimeout:
                        # Do nothing, just try again
                        pass
                    except ConnectionError:
                        # Sleep 5 seconds, then try again
                        sleep(5)
                valid_bool = False
            else:
                if get_extra_info:
                    return False, {'id': pref_oalid, 'valid': False}
                return False

        if get_extra_info:
            return valid_bool, {'id': openalex_id_full, 'valid': valid_bool}
        return valid_bool

    def extra_info(self, api_response, choose_api=None, info_dict={}):
        result = {}
        result["valid"] = True
        # to be implemented
        return result
