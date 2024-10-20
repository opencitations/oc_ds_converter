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


from json import loads
from re import match, sub
from time import sleep
from urllib.parse import quote, unquote

from oc_ds_converter.oc_idmanager.base import IdentifierManager
from requests import ReadTimeout, get
from requests.exceptions import ConnectionError

from oc_ds_converter.oc_idmanager.oc_data_storage.storage_manager import StorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.in_memory_manager import InMemoryStorageManager
#from oc_ds_converter.oc_idmanager.oc_data_storage.sqlite_manager import SqliteStorageManager
from typing import Type, Optional


class ViafManager(IdentifierManager):
    """This class implements an identifier manager for VIAF identifier"""

    def __init__(self, use_api_service=True, storage_manager: Optional[StorageManager] = None):
        """VIAF manager constructor."""
        super(ViafManager, self).__init__()
        self._use_api_service = use_api_service
        if storage_manager is None:
            self.storage_manager = InMemoryStorageManager()
        else:
            self.storage_manager = storage_manager

        self._api = "http://www.viaf.org/viaf/"
        self._use_api_service = use_api_service
        self._p = "viaf:"


    def validated_as_id(self, id_string):
        arxiv_vaidation_value = self.storage_manager.get_value(id_string)
        if isinstance(arxiv_vaidation_value, bool):
            return arxiv_vaidation_value
        else:
            return None

    def is_valid(self, viaf_id, get_extra_info=False):
        viaf = self.normalise(viaf_id, include_prefix=True)
        if not viaf:
            return False
        else:
            arxiv_vaidation_value = self.storage_manager.get_value(viaf)
            if isinstance(arxiv_vaidation_value, bool):
                return arxiv_vaidation_value
            else:
                if get_extra_info:
                    info = self.exists(viaf, get_extra_info=True)
                    self.storage_manager.set_full_value(viaf,info[1])
                    return (info[0] and self.syntax_ok(viaf)), info[1]
                validity_check = self.syntax_ok(viaf) and self.exists(viaf) 
                self.storage_manager.set_value(viaf, validity_check)

                return validity_check

    def normalise(self, id_string, include_prefix=False):
        try:
            if id_string.startswith(self._p):
                viaf_string = id_string[len(self._p):]
            else:
                viaf_string = id_string

            viaf_string = sub("\0+", "", sub("[^0-9]", "", unquote(viaf_string)))
            return "%s%s" % (
                self._p if include_prefix else "",
                viaf_string.strip(),
            )
        except:
            # Any error in processing the VIAF will return None
            return None

    def syntax_ok(self, id_string):

        if not id_string.startswith("viaf:"):
            id_string = self._p + id_string
        return True if match(r"^viaf:[1-9]\d{1,21}$", id_string) else False

    def exists(self, viaf_id_full, get_extra_info=False, allow_extra_api=None):
        valid_bool = True
        viaf_id = viaf_id_full
        extra_info_result = {"id": viaf_id}
        if self._use_api_service:
            viaf_id = self.normalise(viaf_id_full)
            extra_info_result = {"id": viaf_id}
            if viaf_id is not None:
                tentative = 3
                while tentative:
                    tentative -= 1
                    try:
                        r = get(self._api + quote(viaf_id) + '/viaf.json', headers=self._headers, timeout=30)
                        if r.status_code == 200:
                            r.encoding = "utf-8"
                            json_res = loads(r.text)
                            if get_extra_info:
                                try:
                                    result = True if json_res['viafID'] == str(viaf_id) else False
                                    extra_info_result["valid"] = result
                                    return result, extra_info_result
                                except KeyError:
                                    extra_info_result["valid"] = False
                                    return False, extra_info_result
                            try:
                                return True if json_res['viafID'] == str(viaf_id) else False
                            except KeyError:
                                return False
                        elif 400 <= r.status_code < 500:
                            if get_extra_info:
                                extra_info_result["valid"] = False
                                return False, extra_info_result
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
                    extra_info_result["valid"] = False
                    return False, extra_info_result
                return False

        if get_extra_info:
            return valid_bool, extra_info_result
        return valid_bool

    def extra_info(self, api_response, choose_api=None, info_dict={}):
        result = {}
        result["valid"] = True
        # to be implemented
        return result
