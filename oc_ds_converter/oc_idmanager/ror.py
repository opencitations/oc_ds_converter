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
from oc_ds_converter.oc_idmanager.oc_data_storage.storage_manager import StorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.in_memory_manager import InMemoryStorageManager
from oc_ds_converter.oc_idmanager.base import IdentifierManager
from requests import ReadTimeout, get
from requests.exceptions import ConnectionError
from typing import Type, Optional


class RORManager(IdentifierManager):
    """This class implements an identifier manager for ROR identifier"""

    def __init__(self, use_api_service=True, storage_manager:Optional[StorageManager] = None): #, data={},
        """PMCID manager constructor."""
        super(RORManager, self).__init__()
        self._api = "https://api.ror.org/organizations/"
        self._use_api_service = use_api_service
        if storage_manager is None:
            self.storage_manager = InMemoryStorageManager()
        else:
            self.storage_manager = storage_manager
        self._p = "ror:"

    def is_valid(self, ror_id, get_extra_info=False):
        ror_id = self.normalise(ror_id, include_prefix=True)

        if ror_id is None:
            if get_extra_info:
                return False, {"id": ror_id, "valid": False}
            return False
        else:
            ror_id_validation_value = self.storage_manager.get_value(ror_id)
            if isinstance(ror_id_validation_value, bool):
                if get_extra_info:
                    return ror_id_validation_value, {"id": ror_id, "valid": ror_id_validation_value}
                return ror_id_validation_value
            else:
                if get_extra_info:
                    info = self.exists(ror_id, get_extra_info=True)
                    self.storage_manager.set_full_value(ror_id, info[1])
                    return (info[0] and self.syntax_ok(ror_id)), info[1]
                validity_check = self.exists(ror_id) and self.syntax_ok(ror_id)
                self.storage_manager.set_value(ror_id, validity_check)
                return validity_check

    def normalise(self, id_string, include_prefix=False):
        try:
            if id_string.startswith(self._p):
                ror_id_string = id_string[len(self._p):]
            else:
                ror_id_string = id_string
            #  normalize + remove protocol and domain name if they are included in the ID
            ror_id_string = sub("\0+", "", sub("(https://)?ror\\.org/", "", sub('\s+', "", unquote(ror_id_string))))

            return "%s%s" % (
                self._p if include_prefix else "",
                ror_id_string.strip().lower(),
            )
        except:
            # Any error in processing the ROR ID will return None
            return None

    def syntax_ok(self, id_string):
        if not id_string.startswith("ror:"):
            id_string = self._p + id_string
        # the regex only admits the identifier without the protocol and the domain name
        return True if match(r"^ror:0[a-hj-km-np-tv-z|0-9]{6}[0-9]{2}$", id_string) else False

    def exists(self, ror_id_full, get_extra_info=False, allow_extra_api=None):
        valid_bool = True
        if self._use_api_service:
            ror_id = self.normalise(ror_id_full)
            if ror_id is not None:
                tentative = 3
                while tentative:
                    tentative -= 1
                    try:
                        r = get(self._api + ror_id, headers=self._headers, timeout=30)
                        if r.status_code == 200:
                            r.encoding = "utf-8"
                            json_res = loads(r.text)
                            if get_extra_info:
                                extra_info_result = {}
                                try:
                                    result = True if json_res['id'] else False
                                    extra_info_result['valid'] = result
                                    return result, extra_info_result
                                except KeyError:
                                    extra_info_result["valid"] = False
                                    return False, extra_info_result
                            try:
                                return True if json_res['id'] else False
                            except KeyError:
                                return False

                        elif 400 <= r.status_code < 500:
                            if get_extra_info:
                                return False, {"valid": False}
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
                    return False, {"valid": False}
                return False

        if get_extra_info:
            return valid_bool, {"valid": valid_bool}
        return valid_bool

    def extra_info(self, api_response, choose_api=None, info_dict={}):
        result = {}
        result["valid"] = True
        # to be implemented
        return result
