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
import json
import re
from json import loads
from re import match, sub
from time import sleep
from urllib.parse import quote
import datetime

from oc_ds_converter.oc_idmanager.base import IdentifierManager
from requests import ReadTimeout, get
from requests.exceptions import ConnectionError
from oc_ds_converter.oc_idmanager.oc_data_storage.storage_manager import StorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.in_memory_manager import InMemoryStorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.sqlite_manager import SqliteStorageManager
from typing import Type, Optional

# POSSIBLE EXTENSION: adding a new parameter in order to directly use the input orcid - doi map in the orcid manager
class ORCIDManager(IdentifierManager):
    """This class implements an identifier manager for orcid identifier."""

    def __init__(self, use_api_service=True, storage_manager: Optional[Type[StorageManager]] = None):
        """Orcid Manager constructor."""
        super(ORCIDManager, self).__init__()
        self._api = "https://pub.orcid.org/v3.0/"
        self._use_api_service = use_api_service
        if storage_manager is None:
            self.storage_manager = InMemoryStorageManager()
        else:
            self.storage_manager = storage_manager

        self._p = "orcid:"

    def validated_as_id(self, id_string):
        arxiv_vaidation_value = self.storage_manager.get_value(id_string)
        if isinstance(arxiv_vaidation_value, bool):
            return arxiv_vaidation_value
        else:
            return None

    def is_valid(self, id_string, get_extra_info=False):
        orcid = self.normalise(id_string, include_prefix=True)
        if orcid is None:
            if get_extra_info:

                return False, {"id":orcid, "valid": False}
            return False
        else:
            orcid_vaidation_value = self.storage_manager.get_value(orcid)
            if isinstance(orcid_vaidation_value, bool):
                if get_extra_info:
                    return orcid_vaidation_value, {"id": orcid, "valid": orcid_vaidation_value}
                return orcid_vaidation_value
            else:
                if get_extra_info:
                    info = self.exists(orcid, get_extra_info=True)
                    self.storage_manager.set_full_value(orcid,info[1])
                    return (info[0] and self.check_digit(orcid) and self.syntax_ok(orcid)), info[1]
                validity_check = self.exists(orcid) and self.syntax_ok(orcid) and self.check_digit(orcid)
                self.storage_manager.set_value(orcid, validity_check)
                return validity_check


    def normalise(self, id_string, include_prefix=False):
        try:
            orcid_string = sub("[^X0-9]", "", id_string.upper())
            return "%s%s-%s-%s-%s" % (
                self._p if include_prefix else "",
                orcid_string[:4],
                orcid_string[4:8],
                orcid_string[8:12],
                orcid_string[12:16],
            )
        except:  # Any error in processing the id will return None
            return None

    def check_digit(self, orcid):
        if orcid.startswith(self._p):
            spl = orcid.find(self._p) + len(self._p)
            orcid = orcid[spl:]
        total = 0
        for d in sub("[^X0-9]", "", orcid.upper())[:-1]:
            i = 10 if d == "X" else int(d)
            total = (total + i) * 2
        reminder = total % 11
        result = (12 - reminder) % 11
        return (str(result) == orcid[-1]) or (result == 10 and orcid[-1] == "X")

    def syntax_ok(self, id_string):
        if not id_string.startswith(self._p):
            id_string = self._p+id_string
        return True if match("^orcid:([0-9]{4}-){3}[0-9]{3}[0-9X]$", id_string, re.IGNORECASE) else False


    def exists(self, orcid, get_extra_info=False, allow_extra_api=None):
        info_dict = {"id": orcid}
        valid_bool = True
        if self._use_api_service:
            self._headers["Accept"] = "application/json"
            orcid = self.normalise(orcid)
            info_dict = {"id":orcid}
            if orcid is not None:
                tentative = 3
                while tentative:
                    tentative -= 1
                    try:
                        r = get(self._api + quote(orcid), headers=self._headers, timeout=30)
                        if r.status_code == 200:
                            r.encoding = "utf-8"
                            json_res = loads(r.text)
                            valid_bool = json_res.get("orcid-identifier").get("path") == orcid
                            if get_extra_info:
                                info_dict.update(self.extra_info(json_res))
                                return valid_bool, info_dict
                            return valid_bool
                    except ReadTimeout:
                        # Do nothing, just try again
                        pass
                    except ConnectionError:
                        # Sleep 5 seconds, then try again
                        sleep(5)
                valid_bool = False
            else:
                if get_extra_info:
                    info_dict["valid"] = False
                    return False, info_dict
                return False
        if get_extra_info:
            info_dict["valid"] = valid_bool
            return valid_bool, info_dict
        return valid_bool

    def extra_info(self, api_response, choose_api=None, info_dict={}):
        family_name = ""
        given_name = ""
        email = ""
        external_identifiers = {}
        submission_date = ""
        update_date = ""
        try:
            person = api_response["person"]
            try:
                name = person["name"]
                try:
                    family_name = name['family-name']['value']
                except:
                    pass
                try:
                    given_name = name['given-names']['value']
                except:
                    pass
            except:
                given_name = ""
                family_name = ""
            try:
                email = str(person["emails"]["email"]) if person["emails"]["email"] else ""
            except:
                pass
            try:
                external_identifiers = {}
                for y in person["external-identifiers"]:
                    k_vs = {x.get("external-id-type"): x.get("external-id-value") for x in y["external-identifier"]}
                    external_identifiers.update(k_vs)
            except:
                external_identifiers = {}

        except:
            pass

        try:
            history = api_response.get("history")
            try:
                submission_date = self.timestamp_to_date(history["submission-date"]["value"])
            except:
                submission_date = ""
            try:
                update_date = self.timestamp_to_date(history["last-modified-date"]["value"])
            except:
                pass

        except:
            history = ""

        result = {}
        result["valid"] = True
        result["family_name"] = family_name
        result["given_name"] = given_name
        result["email"] = email
        result["external_identifiers"] = external_identifiers
        result["submission_date"] = submission_date
        result["update_date"] = update_date

        return result

    def timestamp_to_date(self, timestamp_value):
        timestamp = timestamp_value / 1000
        date = datetime.datetime.fromtimestamp(timestamp)
        date_string = date.strftime("%Y-%m-%d")
        return date_string
