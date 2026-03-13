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

from re import match, sub

from oc_ds_converter.oc_idmanager.base import IdentifierManager
from oc_ds_converter.oc_idmanager.oc_data_storage.redis_manager import RedisStorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.storage_manager import StorageManager
from oc_ds_converter.oc_idmanager.support import call_api


class CrossrefManager(IdentifierManager):
    """This class implements an identifier manager for Crossref member identifier"""

    def __init__(self, use_api_service: bool = True, storage_manager: StorageManager | None = None, testing: bool = True) -> None:
        """Crossref member ID manager constructor."""
        super(CrossrefManager, self).__init__()
        if storage_manager is None:
            self.storage_manager = RedisStorageManager(testing=testing)
        else:
            self.storage_manager = storage_manager
        self._api = "https://api.crossref.org/members/"
        self._api_funders = "https://api.crossref.org/funders/"
        self._api_works_route = r"https://api.openalex.org/works/"
        self._api_sources_route = r"https://api.openalex.org/sources/"
        self._use_api_service = use_api_service
        self._p = "crossref:"
        self._url_id_pref = "https://openalex.org/"


    def validated_as_id(self, id_string):
        crossref_validation_value = self.storage_manager.get_value(id_string)
        if isinstance(crossref_validation_value, bool):
            return crossref_validation_value
        else:
            return None

    def is_valid(self, cr_member_id, get_extra_info=False):
        cr_member_id = self.normalise(cr_member_id, include_prefix=True)

        if cr_member_id is None:
            if get_extra_info:
                return False, {"id": id_string, "valid": False}
            return False

        id_validation_value = self.storage_manager.get_value(cr_member_id)
        if isinstance(id_validation_value, bool):
            if get_extra_info:
                return id_validation_value, {"id": cr_member_id, "valid": id_validation_value}
            return id_validation_value

        if get_extra_info:
            result = self.exists(cr_member_id, get_extra_info=True)
            if isinstance(result, tuple):
                valid, info = result
                info_dict: dict[str, str | bool | object] = dict(info)
                self.storage_manager.set_full_value(cr_member_id, info_dict)
                return valid and self.syntax_ok(cr_member_id), info
            return False, {"id": cr_member_id, "valid": False}

        exists_result = self.exists(cr_member_id)
        validity_check = self.syntax_ok(cr_member_id) and bool(exists_result)
        self.storage_manager.set_value(cr_member_id, validity_check)
        return validity_check

    def normalise(self, id_string: str, include_prefix: bool = False) -> str | None:
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
        except Exception:
            return None

    def syntax_ok(self, id_string: str) -> bool:
        if not id_string.startswith("crossref:"):
            id_string = self._p + id_string
        return bool(match(r"^crossref:\d+$", id_string))

    def exists(
        self,
        id_string: str,
        get_extra_info: bool = False,
        allow_extra_api: str | None = None,
    ) -> bool | tuple[bool, dict[str, str | bool]]:
        valid_bool = True
        cr_member_id_full = self._p + id_string if not id_string.startswith(self._p) else id_string

        if self._use_api_service:
            cr_member_id = self.normalise(cr_member_id_full)
            if cr_member_id is None:
                if get_extra_info:
                    return False, {"id": cr_member_id_full, "valid": False}
                return False

            pref_cr_member_id = self._p + cr_member_id
            json_res = call_api(url=self._api + cr_member_id, headers=self._headers)
            if json_res and isinstance(json_res, dict):
                message = json_res.get("message")
                if isinstance(message, dict):
                    valid_bool = str(message.get("id", "")) == cr_member_id
                    if get_extra_info:
                        return valid_bool, {"id": pref_cr_member_id, "valid": valid_bool}
                    return valid_bool
            valid_bool = False

        if get_extra_info:
            return valid_bool, {"id": cr_member_id_full, "valid": valid_bool}
        return valid_bool

    def extra_info(
        self,
        api_response: dict[str, object],
        choose_api: str | None = None,
        info_dict: dict[str, object] | None = None,
    ) -> dict[str, object]:
        result: dict[str, object] = {"valid": True}
        return result
