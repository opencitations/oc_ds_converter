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
from re import sub, match
from requests import ReadTimeout, get
from requests.exceptions import ConnectionError
from json import loads
from time import sleep


class OpenAlexManager(IdentifierManager):
    """This class implements an identifier manager for openalex identifier"""

    def __init__(self, data={}, use_api_service=True):
        """OpenAlex manager constructor."""
        super(OpenAlexManager, self).__init__()
        self._api = "https://api.openalex.org/"
        self._use_api_service = use_api_service
        self._p = "openalex:"
        self._url_id_pref = "https://openalex.org/"
        self._data = data
        self._headers = {
            "User-Agent": "Identifier Manager / OpenCitations Indexes "
                          "(http://opencitations.net; mailto:contact@opencitations.net)"
        }

    def is_valid(self, oal_id, get_extra_info=False):
        oal_id = self.normalise(oal_id, include_prefix=True)

        if oal_id is None:
            return False
        else:
            if oal_id not in self._data or self._data[oal_id] is None:
                if get_extra_info:
                    info = self.exists(oal_id, get_extra_info=True)
                    self._data[oal_id] = info[1]
                    return (info[0] and self.syntax_ok(oal_id)), info[1]
                self._data[oal_id] = dict()
                self._data[oal_id]["valid"] = True if (self.exists(oal_id) and self.syntax_ok(oal_id)) else False
                return self._data[oal_id].get("valid")
            if get_extra_info:
                return self._data[oal_id].get("valid"), self._data[oal_id]
            return self._data[oal_id].get("valid")

    def normalise(self, id_string, include_prefix=False):
        try:
            if id_string.startswith(self._p):
                oal_string = id_string[len(self._p):]
            else:
                oal_string = id_string

            oal_string = sub(self._url_id_pref, '', oal_string)
            oal_string = sub(self._api, '', oal_string)
            oal_string = sub("\0+", "", sub("[^WS0-9]", "", oal_string.upper()))
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
        if self._use_api_service:
            oal_id = self.normalise(openalex_id_full)
            if oal_id is not None:
                tentative = 3
                while tentative:
                    tentative -= 1
                    try:
                        r = get(self._api + oal_id, headers=self._headers, timeout=30)
                        if r.status_code == 200:
                            r.encoding = "utf-8"
                            json_res = loads(r.text)
                            if get_extra_info:
                                extra_info_result = {}
                                try:
                                    result = True if json_res['id'] == (self._url_id_pref + oal_id) else False
                                    extra_info_result['valid'] = result
                                    return result, extra_info_result
                                except KeyError:
                                    extra_info_result["valid"] = False
                                    return False, extra_info_result
                            try:
                                return True if json_res['id'] == (self._url_id_pref + oal_id) else False
                            except KeyError:
                                return False
                        if r.status_code == 429:
                            sleep(1)  # only handles per-second rate limits (not per-day rate limits)
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
