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
from urllib.parse import unquote

from oc_ds_converter.oc_idmanager.base import IdentifierManager
from requests import ReadTimeout, get
from requests.exceptions import ConnectionError


class WikipediaManager(IdentifierManager):
    """This class implements an identifier manager for wikidata identifier"""

    def __init__(self, data={}, use_api_service=True):
        """Wikipedia manager constructor."""
        super(WikipediaManager, self).__init__()
        self._api = "https://en.wikipedia.org/w/api.php/"
        self._use_api_service = use_api_service
        self._p = "wikipedia:"
        self._data = data

    def is_valid(self, wikipedia_id, get_extra_info=False):

        wikipedia_id = self.normalise(wikipedia_id, include_prefix=True)

        if wikipedia_id is None:
            return False
        else:
            if wikipedia_id not in self._data or self._data[wikipedia_id] is None:
                if get_extra_info:
                    info = self.exists(wikipedia_id, get_extra_info=True)
                    self._data[wikipedia_id] = info[1]
                    return (info[0] and self.syntax_ok(wikipedia_id)), info[1]
                self._data[wikipedia_id] = dict()
                self._data[wikipedia_id]["valid"] = True if (self.syntax_ok(wikipedia_id) and self.exists(wikipedia_id)) else False
                return self._data[wikipedia_id].get("valid")
            if get_extra_info:
                return self._data[wikipedia_id].get("valid"), self._data[wikipedia_id]
            return self._data[wikipedia_id].get("valid")

    def normalise(self, id_string, include_prefix=False):
        try:
            if id_string.startswith(self._p):
                wikipedia_string = id_string[len(self._p):]
            else:
                wikipedia_string = id_string

            wikipedia_string = sub("\0+", "", sub("[^0-9]", "", unquote(wikipedia_string)))
            return "%s%s" % (
                self._p if include_prefix else "",
                wikipedia_string.strip(),
            )
        except:
            # Any error in processing the MediaWiki pageID will return None
            return None

    def syntax_ok(self, id_string):

        if not id_string.startswith("wikipedia:"):
            id_string = self._p + id_string
        return True if match("^wikipedia:[1-9][0-9]*$", id_string) else False

    def exists(self, wikipedia_id_full, get_extra_info=False, allow_extra_api=None):
        valid_bool = True
        if self._use_api_service:
            wikipedia_id = self.normalise(wikipedia_id_full)
            if wikipedia_id is not None:
                tentative = 3
                while tentative:
                    tentative -= 1
                    try:
                        query_params = {
                            "action": "query",
                            "pageids" : wikipedia_id,
                            "format": "json",
                            "formatversion": "1",  # format of json output (current version 1; might be replaced w/ v.2)
                        }

                        r = get(self._api, params=query_params, headers=self._headers, timeout=30)  # controlla
                        if r.status_code == 200:
                            r.encoding = "utf-8"
                            json_res = loads(r.text)
                            if get_extra_info:
                                extra_info_result = {}
                                try:
                                    result = True if 'title' in json_res['query']['pages'][wikipedia_id].keys() else False
                                    extra_info_result["valid"] = result
                                    return result, extra_info_result
                                except KeyError:
                                    extra_info_result["valid"] = False
                                    return False, extra_info_result
                            try:
                                return True if 'title' in json_res['query']['pages'][wikipedia_id].keys() else False
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
                valid_bool=False
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
