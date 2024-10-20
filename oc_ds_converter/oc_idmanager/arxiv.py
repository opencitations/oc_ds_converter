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


from re import compile, match, search
from time import sleep
from urllib.parse import quote, unquote

import xmltodict
from oc_ds_converter.oc_idmanager import *
from oc_ds_converter.oc_idmanager.base import IdentifierManager
from requests import ReadTimeout, get
from requests.exceptions import ConnectionError
from oc_ds_converter.oc_idmanager.oc_data_storage.storage_manager import StorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.in_memory_manager import InMemoryStorageManager
#from oc_ds_converter.oc_idmanager.oc_data_storage.sqlite_manager import SqliteStorageManager
from typing import Optional, Type, Callable


class ArXivManager(IdentifierManager):
    """This class implements an identifier manager for arxiv identifier"""

    def __init__(self, use_api_service=True, storage_manager:Optional[StorageManager] = None):
        """arxiv manager constructor."""
        super(ArXivManager,self).__init__()
        self._use_api_service = use_api_service
        if storage_manager is None:
            self.storage_manager = InMemoryStorageManager()
        else:
            self.storage_manager = storage_manager

        self._p = "arxiv:"
        self._api = f'https://export.arxiv.org/api/query?search_query=all:'
        self._api_v = f'https://arxiv.org/abs/'
        self._headers = {
            "User-Agent": "Identifier Manager / OpenCitations Indexes "
                          "(http://opencitations.net; mailto:contact@opencitations.net)"
        }


    def validated_as_id(self, id_string):
        arxiv_vaidation_value = self.storage_manager.get_value(id_string)
        if isinstance(arxiv_vaidation_value, bool):
            return arxiv_vaidation_value
        else:
            return None

    def is_valid(self, id_string, get_extra_info=False):
        """Check if an arxiv is valid.

        Args:
            id_string (str): the arxiv to check

        Returns:
            bool: true if the arxiv is valid, false otherwise.
        """

        arxiv = self.normalise(id_string, include_prefix=True)
        if not arxiv:
            return False
        else:
            arxiv_vaidation_value = self.storage_manager.get_value(arxiv)
            if isinstance(arxiv_vaidation_value, bool):

                if get_extra_info:
                    return arxiv_vaidation_value, {"id":arxiv, "valid":arxiv_vaidation_value}
                return arxiv_vaidation_value
            else:
                if get_extra_info:
                    info = self.exists(arxiv, get_extra_info=True)
                    self.storage_manager.set_full_value(arxiv,info[1])
                    return (info[0] and self.syntax_ok(arxiv)), info[1]
                validity_check = self.syntax_ok(arxiv) and self.exists(arxiv) 
                self.storage_manager.set_value(arxiv, validity_check)

                return validity_check

    def normalise(self, id_string, include_prefix=False):
        """It returns the arxiv normalized.

        Args:
            id_string (str): the arxiv to normalize.
            include_prefix (bool, optional): indicates if include the prefix. Defaults to False.

        Returns:
            str: the normalized arxiv
        """
        regex = compile(r'[^0-9v.]')
        regexdot = compile(r'\.+')
        reg_api = compile(r'(https?://export\.arxiv\.org/api/query\?search_query=all:)')
        reg_v_api = compile(r'(https?://arxiv\.org/abs/)')
        reg_v_extrachar = compile(r'\/?ar[Xx]iv\.?')

        if id_string:
            id_string = str(id_string).strip().lower()

            if id_string.startswith(self._p):
                skip_char = len(self._p)
                id_string = id_string[skip_char:]

            id_string = regexdot.sub('.', id_string)
            id_string = reg_v_api.sub('', id_string)
            id_string = reg_api.sub('', id_string)
            id_string = regex.sub('', id_string)
            id_string = reg_v_extrachar.sub('', id_string)

            # First parameter is the replacement, second parameter is your input string

            try:
                id_string = unquote(id_string)
                arxiv_string = search("(\d{4}.\d{4,5}|[a-z\-]+(\.[A-Z]{2})?\/\d{7})(v\d+)", id_string).group(0)

                return "%s%s" % (self._p if include_prefix else "", arxiv_string)
            except:
                try:
                    id_string = unquote(id_string)
                    arxiv_string = search("(\d{4}.\d{4,5}|[a-z\-]+(\.[A-Z]{2})?\/\d{7})(v\d+)?", id_string).group(0)
                    return "%s%s" % (self._p if include_prefix else "", arxiv_string+"v1")
                except:
                    return None

        else:
            return None

    def syntax_ok(self, id_string):
        if not id_string.startswith(self._p):
            id_string = self._p + id_string
        return True if match("arxiv:(\d{4}.\d{4,5}|[a-z\-]+(\.[A-Z]{2})?\/\d{7})(v\d+)?$", id_string) else False


    def exists(self, arxiv_full, get_extra_info=False, allow_extra_api=None):
        """
        Returns True if the id exists, False otherwise. Not all child class check id existence because of API policies
        Args:
            arxiv_full (str): the arxiv string for the api request
        Returns:
            bool: True if the arxiv exists (is registered), False otherwise.
        """
        valid_bool = True
        if self._use_api_service:
            arxiv_full_norm = self.normalise(arxiv_full, include_prefix=False)
            if arxiv_full_norm:
                version = ""
                arxiv_string_match = search("(v\d+)$", arxiv_full_norm)
                if arxiv_string_match:
                    version = arxiv_string_match[1]

                if version and version != "v1":
                    api = self._api_v
                else:
                    if version == "v1":
                        # v1 always exists, and the base API is more efficient,
                        # so we just check the existence of the base ARXIV id
                        arxiv_full_norm = arxiv_full_norm[:-2]

                    api = self._api


                tentative = 3
                while tentative:
                    tentative -= 1
                    try:
                        r = get(
                            api + quote(arxiv_full_norm),
                            headers=self._headers,
                            timeout=30,
                        )
                        if r.status_code == 200:
                            if not version or version =="v1":
                                #data = r.decode('utf-8').text
                                xml_re = r.text
                                obj = xmltodict.parse(f'{xml_re}')
                                feed = obj.get("feed")
                                results = feed.get("opensearch:totalResults")
                                try:
                                    results_n = int(results.get("#text"))
                                except:
                                    results_n = 0

                                if results_n >0:
                                    if get_extra_info:
                                        return True, self.extra_info(obj)
                                    return True
                                else:
                                    if get_extra_info:
                                        return False, {"valid": False}
                                    return False
                            else:
                                if get_extra_info:
                                    return True, {"valid": True}
                                return True
                        else:
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


    def extra_info(self, api_response, choose_api=None, info_dict:dict={}):
        result = {}
        result["valid"] = True
        # to be implemented
        return result
