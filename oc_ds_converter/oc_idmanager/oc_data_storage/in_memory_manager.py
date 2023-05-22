#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2016, Silvio Peroni <essepuntato@gmail.com>
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

from typing import List, Dict

from oc_ds_converter.oc_idmanager.oc_data_storage.storage_manager import StorageManager
import os
import json
import urllib.parse
from pathlib import Path


class InMemoryStorageManager(StorageManager):
    """A concrete implementation of the ``StorageManager`` interface that persistently stores
    the IDs validity values within a in-memory dictionary, which is eventually saved in a json file."""

    def __init__(self, json_file_path: str =None, **params) -> None:
        """
        Constructor of the ``InMemoryStorageManager`` class.
        """
        super().__init__(**params)
        if json_file_path and os.path.exists(json_file_path):
            self.storage_filepath = json_file_path
            o_jfp = open(self.storage_filepath, "r")
            self.id_value_dict = json.load(o_jfp)
        else:
            new_path_dir = os.path.join(os.getcwd(), "storage")
            if not os.path.exists(new_path_dir):
                os.makedirs(new_path_dir)
            filepath = os.path.join(new_path_dir, "id_value.json" )
            self.id_value_dict = dict()
            self.storage_filepath = filepath
            file = open(self.storage_filepath, "w", encoding='utf8')
            json.dump(self.id_value_dict, file)

    def set_full_value(self, id: str, value: dict) -> None:
        """
        It allows to set the counter value of provenance entities.

        :param value: The new counter value to be set
        :type value: dict
        :param id: The id string with prefix
        :type id: str
        :raises ValueError: if ``value`` is neither 0 nor 1 (0 is False, 1 is True).
        :return: None
        """
        id_name = urllib.parse.quote((str(id)), safe=":/()")
        if not isinstance(value, dict):
            raise ValueError("value must be dict")
        if id_name in self.id_value_dict:
            new_info = {k:v for k,v in value.items() if k not in self.id_value_dict[id_name]}
            self.id_value_dict[id_name].update(new_info)
        else:
            self.id_value_dict[id_name] = value

    def set_value(self, id: str, value: bool) -> None:
        """
        It allows to set the counter value of provenance entities.

        :param value: The new counter value to be set
        :type value: bool
        :param id: The id string with prefix
        :type id: str
        :raises ValueError: if ``value`` is neither 0 nor 1 (0 is False, 1 is True).
        :return: None
        """
        id_name = urllib.parse.quote((str(id)), safe=":/()")

        if not isinstance(value, bool):
            raise ValueError("value must be boolean")
        if id_name in self.id_value_dict:
            self.id_value_dict[id_name]["valid"] = value
        else:
            self.id_value_dict[id_name] = {"valid": value}

    def get_value(self, id: str):
        """
        It allows to read the value of the "valid" key of the identifier's dict.

        :param id: The id name
        :type id: str
        :return: The requested id value.
        """

        id_name = urllib.parse.quote(str(id), safe=":/()")
        id_in_dict = self.id_value_dict.get(id_name)
        if id_in_dict:
            return id_in_dict["valid"]
        else:
            return None

    def store_file(self) -> None:
        """
        It stores in a file the dictionary with the validation results
        """
        file = open(self.storage_filepath, "w", encoding='utf8')
        json.dump(self.id_value_dict, file, indent=4)
        file.close()

    def delete_storage(self):
        if os.path.exists(self.storage_filepath):
            os.remove(self.storage_filepath)

    def get_all_keys(self):
        return self.id_value_dict.keys()

