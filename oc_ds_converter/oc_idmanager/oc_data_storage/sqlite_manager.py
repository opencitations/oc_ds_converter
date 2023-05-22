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
import os.path
import sqlite3
import urllib.parse

from oc_ds_converter.oc_idmanager.oc_data_storage.storage_manager import StorageManager


class SqliteStorageManager(StorageManager):
    """A concrete implementation of the ``StorageManager`` interface that persistently stores
    the IDs validity values within a SQLite database."""

    def __init__(self, database: str =None, **params) -> None:
        """
        Constructor of the ``SqliteStorageManager`` class.

        :param database: The name of the database
        :type info_dir: str
        """
        super().__init__(**params)
        sqlite3.threadsafety = 3
        if database and os.path.exists(database):
            self.con = sqlite3.connect(database)
            self.storage_filepath = database
        else:
            new_path_dir = os.path.join(os.getcwd(), "storage")
            if not os.path.exists(new_path_dir):
                os.makedirs(new_path_dir)
            new_path_db = os.path.join(new_path_dir, "id_valid_dict.db")

            self.con = sqlite3.connect(new_path_db)
            self.storage_filepath =new_path_db

        self.cur = self.con.cursor()
        self.cur.execute("""CREATE TABLE IF NOT EXISTS info(
            id TEXT PRIMARY KEY, 
            value INTEGER)""")

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
        id_name = urllib.parse.quote((str(id)), safe=":")
        if not isinstance(value, dict):
            raise ValueError("value must be dict")
        if not isinstance(self.get_value(id_name), bool):
            self.set_value(id_name, value["valid"])

    def set_value(self, id: str, value: bool) -> None :
        """
        It allows to set a value for the validity check of an id.

        :param value: The new counter value to be set
        :type value: bool
        :param id: The id string with prefix
        :type id: str
        :raises ValueError: if ``value`` is neither 0 nor 1 (0 is False, 1 is True).
        :return: None
        """
        id_name = urllib.parse.quote((str(id)), safe=':')
        if not isinstance(value, bool):
            raise ValueError("value must be int boolean")
        validity = 1 if value else 0
        id_val = (id_name, validity)
        self.cur.execute(f"INSERT OR REPLACE INTO info VALUES (?,?)", id_val)
        self.con.commit()

    def get_value(self, id: str):
        """
        It allows to read the value of the identifier.

        :param id: The id name
        :type id: str
        :return: The requested id value (True if valid, False if invalid, None if not found).
        """
        id_name = urllib.parse.quote(str(id), safe=":")
        result = self.cur.execute(f"SELECT value FROM info WHERE id='{id_name}'")
        rows = result.fetchall()
        if len(rows) == 1:
            value = rows[0][0]
            return True if value == 1 else False
        elif len(rows) == 0:
            return None
        else:
            raise(Exception("There is more than one counter for this id. The databse id broken"))

    def delete_storage(self):
        if os.path.exists(self.storage_filepath):
            os.remove(self.storage_filepath)

    def get_all_keys(self):
        ids = [id[0] for id in self.cur.execute("SELECT id FROM info")]
        return ids