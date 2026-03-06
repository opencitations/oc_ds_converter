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


from abc import ABCMeta


class StorageManager(metaclass=ABCMeta):
    """This is the interface that must be implemented by any Storage manager
    for a particular storage approach. It provides the signatures of the methods
    for string and retrieving data."""

    _headers: dict[str, str]

    def __init__(self, **params: object) -> None:
        """Storage manager constructor."""
        for key in params:
            setattr(self, key, params[key])

        self._headers = {
            "User-Agent": "Identifier Manager / OpenCitations Indexes "
            "(http://opencitations.net; mailto:contact@opencitations.net)"
        }

    def set_value(self, id: str, value: bool) -> None:
        pass

    def set_full_value(self, id: str, value: dict[str, str | bool | object]) -> None:
        pass

    def get_value(self, id: str) -> bool | None:
        pass

    def set_multi_value(self, list_of_tuples: list[tuple[str, bool]]) -> None:
        pass

    def delete_storage(self) -> None:
        pass

    def store_file(self) -> None:
        pass

    def get_all_keys(self) -> list[str]:
        return []

