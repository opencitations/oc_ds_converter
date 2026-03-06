#!python
# Copyright (c) 2022 The OpenCitations Index Authors.
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

from abc import ABCMeta, abstractmethod


class DataSource(metaclass=ABCMeta):
    def __init__(self, service: str) -> None:
        self._service = service

    def new(self) -> dict[str, object]:
        return {"date": None, "valid": False, "issn": [], "orcid": []}

    @abstractmethod
    def get(self, resource_id: str) -> object:
        pass

    @abstractmethod
    def mget(self, resources_id: list[str]) -> list[object]:
        pass

    @abstractmethod
    def set(self, resource_id: str, value: object) -> object:
        pass

    @abstractmethod
    def mset(self, resources: dict[str, object]) -> object:
        pass