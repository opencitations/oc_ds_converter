from __future__ import annotations

from oc_ds_converter.oc_idmanager.oc_data_storage.storage_manager import StorageManager


class BatchManager(StorageManager):
    """A simple in-memory dict wrapper for batching validation results before writing to Redis."""

    def __init__(self, **params: object) -> None:
        super().__init__(**params)
        self._data: dict[str, dict[str, bool]] = {}

    def set_value(self, id: str, value: bool) -> None:
        id_name = str(id)
        if id_name in self._data:
            self._data[id_name]["valid"] = value
        else:
            self._data[id_name] = {"valid": value}

    def get_value(self, id: str) -> bool | None:
        id_name = str(id)
        id_in_dict = self._data.get(id_name)
        if id_in_dict:
            return id_in_dict["valid"]
        return None

    def get_validity_list_of_tuples(self) -> list[tuple[str, bool]]:
        return [(k, v["valid"]) for k, v in self._data.items()]

    def delete_storage(self) -> None:
        self._data = {}
