import json
import re
from typing import NamedTuple
from datetime import datetime, timezone


ARCHIVES_PROPNAME = '_archives'
WRITABLES_PROPNAME = '_writables'
IS_SEALED_PROPNAME = '_sealed'
VALUES_PROPNAME = '_values'
TIMESTAMPS_PROPNAME = '_timestamps'


class PathError(Exception):
    """Invalid JSON repository path."""


class NotWritableError(Exception):
    """Attempted write to a non-writable property."""


class SealedError(Exception):
    """Attempted to extend a sealed object."""


class MetapropsPermissionError(Exception):
    """Attempted forbidden meta-property change."""


class JSONReporitory:
    PART = re.compile(r'^[A-Za-z0-9_]+$')

    def __init__(self, s):
        self.obj = json.loads(s, object_hook=_json_object_hook)

    def _iter_parts(self, path):
        if path:
            for part in path.split('/'):
                if JSONReporitory.PART.match(part):
                    yield part
                else:
                    raise PathError

    def _iter_values(self, path):
        value = self.obj
        yield value
        for part in self._iter_parts(path):
            try:
                value = value[part]
            except (TypeError, KeyError):
                raise PathError
            yield value

    def get(self, path):
        obj = self.obj
        for part in self._iter_parts(path):
            obj = getitem(obj, part)
        return obj

    # def set(self, path, new_value):
    #     parts_iter = self._iter_parts(path)
    #     for value in self._iter_values(path):
    #         # TODO: what if the type is not right?
    #         _writables = value.get('_writables', [])
    #         _archives = value.get('_archives', [])
    #         _sealed = value.get('_sealed', False)

    #         part = next(parts_iter)
    #         if _sealed and part not in value:
    #             raise ForbiddenOverride
    #         if part not in _writables:
    #             raise ForbiddenOverride


# def set_metaprops(obj, new_archives: set, new_writables: set, new_is_sealed: bool):
#     old_writables = set(get_writables(obj))
#     old_archives = set(get_archives(obj))
#     old_is_sealed = is_sealed(obj)
#     if old_is_sealed and not new_is_sealed:
#         raise MetapropsPermissionError('Can not unseal a sealed object.')
#     if old_archives - new_archives:
#         raise MetapropsPermissionError('Can not change an archived property to a regular one.')
#     if new_archives & old_writables:
#         pass
#     if old_is_sealed and new_archives - old_writables:
#         raise MetapropsPermissionError('Can not change an archived property to a regular one.')
#     if all([k in old_archives for k in archives]):
#         obj[ARCHIVES_PROPERTY] = archives


def getitem(obj, key):
    if not isinstance(obj, ItemsDict):
        raise PathError
    if key not in obj:
        raise PathError
    item = obj[key]
    if key in obj.archives:
        if not isinstance(item, ItemsArchive):
            raise PathError
        return item.get_last_value()
    return item


# def setitem(obj, key, value):
#     if not isinstance(obj, dict):
#         raise PathError
#     if key not in obj and get_is_sealed(obj):
#         raise NotWritableError
#     if key in obj and key not in get_writables(obj):
#         raise NotWritableError
#     if key in get_archives(obj):
#         archive = obj.get(key)
#         if not isinstance(ItemsArchive, list):
#             archive = ItemsArchive(values=[], timestamps=[])
#         archive.add_item(value, datetime.now(tz=timezone.utc))
#         value = archive
#     obj[key] = value


class ItemsDict:
    """Represents a dictionary of values."""

    def __init__(self, obj):
        assert isinstance(obj, dict)
        self.obj = obj.copy()
        self.obj.pop(VALUES_PROPNAME, None)
        self.obj.pop(TIMESTAMPS_PROPNAME, None)
        archives = self.obj.pop(ARCHIVES_PROPNAME, None)
        writables = self.obj.pop(WRITABLES_PROPNAME, None)
        is_sealed = self.obj.pop(IS_SEALED_PROPNAME, None)
        self.archives = {str(a) for a in archives} if isinstance(archives, list) else set()
        self.writables = {str(w) for w in writables} if isinstance(writables, list) else set()
        self.is_sealed = is_sealed if isinstance(is_sealed, bool) else True

    def _setitem(self, key, value):
        obj = self.obj
        if key not in obj and self.is_sealed:
            raise SealedError()
        if key in obj and key not in self.writables:
            raise NotWritableError
        if key in self.archives:
            try:
                item = obj[key]
            except KeyError:
                archive = ItemsArchive([], [])
            else:
                archive = item if isinstance(item, ItemsArchive) else ItemsArchive([], [])
            archive.add_item(value, datetime.now(tz=timezone.utc))
            value = archive
        obj[key] = value

    def asdict(self):
        d = {
            ARCHIVES_PROPNAME: sorted(self.archives),
            WRITABLES_PROPNAME: sorted(self.writables),
            IS_SEALED_PROPNAME: self.is_sealed,
        }
        d.update(self.obj)
        return d

    def update(self, new):
        self.archives = self.archives | new.archives
        for key, value in new.obj.items():
            self._setitem(key, value)
        self.writables = self.writables & new.writables
        self.is_sealed = self.is_sealed or new.is_sealed

    def getitem(self, key):
        item = self.obj[key]
        if key in self.archives:
            if not isinstance(item, ItemsArchive):
                raise KeyError
            return item.get_last_value()
        return item


class ItemsArchive(NamedTuple):
    """Represents an archive of values."""

    values: list
    timestamps: list

    def add_item(self, value, timestamp):
        self.values.append(value)
        self.timestamps.append(timestamp)

    def get_last_value(self):
        return self.values[-1]

    def get_last_timestamp(self):
        return self.timestamps[-1]

    def asdict(self):
        return {
            VALUES_PROPNAME: self.values,
            TIMESTAMPS_PROPNAME: self.timestamps,
        }


def _json_object_hook(obj):
    try:
        values = obj[VALUES_PROPNAME]
        timestamps = obj[TIMESTAMPS_PROPNAME]
        if isinstance(values, list) and isinstance(timestamps, list) and len(values) == len(timestamps) > 0:
            return ItemsArchive(values=values, timestamps=timestamps)
    except KeyError:
        pass
    return ItemsDict(obj)


def _json_default_hook(obj):
    if isinstance(obj, (ItemsArchive, ItemsDict)):
        return obj.asdict()
    raise TypeError
