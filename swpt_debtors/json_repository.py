import json
import re


ARCHIVES_PROPERTY = '_archives'
WRITABLES_PROPERTY = '_writables'
SEALED_PROPERTY = '_sealed'


class PathError(Exception):
    """Invalid JSON repository path."""


class NotWritableError(Exception):
    """Attempted write to a non-writable property."""


class JSONReporitory:
    PART = re.compile(r'^[A-Za-z0-9_]+$')

    def __init__(self, s):
        self.obj = json.loads(s)

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

    def set(self, path, new_value):
        parts_iter = self._iter_parts(path)
        for value in self._iter_values(path):
            # TODO: what if the type is not right?
            _writables = value.get('_writables', [])
            _archives = value.get('_archives', [])
            _sealed = value.get('_sealed', False)

            part = next(parts_iter)
            if _sealed and part not in value:
                raise ForbiddenOverride
            if part not in _writables:
                raise ForbiddenOverride


def get_archives(obj):
    archives = obj.get(ARCHIVES_PROPERTY)
    return archives if isinstance(archives, list) else []


def get_writables(obj):
    writables = obj.get(WRITABLES_PROPERTY)
    return writables if isinstance(writables, list) else []


def is_sealed(obj):
    is_sealed = obj.get(SEALED_PROPERTY)
    return is_sealed if isinstance(is_sealed, bool) else True


def getitem(obj, key):
    if not isinstance(obj, dict):
        raise PathError
    if key not in obj:
        raise PathError
    item = obj[key]
    if key in get_archives(obj):
        if not isinstance(item, list) or len(item) == 0:
            raise PathError
        return item[-1]
    return item


def setitem(obj, key, value):
    if not isinstance(obj, dict):
        raise PathError
    if key not in obj and is_sealed(obj):
        raise NotWritableError
    if key in obj and key not in get_writables(obj):
        raise NotWritableError
    if key in get_archives(obj):
        archived_values = obj.get(key)
        if not isinstance(archived_values, list):
            archived_values = []
        archived_values.append(value)
    obj[key] = value
