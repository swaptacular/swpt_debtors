import json
import re


class InvalidPath(Exception):
    """Invalid JSON repository path."""


class ForbiddenOverride(Exception):
    """Attempted override of a non-writable property."""


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
                    raise InvalidPath

    def _iter_values(self, path):
        value = self.obj
        yield value
        for part in self._iter_parts(path):
            try:
                value = value[part]
            except (TypeError, KeyError):
                raise InvalidPath
            yield value

    def get(self, path):
        *_, last = self._iter_values(path)
        return last

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


def _getitem(value, key):
    try:
        return value[key]
    except (TypeError, KeyError):
        raise InvalidPath
