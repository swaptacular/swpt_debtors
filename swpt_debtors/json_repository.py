import json
import re
from datetime import datetime, timezone
from collections import abc
import iso8601


class PathError(Exception):
    """Invalid JSON repository path."""


class ForbiddenChangeError(Exception):
    """Attempted a forbidden update."""


class JSONReporitory:
    URL_PART = re.compile(r'^[A-Za-z0-9_]+$')

    def __init__(self, s):
        self._obj = json.loads(s, object_hook=_json_object_hook)

    def _get_path_parts(self, path):
        parts = []
        if path:
            for part in path.split('/'):
                if JSONReporitory.URL_PART.match(part):
                    parts.append(part)
                else:
                    raise PathError
        return parts

    def _follow_path(self, path):
        obj = self._obj
        parts = self._get_path_parts(path)
        if parts:
            for part in parts[:-1]:
                try:
                    obj = obj[part]
                except (TypeError, KeyError):
                    raise PathError
            if not isinstance(obj, Pledge):
                raise PathError
            return obj, parts[-1]
        return obj, None

    def get(self, path):
        obj, propname = self._follow_path(path)
        return obj if propname is None else obj.get_json_property(propname)

    def set(self, path, value):
        obj, propname = self._follow_path(path)
        if propname is not None:
            # Try to override the value of the property directly.
            try:
                obj[propname] = value
                return
            except ForbiddenChangeError:
                pass
            prop = obj.get_json_property(propname)
        else:
            prop = obj

        # If the value is a dictionary, try to revise it.
        if not isinstance(prop, Pledge):
            raise ForbiddenChangeError
        prop.revise(value)

    def delete(self, path):
        obj, propname = self._follow_path(path)
        if propname is None:
            raise ForbiddenChangeError
        obj.delete_json_property(propname)


class Pledge(abc.MutableMapping):
    """Represents a dictionary of values and a revising policy."""

    WRITABLES_PROPNAME = '$writable'
    IS_SEALED_PROPNAME = '$isSealed'
    CREATED_AT_PROPNAME = '$createdAt'
    REVISED_AT_PROPNAME = '$revisedAt'

    RESERVED_PROPNAMES = {
        WRITABLES_PROPNAME,
        IS_SEALED_PROPNAME,
        CREATED_AT_PROPNAME,
        REVISED_AT_PROPNAME,
    }

    def __init__(self, obj, created_at=None):
        assert isinstance(obj, dict)
        self._obj = obj.copy()

        writables = self._obj.get(Pledge.WRITABLES_PROPNAME)
        writables = writables if isinstance(writables, list) else []
        self.writables = [s for s in writables if isinstance(s, str)]
        self.is_sealed = self._obj.get(Pledge.IS_SEALED_PROPNAME) is not False
        self.created_at = created_at
        self.revised_at = None
        if created_at is None:
            try:
                self.created_at = iso8601.parse_date(self._obj.get(Pledge.CREATED_AT_PROPNAME))
            except iso8601.ParseError:
                pass
            try:
                self.revised_at = iso8601.parse_date(self._obj.get(Pledge.REVISED_AT_PROPNAME))
            except iso8601.ParseError:
                pass

        for propname in Pledge.RESERVED_PROPNAMES:
            self._obj.pop(propname, None)

    def __getitem__(self, key):
        return self._obj[key]

    def __setitem__(self, key, value):
        if self.is_change_forbidden(key):
            raise ForbiddenChangeError
        self._obj[key] = value

    def __delitem__(self, key):
        if self.is_sealed or self.is_change_forbidden(key):
            raise ForbiddenChangeError
        del self._obj[key]

    def __iter__(self):
        return iter(self._obj)

    def __len__(self):
        return len(self._obj)

    def __repr__(self):
        asdict = str(self.asdict())
        return f'Pledge({asdict})'

    def get_json_property(self, propname):
        try:
            return self[propname]
        except KeyError:
            raise PathError

    def delete_json_property(self, propname):
        try:
            del self[propname]
        except KeyError:
            raise PathError

    def is_change_forbidden(self, key):
        extends_sealed = key not in self and self.is_sealed
        changes_reserved = key in Pledge.RESERVED_PROPNAMES
        changes_not_writable = key in self and key not in self.writables
        return extends_sealed or changes_reserved or changes_not_writable

    def revise(self, new_version):
        removed_keys = (k for k in self if k not in new_version)
        for key in removed_keys:
            del self[key]
        self.update(new_version)
        self.writables = self.writables & new_version.writables
        self.is_sealed = self.is_sealed or new_version.is_sealed
        self.revised_at = datetime.now(tz=timezone.utc)

    def asdict(self):
        d = self._obj.copy()
        if self.writables:
            d[Pledge.WRITABLES_PROPNAME] = sorted(self.writables)
        if not self.is_sealed:
            d[Pledge.IS_SEALED_PROPNAME] = False
        if self.created_at:
            d[Pledge.CREATED_AT_PROPNAME] = self.created_at.isoformat()
        if self.revised_at:
            d[Pledge.REVISED_AT_PROPNAME] = self.revised_at.isoformat()
        return d


def _json_object_hook(obj):
    return Pledge(obj)


def _json_default(obj):
    if isinstance(obj, Pledge):
        return obj.asdict()
    raise TypeError
