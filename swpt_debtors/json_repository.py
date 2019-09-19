import json
import re
from datetime import datetime, timezone
from collections import abc
from functools import partial
import iso8601


class PathError(Exception):
    """Invalid repository path."""


class IllegalChange(Exception):
    """Attempted illegal change."""


class PledgesReporitory:
    URL_PART = re.compile(r'^[A-Za-z0-9_]+$')

    def __init__(self, json_str):
        self._obj = json.loads(json_str, object_hook=partial(_json_object_hook, None))

    def _get_path_parts(self, path):
        parts = []
        if path:
            for part in path.split('/'):
                if PledgesReporitory.URL_PART.match(part):
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
        if propname is None:
            prop = obj
        else:
            prop = _get_json_property(obj, propname)
        return json.dumps(prop, default=_json_default)

    def put(self, path, json_str):
        current_ts = datetime.now(tz=timezone.utc)
        value = json.loads(json_str, object_hook=partial(_json_object_hook, current_ts))
        obj, propname = self._follow_path(path)
        if propname is None:
            prop = obj
        else:
            # Try to override the value of the property.
            try:
                obj[propname] = value
                return
            except IllegalChange:
                pass
            prop = _get_json_property(obj, propname)

        # If the property is a pledge, try to revise it.
        if not isinstance(prop, Pledge):
            raise IllegalChange
        prop.revise(value)

    def delete(self, path):
        obj, propname = self._follow_path(path)
        if propname is None:
            raise IllegalChange
        _delete_json_property(obj, propname)


class Pledge(abc.MutableMapping):
    """Represents a dictionary of values and a revising policy."""

    WRITABLES_PROPNAME = '$writable'
    IS_SEALED_PROPNAME = '$sealed'
    CREATED_AT_PROPNAME = '$created'
    REVISED_AT_PROPNAME = '$revised'

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
            raise IllegalChange
        self._obj[key] = value

    def __delitem__(self, key):
        if self.is_sealed or self.is_change_forbidden(key):
            raise IllegalChange
        del self._obj[key]

    def __iter__(self):
        return iter(self._obj)

    def __len__(self):
        return len(self._obj)

    def __repr__(self):
        asdict = str(self.asdict())
        return f'Pledge({asdict})'

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
            d[Pledge.CREATED_AT_PROPNAME] = self.created_at.isoformat(timespec='seconds')
        if self.revised_at:
            d[Pledge.REVISED_AT_PROPNAME] = self.revised_at.isoformat(timespec='seconds')
        return d


def _json_object_hook(created_at, obj):
    return Pledge(obj, created_at)


def _json_default(obj):
    if isinstance(obj, Pledge):
        return obj.asdict()
    raise TypeError


def _get_json_property(obj, propname):
    try:
        return obj[propname]
    except KeyError:
        raise PathError


def _delete_json_property(obj, propname):
    try:
        del obj[propname]
    except KeyError:
        raise PathError
