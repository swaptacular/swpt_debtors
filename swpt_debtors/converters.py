from urllib.parse import urlparse
from werkzeug.routing import BaseConverter, ValidationError, Map, Rule
from werkzeug.exceptions import NotFound
from flask import request

MIN_INT64 = -1 << 63
MAX_INT64 = (1 << 63) - 1
MAX_UINT64 = (1 << 64) - 1

_I64_SPAN = MAX_UINT64 + 1


def convert_i64_to_u64(value: int) -> int:
    if value > MAX_INT64 or value < MIN_INT64:
        raise ValueError()
    if value >= 0:
        return value
    return value + _I64_SPAN


def convert_u64_to_i64(value: int) -> int:
    if value > MAX_UINT64 or value < 0:
        raise ValueError()
    if value <= MAX_INT64:
        return value
    return value - _I64_SPAN


class Int64Converter(BaseConverter):
    regex = r"\d{1,20}"

    def to_python(self, value):
        value = int(value)
        try:
            return convert_u64_to_i64(value)
        except ValueError:
            raise ValidationError()

    def to_url(self, value):
        value = int(value)
        return str(convert_i64_to_u64(value))


rules = [
    Rule('/debtors/<i64:debtorId>', endpoint='debtor'),
    Rule('/creditors/<i64:creditorId>', endpoint='creditor'),
]
assert not any(str(r).endswith("/") for r in rules), 'a rule ends with "/".'
assert len(set(r.endpoint for r in rules)) == len(rules), 'multiple rules for a single endpoint.'

url_map = Map(rules, converters={'i64': Int64Converter}, strict_slashes=False, redirect_defaults=False)
urls = url_map.bind('localhost', url_scheme='https')  # Use `PREFERRED_URL_SCHEME` instead.


def match_url_to_endpoint(absolute_url, endpoint):
    scheme, netloc, path, *_ = urlparse(absolute_url)
    if scheme == request.scheme and netloc == request.host:
        matched_endpoint, kw = urls.match(path)
        if matched_endpoint == endpoint:
            return kw
    raise NotFound
