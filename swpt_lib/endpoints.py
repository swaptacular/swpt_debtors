import os
from urllib.parse import urlsplit, urlunsplit
from werkzeug.routing import Map, Rule, BuildError as WerkzeugBuildError
from werkzeug.exceptions import NotFound
from .utils import Int64Converter


rules = [
    Rule('/debtors/<i64:debtorId>', endpoint='debtor'),
    Rule('/creditors/<i64:creditorId>', endpoint='creditor'),
]

__doc__ = """
Build and matche URLs to endpoints.

The available endpoints are:

""" + '\n'.join(f'{r.endpoint}\n  {r}\n' for r in rules) + '\n'


class MatchError(Exception):
    """The URL does not match the endpoint."""


class BuildError(Exception):
    """An URL can not be build for the endpoint."""


def match_url(endpoint, url):
    """Try to to match an absolute URL to given endpoint.

    :param endpoint: The name of the endpoint
    :type endpoint: str

    :param url: The absolute URL that should be matched
    :type url: str

    :return: A `dict` of arguments extracted from the URL

    Raises `MatchError` if the URL does not match the endpoint.
    """

    try:
        scheme, netloc, path, *_ = urlsplit(url)
    except ValueError:
        raise MatchError(url)

    if scheme != _get_url_scheme() or netloc != _get_server_name():
        raise MatchError(url)

    try:
        matched_endpoint, kw = _urls.match(path)
    except NotFound:
        raise MatchError(url)

    if matched_endpoint != endpoint:
        raise MatchError(url)

    return kw


def build_url(endpoint, **kw):
    """Try to build an absolute URL for a given endpoint and arguments.

    :param endpoint: The name of the endpoint
    :type endpoint: str

    :param kw: The keyword arguments required by the particular endpoint

    :return: The absolute URL

    Raises `BuildError` if an URL can not be build for the endpoint.
    """

    try:
        path = _urls.build(endpoint, kw)
    except WerkzeugBuildError:
        raise BuildError()
    return urlunsplit((_get_url_scheme(), _get_server_name(), path, '', ''))


def _get_url_scheme():
    varname = 'SWPT_URL_SCHEME'
    return os.environ.get(varname, 'https')


def _get_server_name():
    varname = 'SWPT_SERVER_NAME'
    try:
        return os.environ[varname]
    except KeyError:
        raise Exception(f'The {varname} environment variable is not set.')


assert not any(str(r).endswith("/") for r in rules), 'a rule ends with "/".'
assert len(set(r.endpoint for r in rules)) == len(rules), 'multiple rules for a single endpoint.'
_urls = Map(rules, converters={'i64': Int64Converter}).bind('localhost')
