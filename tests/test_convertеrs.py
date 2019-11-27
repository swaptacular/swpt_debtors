import pytest
from swpt_lib import converters as c

MIN_INT64 = -1 << 63
MAX_INT64 = (1 << 63) - 1
MAX_UINT64 = (1 << 64) - 1


def test_i64_to_slug():
    assert c.i64_to_slug(0) == '0'
    assert c.i64_to_slug(1) == '1'
    assert c.i64_to_slug(MAX_INT64) == str(MAX_INT64)
    assert c.i64_to_slug(-1) == str(MAX_UINT64)
    assert c.i64_to_slug(MIN_INT64) == str(MAX_INT64 + 1)
    with pytest.raises(ValueError):
        c.i64_to_slug(MAX_INT64 + 1)
    with pytest.raises(ValueError):
        c.i64_to_slug(MIN_INT64 - 1)


def test_slug_to_i64():
    assert c.slug_to_i64('0') == 0
    assert c.slug_to_i64('1') == 1
    assert c.slug_to_i64(str(MAX_INT64)) == MAX_INT64
    assert c.slug_to_i64(str(MAX_UINT64)) == -1
    assert c.slug_to_i64(str(MAX_INT64 + 1)) == MIN_INT64
    with pytest.raises(ValueError):
        c.slug_to_i64('-1')
    with pytest.raises(ValueError):
        c.slug_to_i64(str(MAX_UINT64 + 1))


def test_werkzeug_converter():
    from werkzeug.routing import Map, Rule
    from werkzeug.exceptions import NotFound

    m = Map([
        Rule('/debtors/<i64:debtorId>', endpoint='debtors'),
    ], converters={'i64': c.Int64Converter})
    urls = m.bind('example.com', '/')

    # Test URL match:
    assert urls.match('/debtors/0') == ('debtors', {'debtorId': 0})
    assert urls.match('/debtors/1') == ('debtors', {'debtorId': 1})
    assert urls.match('/debtors/9223372036854775807') == ('debtors', {'debtorId': 9223372036854775807})
    assert urls.match('/debtors/9223372036854775808') == ('debtors', {'debtorId': -9223372036854775808})
    assert urls.match('/debtors/18446744073709551615') == ('debtors', {'debtorId': -1})
    with pytest.raises(NotFound):
        assert urls.match('/debtors/1x')
    with pytest.raises(NotFound):
        assert urls.match('/debtors/18446744073709551616')
    with pytest.raises(NotFound):
        assert urls.match('/debtors/-1')

    # Test URL build:
    assert urls.build('debtors', {'debtorId': 0}) == '/debtors/0'
    assert urls.build('debtors', {'debtorId': 1}) == '/debtors/1'
    assert urls.build('debtors', {'debtorId': 9223372036854775807}) == '/debtors/9223372036854775807'
    assert urls.build('debtors', {'debtorId': -9223372036854775808}) == '/debtors/9223372036854775808'
    with pytest.raises(ValueError):
        assert urls.build('debtors', {'debtorId': 9223372036854775808})
    with pytest.raises(ValueError):
        assert urls.build('debtors', {'debtorId': -9223372036854775809})
    with pytest.raises(ValueError):
        assert urls.build('debtors', {'debtorId': '1x'})
