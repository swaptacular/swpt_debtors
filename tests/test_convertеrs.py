import pytest
from werkzeug.routing import ValidationError
from swpt_debtors import converters as c


def test_convert_i64_to_u64():
    assert c.MAX_INT64 - c.MIN_INT64 == c.MAX_UINT64
    assert c.convert_i64_to_u64(0) == 0
    assert c.convert_i64_to_u64(1) == 1
    assert c.convert_i64_to_u64(c.MAX_INT64) == c.MAX_INT64
    assert c.convert_i64_to_u64(-1) == c.MAX_UINT64
    assert c.convert_i64_to_u64(c.MIN_INT64) == c.MAX_INT64 + 1
    with pytest.raises(ValueError):
        c.convert_i64_to_u64(c.MAX_INT64 + 1)
    with pytest.raises(ValueError):
        c.convert_i64_to_u64(c.MIN_INT64 - 1)


def test_convert_u64_to_i64():
    assert c.convert_u64_to_i64(0) == 0
    assert c.convert_u64_to_i64(1) == 1
    assert c.convert_u64_to_i64(c.MAX_INT64) == c.MAX_INT64
    assert c.convert_u64_to_i64(c.MAX_UINT64) == -1
    assert c.convert_u64_to_i64(c.MAX_INT64 + 1) == c.MIN_INT64
    with pytest.raises(ValueError):
        c.convert_u64_to_i64(-1)
    with pytest.raises(ValueError):
        c.convert_u64_to_i64(c.MAX_UINT64 + 1)


def test_werkzeug_converter():
    from werkzeug.routing import Map, Rule
    from werkzeug.exceptions import NotFound

    m = Map([
        Rule('/debtors/<i64:debtorId>', endpoint='debtors'),
    ], converters={'i64': c.Int64Converter})
    urls = m.bind('example.com', '/')

    # Test URL match:
    assert urls.match('/debtors/0', 'GET') == ('debtors', {'debtorId': 0})
    assert urls.match('/debtors/1', 'GET') == ('debtors', {'debtorId': 1})
    assert urls.match('/debtors/9223372036854775807', 'GET') == ('debtors', {'debtorId': 9223372036854775807})
    assert urls.match('/debtors/9223372036854775808', 'GET') == ('debtors', {'debtorId': -9223372036854775808})
    assert urls.match('/debtors/18446744073709551615', 'GET') == ('debtors', {'debtorId': -1})
    with pytest.raises(NotFound):
        assert urls.match('/debtors/1x', 'GET')
    with pytest.raises(NotFound):
        assert urls.match('/debtors/18446744073709551616', 'GET')
    with pytest.raises(NotFound):
        assert urls.match('/debtors/-1', 'GET')

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
