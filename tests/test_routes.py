from urllib.parse import urljoin, urlparse
import pytest
import iso8601
from swpt_debtors import procedures as p


@pytest.fixture(scope='function')
def client(app, db_session):
    return app.test_client()


@pytest.fixture(scope='function')
def debtor(db_session):
    return p.lock_or_create_debtor(123)


def _get_all_pages(client, url, page_type, streaming=False):
    r = client.get(url)
    assert r.status_code == 200

    data = r.get_json()
    assert data['type'] == page_type
    assert urlparse(data['uri']) == urlparse(url)
    if streaming:
        assert 'next' in data or 'forthcoming' in data
        assert 'next' not in data or 'forthcoming' not in data
    else:
        assert 'forthcoming' not in data

    items = data['items']
    assert isinstance(items, list)

    if 'next' in data:
        items.extend(_get_all_pages(client, urljoin(url, data['next']), page_type, streaming))

    return items


def test_auto_genereate_creditor_id(client):
    r = client.post('/debtors/.debtor-reserve', json={})
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'DebtorReservation'
    assert isinstance(data['debtorId'], int)
    assert isinstance(data['reservationId'], int)
    assert iso8601.parse_date(data['validUntil'])
    assert iso8601.parse_date(data['createdAt'])


def test_create_debtor(client):
    r = client.get('/debtors/2/')
    assert r.status_code == 403

    r = client.post('/debtors/2/reserve', headers={'X-Swpt-User-Id': 'creditors:2'}, json={})
    assert r.status_code == 403

    r = client.post('/debtors/2/reserve', json={})
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'DebtorReservation'
    assert data['debtorId'] == 2
    assert isinstance(data['reservationId'], int)
    assert iso8601.parse_date(data['validUntil'])
    assert iso8601.parse_date(data['createdAt'])
    reservation_id = data['reservationId']

    r = client.post('/debtors/2/reserve', json={})
    assert r.status_code == 409

    r = client.get('/debtors/2/')
    assert r.status_code == 403

    r = client.post('/debtors/2/activate', json={
        'reservationId': 123,
    })
    assert r.status_code == 422
    assert 'reservationId' in r.get_json()['errors']['json']

    r = client.post('/debtors/2/activate', json={
        'reservationId': reservation_id,
    })
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'Debtor'
    assert data['uri'] == '/debtors/2/'
    assert iso8601.parse_date(data['createdAt'])

    r = client.post('/debtors/2/activate', json={
        'reservationId': reservation_id,
    })
    assert r.status_code == 200

    r = client.post('/debtors/3/activate', json={
        'reservationId': 123,
    })
    assert r.status_code == 422
    assert 'reservationId' in r.get_json()['errors']['json']

    r = client.post('/debtors/3/activate', json={})
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'Debtor'
    assert data['uri'] == '/debtors/3/'
    assert data['balance'] == 0
    assert iso8601.parse_date(data['createdAt'])

    r = client.post('/debtors/3/activate', json={})
    assert r.status_code == 409

    r = client.get('/debtors/2/')
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'Debtor'
    assert data['uri'] == '/debtors/2/'
    assert data['balance'] == 0
    assert iso8601.parse_date(data['createdAt'])

    r = client.get('/debtors/3/')
    assert r.status_code == 200
    assert 'max-age' in r.headers['Cache-Control']

    r = client.post('/debtors/3/deactivate', headers={'X-Swpt-User-Id': 'debtors:3'}, json={})
    assert r.status_code == 403

    r = client.post('/debtors/3/deactivate', headers={'X-Swpt-User-Id': 'debtors-supervisor'}, json={})
    assert r.status_code == 403

    r = client.post('/debtors/3/deactivate', headers={'X-Swpt-User-Id': 'debtors-superuser'}, json={})
    assert r.status_code == 204

    r = client.post('/debtors/3/deactivate', json={})
    assert r.status_code == 204

    r = client.get('/debtors/3/')
    assert r.status_code == 403

    r = client.post('/debtors/3/deactivate', json={})
    assert r.status_code == 204


def test_get_debtors_list(client):
    r = client.post('/debtors/1/reserve', json={})
    assert r.status_code == 200
    r = client.post('/debtors/2/activate', json={})
    assert r.status_code == 200
    r = client.post('/debtors/3/activate', json={})
    assert r.status_code == 200
    r = client.post('/debtors/9223372036854775808/activate', json={})
    assert r.status_code == 200
    r = client.post('/debtors/18446744073709551615/activate', json={})
    assert r.status_code == 200

    r = client.get('/debtors/.list')
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'DebtorsList'
    assert data['uri'] == '/debtors/.list'
    assert data['itemsType'] == 'ObjectReference'
    assert data['first'] == '/debtors/9223372036854775808/enumerate'

    entries = _get_all_pages(client, data['first'], page_type='ObjectReferencesPage')
    assert entries == [
        {'uri': '/debtors/9223372036854775808/'},
        {'uri': '/debtors/18446744073709551615/'},
        {'uri': '/debtors/2/'},
        {'uri': '/debtors/3/'},
    ]


def test_change_debtor_policy(client, debtor):
    r = client.get('/debtors/123/policy')
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'DebtorPolicy'
    assert data['uri'] == '/debtors/123/policy'
    assert data['interestRateTarget'] == 0.0
    assert data['interestRateLowerLimits'] == []
    assert data['balanceLowerLimits'] == []

    r = client.patch('/debtors/123/policy', json={
        'interestRateLowerLimits': [
            {'type': 'InterestRateLowerLimit', 'enforcedUntil': '2100-12-31', 'value': -10.0},
            {'type': 'InterestRateLowerLimit', 'enforcedUntil': '2050-12-31', 'value': 0.0},
        ],
        'balanceLowerLimits': [
            {'type': 'BalanceLowerLimit', 'enforcedUntil': '2100-12-31', 'value': -1000},
            {'type': 'BalanceLowerLimit', 'enforcedUntil': '2050-12-31', 'value': -500},
        ],
    })
    assert r.status_code == 200

    r = client.get('/debtors/123/policy')
    assert r.status_code == 200
    data = r.get_json()
    assert data['interestRateTarget'] == 0.0
    assert data['interestRateLowerLimits'] == [
        {'type': 'InterestRateLowerLimit', 'enforcedUntil': '2050-12-31', 'value': 0.0},
        {'type': 'InterestRateLowerLimit', 'enforcedUntil': '2100-12-31', 'value': -10.0},
    ]
    assert data['balanceLowerLimits'] == [
        {'type': 'BalanceLowerLimit', 'enforcedUntil': '2050-12-31', 'value': -500},
        {'type': 'BalanceLowerLimit', 'enforcedUntil': '2100-12-31', 'value': -1000},
    ]

    r = client.patch('/debtors/123/policy', json={
        'interestRateTarget': 5.0,
        'balanceLowerLimits': [
            {'type': 'BalanceLowerLimit', 'enforcedUntil': '2030-12-31', 'value': -200},
        ],
    })
    assert r.status_code == 200
    data = r.get_json()
    assert data['interestRateTarget'] == 5.0
    assert data['interestRateLowerLimits'] == [
        {'type': 'InterestRateLowerLimit', 'enforcedUntil': '2050-12-31', 'value': 0.0},
        {'type': 'InterestRateLowerLimit', 'enforcedUntil': '2100-12-31', 'value': -10.0},
    ]
    assert data['balanceLowerLimits'] == [
        {'type': 'BalanceLowerLimit', 'enforcedUntil': '2030-12-31', 'value': -200},
        {'type': 'BalanceLowerLimit', 'enforcedUntil': '2050-12-31', 'value': -500},
        {'type': 'BalanceLowerLimit', 'enforcedUntil': '2100-12-31', 'value': -1000},
    ]

    r = client.patch('/debtors/666/policy', json={})
    assert r.status_code == 404

    r = client.patch('/debtors/123/policy', json={})
    assert r.status_code == 200
    data = r.get_json()
    assert data['interestRateTarget'] == 5.0

    r = client.patch('/debtors/123/policy', json={
        'balanceLowerLimits': 100 * [
            {'type': 'BalanceLowerLimit', 'enforcedUntil': '2030-12-31', 'value': -200},
        ],
    })
    assert r.status_code == 409

    for _ in range(7):
        r = client.patch('/debtors/123/policy', json={})
        assert r.status_code == 200
    r = client.patch('/debtors/123/policy', json={})
    assert r.status_code == 403


def test_initiate_transfer(client, debtor):
    r = client.get('/debtors/666/transfers/')
    assert r.status_code == 404

    r = client.get('/debtors/123/transfers/')
    assert r.status_code == 200
    data = r.get_json()
    assert data['debtor'] == {'uri': '/debtors/123/'}
    assert data['type'] == 'TransfersList'
    assert data['uri'] == '/debtors/123/transfers/'
    assert data['items'] == []

    json_request_body = {
        'amount': 1000,
        'noteFormat': 'fmt',
        'note': 'test',
        'creditorId': 1111,
        'transferUuid': '123e4567-e89b-12d3-a456-426655440000',
    }
    r = client.post('/debtors/123/transfers/', json=json_request_body)
    assert r.status_code == 201
    data = r.get_json()
    assert data['amount'] == 1000
    assert iso8601.parse_date(data['initiatedAt'])
    assert iso8601.parse_date(data['finalizedAt'])
    assert data['isFinalized'] is False
    assert data['errors'] == []
    assert data['creditorId'] == 1111
    assert data['type'] == 'Transfer'
    assert data['uri'] == '/debtors/123/transfers/123e4567-e89b-12d3-a456-426655440000'
    assert data['noteFormat'] == 'fmt'
    assert data['note'] == 'test'
    assert data['debtor'] == {'uri': '/debtors/123/'}
    assert data['isSuccessful'] is False
    assert r.headers['Location'] == 'http://example.com/debtors/123/transfers/123e4567-e89b-12d3-a456-426655440000'

    r = client.get('/debtors/123/transfers/123e4567-e89b-12d3-a456-426655440000')
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'Transfer'
    assert data['uri'] == '/debtors/123/transfers/123e4567-e89b-12d3-a456-426655440000'
    assert data['amount'] == 1000

    r = client.post('/debtors/123/transfers/', json=json_request_body)
    assert r.status_code == 303
    assert r.headers['Location'] == 'http://example.com/debtors/123/transfers/123e4567-e89b-12d3-a456-426655440000'

    json_request_body['amount'] += 1
    r = client.post('/debtors/123/transfers/', json=json_request_body)
    assert r.status_code == 409

    r = client.post('/debtors/555/transfers/', json=json_request_body)
    assert r.status_code == 404

    r = client.get('/debtors/123/transfers/')
    assert r.status_code == 200
    data = r.get_json()
    assert sorted(data['items']) == [
        {'uri': '123e4567-e89b-12d3-a456-426655440000'},
    ]

    r = client.delete('/debtors/123/transfers/123e4567-e89b-12d3-a456-426655440001')
    assert r.status_code == 204

    r = client.get('/debtors/123/transfers/')
    assert r.status_code == 200
    data = r.get_json()
    assert sorted(data['items']) == [
        {'uri': '123e4567-e89b-12d3-a456-426655440000'},
    ]

    for i in range(2, 12):
        suffix = '{:0>4}'.format(i)
        json_request_body = {
            'amount': 1,
            'creditorId': 1111,
            'transferUuid': f'123e4567-e89b-12d3-a456-42665544{suffix}',
        }
        r = client.post('/debtors/123/transfers/', json=json_request_body)
        if i == 11:
            assert r.status_code == 403
        else:
            assert r.status_code == 201


def test_cancel_transfer(client, debtor):
    json_request_body = {
        'amount': 1000,
        'note': 'test',
        'creditorId': 1111,
        'transferUuid': '123e4567-e89b-12d3-a456-426655440000',
    }
    r = client.post('/debtors/123/transfers/', json=json_request_body)
    assert r.status_code == 201
    data = r.get_json()
    assert data['isFinalized'] is False

    r = client.post('/debtors/123/transfers/123e4567-e89b-12d3-a456-426655440001', json={})
    assert r.status_code == 404

    r = client.post('/debtors/123/transfers/123e4567-e89b-12d3-a456-426655440000', json={})
    assert r.status_code == 200

    r = client.get('/debtors/123/transfers/123e4567-e89b-12d3-a456-426655440000')
    assert r.status_code == 200
    data = r.get_json()
    assert data['isFinalized'] is True
    assert data['isSuccessful'] is False

    r = client.post('/debtors/123/transfers/123e4567-e89b-12d3-a456-426655440000', json={})
    assert r.status_code == 200
