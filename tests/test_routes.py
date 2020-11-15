import pytest
import iso8601
from swpt_debtors import procedures as p


@pytest.fixture(scope='function')
def client(app, db_session):
    return app.test_client()


@pytest.fixture(scope='function')
def debtor(db_session):
    return p.lock_or_create_debtor(123)


def test_create_debtor(client):
    r = client.get('/debtors/123/')
    assert r.status_code == 404

    r = client.post('/debtors/123/', json={})
    assert r.status_code == 201
    assert r.headers['Location'] == 'http://example.com/debtors/123/'
    data = r.get_json()
    assert data['balance'] == 0
    assert data['isActive'] is False
    assert data['type'] == 'Debtor'
    assert data['uri'] == '/debtors/123/'

    r = client.post('/debtors/123/', json={})
    assert r.status_code == 409

    r = client.get('/debtors/123/')
    assert r.status_code == 200
    assert 'max-age' in r.headers['Cache-Control']
    data = r.get_json()
    assert data['balance'] == 0
    assert data['isActive'] is False
    assert data['type'] == 'Debtor'
    assert data['uri'] == '/debtors/123/'


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
    assert data['type'] == 'TransfersCollection'
    assert data['uri'] == '/debtors/123/transfers/'
    assert data['totalItems'] == 0
    assert data['items'] == []

    json_request_body = {
        'amount': 1000,
        'note': 'test',
        'recipientCreditorId': 1111,
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
    assert data['recipientCreditorId'] == 1111
    assert data['type'] == 'Transfer'
    assert data['uri'] == '/debtors/123/transfers/123e4567-e89b-12d3-a456-426655440000'
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
    assert data['totalItems'] == 1
    assert sorted(data['items']) == [
        '123e4567-e89b-12d3-a456-426655440000',
    ]

    r = client.delete('/debtors/123/transfers/123e4567-e89b-12d3-a456-426655440001')
    assert r.status_code == 204

    r = client.get('/debtors/123/transfers/')
    assert r.status_code == 200
    data = r.get_json()
    assert data['totalItems'] == 1
    assert sorted(data['items']) == [
        '123e4567-e89b-12d3-a456-426655440000',
    ]

    for i in range(2, 12):
        suffix = '{:0>4}'.format(i)
        json_request_body = {
            'amount': 1,
            'recipientCreditorId': 1111,
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
        'recipientCreditorId': 1111,
        'transferUuid': '123e4567-e89b-12d3-a456-426655440000',
    }
    r = client.post('/debtors/123/transfers/', json=json_request_body)
    assert r.status_code == 201
    data = r.get_json()
    assert data['isFinalized'] is False

    r = client.patch('/debtors/123/transfers/123e4567-e89b-12d3-a456-426655440001', json={
        'isFinalized': True, 'isSuccessful': False})
    assert r.status_code == 404

    r = client.patch('/debtors/123/transfers/123e4567-e89b-12d3-a456-426655440000', json={
        'isFinalized': True, 'isSuccessful': False})
    assert r.status_code == 200

    r = client.get('/debtors/123/transfers/123e4567-e89b-12d3-a456-426655440000')
    assert r.status_code == 200
    data = r.get_json()
    assert data['isFinalized'] is True
    assert data['isSuccessful'] is False

    r = client.patch('/debtors/123/transfers/123e4567-e89b-12d3-a456-426655440000', json={
        'isFinalized': False, 'isSuccessful': False})
    assert r.status_code == 409
