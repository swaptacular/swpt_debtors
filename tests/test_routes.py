import pytest
from swpt_debtors import procedures as p


@pytest.fixture(scope='function')
def client(app, db_session):
    return app.test_client()


@pytest.fixture(scope='function')
def debtor(db_session):
    return p.get_or_create_debtor(123)


def test_create_debtor(client):
    r = client.get('/debtors/123')
    assert r.status_code == 404

    r = client.post('/debtors/123', json={})
    assert r.status_code == 201
    assert r.headers['Location'] == 'http://example.com/debtors/123'
    data = r.get_json()
    assert data['balance'] == 0
    assert data['isActive'] is False
    assert data['type'] == 'Debtor'
    assert data['uri'] == 'http://example.com/debtors/123'

    r = client.post('/debtors/123', json={})
    assert r.status_code == 409

    r = client.get('/debtors/123')
    assert r.status_code == 200
    data = r.get_json()
    assert data['balance'] == 0
    assert data['isActive'] is False
    assert data['type'] == 'Debtor'
    assert data['uri'] == 'http://example.com/debtors/123'


def test_change_debtor_policy(client, debtor):
    r = client.get('/debtors/123/policy')
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'DebtorPolicy'
    assert data['uri'] == 'http://example.com/debtors/123/policy'
    assert data['interestRateTarget'] == 0.0
    assert data['interestRateLowerLimits'] == []
    assert data['balanceLowerLimits'] == []

    r = client.patch('/debtors/123/policy', json={
        'interestRateTarget': None,
        'interestRateLowerLimits': [
            {'enforcedUntil': '2100-12-31', 'value': -10.0},
            {'enforcedUntil': '2050-12-31', 'value': 0.0},
        ],
        'balanceLowerLimits': [
            {'enforcedUntil': '2100-12-31', 'value': -1000},
            {'enforcedUntil': '2050-12-31', 'value': -500},
        ],
    })
    assert r.status_code == 200

    r = client.get('/debtors/123/policy')
    assert r.status_code == 200
    data = r.get_json()
    assert data['interestRateTarget'] == 0.0
    assert data['interestRateLowerLimits'] == [
        {'enforcedUntil': '2050-12-31', 'value': 0.0},
        {'enforcedUntil': '2100-12-31', 'value': -10.0},
    ]
    assert data['balanceLowerLimits'] == [
        {'enforcedUntil': '2050-12-31', 'value': -500},
        {'enforcedUntil': '2100-12-31', 'value': -1000},
    ]

    r = client.patch('/debtors/123/policy', json={
        'interestRateTarget': 5.0,
        'balanceLowerLimits': [
            {'enforcedUntil': '2030-12-31', 'value': -200},
        ],
    })
    assert r.status_code == 200
    data = r.get_json()
    assert data['interestRateTarget'] == 5.0
    assert data['interestRateLowerLimits'] == [
        {'enforcedUntil': '2050-12-31', 'value': 0.0},
        {'enforcedUntil': '2100-12-31', 'value': -10.0},
    ]
    assert data['balanceLowerLimits'] == [
        {'enforcedUntil': '2030-12-31', 'value': -200},
        {'enforcedUntil': '2050-12-31', 'value': -500},
        {'enforcedUntil': '2100-12-31', 'value': -1000},
    ]

    r = client.patch('/debtors/666/policy', json={})
    assert r.status_code == 404

    r = client.patch('/debtors/123/policy', json={})
    assert r.status_code == 200
    data = r.get_json()
    assert data['interestRateTarget'] == 5.0

    r = client.patch('/debtors/123/policy', json={
        'balanceLowerLimits': 100 * [
            {'enforcedUntil': '2030-12-31', 'value': -200},
        ],
    })
    assert r.status_code == 409
