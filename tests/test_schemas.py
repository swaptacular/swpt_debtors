from datetime import date
from swpt_debtors import schemas
from swpt_debtors.lower_limits import LowerLimit


def test_interest_rate_lower_limit_schema():
    s = schemas.InterestRateLowerLimitSchema()
    data = s.load({'value': 5.6, 'enforcedUntil': '2020-10-25'})
    assert isinstance(data, LowerLimit)
    assert data.value == 5.6
    assert data.cutoff == date(2020, 10, 25)
    assert s.dump(data) == {'value': 5.6, 'enforcedUntil': '2020-10-25'}


def test_balance_lower_limit_schema():
    s = schemas.BalanceLowerLimitSchema()
    data = s.load({'value': 1000, 'enforcedUntil': '2020-10-25'})
    assert isinstance(data, LowerLimit)
    assert data.value == 1000
    assert data.cutoff == date(2020, 10, 25)
    assert s.dump(data) == {'value': 1000, 'enforcedUntil': '2020-10-25'}


def test_debtor_policy_update_request_schema():
    s = schemas.DebtorPolicyUpdateRequestSchema()
    data = s.load({})
    assert data['balance_lower_limits'] == []
    assert data['interest_rate_lower_limits'] == []
    assert data['interest_rate_target'] is None
    data = s.load({
        'balanceLowerLimits': [{'value': 1000, 'enforcedUntil': '2020-10-25'}],
        'interestRateLowerLimits': [{'value': 5.6, 'enforcedUntil': '2020-10-25'}],
        'interestRateTarget': 6.1,
    })
    assert len(data['balance_lower_limits']) == 1
    assert data['balance_lower_limits'][0].value == 1000
    assert len(data['interest_rate_lower_limits']) == 1
    assert data['interest_rate_lower_limits'][0].value == 5.6
    assert data['interest_rate_target'] == 6.1
