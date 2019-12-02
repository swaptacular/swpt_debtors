from datetime import date
from swpt_debtors import schemas
from swpt_debtors.lower_limits import LowerLimit


def test_interest_rate_lower_limit():
    s = schemas.InterestRateLowerLimitSchema()
    data = s.load({'value': 5.6, 'enforcedUntil': '2020-10-25'})
    assert isinstance(data, LowerLimit)
    assert data.value == 5.6
    assert data.cutoff == date(2020, 10, 25)
    assert s.dump(data) == {'value': 5.6, 'enforcedUntil': '2020-10-25'}


def test_balance_lower_limit():
    s = schemas.BalanceLowerLimitSchema()
    data = s.load({'value': 1000, 'enforcedUntil': '2020-10-25'})
    assert isinstance(data, LowerLimit)
    assert data.value == 1000
    assert data.cutoff == date(2020, 10, 25)
    assert s.dump(data) == {'value': 1000, 'enforcedUntil': '2020-10-25'}
