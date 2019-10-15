from datetime import date
from swpt_debtors.models import Limit, Debtor

A_DATE = date(1900, 1, 1)


def test_limit_properties(db_session):
    limits = [
        Limit(10, A_DATE, date(2000, 1, 1)),
        Limit(20, A_DATE, date(2000, 1, 2)),
    ]
    d = Debtor(debtor_id=1)
    assert d.balance_lower_limits == []
    assert d.interest_rate_lower_limits == []
    assert d.interest_rate_upper_limits == []
    d.balance_lower_limits = limits
    d.interest_rate_lower_limits = limits
    d.interest_rate_upper_limits = limits
    assert d.balance_lower_limits == limits
    assert d.interest_rate_lower_limits == limits
    assert d.interest_rate_upper_limits == limits
    db_session.add(d)
    db_session.commit()
