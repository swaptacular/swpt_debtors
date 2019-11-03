from datetime import date
from swpt_debtors.models import Limit, LimitSequence, Debtor

A_DATE = date(1900, 1, 1)


def test_limit_properties(db_session):
    lower_limits = LimitSequence([
        Limit(10, A_DATE, date(2000, 1, 1)),
        Limit(20, A_DATE, date(2000, 1, 2)),
    ], lower_limits=True)
    upper_limits = LimitSequence([
        Limit(10, A_DATE, date(2000, 1, 1)),
        Limit(20, A_DATE, date(2000, 1, 2)),
    ], upper_limits=True)
    d = Debtor(debtor_id=1)
    assert len(d.balance_lower_limits) == 0
    assert len(d.interest_rate_lower_limits) == 0
    assert len(d.interest_rate_upper_limits) == 0
    d.balance_lower_limits = lower_limits
    d.interest_rate_lower_limits = lower_limits
    d.interest_rate_upper_limits = upper_limits
    assert d.balance_lower_limits == lower_limits
    assert d.interest_rate_lower_limits == lower_limits
    assert d.interest_rate_upper_limits == upper_limits
    db_session.add(d)
    db_session.commit()

    # Set to an empty list.
    d = Debtor.get_instance(1)
    assert d.balance_lower_limits == lower_limits
    assert d.bll_values is not None
    assert d.bll_kickoffs is not None
    assert d.bll_cutoffs is not None
    d.balance_lower_limits = LimitSequence(lower_limits=True)
    assert d.bll_values is None
    assert d.bll_kickoffs is None
    assert d.bll_cutoffs is None

    # Purge expired.
    lower_limits.purge_expired(date(1999, 1, 2))
    assert len(lower_limits) == 2
    lower_limits.purge_expired(date(2000, 1, 2))
    assert len(lower_limits) == 1
    assert len(upper_limits) == 2
    upper_limits.purge_expired(date(2010, 1, 2))
    assert len(upper_limits) == 0
