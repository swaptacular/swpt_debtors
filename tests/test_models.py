from datetime import date
from swpt_debtors.models import Limit, LimitSequence, Debtor


def test_limit_properties(db_session):
    lower_limits = LimitSequence([
        Limit(10, date(2000, 1, 1)),
        Limit(20, date(2000, 1, 2)),
    ], lower_limits=True)
    assert len(lower_limits.current_limits(date(2000, 1, 1))) == 2
    assert len(lower_limits.current_limits(date(2000, 1, 2))) == 1
    assert len(lower_limits.current_limits(date(2000, 1, 3))) == 0
    assert lower_limits.current_limits(date(2000, 1, 1)).apply_to_value(0) == 20
    assert lower_limits.current_limits(date(2000, 1, 2)).apply_to_value(0) == 20
    assert lower_limits.current_limits(date(2000, 1, 3)).apply_to_value(0) == 0
    upper_limits = LimitSequence([
        Limit(10, date(2000, 1, 1)),
        Limit(20, date(2000, 1, 2)),
    ], upper_limits=True)
    assert len(upper_limits.current_limits(date(2000, 1, 1))) == 2
    assert len(upper_limits.current_limits(date(2000, 1, 2))) == 1
    assert len(upper_limits.current_limits(date(2000, 1, 3))) == 0
    assert upper_limits.current_limits(date(2000, 1, 1)).apply_to_value(30) == 10
    assert upper_limits.current_limits(date(2000, 1, 2)).apply_to_value(30) == 20
    assert upper_limits.current_limits(date(2000, 1, 3)).apply_to_value(30) == 30

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
    assert d.bll_cutoffs is not None
    d.balance_lower_limits = LimitSequence(lower_limits=True)
    assert d.bll_values is None
    assert d.bll_cutoffs is None
