from datetime import date
from swpt_debtors.lower_limits import LowerLimit, LowerLimitSequence
from swpt_debtors.models import Debtor


def test_limit_properties(db_session):
    lower_limits = LowerLimitSequence([
        LowerLimit(10, date(2000, 1, 1)),
        LowerLimit(20, date(2000, 1, 2)),
    ])
    assert len(lower_limits.current_limits(date(2000, 1, 1))) == 2
    assert len(lower_limits.current_limits(date(2000, 1, 2))) == 1
    assert len(lower_limits.current_limits(date(2000, 1, 3))) == 0
    assert lower_limits.current_limits(date(2000, 1, 1)).apply_to_value(0) == 20
    assert lower_limits.current_limits(date(2000, 1, 2)).apply_to_value(0) == 20
    assert lower_limits.current_limits(date(2000, 1, 3)).apply_to_value(0) == 0

    d = Debtor(debtor_id=1)
    assert len(d.balance_lower_limits) == 0
    assert len(d.interest_rate_lower_limits) == 0
    d.balance_lower_limits = lower_limits
    d.interest_rate_lower_limits = lower_limits
    assert list(d.balance_lower_limits) == list(lower_limits)
    assert list(d.interest_rate_lower_limits) == list(lower_limits)
    db_session.add(d)
    db_session.commit()

    # Set to an empty list.
    d = Debtor.get_instance(1)
    assert list(d.balance_lower_limits) == list(lower_limits)
    assert d.bll_values is not None
    assert d.bll_cutoffs is not None
    d.balance_lower_limits = LowerLimitSequence()
    assert d.bll_values is None
    assert d.bll_cutoffs is None
