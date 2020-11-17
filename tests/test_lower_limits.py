import pytest
from datetime import date, timedelta
from swpt_debtors.lower_limits import LowerLimit, LowerLimitSequence, TooLongLimitSequence


def test_add_limit_to_list(app):
    limits = LowerLimitSequence()
    limits.add_limit(LowerLimit(30, date(2000, 1, 1)))
    limits.add_limit(LowerLimit(20, date(2000, 1, 2)))
    limits.add_limit(LowerLimit(10, date(2000, 1, 3)))
    assert [l.value for l in limits] == [30, 20, 10]
    limits.add_limit(LowerLimit(25, date(2000, 1, 4)))
    assert [l.value for l in limits] == [30, 25]
    assert [l.cutoff for l in limits] == [date(2000, 1, 1), date(2000, 1, 4)]
    limits.add_limit(LowerLimit(30, date(2000, 1, 3)))
    assert [l.value for l in limits] == [30, 25]
    assert [l.cutoff for l in limits] == [date(2000, 1, 3), date(2000, 1, 4)]

    # Add an already existing limit.
    limits.add_limit(LowerLimit(30, date(2000, 1, 3)))
    assert [l.value for l in limits] == [30, 25]


def test_add_limit_max_count(app):
    today = date(2000, 1, 1)
    limits = LowerLimitSequence()
    to_add = [LowerLimit(-i, today + timedelta(days=i)) for i in range(11)]
    for limit in to_add[:10]:
        limits.add_limit(limit)
    with pytest.raises(TooLongLimitSequence):
        limits.add_limit(to_add[10])


def test_add_limit_to_list_eliminator(app):
    limits = LowerLimitSequence([
        LowerLimit(10, date(2000, 1, 1)),
        LowerLimit(20, date(2000, 1, 2)),
        LowerLimit(30, date(2000, 1, 3)),
    ])
    assert [limit.value for limit in limits] == [10, 20, 30]
    limits.add_limit(LowerLimit(10, date(2000, 1, 1)))
    assert [l.value for l in limits] == [30]


def test_repr(app):
    limits = LowerLimitSequence()
    assert str(limits) == 'LowerLimitSequence([])'


def test_add_limits(app):
    limits = LowerLimitSequence()
    today = date(2000, 1, 1)
    to_add = [LowerLimit(-i, today + timedelta(days=i)) for i in range(11)]
    with pytest.raises(TooLongLimitSequence):
        limits.add_limits(to_add)

    limits.add_limits(to_add[:10])
    assert list(limits) == to_add[:10]
    limits.add_limits(to_add[5:10])
    assert list(limits) == to_add[:10]
    limits.add_limits(to_add[:5])
    assert list(limits) == to_add[:10]

    with pytest.raises(TooLongLimitSequence):
        limits.add_limits([to_add[10]])

    # Add eliminating limit
    future = date(2100, 1, 1)
    limits.add_limits([LowerLimit(1, future)])
    assert list(limits) == [LowerLimit(1, future)]
