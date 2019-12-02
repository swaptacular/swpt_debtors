from datetime import date
from swpt_debtors.lower_limits import LowerLimit, LowerLimitSequence


def test_add_limit_to_list():
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


def test_add_limit_to_list_eliminator():
    limits = LowerLimitSequence([
        LowerLimit(10, date(2000, 1, 1)),
        LowerLimit(20, date(2000, 1, 2)),
        LowerLimit(30, date(2000, 1, 3)),
    ])
    assert [l.value for l in limits] == [10, 20, 30]
    limits.add_limit(LowerLimit(10, date(2000, 1, 1)))
    assert [l.value for l in limits] == [30]
