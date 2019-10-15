from datetime import date
from swpt_debtors import __version__
from swpt_debtors.models import Limit
from swpt_debtors import procedures as p

A_DATE = date(1900, 1, 1)


def test_version(db_session):
    assert __version__


def test_add_limit_to_list_upper():
    limits = []
    p._add_limit_to_list(limits, Limit(10, A_DATE, date(2000, 1, 1)), upper_limit=True)
    p._add_limit_to_list(limits, Limit(20, A_DATE, date(2000, 1, 2)), upper_limit=True)
    p._add_limit_to_list(limits, Limit(30, A_DATE, date(2000, 1, 3)), upper_limit=True)
    assert [l.value for l in limits] == [10, 20, 30]
    p._add_limit_to_list(limits, Limit(25, A_DATE, date(2000, 1, 4)), upper_limit=True)
    assert [l.value for l in limits] == [10, 20, 25]
    p._add_limit_to_list(limits, Limit(30, A_DATE, date(2000, 1, 3)), upper_limit=True)
    assert [l.value for l in limits] == [10, 20, 25]

    # Add an already existing limit.
    p._add_limit_to_list(limits, Limit(30, A_DATE, date(2000, 1, 3)), upper_limit=True)
    assert [l.value for l in limits] == [10, 20, 25]


def test_add_limit_to_list_lower():
    limits = []
    p._add_limit_to_list(limits, Limit(30, A_DATE, date(2000, 1, 1)), lower_limit=True)
    p._add_limit_to_list(limits, Limit(20, A_DATE, date(2000, 1, 2)), lower_limit=True)
    p._add_limit_to_list(limits, Limit(10, A_DATE, date(2000, 1, 3)), lower_limit=True)
    assert [l.value for l in limits] == [30, 20, 10]
    p._add_limit_to_list(limits, Limit(25, A_DATE, date(2000, 1, 4)), lower_limit=True)
    assert [l.value for l in limits] == [30, 25]
    assert [l.cutoff for l in limits] == [date(2000, 1, 1), date(2000, 1, 4)]
    p._add_limit_to_list(limits, Limit(30, A_DATE, date(2000, 1, 3)), lower_limit=True)
    assert [l.value for l in limits] == [30, 25]
    assert [l.cutoff for l in limits] == [date(2000, 1, 3), date(2000, 1, 4)]

    # Add an already existing limit.
    p._add_limit_to_list(limits, Limit(30, A_DATE, date(2000, 1, 3)), lower_limit=True)
    assert [l.value for l in limits] == [30, 25]
