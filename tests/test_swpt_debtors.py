from datetime import date
from swpt_debtors import __version__
from swpt_debtors.models import Limit
from swpt_debtors import procedures as p

A_DATE = date(1900, 1, 1)


def test_version(db_session):
    assert __version__


def test_add_limit_to_list():
    limits1 = [
        Limit(10, A_DATE, date(2000, 1, 1)),
        Limit(20, A_DATE, date(2000, 1, 2)),
    ]
    limits2 = p._add_limit_to_list(limits1, Limit(30, A_DATE, date(2000, 1, 3)), upper_limit=True)
    assert [l.value for l in limits2] == [10, 20, 30]
    limits3 = p._add_limit_to_list(limits2, Limit(25, A_DATE, date(2000, 1, 4)), upper_limit=True)
    assert [l.value for l in limits3] == [10, 20, 25]
    limits4 = p._add_limit_to_list(limits3, Limit(30, A_DATE, date(2000, 1, 3)), upper_limit=True)
    assert [l.value for l in limits4] == [10, 20, 25]
