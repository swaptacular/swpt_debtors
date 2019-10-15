from swpt_debtors import actors as a

D_ID = -1


def test_create_debtor(db_session):
    a.create_debtor(
        debtor_id=D_ID,
    )


def test_terminate_debtor(db_session):
    a.terminate_debtor(
        debtor_id=D_ID,
    )


def test_update_debtor_balance(db_session):
    a.update_debtor_balance(
        debtor_id=D_ID,
        balance=0,
        status=0,
        update_seqnum=1234,
        update_ts='2019-10-01T00:00:00Z',
    )
