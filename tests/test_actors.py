from swpt_debtors import actors as a

D_ID = -1
C_ID = 1


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
        update_seqnum=1234,
        update_ts='2019-10-01T00:00:00Z',
    )


def test_on_account_change_signal(db_session):
    a.on_account_change_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        change_seqnum=0,
        change_ts='2019-10-01T00:00:00Z',
        principal=1000,
        interest=12.5,
        interest_rate=-0.5,
        last_outgoing_transfer_date='2018-10-01',
        status=0,
    )
