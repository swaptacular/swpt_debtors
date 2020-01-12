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


def test_on_account_change_signal(db_session):
    a.on_account_change_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        change_seqnum=0,
        change_ts='2019-10-01T00:00:00Z',
        principal=1000,
        interest=12.5,
        interest_rate=-0.5,
        last_transfer_seqnum=0,
        last_outgoing_transfer_date='2018-10-01',
        creation_date='2018-10-1',
        status=0,
    )


def test_on_prepared_issuing_transfer_signal(db_session):
    a.on_prepared_issuing_transfer_signal(
        debtor_id=D_ID,
        sender_creditor_id=2,
        transfer_id=1,
        coordinator_type='issuing',
        recipient_creditor_id=C_ID,
        sender_locked_amount=1000,
        prepared_at_ts='2019-10-01T00:00:00Z',
        coordinator_id=C_ID,
        coordinator_request_id=1,
    )


def test_on_rejected_issuing_transfer_signal(db_session):
    a.on_rejected_issuing_transfer_signal(
        coordinator_type='issuing',
        coordinator_id=C_ID,
        coordinator_request_id=1,
        details={'error_code': '123456', 'message': 'Oops!'},
    )
