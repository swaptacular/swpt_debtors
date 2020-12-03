from datetime import datetime, timezone
from swpt_debtors import procedures as p
from swpt_debtors import actors as a

D_ID = -1
C_ID = 1


def test_on_rejected_config_signal(db_session):
    a.on_rejected_config_signal(
        debtor_id=D_ID,
        creditor_id=p.ROOT_CREDITOR_ID,
        config_ts='2019-10-01T00:00:00Z',
        config_seqnum=123,
        negligible_amount=p.HUGE_NEGLIGIBLE_AMOUNT,
        config='',
        config_flags=0,
        rejection_code='TEST_REJECTION',
        ts='2019-10-01T00:00:00Z',
    )


def test_on_account_update_signal(db_session):
    a.on_account_update_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        last_change_seqnum=0,
        last_change_ts='2019-10-01T00:00:00Z',
        principal=1000,
        interest=12.5,
        interest_rate=-0.5,
        last_interest_rate_change_ts='1970-01-01T00:00:00+00:00',
        demurrage_rate=-50.0,
        commit_period=30 * 24 * 60 * 60,
        last_transfer_number=0,
        last_transfer_committed_at='1970-01-01T00:00:00+00:00',
        last_config_ts='1970-01-01T00:00:00Z',
        last_config_seqnum=0,
        creation_date='2018-10-1',
        negligible_amount=2.0,
        config='',
        config_flags=0,
        status_flags=0,
        ts=datetime.now(tz=timezone.utc).isoformat(),
        ttl=1000000,
    )


def test_on_prepared_issuing_transfer_signal(db_session):
    a.on_prepared_issuing_transfer_signal(
        debtor_id=D_ID,
        creditor_id=2,
        transfer_id=1,
        coordinator_type='issuing',
        coordinator_id=C_ID,
        coordinator_request_id=1,
        locked_amount=1000,
        recipient=str(C_ID),
        prepared_at='2019-10-01T00:00:00Z',
    )


def test_on_rejected_issuing_transfer_signal(db_session):
    a.on_rejected_issuing_transfer_signal(
        coordinator_type='issuing',
        coordinator_id=C_ID,
        coordinator_request_id=1,
        status_code='TEST',
        total_locked_amount=0,
        debtor_id=D_ID,
        creditor_id=p.ROOT_CREDITOR_ID,
    )


def test_on_finalized_issuing_transfer_signal(db_session):
    a.on_finalized_issuing_transfer_signal(
        debtor_id=D_ID,
        creditor_id=p.ROOT_CREDITOR_ID,
        transfer_id=123,
        coordinator_type='issuing',
        coordinator_id=D_ID,
        coordinator_request_id=678,
        recipient='1235',
        prepared_at='2019-10-01T00:00:00Z',
        ts='2019-10-01T00:00:00Z',
        committed_amount=100,
        status_code='OK',
        total_locked_amount=0,
    )


def test_on_account_purge_signal(db_session):
    a.on_account_purge_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        creation_date='2019-10-01',
    )


def test_on_account_maintenance_signal(db_session):
    a.on_account_maintenance_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        request_ts='2019-10-01T00:00:00Z',
    )
