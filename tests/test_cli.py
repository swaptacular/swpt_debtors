from uuid import UUID
from datetime import datetime, timezone
from swpt_debtors.models import RunningTransfer, Account
from swpt_debtors.extensions import db

TEST_UUID = UUID('123e4567-e89b-12d3-a456-426655440000')


def test_collect_running_transfers(app_unsafe_session):
    app = app_unsafe_session
    running_transfer = RunningTransfer(
        debtor_id=1,
        transfer_uuid=TEST_UUID,
        recipient_creditor_id=1111,
        amount=1500,
        finalized_at_ts=datetime(2000, 1, 1, tzinfo=timezone.utc)
    )
    db.session.add(running_transfer)
    db.session.commit()
    db.engine.execute('ANALYZE running_transfer')
    assert len(RunningTransfer.query.all()) == 1
    runner = app.test_cli_runner()
    result = runner.invoke(args=['swpt_debtors', 'collect_running_transfers', '--quit-early'])
    assert result.exit_code == 0
    assert len(RunningTransfer.query.all()) == 0


def test_purge_deleted_account(app_unsafe_session):
    from swpt_debtors.models import PurgeDeletedAccountSignal

    past_ts = datetime(1900, 1, 1, tzinfo=timezone.utc)
    future_ts = datetime(2100, 1, 1, tzinfo=timezone.utc)
    app = app_unsafe_session
    Account.query.delete()
    PurgeDeletedAccountSignal.query.delete()
    db.session.commit()
    db.session.add(Account(
        debtor_id=1,
        creditor_id=2,
        change_seqnum=0,
        change_ts=past_ts,
        principal=0,
        interest=0.0,
        interest_rate=0.0,
        last_outgoing_transfer_date=past_ts,
        status=Account.STATUS_DELETED_FLAG | Account.STATUS_SCHEDULED_FOR_DELETION_FLAG,
    ))
    db.session.add(Account(
        debtor_id=11,
        creditor_id=22,
        change_seqnum=0,
        change_ts=future_ts,
        principal=0,
        interest=0.0,
        interest_rate=0.0,
        last_outgoing_transfer_date=past_ts,
        status=Account.STATUS_DELETED_FLAG | Account.STATUS_SCHEDULED_FOR_DELETION_FLAG,
    ))
    db.session.add(Account(
        debtor_id=111,
        creditor_id=222,
        change_seqnum=0,
        change_ts=past_ts,
        principal=0,
        interest=0.0,
        interest_rate=0.0,
        last_outgoing_transfer_date=past_ts,
        status=0,
    ))
    db.session.commit()
    db.engine.execute('ANALYZE account')
    assert len(Account.query.all()) == 3
    assert len(PurgeDeletedAccountSignal.query.all()) == 0
    runner = app.test_cli_runner()
    result = runner.invoke(args=['swpt_debtors', 'scan_accounts', '--days', '0.000001', '--quit-early'])
    assert result.exit_code == 0
    assert len(Account.query.all()) == 2
    purge_signals = PurgeDeletedAccountSignal.query.all()
    assert len(purge_signals) == 1
    ps = purge_signals[0]
    assert ps.debtor_id == 1
    assert ps.creditor_id == 2
    assert ps.if_deleted_before > past_ts
    Account.query.delete()
    db.session.commit()
