from uuid import UUID
from datetime import datetime, timezone, timedelta
from swpt_debtors.models import RunningTransfer, Account, ROOT_CREDITOR_ID
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


def test_scan_accounts(app_unsafe_session):
    from swpt_debtors.models import Debtor, PurgeDeletedAccountSignal, ChangeInterestRateSignal, \
        CapitalizeInterestSignal, ZeroOutNegativeBalanceSignal

    current_ts = datetime.now(tz=timezone.utc)
    past_ts = datetime(1900, 1, 1, tzinfo=timezone.utc)
    future_ts = datetime(2100, 1, 1, tzinfo=timezone.utc)
    app = app_unsafe_session
    Debtor.query.delete()
    Account.query.delete()
    PurgeDeletedAccountSignal.query.delete()
    ChangeInterestRateSignal.query.delete()
    CapitalizeInterestSignal.query.delete()
    ZeroOutNegativeBalanceSignal.query.delete()
    db.session.commit()
    db.session.add(Debtor(debtor_id=111, interest_rate_target=5.55))
    db.session.add(Account(
        debtor_id=1,
        creditor_id=ROOT_CREDITOR_ID,
        change_seqnum=0,
        change_ts=past_ts,
        principal=-10,
        interest=0.0,
        interest_rate=0.0,
        last_outgoing_transfer_date=past_ts,
        status=0,
    ))
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
        change_ts=current_ts - timedelta(days=3653),
        principal=0,
        interest=100.0,
        interest_rate=10.0,
        last_outgoing_transfer_date=past_ts,
        status=0,
    ))
    db.session.add(Account(
        debtor_id=1111,
        creditor_id=2222,
        change_seqnum=0,
        change_ts=current_ts - timedelta(days=3653),
        principal=10,
        interest=-20.0,
        interest_rate=10.0,
        last_outgoing_transfer_date=past_ts,
        status=0,
    ))
    db.session.commit()
    db.engine.execute('ANALYZE account')
    assert len(Account.query.all()) == 5
    assert len(PurgeDeletedAccountSignal.query.all()) == 0
    assert len(ChangeInterestRateSignal.query.all()) == 0
    assert len(CapitalizeInterestSignal.query.all()) == 0
    assert len(ZeroOutNegativeBalanceSignal.query.all()) == 0
    runner = app.test_cli_runner()
    result = runner.invoke(args=['swpt_debtors', 'scan_accounts', '--days', '0.000001', '--quit-early'])
    assert result.exit_code == 0
    assert len(Account.query.all()) == 4

    purge_signals = PurgeDeletedAccountSignal.query.all()
    assert len(purge_signals) == 1
    ps = purge_signals[0]
    assert ps.debtor_id == 1
    assert ps.creditor_id == 2
    assert ps.if_deleted_before > past_ts

    change_interest_rate_signals = ChangeInterestRateSignal.query.all()
    assert len(change_interest_rate_signals) >= 1
    cirs = change_interest_rate_signals[0]
    assert cirs.debtor_id == 111
    assert cirs.creditor_id == 222
    assert cirs.interest_rate == 5.55

    capitalize_interest_signals = CapitalizeInterestSignal.query.all()
    assert len(capitalize_interest_signals) >= 1
    cis = capitalize_interest_signals[0]
    assert cis.debtor_id == 111
    assert cis.creditor_id == 222
    assert 300 > cis.accumulated_interest_threshold > 50

    zero_out_negative_balance_signals = ZeroOutNegativeBalanceSignal.query.all()
    assert len(zero_out_negative_balance_signals) >= 1
    zonbs = zero_out_negative_balance_signals[0]
    assert zonbs.debtor_id == 1111
    assert zonbs.creditor_id == 2222
    assert zonbs.last_outgoing_transfer_date < (current_ts - timedelta(days=7)).date()

    Debtor.query.delete()
    Account.query.delete()
    PurgeDeletedAccountSignal.query.delete()
    ChangeInterestRateSignal.query.delete()
    CapitalizeInterestSignal.query.delete()
    ZeroOutNegativeBalanceSignal.query.delete()
    db.session.commit()
