import pytest
from uuid import UUID
from datetime import datetime, timezone, timedelta, date
from swpt_debtors.models import RunningTransfer, Account, InitiatedTransfer, Debtor, \
    ConfigureAccountSignal, ROOT_CREDITOR_ID, BEGINNING_OF_TIME
from swpt_debtors.extensions import db
from swpt_debtors import procedures

TEST_UUID = UUID('123e4567-e89b-12d3-a456-426655440000')


@pytest.fixture(scope='function')
def app_unsafe_session(app_unsafe_session):
    from swpt_debtors.models import Debtor, CapitalizeInterestSignal, ChangeInterestRateSignal, \
        TryToDeleteAccountSignal, RunningTransfer, PrepareTransferSignal

    try:
        yield app_unsafe_session
    finally:
        db.session.rollback()
        Debtor.query.delete()
        Account.query.delete()
        ChangeInterestRateSignal.query.delete()
        CapitalizeInterestSignal.query.delete()
        TryToDeleteAccountSignal.query.delete()
        RunningTransfer.query.delete()
        PrepareTransferSignal.query.delete()
        db.session.commit()


def test_scan_running_transfers(app_unsafe_session):
    app = app_unsafe_session
    running_transfer = RunningTransfer(
        debtor_id=1,
        transfer_uuid=TEST_UUID,
        recipient_creditor_id=1111,
        amount=1500,
        transfer_note_format='',
        transfer_note='',
        issuing_transfer_id=666,
        started_at=datetime(2000, 1, 1, tzinfo=timezone.utc)
    )
    db.session.add(running_transfer)
    db.session.commit()
    db.engine.execute('ANALYZE running_transfer')
    assert len(RunningTransfer.query.all()) == 1
    runner = app.test_cli_runner()
    result = runner.invoke(args=['swpt_debtors', 'scan_running_transfers', '--days', '0.000001', '--quit-early'])
    assert result.exit_code == 0
    assert len(RunningTransfer.query.all()) == 0


def test_scan_accounts(app_unsafe_session):
    from swpt_debtors.models import Debtor, ChangeInterestRateSignal, \
        CapitalizeInterestSignal, TryToDeleteAccountSignal

    some_date = date(2018, 10, 20)
    current_ts = datetime.now(tz=timezone.utc)
    past_ts = datetime(1970, 1, 1, tzinfo=timezone.utc)
    future_ts = datetime(2100, 1, 1, tzinfo=timezone.utc)
    app = app_unsafe_session
    db.session.add(Debtor(debtor_id=111, interest_rate_target=5.55))
    db.session.add(Account(
        debtor_id=1,
        creditor_id=ROOT_CREDITOR_ID,
        last_change_seqnum=0,
        last_change_ts=past_ts,
        principal=-10,
        interest=0.0,
        interest_rate=0.0,
        last_interest_rate_change_ts=BEGINNING_OF_TIME,
        creation_date=some_date,
        negligible_amount=2.0,
        config_flags=0,
        status_flags=0,
    ))
    db.session.add(Account(
        debtor_id=1,
        creditor_id=2,
        last_change_seqnum=0,
        last_change_ts=past_ts,
        principal=0,
        interest=0.0,
        interest_rate=0.0,
        last_interest_rate_change_ts=BEGINNING_OF_TIME,
        creation_date=some_date,
        negligible_amount=2.0,
        config_flags=Account.CONFIG_SCHEDULED_FOR_DELETION_FLAG,
        status_flags=Account.STATUS_DELETED_FLAG,
    ))
    db.session.add(Account(
        debtor_id=11,
        creditor_id=22,
        last_change_seqnum=0,
        last_change_ts=future_ts,
        principal=0,
        interest=0.0,
        interest_rate=0.0,
        last_interest_rate_change_ts=BEGINNING_OF_TIME,
        creation_date=some_date,
        negligible_amount=2.0,
        config_flags=Account.CONFIG_SCHEDULED_FOR_DELETION_FLAG,
        status_flags=Account.STATUS_DELETED_FLAG,
    ))
    db.session.add(Account(
        debtor_id=111,
        creditor_id=222,
        last_change_seqnum=0,
        last_change_ts=current_ts - timedelta(days=3653),
        principal=0,
        interest=100.0,
        interest_rate=10.0,
        last_interest_rate_change_ts=BEGINNING_OF_TIME,
        creation_date=some_date,
        negligible_amount=2.0,
        config_flags=0,
        status_flags=0,
        last_interest_capitalization_ts=current_ts,
    ))
    db.session.add(Account(
        debtor_id=1111,
        creditor_id=2222,
        last_change_seqnum=0,
        last_change_ts=current_ts - timedelta(days=3653),
        principal=10,
        interest=-20.0,
        interest_rate=10.0,
        last_interest_rate_change_ts=BEGINNING_OF_TIME,
        creation_date=some_date,
        negligible_amount=2.0,
        config_flags=0,
        status_flags=0,
        last_interest_capitalization_ts=current_ts,
    ))
    db.session.add(Account(
        debtor_id=11111,
        creditor_id=22222,
        last_change_seqnum=0,
        last_change_ts=current_ts - timedelta(days=3653),
        principal=50,
        interest=0.0,
        interest_rate=0.0,
        last_interest_rate_change_ts=BEGINNING_OF_TIME,
        creation_date=some_date,
        negligible_amount=50.0,
        config_flags=Account.CONFIG_SCHEDULED_FOR_DELETION_FLAG,
        status_flags=Account.STATUS_ESTABLISHED_INTEREST_RATE_FLAG,
    ))
    db.session.commit()
    db.engine.execute('ANALYZE account')
    assert len(Account.query.all()) == 6
    assert len(ChangeInterestRateSignal.query.all()) == 0
    assert len(CapitalizeInterestSignal.query.all()) == 0
    runner = app.test_cli_runner()
    result = runner.invoke(args=['swpt_debtors', 'scan_accounts', '--hours', '0.000024', '--quit-early'])
    assert result.exit_code == 0
    assert len(Account.query.all()) == 6

    change_interest_rate_signals = ChangeInterestRateSignal.query.all()
    assert len(change_interest_rate_signals) == 2
    assert sorted([x.debtor_id for x in change_interest_rate_signals]) == [111, 1111]
    assert sorted([x.creditor_id for x in change_interest_rate_signals]) == [222, 2222]
    assert sorted([x.interest_rate for x in change_interest_rate_signals]) == [0.0, 5.55]

    capitalize_interest_signals = CapitalizeInterestSignal.query.all()
    assert len(capitalize_interest_signals) == 0

    try_to_delete_account_signals = TryToDeleteAccountSignal.query.all()
    assert len(try_to_delete_account_signals) == 1
    ttdas = try_to_delete_account_signals[0]
    assert ttdas.debtor_id == 11111
    assert ttdas.creditor_id == 22222

    # Ensure attempt to delete an account are not made too often.
    Account.query.filter_by(debtor_id=11111, creditor_id=22222).update(
        {Account.is_muted: False},
        synchronize_session=False,
    )
    db.session.commit()
    db.engine.execute('ANALYZE account')
    runner = app.test_cli_runner()
    result = runner.invoke(args=['swpt_debtors', 'scan_accounts', '--hours', '0.000024', '--quit-early'])
    assert result.exit_code == 0
    assert len(TryToDeleteAccountSignal.query.all()) == 1


def test_scan_accounts_capitalize_interest(app_unsafe_session):
    from swpt_debtors.models import CapitalizeInterestSignal, ChangeInterestRateSignal, \
        TryToDeleteAccountSignal

    some_date = date(2018, 10, 20)
    current_ts = datetime.now(tz=timezone.utc)
    app = app_unsafe_session
    db.session.add(Account(
        debtor_id=111,
        creditor_id=222,
        last_change_seqnum=0,
        last_change_ts=current_ts - timedelta(days=3653),
        principal=0,
        interest=100.0,
        interest_rate=10.0,
        last_interest_rate_change_ts=BEGINNING_OF_TIME,
        creation_date=some_date,
        negligible_amount=2.0,
        config_flags=0,
        status_flags=0,
    ))
    db.session.add(Account(
        debtor_id=1111,
        creditor_id=2222,
        last_change_seqnum=0,
        last_change_ts=current_ts - timedelta(days=3653),
        principal=10,
        interest=-20.0,
        interest_rate=10.0,
        last_interest_rate_change_ts=BEGINNING_OF_TIME,
        creation_date=some_date,
        negligible_amount=2.0,
        config_flags=0,
        status_flags=0,
    ))
    db.session.commit()
    db.engine.execute('ANALYZE account')
    assert len(Account.query.all()) == 2
    assert len(CapitalizeInterestSignal.query.all()) == 0
    runner = app.test_cli_runner()
    result = runner.invoke(args=['swpt_debtors', 'scan_accounts', '--hours', '0.000024', '--quit-early'])
    assert result.exit_code == 0
    assert len(Account.query.all()) == 2

    change_interest_rate_signals = ChangeInterestRateSignal.query.all()
    assert len(change_interest_rate_signals) == 0

    try_to_delete_account_signals = TryToDeleteAccountSignal.query.all()
    assert len(try_to_delete_account_signals) == 0

    capitalize_interest_signals = CapitalizeInterestSignal.query.all()
    assert len(capitalize_interest_signals) == 2
    assert sorted([x.debtor_id for x in capitalize_interest_signals]) == [111, 1111]
    assert sorted([x.creditor_id for x in capitalize_interest_signals]) == [222, 2222]

    # Ensure interest is not capitalized too often.
    Account.query.filter_by(debtor_id=111, creditor_id=222).update(
        {Account.interest: 200.0, Account.is_muted: False},
        synchronize_session=False,
    )
    db.session.commit()
    db.engine.execute('ANALYZE account')
    runner = app.test_cli_runner()
    result = runner.invoke(args=['swpt_debtors', 'scan_accounts', '--hours', '0.000024', '--quit-early'])
    assert result.exit_code == 0
    assert len(CapitalizeInterestSignal.query.all()) == 2


def test_scan_accounts_deactivate_debtor(app_unsafe_session):
    from swpt_debtors.models import Debtor

    past_ts = datetime(1970, 1, 1, tzinfo=timezone.utc)
    app = app_unsafe_session
    db.session.add(Debtor(debtor_id=1, status_flags=Debtor.STATUS_IS_ACTIVATED_FLAG))
    procedures.initiate_transfer(1, TEST_UUID, 1, 50, '', '')
    db.session.add(Debtor(debtor_id=2, status_flags=Debtor.STATUS_IS_ACTIVATED_FLAG))
    db.session.add(Account(
        debtor_id=1,
        creditor_id=ROOT_CREDITOR_ID,
        last_change_seqnum=0,
        last_change_ts=past_ts,
        principal=0,
        interest=0.0,
        interest_rate=0.0,
        last_interest_rate_change_ts=BEGINNING_OF_TIME,
        creation_date=date(2018, 10, 20),
        negligible_amount=2.0,
        config_flags=0,
        status_flags=Account.STATUS_DELETED_FLAG,
        last_heartbeat_ts=past_ts,
    ))
    db.session.commit()
    db.engine.execute('ANALYZE account')
    assert len(Debtor.query.all()) == 2
    assert len(Account.query.all()) == 1
    runner = app.test_cli_runner()
    result = runner.invoke(args=['swpt_debtors', 'scan_accounts', '--hours', '0.000024', '--quit-early'])
    assert result.exit_code == 0
    assert len(Debtor.query.all()) == 2
    assert len(Account.query.all()) == 0


def _create_new_debtor(debtor_id: int, activate: bool = False):
    debtor = procedures.reserve_debtor(debtor_id)
    if activate:
        procedures.activate_debtor(debtor_id, debtor.reservation_id)


@pytest.mark.unsafe
def test_scan_debtors(app_unsafe_session, current_ts):
    Debtor.query.delete()
    ConfigureAccountSignal.query.delete()
    db.session.commit()

    _create_new_debtor(1, activate=False)
    _create_new_debtor(2, activate=False)
    _create_new_debtor(3, activate=True)
    _create_new_debtor(4, activate=True)
    _create_new_debtor(5, activate=True)
    _create_new_debtor(6, activate=True)
    Debtor.query.filter_by(debtor_id=1).update({
        'created_at': current_ts - timedelta(days=30),
    })
    procedures.deactivate_debtor(3)
    procedures.deactivate_debtor(4)
    procedures.deactivate_debtor(6)
    Debtor.query.filter_by(debtor_id=3).update({
        'created_at': current_ts - timedelta(days=3000),
        'deactivation_date': (current_ts - timedelta(days=3000)).date(),
    })
    Debtor.query.filter_by(debtor_id=4).update({
        'created_at': current_ts - timedelta(days=3000),
        'deactivation_date': (current_ts - timedelta(days=3000)).date(),
    })
    db.session.commit()
    app = app_unsafe_session
    assert len(Debtor.query.all()) == 6

    db.engine.execute('ANALYZE account')
    runner = app.test_cli_runner()
    result = runner.invoke(args=['swpt_debtors', 'scan_debtors', '--days', '0.000001', '--quit-early'])
    assert result.exit_code == 0

    debtors = Debtor.query.all()
    assert len(debtors) == 5
    assert sorted([d.debtor_id for d in debtors]) == [2, 3, 4, 5, 6]

    Debtor.query.delete()
    ConfigureAccountSignal.query.delete()
    db.session.commit()
