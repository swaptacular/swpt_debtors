import pytest
from uuid import UUID
from datetime import datetime, timezone, timedelta, date
from swpt_debtors.models import Debtor, RunningTransfer, PrepareTransferSignal, ConfigureAccountSignal, \
    ROOT_CREDITOR_ID, TS0
from swpt_debtors.extensions import db
from swpt_debtors import procedures

TEST_UUID = UUID('123e4567-e89b-12d3-a456-426655440000')
MIN_DEBTOR_ID = 4294967296


@pytest.fixture(scope='function')
def app_unsafe_session(app_unsafe_session):
    try:
        yield app_unsafe_session
    finally:
        db.session.rollback()
        Debtor.query.delete()
        RunningTransfer.query.delete()
        PrepareTransferSignal.query.delete()
        db.session.commit()


def _create_new_debtor(debtor_id: int, activate: bool = False):
    debtor = procedures.reserve_debtor(debtor_id)
    if activate:
        procedures.activate_debtor(debtor_id, debtor.reservation_id)


def test_scan_debtors(app_unsafe_session, current_ts):
    Debtor.query.delete()
    ConfigureAccountSignal.query.delete()
    db.session.commit()

    _create_new_debtor(MIN_DEBTOR_ID + 1, activate=False)
    _create_new_debtor(MIN_DEBTOR_ID + 2, activate=False)
    _create_new_debtor(MIN_DEBTOR_ID + 3, activate=True)
    _create_new_debtor(MIN_DEBTOR_ID + 4, activate=True)
    _create_new_debtor(MIN_DEBTOR_ID + 5, activate=True)
    _create_new_debtor(MIN_DEBTOR_ID + 6, activate=True)
    Debtor.query.filter_by(debtor_id=MIN_DEBTOR_ID + 1).update({
        'created_at': current_ts - timedelta(days=30),
    })
    procedures.deactivate_debtor(MIN_DEBTOR_ID + 3)
    procedures.deactivate_debtor(MIN_DEBTOR_ID + 4)
    procedures.deactivate_debtor(MIN_DEBTOR_ID + 6)
    Debtor.query.filter_by(debtor_id=MIN_DEBTOR_ID + 3).update({
        'created_at': current_ts - timedelta(days=3000),
        'deactivation_date': (current_ts - timedelta(days=3000)).date(),
    })
    Debtor.query.filter_by(debtor_id=MIN_DEBTOR_ID + 4).update({
        'created_at': current_ts - timedelta(days=3000),
        'deactivation_date': (current_ts - timedelta(days=300)).date(),
    })
    Debtor.query.filter_by(debtor_id=MIN_DEBTOR_ID + 5).update({
        'last_config_ts': current_ts - timedelta(days=3000),
    })
    db.session.commit()
    app = app_unsafe_session
    assert len(Debtor.query.all()) == 6

    db.engine.execute('ANALYZE debtor')
    runner = app.test_cli_runner()
    result = runner.invoke(args=['swpt_debtors', 'scan_debtors', '--days', '0.000001', '--quit-early'])
    assert result.exit_code == 0

    debtors = Debtor.query.all()
    assert len(debtors) == 4
    assert sorted([d.debtor_id - MIN_DEBTOR_ID for d in debtors]) == [2, 4, 5, 6]

    config_errors = {debtor.debtor_id: debtor.config_error for debtor in sorted(debtors, key=lambda d: d.debtor_id)}
    assert config_errors.pop(MIN_DEBTOR_ID + 5) == 'CONFIGURATION_IS_NOT_EFFECTUAL'
    assert all([v is None for v in config_errors.values()])

    Debtor.query.delete()
    ConfigureAccountSignal.query.delete()
    db.session.commit()
