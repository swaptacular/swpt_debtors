import sqlalchemy
from unittest.mock import Mock
from uuid import UUID
from datetime import timedelta
from swpt_debtors.models import Debtor, FinalizeTransferSignal
from swpt_debtors.extensions import db
from swpt_debtors import procedures
from swpt_pythonlib.utils import ShardingRealm

TEST_UUID = UUID("123e4567-e89b-12d3-a456-426655440000")
MIN_DEBTOR_ID = 4294967296


def _create_new_debtor(debtor_id: int, activate: bool = False):
    debtor = procedures.reserve_debtor(debtor_id)
    if activate:
        procedures.activate_debtor(debtor_id, str(debtor.reservation_id))


def test_scan_debtors(app, db_session, current_ts):
    _create_new_debtor(MIN_DEBTOR_ID + 1, activate=False)
    _create_new_debtor(MIN_DEBTOR_ID + 2, activate=False)
    _create_new_debtor(MIN_DEBTOR_ID + 3, activate=True)
    _create_new_debtor(MIN_DEBTOR_ID + 4, activate=True)
    _create_new_debtor(MIN_DEBTOR_ID + 5, activate=True)
    _create_new_debtor(MIN_DEBTOR_ID + 6, activate=True)
    Debtor.query.filter_by(debtor_id=MIN_DEBTOR_ID + 1).update(
        {
            "created_at": current_ts - timedelta(days=30),
        }
    )
    procedures.deactivate_debtor(MIN_DEBTOR_ID + 3)
    procedures.deactivate_debtor(MIN_DEBTOR_ID + 4)
    procedures.deactivate_debtor(MIN_DEBTOR_ID + 6)
    Debtor.query.filter_by(debtor_id=MIN_DEBTOR_ID + 3).update(
        {
            "created_at": current_ts - timedelta(days=3000),
            "deactivation_date": (current_ts - timedelta(days=3000)).date(),
        }
    )
    Debtor.query.filter_by(debtor_id=MIN_DEBTOR_ID + 4).update(
        {
            "created_at": current_ts - timedelta(days=3000),
            "deactivation_date": (current_ts - timedelta(days=300)).date(),
        }
    )
    Debtor.query.filter_by(debtor_id=MIN_DEBTOR_ID + 5).update(
        {
            "last_config_ts": current_ts - timedelta(days=3000),
        }
    )
    db.session.commit()

    assert len(Debtor.query.all()) == 6

    with db.engine.connect() as conn:
        conn.execute(sqlalchemy.text("ANALYZE debtor"))

    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_debtors",
            "scan_debtors",
            "--days",
            "0.000001",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0

    debtors = Debtor.query.all()
    assert len(debtors) == 4
    assert sorted([d.debtor_id - MIN_DEBTOR_ID for d in debtors]) == [
        2,
        4,
        5,
        6,
    ]

    config_errors = {
        debtor.debtor_id: debtor.config_error
        for debtor in sorted(debtors, key=lambda d: d.debtor_id)
    }
    assert (
        config_errors.pop(MIN_DEBTOR_ID + 5)
        == "CONFIGURATION_IS_NOT_EFFECTUAL"
    )
    assert all([v is None for v in config_errors.values()])


def test_delete_parent_debtors(app, db_session, current_ts):
    _create_new_debtor(MIN_DEBTOR_ID, activate=True)
    db.session.commit()

    orig_sharding_realm = app.config["SHARDING_REALM"]
    app.config["SHARDING_REALM"] = ShardingRealm("1.#")
    app.config["DELETE_PARENT_SHARD_RECORDS"] = True
    assert len(Debtor.query.all()) == 1

    with db.engine.connect() as conn:
        conn.execute(sqlalchemy.text("ANALYZE debtor"))

    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_debtors",
            "scan_debtors",
            "--days",
            "0.000001",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0

    debtors = Debtor.query.all()
    assert len(debtors) == 0

    app.config["DELETE_PARENT_SHARD_RECORDS"] = False
    app.config["SHARDING_REALM"] = orig_sharding_realm


def test_flush_messages(mocker, app, db_session):
    send_signalbus_message = Mock()
    mocker.patch(
        "swpt_debtors.models.FinalizeTransferSignal.send_signalbus_message",
        new_callable=send_signalbus_message,
    )
    db.session.commit()
    fts = FinalizeTransferSignal(
        creditor_id=0,
        debtor_id=-1,
        transfer_id=666,
        coordinator_id=0,
        coordinator_request_id=777,
        committed_amount=0,
        transfer_note_format="",
        transfer_note="",
    )
    db.session.add(fts)
    db.session.commit()
    assert len(FinalizeTransferSignal.query.all()) == 1
    db.session.commit()

    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_debtors",
            "flush_messages",
            "FinalizeTransferSignal",
            "--wait",
            "0.1",
            "--quit-early",
        ]
    )
    assert result.exit_code == 1
    assert send_signalbus_message.called_once()
    assert len(FinalizeTransferSignal.query.all()) == 0


def test_consume_messages(app):
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=["swpt_debtors", "consume_messages", "--url=INVALID"]
    )
    assert result.exit_code == 1
