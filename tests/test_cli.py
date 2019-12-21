from uuid import UUID
from datetime import datetime, timezone
from swpt_debtors.models import RunningTransfer
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
