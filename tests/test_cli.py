import pytest
from uuid import UUID
from datetime import datetime, timezone
from swpt_debtors.models import RunningTransfer

TEST_UUID = UUID('123e4567-e89b-12d3-a456-426655440000')


@pytest.fixture(scope='function')
def running_transfer(db_session):
    running_transfer = RunningTransfer(
        debtor_id=1,
        transfer_uuid=TEST_UUID,
        recipient_creditor_id=1111,
        amount=1500,
        finalized_at_ts=datetime(2000, 1, 1, tzinfo=timezone.utc)
    )
    db_session.add(running_transfer)
    db_session.flush()
    return running_transfer


def test_flush_running_transfers(app, db_session, running_transfer):
    assert len(RunningTransfer.query.all()) == 1
    runner = app.test_cli_runner()
    result = runner.invoke(args=['swpt_debtors', 'flush_running_transfers', '--days', '-10.0'])
    assert '1 ' in result.output
    assert 'deleted' in result.output
    assert len(RunningTransfer.query.all()) == 0
