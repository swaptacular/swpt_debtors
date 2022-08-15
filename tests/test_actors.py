import pytest
from datetime import datetime, timezone, date
from swpt_pythonlib.rabbitmq import MessageProperties
from swpt_debtors import procedures as p

D_ID = -1
C_ID = 1


@pytest.fixture(scope='function')
def actors():
    from swpt_debtors import actors
    return actors


def test_on_rejected_config_signal(db_session, actors):
    actors._on_rejected_config_signal(
        debtor_id=D_ID,
        creditor_id=p.ROOT_CREDITOR_ID,
        config_ts=datetime.fromisoformat('2019-10-01T00:00:00+00:00'),
        config_seqnum=123,
        negligible_amount=p.HUGE_NEGLIGIBLE_AMOUNT,
        config_data='',
        config_flags=0,
        rejection_code='TEST_REJECTION',
        ts=datetime.fromisoformat('2019-10-01T00:00:00+00:00'),
    )


def test_on_account_update_signal(db_session, actors):
    actors._on_account_update_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        last_change_seqnum=0,
        last_change_ts=datetime.fromisoformat('2019-10-01T00:00:00+00:00'),
        principal=1000,
        interest_rate=-0.5,
        last_config_ts=datetime.fromisoformat('1970-01-01T00:00:00+00:00'),
        last_config_seqnum=0,
        creation_date=date.fromisoformat('2018-10-01'),
        negligible_amount=2.0,
        config_data='',
        config_flags=0,
        account_id='0',
        transfer_note_max_bytes=500,
        ts=datetime.now(tz=timezone.utc),
        ttl=1000000,
    )


def test_on_prepared_issuing_transfer_signal(db_session, actors):
    actors._on_prepared_issuing_transfer_signal(
        debtor_id=D_ID,
        creditor_id=2,
        transfer_id=1,
        coordinator_type='issuing',
        coordinator_id=C_ID,
        coordinator_request_id=1,
        locked_amount=1000,
        recipient=str(C_ID),
        prepared_at=datetime.fromisoformat('2019-10-01T00:00:00+00:00'),
    )


def test_on_rejected_issuing_transfer_signal(db_session, actors):
    actors._on_rejected_issuing_transfer_signal(
        coordinator_type='issuing',
        coordinator_id=C_ID,
        coordinator_request_id=1,
        status_code='TEST',
        total_locked_amount=0,
        debtor_id=D_ID,
        creditor_id=p.ROOT_CREDITOR_ID,
    )


def test_on_finalized_issuing_transfer_signal(db_session, actors):
    actors._on_finalized_issuing_transfer_signal(
        debtor_id=D_ID,
        creditor_id=p.ROOT_CREDITOR_ID,
        transfer_id=123,
        coordinator_type='issuing',
        coordinator_id=D_ID,
        coordinator_request_id=678,
        recipient='1235',
        prepared_at=datetime.fromisoformat('2019-10-01T00:00:00+00:00'),
        ts=datetime.fromisoformat('2019-10-01T00:00:00+00:00'),
        committed_amount=100,
        status_code='OK',
        total_locked_amount=0,
    )


def test_on_account_purge_signal(db_session, actors):
    actors._on_account_purge_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        creation_date=date.fromisoformat('2019-10-01'),
    )


def test_consumer(db_session, actors):
    consumer = actors.SmpConsumer()

    props = MessageProperties(content_type="xxx")
    assert consumer.process_message(b'body', props) is False

    props = MessageProperties(content_type="application/json", type="xxx")
    assert consumer.process_message(b'body', props) is False

    props = MessageProperties(content_type="application/json", type="AccountPurge")
    assert consumer.process_message(b'body', props) is False

    props = MessageProperties(content_type="application/json", type="AccountPurge")
    assert consumer.process_message(b'{}', props) is False

    props = MessageProperties(content_type="application/json", type="AccountPurge")
    with pytest.raises(RuntimeError, match='The agent is not responsible for this debtor.'):
        consumer.process_message(b'''
        {
          "type": "AccountPurge",
          "debtor_id": 1,
          "creditor_id": 2,
          "creation_date": "2098-12-31",
          "ts": "2099-12-31T00:00:00+00:00"
        }
        ''', props)

    props = MessageProperties(content_type="application/json", type="AccountPurge")
    assert consumer.process_message(b'''
    {
      "type": "AccountPurge",
      "debtor_id": 4294967296,
      "creditor_id": 2,
      "creation_date": "2098-12-31",
      "ts": "2099-12-31T00:00:00+00:00"
    }
    ''', props) is True
