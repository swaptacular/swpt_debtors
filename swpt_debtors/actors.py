import iso8601
from swpt_debtors.extensions import broker, APP_QUEUE_NAME
from swpt_debtors import procedures
from swpt_debtors.models import CT_ISSUING, MIN_INT32, MAX_INT32, MIN_INT64, MAX_INT64, \
    CONFIG_DATA_MAX_BYTES, TRANSFER_NOTE_MAX_BYTES


@broker.actor(queue_name=APP_QUEUE_NAME, event_subscription=True)
def on_rejected_config_signal(
        debtor_id: int,
        creditor_id: int,
        config_ts: str,
        config_seqnum: int,
        negligible_amount: float,
        config_data: str,
        config_flags: int,
        rejection_code: str,
        ts: str,
        *args, **kwargs) -> None:

    assert rejection_code == '' or len(rejection_code) <= 30 and rejection_code.encode('ascii')
    assert len(config_data) <= CONFIG_DATA_MAX_BYTES and len(config_data.encode('utf8')) <= CONFIG_DATA_MAX_BYTES

    procedures.process_rejected_config_signal(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        config_ts=iso8601.parse_date(config_ts),
        config_seqnum=config_seqnum,
        negligible_amount=negligible_amount,
        config_data=config_data,
        config_flags=config_flags,
        rejection_code=rejection_code,
    )


@broker.actor(queue_name=APP_QUEUE_NAME, event_subscription=True)
def on_account_update_signal(
        debtor_id: int,
        creditor_id: int,
        creation_date: str,
        last_change_ts: str,
        last_change_seqnum: int,
        principal: int,
        interest_rate: float,
        last_config_ts: str,
        last_config_seqnum: int,
        negligible_amount: float,
        config_data: str,
        config_flags: int,
        account_id: str,
        transfer_note_max_bytes: int,
        ts: str,
        ttl: int,
        *args, **kwargs) -> None:

    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert MIN_INT32 <= last_change_seqnum <= MAX_INT32
    assert MIN_INT32 <= last_config_seqnum <= MAX_INT32
    assert negligible_amount >= 0.0
    assert MIN_INT32 <= config_flags <= MAX_INT32
    assert ttl > 0
    assert 0 <= transfer_note_max_bytes <= TRANSFER_NOTE_MAX_BYTES
    assert len(config_data) <= CONFIG_DATA_MAX_BYTES and len(config_data.encode('utf8')) <= CONFIG_DATA_MAX_BYTES
    assert account_id == '' or len(account_id) <= 100 and account_id.encode('ascii')

    procedures.process_account_update_signal(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        creation_date=iso8601.parse_date(creation_date).date(),
        last_change_ts=iso8601.parse_date(last_change_ts),
        last_change_seqnum=last_change_seqnum,
        principal=principal,
        interest_rate=interest_rate,
        last_config_ts=iso8601.parse_date(last_config_ts),
        last_config_seqnum=last_config_seqnum,
        negligible_amount=negligible_amount,
        config_data=config_data,
        config_flags=config_flags,
        account_id=account_id,
        transfer_note_max_bytes=transfer_note_max_bytes,
        ts=iso8601.parse_date(ts),
        ttl=ttl,
    )


@broker.actor(queue_name=APP_QUEUE_NAME, event_subscription=True)
def on_account_purge_signal(
        debtor_id: int,
        creditor_id: int,
        creation_date: str,
        *args, **kwargs) -> None:

    procedures.process_account_purge_signal(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        creation_date=iso8601.parse_date(creation_date).date(),
    )


@broker.actor(queue_name=APP_QUEUE_NAME, event_subscription=True)
def on_prepared_issuing_transfer_signal(
        debtor_id: int,
        creditor_id: int,
        transfer_id: int,
        coordinator_type: str,
        coordinator_id: int,
        coordinator_request_id: int,
        locked_amount: int,
        recipient: str,
        *args, **kwargs) -> None:

    assert coordinator_type == CT_ISSUING

    procedures.process_prepared_issuing_transfer_signal(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        transfer_id=transfer_id,
        coordinator_id=coordinator_id,
        coordinator_request_id=coordinator_request_id,
        locked_amount=locked_amount,
        recipient=recipient,
    )


@broker.actor(queue_name=APP_QUEUE_NAME, event_subscription=True)
def on_rejected_issuing_transfer_signal(
        coordinator_type: str,
        coordinator_id: int,
        coordinator_request_id: int,
        status_code: str,
        total_locked_amount: int,
        debtor_id: int,
        creditor_id: int,
        *args, **kwargs) -> None:

    assert coordinator_type == CT_ISSUING
    assert status_code == '' or len(status_code) <= 30 and status_code.encode('ascii')
    assert 0 <= total_locked_amount <= MAX_INT64

    procedures.process_rejected_issuing_transfer_signal(
        coordinator_id=coordinator_id,
        coordinator_request_id=coordinator_request_id,
        status_code=status_code,
        total_locked_amount=total_locked_amount,
        debtor_id=debtor_id,
        creditor_id=creditor_id,
    )


@broker.actor(queue_name=APP_QUEUE_NAME, event_subscription=True)
def on_finalized_issuing_transfer_signal(
        debtor_id: int,
        creditor_id: int,
        transfer_id: int,
        coordinator_type: str,
        coordinator_id: int,
        coordinator_request_id: int,
        recipient: str,
        prepared_at: str,
        ts: str,
        committed_amount: int,
        status_code: str,
        total_locked_amount: int,
        *args, **kwargs) -> None:

    assert coordinator_type == CT_ISSUING
    assert status_code == '' or len(status_code) <= 30 and status_code.encode('ascii')
    assert 0 <= total_locked_amount <= MAX_INT64

    procedures.process_finalized_issuing_transfer_signal(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        transfer_id=transfer_id,
        coordinator_id=coordinator_id,
        coordinator_request_id=coordinator_request_id,
        recipient=recipient,
        committed_amount=committed_amount,
        status_code=status_code,
        total_locked_amount=total_locked_amount,
    )
