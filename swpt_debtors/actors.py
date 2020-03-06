import iso8601
from .extensions import broker, APP_QUEUE_NAME
from . import procedures


@broker.actor(queue_name=APP_QUEUE_NAME)
def create_debtor(debtor_id: int) -> None:
    """Make sure a debtor with ID `debtor_id` exists."""

    procedures.lock_or_create_debtor(debtor_id)


@broker.actor(queue_name=APP_QUEUE_NAME)
def deactivate_debtor(debtor_id: int) -> None:
    """Permanently deactivate a debtor."""

    procedures.deactivate_debtor(debtor_id)


@broker.actor(queue_name=APP_QUEUE_NAME, event_subscription=True)
def on_account_change_signal(
        debtor_id: int,
        creditor_id: int,
        change_ts: str,
        change_seqnum: int,
        principal: int,
        interest: float,
        interest_rate: float,
        last_transfer_seqnum: int,
        last_outgoing_transfer_date: str,
        last_config_signal_ts: str,
        last_config_signal_seqnum: int,
        creation_date: str,
        negligible_amount: float,
        status: int,
        *args, **kwargs) -> None:

    procedures.process_account_change_signal(
        debtor_id,
        creditor_id,
        iso8601.parse_date(change_ts),
        change_seqnum,
        principal,
        interest,
        interest_rate,
        iso8601.parse_date(last_outgoing_transfer_date).date(),
        iso8601.parse_date(creation_date).date(),
        negligible_amount,
        status,
    )


@broker.actor(queue_name=APP_QUEUE_NAME, event_subscription=True)
def on_prepared_issuing_transfer_signal(
        debtor_id: int,
        sender_creditor_id: int,
        transfer_id: int,
        coordinator_type: str,
        coordinator_id: int,
        coordinator_request_id: int,
        sender_locked_amount: int,
        recipient_creditor_id: int,
        prepared_at_ts: str,
        *args, **kwargs) -> None:

    assert coordinator_type == 'issuing'
    procedures.process_prepared_issuing_transfer_signal(
        debtor_id,
        sender_creditor_id,
        transfer_id,
        coordinator_id,
        coordinator_request_id,
        sender_locked_amount,
        recipient_creditor_id,
    )


@broker.actor(queue_name=APP_QUEUE_NAME, event_subscription=True)
def on_rejected_issuing_transfer_signal(
        coordinator_type: str,
        coordinator_id: int,
        coordinator_request_id: int,
        details: dict,
        *args, **kwargs) -> None:

    assert coordinator_type == 'issuing'
    assert type(details) is dict
    procedures.process_rejected_issuing_transfer_signal(
        coordinator_id,
        coordinator_request_id,
        details,
    )


@broker.actor(queue_name=APP_QUEUE_NAME, event_subscription=True)
def on_finalized_issuing_transfer_signal(
        debtor_id: int,
        sender_creditor_id: int,
        transfer_id: int,
        coordinator_type: str,
        coordinator_id: int,
        coordinator_request_id: int,
        recipient_creditor_id: int,
        prepared_at_ts: str,
        finalized_at_ts: str,
        committed_amount: int,
        *args, **kwargs) -> None:

    assert sender_creditor_id == procedures.ROOT_CREDITOR_ID
    assert coordinator_type == 'issuing'
    procedures.process_finalized_issuing_transfer_signal(
        debtor_id,
        transfer_id,
        coordinator_id,
        coordinator_request_id,
        recipient_creditor_id,
        committed_amount,
    )


@broker.actor(queue_name=APP_QUEUE_NAME, event_subscription=True)
def on_account_purge_signal(
        debtor_id: int,
        creditor_id: int,
        creation_date: str,
        *args, **kwargs) -> None:

    procedures.process_account_purge_signal(
        debtor_id,
        creditor_id,
        iso8601.parse_date(creation_date).date(),
    )


@broker.actor(queue_name=APP_QUEUE_NAME, event_subscription=True)
def on_account_maintenance_signal(
        debtor_id: int,
        creditor_id: int,
        request_ts: str,
        *args, **kwargs) -> None:

    procedures.process_account_maintenance_signal(
        debtor_id,
        creditor_id,
        iso8601.parse_date(request_ts),
    )
