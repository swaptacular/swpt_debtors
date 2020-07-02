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
def on_account_update_signal(
        debtor_id: int,
        creditor_id: int,
        last_change_ts: str,
        last_change_seqnum: int,
        principal: int,
        interest: float,
        interest_rate: float,
        demurrage_rate: float,
        commit_period: int,
        last_interest_rate_change_ts: str,
        last_transfer_number: int,
        last_transfer_committed_at: str,
        last_outgoing_transfer_date: str,
        last_config_ts: str,
        last_config_seqnum: int,
        creation_date: str,
        negligible_amount: float,
        config: str,
        config_flags: int,
        status_flags: int,
        ts: str,
        ttl: int,
        *args, **kwargs) -> None:

    procedures.process_account_update_signal(
        debtor_id,
        creditor_id,
        iso8601.parse_date(last_change_ts),
        last_change_seqnum,
        principal,
        interest,
        interest_rate,
        iso8601.parse_date(last_interest_rate_change_ts),
        iso8601.parse_date(last_outgoing_transfer_date).date(),
        iso8601.parse_date(creation_date).date(),
        negligible_amount,
        config_flags,
        status_flags,
        iso8601.parse_date(ts),
        ttl,
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

    assert coordinator_type == 'issuing'
    procedures.process_prepared_issuing_transfer_signal(
        debtor_id,
        creditor_id,
        transfer_id,
        coordinator_id,
        coordinator_request_id,
        locked_amount,
        recipient,
    )


@broker.actor(queue_name=APP_QUEUE_NAME, event_subscription=True)
def on_rejected_issuing_transfer_signal(
        coordinator_type: str,
        coordinator_id: int,
        coordinator_request_id: int,
        rejection_code: str,
        available_amount: int,
        total_locked_amount: int,
        debtor_id: int,
        creditor_id: int,
        *args, **kwargs) -> None:

    assert coordinator_type == 'issuing'
    procedures.process_rejected_issuing_transfer_signal(
        coordinator_id,
        coordinator_request_id,
        rejection_code,
        available_amount,
        total_locked_amount,
        debtor_id,
        creditor_id,
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
        *args, **kwargs) -> None:

    assert coordinator_type == 'issuing'
    procedures.process_finalized_issuing_transfer_signal(
        debtor_id,
        creditor_id,
        transfer_id,
        coordinator_id,
        coordinator_request_id,
        recipient,
        committed_amount,
        status_code,
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
