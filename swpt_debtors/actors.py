import iso8601
from .extensions import broker, APP_QUEUE_NAME
from . import procedures


@broker.actor(queue_name=APP_QUEUE_NAME)
def create_debtor(debtor_id: int) -> None:
    """Creates a new debtor."""


@broker.actor(queue_name=APP_QUEUE_NAME)
def terminate_debtor(debtor_id: int) -> None:
    """Permanently terminates a debtor."""


@broker.actor(queue_name=APP_QUEUE_NAME)
def update_debtor_balance(
        debtor_id: int,
        balance: int,
        update_seqnum: int,
        update_ts: str) -> None:
    """Updates the balance of the debtor's account."""

    update_ts = iso8601.parse_date(update_ts)


@broker.actor(queue_name=APP_QUEUE_NAME, event_subscription=True)
def on_account_change_signal(
        debtor_id: int,
        creditor_id: int,
        change_seqnum: int,
        change_ts: str,
        principal: int,
        interest: float,
        interest_rate: float,
        last_outgoing_transfer_date: str,
        status: int) -> None:
    procedures.process_account_change_signal(
        debtor_id,
        creditor_id,
        change_seqnum,
        iso8601.parse_date(change_ts),
        principal,
        interest,
        interest_rate,
        iso8601.parse_date(last_outgoing_transfer_date).date(),
        status,
    )


@broker.actor(queue_name=APP_QUEUE_NAME, event_subscription=True)
def on_prepared_issuing_transfer_signal(
        debtor_id: int,
        sender_creditor_id: int,
        transfer_id: int,
        coordinator_type: str,
        recipient_creditor_id: int,
        sender_locked_amount: int,
        prepared_at_ts: str,
        coordinator_id: int,
        coordinator_request_id: int) -> None:
    assert coordinator_type == 'payment'
    procedures.process_prepared_payment_transfer_signal(
        debtor_id,
        sender_creditor_id,
        transfer_id,
        recipient_creditor_id,
        sender_locked_amount,
        coordinator_id,
        coordinator_request_id,
    )


@broker.actor(queue_name=APP_QUEUE_NAME, event_subscription=True)
def on_rejected_issuing_transfer_signal(
        coordinator_type: str,
        coordinator_id: int,
        coordinator_request_id: int,
        details: dict) -> None:
    assert coordinator_type == 'issuing'
    procedures.process_rejected_payment_transfer_signal(
        coordinator_id,
        coordinator_request_id,
        details,
    )
