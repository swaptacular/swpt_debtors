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
        status: int,
        update_ts: str) -> None:
    """Updates the balance of the debtor's account."""

    update_ts = iso8601.parse_date(update_ts)
