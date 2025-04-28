import logging
import json
from datetime import datetime, date
from marshmallow import ValidationError
from swpt_pythonlib import rabbitmq
import swpt_pythonlib.protocol_schemas as ps
from swpt_debtors import procedures
from swpt_debtors.models import CT_ISSUING, is_valid_debtor_id
from swpt_debtors.schemas import ActivateDebtorMessageSchema


def _on_rejected_config_signal(
    debtor_id: int,
    creditor_id: int,
    config_ts: datetime,
    config_seqnum: int,
    negligible_amount: float,
    config_data: str,
    config_flags: int,
    rejection_code: str,
    ts: datetime,
    *args,
    **kwargs
) -> None:
    procedures.process_rejected_config_signal(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        config_ts=config_ts,
        config_seqnum=config_seqnum,
        negligible_amount=negligible_amount,
        config_data=config_data,
        config_flags=config_flags,
        rejection_code=rejection_code,
    )


def _on_account_update_signal(
    debtor_id: int,
    creditor_id: int,
    creation_date: date,
    last_change_ts: datetime,
    last_change_seqnum: int,
    principal: int,
    interest_rate: float,
    last_config_ts: datetime,
    last_config_seqnum: int,
    negligible_amount: float,
    config_data: str,
    config_flags: int,
    account_id: str,
    transfer_note_max_bytes: int,
    ts: datetime,
    ttl: int,
    *args,
    **kwargs
) -> None:
    procedures.process_account_update_signal(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        creation_date=creation_date,
        last_change_ts=last_change_ts,
        last_change_seqnum=last_change_seqnum,
        principal=principal,
        interest_rate=interest_rate,
        last_config_ts=last_config_ts,
        last_config_seqnum=last_config_seqnum,
        negligible_amount=negligible_amount,
        config_data=config_data,
        config_flags=config_flags,
        account_id=account_id,
        transfer_note_max_bytes=transfer_note_max_bytes,
        ts=ts,
        ttl=ttl,
    )


def _on_account_purge_signal(
    debtor_id: int, creditor_id: int, creation_date: date, *args, **kwargs
) -> None:
    procedures.process_account_purge_signal(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        creation_date=creation_date,
    )


def _on_prepared_issuing_transfer_signal(
    debtor_id: int,
    creditor_id: int,
    transfer_id: int,
    coordinator_type: str,
    coordinator_id: int,
    coordinator_request_id: int,
    locked_amount: int,
    recipient: str,
    *args,
    **kwargs
) -> None:
    if coordinator_type != CT_ISSUING:  # pragma: no cover
        _LOGGER.error('Unexpected coordinator type: "%s"', coordinator_type)
        return

    procedures.process_prepared_issuing_transfer_signal(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        transfer_id=transfer_id,
        coordinator_id=coordinator_id,
        coordinator_request_id=coordinator_request_id,
        locked_amount=locked_amount,
        recipient=recipient,
    )


def _on_rejected_issuing_transfer_signal(
    coordinator_type: str,
    coordinator_id: int,
    coordinator_request_id: int,
    status_code: str,
    total_locked_amount: int,
    debtor_id: int,
    creditor_id: int,
    *args,
    **kwargs
) -> None:
    if coordinator_type != CT_ISSUING:  # pragma: no cover
        _LOGGER.error('Unexpected coordinator type: "%s"', coordinator_type)
        return

    procedures.process_rejected_issuing_transfer_signal(
        coordinator_id=coordinator_id,
        coordinator_request_id=coordinator_request_id,
        status_code=status_code,
        total_locked_amount=total_locked_amount,
        debtor_id=debtor_id,
        creditor_id=creditor_id,
    )


def _on_finalized_issuing_transfer_signal(
    debtor_id: int,
    creditor_id: int,
    transfer_id: int,
    coordinator_type: str,
    coordinator_id: int,
    coordinator_request_id: int,
    prepared_at: datetime,
    ts: datetime,
    committed_amount: int,
    status_code: str,
    total_locked_amount: int,
    *args,
    **kwargs
) -> None:
    if coordinator_type != CT_ISSUING:  # pragma: no cover
        _LOGGER.error('Unexpected coordinator type: "%s"', coordinator_type)
        return

    procedures.process_finalized_issuing_transfer_signal(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        transfer_id=transfer_id,
        coordinator_id=coordinator_id,
        coordinator_request_id=coordinator_request_id,
        committed_amount=committed_amount,
        status_code=status_code,
        total_locked_amount=total_locked_amount,
    )


def _on_activate_debtor_signal(
    debtor_id: int, reservation_id: str, *args, **kwargs
) -> None:
    try:
        procedures.activate_debtor(debtor_id, reservation_id)
    except (procedures.InvalidReservationId, procedures.DebtorExists):
        pass


_MESSAGE_TYPES = {
    "RejectedConfig": (
        ps.RejectedConfigMessageSchema(),
        _on_rejected_config_signal,
    ),
    "AccountUpdate": (
        ps.AccountUpdateMessageSchema(),
        _on_account_update_signal,
    ),
    "AccountPurge": (ps.AccountPurgeMessageSchema(), _on_account_purge_signal),
    "PreparedTransfer": (
        ps.PreparedTransferMessageSchema(),
        _on_prepared_issuing_transfer_signal,
    ),
    "RejectedTransfer": (
        ps.RejectedTransferMessageSchema(),
        _on_rejected_issuing_transfer_signal,
    ),
    "FinalizedTransfer": (
        ps.FinalizedTransferMessageSchema(),
        _on_finalized_issuing_transfer_signal,
    ),
    "ActivateDebtor": (
        ActivateDebtorMessageSchema(),
        _on_activate_debtor_signal,
    ),
}

_LOGGER = logging.getLogger(__name__)


TerminatedConsumtion = rabbitmq.TerminatedConsumtion


class SmpConsumer(rabbitmq.Consumer):
    """Passes messages to proper handlers (actors)."""

    def process_message(self, body, properties):
        content_type = getattr(properties, "content_type", None)
        if content_type != "application/json":
            _LOGGER.error('Unknown message content type: "%s"', content_type)
            return False

        massage_type = getattr(properties, "type", None)
        try:
            schema, actor = _MESSAGE_TYPES[massage_type]
        except KeyError:
            _LOGGER.error('Unknown message type: "%s"', massage_type)
            return False

        try:
            obj = json.loads(body.decode("utf8"))
        except (UnicodeError, json.JSONDecodeError):
            _LOGGER.error(
                "The message does not contain a valid JSON document."
            )
            return False

        try:
            message_content = schema.load(obj)
        except ValidationError as e:
            _LOGGER.error("Message validation error: %s", str(e))
            return False

        if not is_valid_debtor_id(message_content["debtor_id"]):
            raise RuntimeError("The agent is not responsible for this debtor.")

        actor(**message_content)
        return True
