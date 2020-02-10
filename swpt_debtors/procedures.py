from datetime import datetime, date, timedelta, timezone
from uuid import UUID
from typing import TypeVar, Optional, Callable, List
from flask import current_app
from sqlalchemy.exc import IntegrityError
from swpt_lib.utils import is_later_event
from .extensions import db
from .lower_limits import LowerLimitSequence, TooLongLimitSequenceError
from .models import Debtor, Account, ChangeInterestRateSignal, FinalizePreparedTransferSignal, \
    InitiatedTransfer, RunningTransfer, PrepareTransferSignal, ConfigureAccountSignal, \
    MIN_INT16, MAX_INT16, MIN_INT32, MAX_INT32, MIN_INT64, MAX_INT64, ROOT_CREDITOR_ID

T = TypeVar('T')
atomic: Callable[[T], T] = db.atomic


class DebtorDoesNotExistError(Exception):
    """The debtor does not exist."""


class DebtorExistsError(Exception):
    """The same debtor record already exists."""


class TransferExistsError(Exception):
    """The same initiated transfer record already exists."""

    def __init__(self, transfer: InitiatedTransfer):
        self.transfer = transfer


class TransfersConflictError(Exception):
    """A different transfer with the same UUID already exists."""


class TooManyManagementActionsError(Exception):
    """Too many management actions per month by a debtor."""


class ConflictingPolicyError(Exception):
    """The new debtor policy conflicts with the old one."""

    def __init__(self, message: str):
        self.message = message


@atomic
def get_debtor(debtor_id: int) -> Optional[Debtor]:
    return Debtor.get_instance(debtor_id)


@atomic
def create_new_debtor(debtor_id: int) -> Optional[Debtor]:
    assert MIN_INT64 <= debtor_id <= MAX_INT64
    debtor = Debtor(debtor_id=debtor_id)
    db.session.add(debtor)
    try:
        db.session.flush()
    except IntegrityError:
        raise DebtorExistsError(debtor_id)
    _insert_configure_account_signal(debtor_id)
    return debtor


@atomic
def lock_or_create_debtor(debtor_id: int) -> Debtor:
    assert MIN_INT64 <= debtor_id <= MAX_INT64
    debtor = Debtor.lock_instance(debtor_id)
    if debtor is None:
        debtor = Debtor(debtor_id=debtor_id)
        with db.retry_on_integrity_error():
            db.session.add(debtor)
        _insert_configure_account_signal(debtor_id)
    return debtor


@atomic
def update_debtor_balance(debtor_id: int, balance: int, balance_ts: datetime) -> None:
    debtor = Debtor.lock_instance(debtor_id)
    if debtor is None:
        # It the debtor does not exist, we create a new deactivated
        # debtor. That way, we know that the debtor's account will be
        # deleted from the `accounts` service (eventually).
        debtor = Debtor(debtor_id=debtor_id, deactivated_at_date=datetime.now(tz=timezone.utc).date())
        with db.retry_on_integrity_error():
            db.session.add(debtor)
    debtor.balance = balance
    debtor.balance_ts = balance_ts
    debtor.status |= Debtor.STATUS_HAS_ACCOUNT_FLAG


@atomic
def deactivate_debtor(debtor_id: int, deleted_account: bool = False) -> Optional[Debtor]:
    debtor = Debtor.lock_instance(debtor_id)
    if debtor:
        if debtor.deactivated_at_date is None:
            debtor.deactivated_at_date = datetime.now(tz=timezone.utc).date()
        if deleted_account:
            debtor.status &= ~Debtor.STATUS_HAS_ACCOUNT_FLAG
        debtor.initiated_transfers_count = 0
        InitiatedTransfer.query.filter_by(debtor_id=debtor_id).delete(synchronize_session=False)
    return debtor


@atomic
def update_debtor_policy(
        debtor_id: int,
        interest_rate_target: Optional[float],
        new_interest_rate_limits: LowerLimitSequence,
        new_balance_limits: LowerLimitSequence) -> Debtor:

    debtor = _throttle_debtor_actions(debtor_id)
    current_ts = datetime.now(tz=timezone.utc)
    date_week_ago = (current_ts - timedelta(days=7)).date()

    interest_rate_lower_limits = debtor.interest_rate_lower_limits
    interest_rate_lower_limits = interest_rate_lower_limits.current_limits(date_week_ago)
    try:
        interest_rate_lower_limits.add_limits(new_interest_rate_limits)
    except TooLongLimitSequenceError:
        raise ConflictingPolicyError('There are too many interest rate limits.')

    balance_lower_limits = debtor.balance_lower_limits
    balance_lower_limits = balance_lower_limits.current_limits(date_week_ago)
    try:
        balance_lower_limits.add_limits(new_balance_limits)
    except TooLongLimitSequenceError:
        raise ConflictingPolicyError('There are too many balance limits.')

    if interest_rate_target is not None:
        debtor.interest_rate_target = interest_rate_target
    debtor.interest_rate_lower_limits = interest_rate_lower_limits
    debtor.balance_lower_limits = balance_lower_limits
    return debtor


@atomic
def get_debtor_transfer_uuids(debtor_id: int) -> List[UUID]:
    debtor_query = Debtor.query.filter_by(debtor_id=debtor_id)
    if not db.session.query(debtor_query.exists()).scalar():
        raise DebtorDoesNotExistError()

    rows = db.session.query(InitiatedTransfer.transfer_uuid).filter_by(debtor_id=debtor_id).all()
    return [uuid for (uuid,) in rows]


@atomic
def get_initiated_transfer(debtor_id: int, transfer_uuid: UUID) -> Optional[InitiatedTransfer]:
    return InitiatedTransfer.get_instance((debtor_id, transfer_uuid))


@atomic
def delete_initiated_transfer(debtor_id: int, transfer_uuid: UUID) -> bool:
    n = InitiatedTransfer.query.filter_by(
        debtor_id=debtor_id,
        transfer_uuid=transfer_uuid,
    ).delete(synchronize_session=False)
    if n == 1:
        Debtor.query.filter_by(debtor_id=debtor_id).update({
            Debtor.initiated_transfers_count: Debtor.initiated_transfers_count - 1,
        }, synchronize_session=False)
        return True
    assert n == 0
    return False


@atomic
def initiate_transfer(
        debtor_id: int,
        transfer_uuid: UUID,
        recipient_creditor_id: Optional[int],
        recipient_uri: str,
        amount: int,
        transfer_info: dict) -> InitiatedTransfer:

    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert recipient_creditor_id is None or MIN_INT64 <= recipient_creditor_id <= MAX_INT64
    assert 0 < amount <= MAX_INT64

    debtor = _throttle_debtor_actions(debtor_id)
    _raise_error_if_too_many_transfers(debtor)
    _raise_error_if_transfer_exists(debtor_id, transfer_uuid, recipient_uri, amount, transfer_info)

    if recipient_creditor_id is None:
        new_transfer = InitiatedTransfer(
            debtor_id=debtor_id,
            transfer_uuid=transfer_uuid,
            recipient_uri=recipient_uri,
            amount=amount,
            transfer_info=transfer_info,
            finalized_at_ts=datetime.now(tz=timezone.utc),
            error={'errorCode': 'DEB001', 'message': 'Unrecognized recipient URI.'},
        )
    else:
        _insert_running_transfer_or_raise_conflict_error(
            debtor=debtor,
            transfer_uuid=transfer_uuid,
            recipient_creditor_id=recipient_creditor_id,
            amount=amount,
            transfer_info=transfer_info,
        )
        new_transfer = InitiatedTransfer(
            debtor_id=debtor_id,
            transfer_uuid=transfer_uuid,
            recipient_uri=recipient_uri,
            amount=amount,
            transfer_info=transfer_info,
        )
    with db.retry_on_integrity_error():
        db.session.add(new_transfer)

    debtor.initiated_transfers_count += 1
    return new_transfer


@atomic
def process_account_purge_signal(debtor_id: int, creditor_id: int, creation_date: date) -> None:
    account = Account.lock_instance((debtor_id, creditor_id))
    if account and account.creation_date == creation_date:
        db.session.delete(account)
        if creditor_id == ROOT_CREDITOR_ID:
            deactivate_debtor(debtor_id, deleted_account=True)


@atomic
def process_rejected_issuing_transfer_signal(coordinator_id: int, coordinator_request_id: int, details: dict) -> None:
    rt = _find_running_transfer(coordinator_id, coordinator_request_id)
    if rt and not rt.is_finalized:
        _finalize_initiated_transfer(rt.debtor_id, rt.transfer_uuid, error=details)
        db.session.delete(rt)


@atomic
def process_prepared_issuing_transfer_signal(
        debtor_id: int,
        sender_creditor_id: int,
        transfer_id: int,
        coordinator_id: int,
        coordinator_request_id: int,
        sender_locked_amount: int,
        recipient_creditor_id: int) -> None:

    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= sender_creditor_id <= MAX_INT64
    assert MIN_INT64 <= transfer_id <= MAX_INT64
    assert sender_locked_amount > 0

    rt = _find_running_transfer(coordinator_id, coordinator_request_id)
    if rt:
        assert rt.debtor_id == debtor_id
        assert rt.amount == sender_locked_amount
        assert ROOT_CREDITOR_ID == sender_creditor_id
        assert rt.recipient_creditor_id == recipient_creditor_id

        if not rt.is_finalized:
            # We finalize the `RunningTransfer` record here, but we
            # deliberately do not finalize the corresponding
            # `InitiatedTransfer` record yet (it will be finalized
            # when the `FinalizedTransferSignal` is received). We do
            # this to avoid reporting a success too early, or even
            # incorrectly in the case of a database crash.
            rt.issuing_transfer_id = transfer_id

        if rt.issuing_transfer_id == transfer_id:
            db.session.add(FinalizePreparedTransferSignal(
                debtor_id=rt.debtor_id,
                sender_creditor_id=ROOT_CREDITOR_ID,
                transfer_id=transfer_id,
                committed_amount=rt.amount,
                transfer_info=rt.transfer_info,
            ))
            return

    # The newly prepared transfer is dismissed.
    db.session.add(FinalizePreparedTransferSignal(
        debtor_id=debtor_id,
        sender_creditor_id=sender_creditor_id,
        transfer_id=transfer_id,
        committed_amount=0,
        transfer_info={},
    ))


@atomic
def process_finalized_issuing_transfer_signal(
        debtor_id: int,
        transfer_id: int,
        coordinator_id: int,
        coordinator_request_id: int,
        recipient_creditor_id: int,
        committed_amount: int) -> None:

    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= transfer_id <= MAX_INT64
    rt = _find_running_transfer(coordinator_id, coordinator_request_id)
    if (rt and rt.debtor_id == debtor_id and rt.issuing_transfer_id == transfer_id):
        assert rt.recipient_creditor_id == recipient_creditor_id

        # When `committed_amount` is zero, the `InitiatedTransfer`
        # record has been already finalized (with an error).
        if committed_amount != 0:
            assert committed_amount == rt.amount
            _finalize_initiated_transfer(rt.debtor_id, rt.transfer_uuid, finalized_at_ts=datetime.now(tz=timezone.utc))

        db.session.delete(rt)


@atomic
def process_account_change_signal(
        debtor_id: int,
        creditor_id: int,
        change_ts: datetime,
        change_seqnum: int,
        principal: int,
        interest: float,
        interest_rate: float,
        last_outgoing_transfer_date: date,
        creation_date: date,
        negligible_amount: float,
        status: int) -> None:

    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert MIN_INT32 <= change_seqnum <= MAX_INT32
    assert -MAX_INT64 <= principal <= MAX_INT64
    assert -100 < interest_rate <= 100.0
    assert negligible_amount >= 2.0
    assert MIN_INT16 <= status <= MAX_INT16

    account = Account.lock_instance((debtor_id, creditor_id))
    if account:
        this_event = (change_ts, change_seqnum)
        prev_event = (account.change_ts, account.change_seqnum)
        if this_event == prev_event:
            account.last_heartbeat_ts = datetime.now(tz=timezone.utc)
        if not is_later_event(this_event, prev_event):
            return
        account.change_seqnum = change_seqnum
        account.change_ts = change_ts
        account.principal = principal
        account.interest = interest
        account.interest_rate = interest_rate
        account.last_outgoing_transfer_date = last_outgoing_transfer_date
        account.creation_date = creation_date
        account.negligible_amount = negligible_amount
        account.status = status
        account.do_not_send_signals_until_ts = None
        account.last_heartbeat_ts = datetime.now(tz=timezone.utc)
    else:
        account = Account(
            debtor_id=debtor_id,
            creditor_id=creditor_id,
            change_seqnum=change_seqnum,
            change_ts=change_ts,
            principal=principal,
            interest=interest,
            interest_rate=interest_rate,
            last_outgoing_transfer_date=last_outgoing_transfer_date,
            creation_date=creation_date,
            negligible_amount=negligible_amount,
            status=status,
        )
        with db.retry_on_integrity_error():
            db.session.add(account)

    if account.creditor_id == ROOT_CREDITOR_ID:
        # If this is a debtor's account, we must update debtor's
        # `balance` and `balance_ts` columns. (Or even create a
        # debtor, if it does not exist.)
        balance = MIN_INT64 if account.is_overflown else account.principal
        balance_ts = account.change_ts
        update_debtor_balance(debtor_id, balance, balance_ts)
    elif not account.status & Account.STATUS_ESTABLISHED_INTEREST_RATE_FLAG:
        # When the account does not have an interest rate set yet, we
        # should immediately send a `ChangeInterestRateSignal`.
        debtor = Debtor.get_instance(debtor_id)
        if debtor:
            signalbus_max_delay = timedelta(days=current_app.config['APP_SIGNALBUS_MAX_DELAY_DAYS'])
            account.do_not_send_signals_until_ts = datetime.now(tz=timezone.utc) + signalbus_max_delay
            insert_change_interest_rate_signal(debtor_id, creditor_id, debtor.interest_rate)


@atomic
def insert_change_interest_rate_signal(debtor_id: int, creditor_id: int, interest_rate: float) -> None:
    db.session.add(ChangeInterestRateSignal(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        interest_rate=interest_rate,
    ))


def _insert_running_transfer_or_raise_conflict_error(
        debtor: Debtor,
        transfer_uuid: UUID,
        recipient_creditor_id: int,
        amount: int,
        transfer_info: dict) -> RunningTransfer:

    running_transfer = RunningTransfer(
        debtor_id=debtor.debtor_id,
        transfer_uuid=transfer_uuid,
        recipient_creditor_id=recipient_creditor_id,
        amount=amount,
        transfer_info=transfer_info,
    )
    db.session.add(running_transfer)
    try:
        db.session.flush()
    except IntegrityError:
        raise TransfersConflictError()

    db.session.add(PrepareTransferSignal(
        debtor_id=debtor.debtor_id,
        coordinator_request_id=running_transfer.issuing_coordinator_request_id,
        min_amount=amount,
        max_amount=amount,
        sender_creditor_id=ROOT_CREDITOR_ID,
        recipient_creditor_id=recipient_creditor_id,
        minimum_account_balance=debtor.minimum_account_balance,
    ))
    return running_transfer


def _raise_error_if_transfer_exists(
        debtor_id: int,
        transfer_uuid: UUID,
        recipient_uri: str,
        amount: int,
        transfer_info: dict) -> None:

    t = InitiatedTransfer.query.filter_by(debtor_id=debtor_id, transfer_uuid=transfer_uuid).one_or_none()
    if t:
        if t.recipient_uri == recipient_uri and t.amount == amount and t.transfer_info == transfer_info:
            raise TransferExistsError(t)
        raise TransfersConflictError()


def _raise_error_if_too_many_transfers(debtor: Debtor) -> None:
    if debtor.initiated_transfers_count >= current_app.config['APP_MAX_TRANSFERS_PER_MONTH']:
        raise TransfersConflictError()


def _throttle_debtor_actions(debtor_id: int) -> Debtor:
    debtor = Debtor.query.filter_by(
        debtor_id=debtor_id,
        deactivated_at_date=None,
    ).with_for_update().one_or_none()
    if debtor is None:
        raise DebtorDoesNotExistError()

    current_date = datetime.now(tz=timezone.utc).date()
    number_of_elapsed_days = (current_date - debtor.actions_throttle_date).days
    if number_of_elapsed_days > 30:  # pragma: no cover
        debtor.actions_throttle_count = 0
        debtor.actions_throttle_date = current_date
    if debtor.actions_throttle_count >= current_app.config['APP_MAX_TRANSFERS_PER_MONTH']:
        raise TooManyManagementActionsError()
    debtor.actions_throttle_count += 1
    debtor.status |= Debtor.STATUS_HAS_ACTIVITY_FLAG
    return debtor


def _find_running_transfer(coordinator_id: int, coordinator_request_id: int) -> Optional[RunningTransfer]:
    assert MIN_INT64 <= coordinator_id <= MAX_INT64
    assert MIN_INT64 < coordinator_request_id <= MAX_INT64

    return RunningTransfer.query.filter_by(
        debtor_id=coordinator_id,
        issuing_coordinator_request_id=coordinator_request_id,
    ).with_for_update().one_or_none()


def _finalize_initiated_transfer(
        debtor_id: int,
        transfer_uuid: int,
        finalized_at_ts: datetime = None,
        error: dict = None) -> None:

    initiated_transfer = InitiatedTransfer.lock_instance((debtor_id, transfer_uuid))
    if initiated_transfer and initiated_transfer.finalized_at_ts is None:
        initiated_transfer.finalized_at_ts = finalized_at_ts or datetime.now(tz=timezone.utc)
        initiated_transfer.is_successful = error is None
        if error is not None:
            initiated_transfer.error = error


def _insert_configure_account_signal(debtor_id: int) -> None:
    db.session.add(ConfigureAccountSignal(
        debtor_id=debtor_id,
        change_ts=datetime.now(tz=timezone.utc),
    ))
