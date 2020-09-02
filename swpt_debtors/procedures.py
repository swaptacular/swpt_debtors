from datetime import datetime, date, timedelta, timezone
from uuid import UUID
from typing import TypeVar, Optional, Callable, List
from flask import current_app
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql.expression import true
from swpt_lib.utils import Seqnum, u64_to_i64
from .extensions import db
from .lower_limits import LowerLimitSequence, TooLongLimitSequenceError
from .models import Debtor, Account, ChangeInterestRateSignal, FinalizeTransferSignal, \
    InitiatedTransfer, RunningTransfer, PrepareTransferSignal, ConfigureAccountSignal, \
    MIN_INT32, MAX_INT32, MIN_INT64, MAX_INT64, ROOT_CREDITOR_ID

T = TypeVar('T')
atomic: Callable[[T], T] = db.atomic

TD_SECOND = timedelta(seconds=1)


class DebtorDoesNotExistError(Exception):
    """The debtor does not exist."""


class DebtorExistsError(Exception):
    """The same debtor record already exists."""


class TransferDoesNotExistError(Exception):
    """The transfer does not exist."""


class TransferExistsError(Exception):
    """The same initiated transfer record already exists."""

    def __init__(self, transfer: InitiatedTransfer):
        self.transfer = transfer


class TransfersConflictError(Exception):
    """A different transfer with conflicting UUID already exists."""


class TransferUpdateConflictError(Exception):
    """The requested transfer update is not possible."""


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
def create_new_debtor(debtor_id: int) -> Debtor:
    assert MIN_INT64 <= debtor_id <= MAX_INT64

    debtor = Debtor(debtor_id=debtor_id)
    db.session.add(debtor)
    try:
        db.session.flush()
    except IntegrityError:
        raise DebtorExistsError()
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
        # If the debtor does not exist, we create a brand new
        # deactivated debtor. That way we guarantee that the debtor's
        # account will be (eventually) deleted from the
        # `swpt_accounts` service when it is no longer used.
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
            debtor.bll_values = None
            debtor.bll_cutoffs = None
            debtor.irll_values = None
            debtor.irll_cutoffs = None
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
def cancel_transfer(debtor_id: int, transfer_uuid: UUID) -> InitiatedTransfer:
    initiated_transfer = InitiatedTransfer.lock_instance((debtor_id, transfer_uuid))

    if not initiated_transfer:
        raise TransferDoesNotExistError()

    if initiated_transfer.is_successful:
        raise TransferUpdateConflictError()

    if not initiated_transfer.is_finalized:
        rt = RunningTransfer.lock_instance((debtor_id, transfer_uuid))

        # The `InitiatedTransfer` and `RunningTransfer` records are
        # created together, and whenever the `RunningTransfer` gets
        # removed, the `InitiatedTransfer` gets finalizad.
        assert rt

        if rt.is_settled:
            raise TransferUpdateConflictError()
        initiated_transfer.finalized_at_ts = datetime.now(tz=timezone.utc)
        initiated_transfer.error = {'errorCode': 'CANCELED_TRANSFER'}
        db.session.delete(rt)

    assert initiated_transfer.is_finalized and not initiated_transfer.is_successful
    return initiated_transfer


@atomic
def delete_initiated_transfer(debtor_id: int, transfer_uuid: UUID) -> bool:
    number_of_deleted_rows = InitiatedTransfer.query.\
        filter_by(debtor_id=debtor_id, transfer_uuid=transfer_uuid).\
        delete(synchronize_session=False)

    assert number_of_deleted_rows in [0, 1]
    if number_of_deleted_rows == 1:
        Debtor.query.\
            filter_by(debtor_id=debtor_id).\
            update({Debtor.initiated_transfers_count: Debtor.initiated_transfers_count - 1}, synchronize_session=False)

    return number_of_deleted_rows == 1


@atomic
def initiate_transfer(
        debtor_id: int,
        transfer_uuid: UUID,
        recipient_creditor_id: int,
        amount: int,
        transfer_note: str) -> InitiatedTransfer:

    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= recipient_creditor_id <= MAX_INT64
    assert 0 < amount <= MAX_INT64

    _raise_error_if_transfer_exists(debtor_id, transfer_uuid, recipient_creditor_id, amount, transfer_note)
    debtor = _throttle_debtor_actions(debtor_id)
    _increment_initiated_transfers_count(debtor)

    _insert_running_transfer_or_raise_conflict_error(
        debtor=debtor,
        transfer_uuid=transfer_uuid,
        recipient_creditor_id=recipient_creditor_id,
        amount=amount,
        transfer_note=transfer_note,
    )

    new_transfer = InitiatedTransfer(
        debtor_id=debtor_id,
        transfer_uuid=transfer_uuid,
        recipient_creditor_id=recipient_creditor_id,
        amount=amount,
        transfer_note=transfer_note,
    )

    with db.retry_on_integrity_error():
        db.session.add(new_transfer)

    return new_transfer


@atomic
def process_account_purge_signal(debtor_id: int, creditor_id: int, creation_date: date) -> None:
    account = Account.lock_instance((debtor_id, creditor_id))
    if account and account.creation_date == creation_date:
        db.session.delete(account)
        if creditor_id == ROOT_CREDITOR_ID:
            deactivate_debtor(debtor_id, deleted_account=True)


@atomic
def process_rejected_issuing_transfer_signal(
        coordinator_id: int,
        coordinator_request_id: int,
        status_code: str,
        total_locked_amount: int,
        debtor_id: int,
        sender_creditor_id: int) -> None:

    assert status_code == '' or len(status_code) <= 30 and status_code.encode('ascii')
    assert 0 <= total_locked_amount <= MAX_INT64
    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= sender_creditor_id <= MAX_INT64

    rt = _find_running_transfer(coordinator_id, coordinator_request_id)
    if rt and not rt.is_settled:
        if rt.debtor_id == debtor_id and ROOT_CREDITOR_ID == sender_creditor_id:
            error = {
                'errorCode': status_code,
                'totalLockedAmount': total_locked_amount,
            }
        else:  # pragma:  no cover
            error = {
                'errorCode': 'UNEXPECTED_ERROR',
                'totalLockedAmount': 0,
            }
        _finalize_initiated_transfer(rt.debtor_id, rt.transfer_uuid, error=error)
        db.session.delete(rt)


@atomic
def process_prepared_issuing_transfer_signal(
        debtor_id: int,
        sender_creditor_id: int,
        transfer_id: int,
        coordinator_id: int,
        coordinator_request_id: int,
        sender_locked_amount: int,
        recipient: str) -> None:

    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= sender_creditor_id <= MAX_INT64
    assert MIN_INT64 <= transfer_id <= MAX_INT64
    assert 0 < sender_locked_amount <= MAX_INT64

    recipient_creditor_id = u64_to_i64(int(recipient))
    rt = _find_running_transfer(coordinator_id, coordinator_request_id)
    rt_matches_the_signal = (
        rt is not None
        and rt.debtor_id == debtor_id
        and ROOT_CREDITOR_ID == sender_creditor_id
        and rt.recipient_creditor_id == recipient_creditor_id
        and rt.amount <= sender_locked_amount
    )
    if rt_matches_the_signal:
        assert rt is not None
        if not rt.is_settled:
            # We settle the `RunningTransfer` record here, but we do
            # not finalize the corresponding `InitiatedTransfer`
            # record yet (it will be finalized when the
            # `FinalizedTransferSignal` is received).
            rt.issuing_transfer_id = transfer_id

        if rt.issuing_transfer_id == transfer_id:
            db.session.add(FinalizeTransferSignal(
                debtor_id=rt.debtor_id,
                sender_creditor_id=ROOT_CREDITOR_ID,
                transfer_id=transfer_id,
                coordinator_id=coordinator_id,
                coordinator_request_id=coordinator_request_id,
                committed_amount=rt.amount,
                transfer_note=rt.transfer_note,
            ))
            return

    # The newly prepared transfer is dismissed.
    db.session.add(FinalizeTransferSignal(
        debtor_id=debtor_id,
        sender_creditor_id=sender_creditor_id,
        transfer_id=transfer_id,
        coordinator_id=coordinator_id,
        coordinator_request_id=coordinator_request_id,
        committed_amount=0,
        transfer_note='',
    ))


@atomic
def process_finalized_issuing_transfer_signal(
        debtor_id: int,
        sender_creditor_id: int,
        transfer_id: int,
        coordinator_id: int,
        coordinator_request_id: int,
        recipient: str,
        committed_amount: int,
        status_code: str) -> None:

    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= sender_creditor_id <= MAX_INT64
    assert MIN_INT64 <= transfer_id <= MAX_INT64
    assert 0 <= committed_amount <= MAX_INT64
    assert 0 <= len(status_code.encode('ascii')) <= 30

    recipient_creditor_id = u64_to_i64(int(recipient))
    rt = _find_running_transfer(coordinator_id, coordinator_request_id)
    rt_matches_the_signal = (
        rt is not None
        and rt.debtor_id == debtor_id
        and ROOT_CREDITOR_ID == sender_creditor_id
        and rt.issuing_transfer_id == transfer_id
    )
    if rt_matches_the_signal:
        assert rt is not None
        if committed_amount == rt.amount and recipient_creditor_id == rt.recipient_creditor_id:
            error = None
        else:  # pragma: no cover
            error = {'errorCode': status_code}
        _finalize_initiated_transfer(rt.debtor_id, rt.transfer_uuid, error=error)
        db.session.delete(rt)


@atomic
def process_account_update_signal(
        debtor_id: int,
        creditor_id: int,
        last_change_ts: datetime,
        last_change_seqnum: int,
        principal: int,
        interest: float,
        interest_rate: float,
        last_interest_rate_change_ts: datetime,
        creation_date: date,
        negligible_amount: float,
        config_flags: int,
        status_flags: int,
        ts: datetime,
        ttl: int) -> None:

    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert MIN_INT32 <= last_change_seqnum <= MAX_INT32
    assert -MAX_INT64 <= principal <= MAX_INT64
    assert -100 < interest_rate <= 100.0
    assert negligible_amount >= 0.0
    assert MIN_INT32 <= config_flags <= MAX_INT32
    assert MIN_INT32 <= status_flags <= MAX_INT32
    assert ttl > 0

    current_ts = datetime.now(tz=timezone.utc)
    ts = min(ts, current_ts)
    if (current_ts - ts).total_seconds() > ttl:
        return

    account = Account.lock_instance((debtor_id, creditor_id))
    if account:
        if ts > account.last_heartbeat_ts:
            account.last_heartbeat_ts = ts
        prev_event = (account.creation_date, account.last_change_ts, Seqnum(account.last_change_seqnum))
        this_event = (creation_date, last_change_ts, Seqnum(last_change_seqnum))
        if this_event <= prev_event:
            return
        account.last_change_seqnum = last_change_seqnum
        account.last_change_ts = last_change_ts
        account.principal = principal
        account.interest = interest
        account.interest_rate = interest_rate
        account.last_interest_rate_change_ts = last_interest_rate_change_ts
        account.creation_date = creation_date
        account.negligible_amount = negligible_amount
        account.config_flags = config_flags
        account.status_flags = status_flags
    else:
        account = Account(
            debtor_id=debtor_id,
            creditor_id=creditor_id,
            last_change_seqnum=last_change_seqnum,
            last_change_ts=last_change_ts,
            principal=principal,
            interest=interest,
            interest_rate=interest_rate,
            last_interest_rate_change_ts=last_interest_rate_change_ts,
            creation_date=creation_date,
            negligible_amount=negligible_amount,
            config_flags=config_flags,
            status_flags=status_flags,
            last_heartbeat_ts=ts,
        )
        with db.retry_on_integrity_error():
            db.session.add(account)

    if account.creditor_id == ROOT_CREDITOR_ID:
        balance = MIN_INT64 if account.is_overflown else account.principal
        balance_ts = account.last_change_ts
        update_debtor_balance(debtor_id, balance, balance_ts)
    elif not account.status_flags & Account.STATUS_ESTABLISHED_INTEREST_RATE_FLAG:
        cutoff_ts = current_ts - Account.get_interest_rate_change_min_interval()
        debtor = Debtor.get_instance(debtor_id)
        if debtor and account.last_interest_rate_change_ts < cutoff_ts:
            account.is_muted = True
            account.last_maintenance_request_ts = current_ts
            insert_change_interest_rate_signal(debtor_id, creditor_id, debtor.interest_rate, current_ts)


@atomic
def process_account_maintenance_signal(debtor_id: int, creditor_id: int, request_ts: datetime) -> None:
    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= creditor_id <= MAX_INT64

    Account.query.\
        filter_by(debtor_id=debtor_id, creditor_id=creditor_id).\
        filter(Account.is_muted == true()).\
        filter(Account.last_maintenance_request_ts <= request_ts + TD_SECOND).\
        update({Account.is_muted: False}, synchronize_session=False)


@atomic
def insert_change_interest_rate_signal(
        debtor_id: int,
        creditor_id: int,
        interest_rate: float,
        request_ts: datetime) -> None:

    db.session.add(ChangeInterestRateSignal(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        interest_rate=interest_rate,
        request_ts=request_ts,
    ))


def _insert_running_transfer_or_raise_conflict_error(
        debtor: Debtor,
        transfer_uuid: UUID,
        recipient_creditor_id: int,
        amount: int,
        transfer_note: str) -> RunningTransfer:

    running_transfer = RunningTransfer(
        debtor_id=debtor.debtor_id,
        transfer_uuid=transfer_uuid,
        recipient_creditor_id=recipient_creditor_id,
        amount=amount,
        transfer_note=transfer_note,
    )
    db.session.add(running_transfer)
    try:
        db.session.flush()
    except IntegrityError:
        raise TransfersConflictError()

    db.session.add(PrepareTransferSignal(
        debtor_id=debtor.debtor_id,
        coordinator_request_id=running_transfer.issuing_coordinator_request_id,
        min_locked_amount=amount,
        max_locked_amount=amount,
        sender_creditor_id=ROOT_CREDITOR_ID,
        recipient_creditor_id=recipient_creditor_id,
        min_account_balance=debtor.min_account_balance,
    ))
    return running_transfer


def _raise_error_if_transfer_exists(
        debtor_id: int,
        transfer_uuid: UUID,
        recipient_creditor_id: int,
        amount: int,
        transfer_note: str) -> None:

    t = InitiatedTransfer.query.filter_by(debtor_id=debtor_id, transfer_uuid=transfer_uuid).one_or_none()
    if t:
        is_same_transfer = (
            t.recipient_creditor_id == recipient_creditor_id
            and t.amount == amount
            and t.transfer_note == transfer_note
        )
        if is_same_transfer:
            raise TransferExistsError(t)
        raise TransfersConflictError()


def _increment_initiated_transfers_count(debtor: Debtor) -> None:
    if debtor.initiated_transfers_count >= current_app.config['APP_MAX_TRANSFERS_PER_MONTH']:
        raise TransfersConflictError()
    debtor.initiated_transfers_count += 1


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
        ts=datetime.now(tz=timezone.utc),
    ))
