from datetime import datetime, date, timedelta, timezone
from uuid import UUID
from typing import TypeVar, Optional, Callable, Tuple, List
from sqlalchemy.exc import IntegrityError
from .extensions import db
from .lower_limits import LowerLimitSequence, TooLongLimitSequenceError
from .models import Debtor, Account, ChangeInterestRateSignal, \
    InitiatedTransfer, RunningTransfer, PrepareTransferSignal, increment_seqnum, \
    MIN_INT16, MAX_INT16, MIN_INT32, MAX_INT32, MIN_INT64, MAX_INT64, ROOT_CREDITOR_ID

T = TypeVar('T')
atomic: Callable[[T], T] = db.atomic

TD_ZERO = timedelta(seconds=0)
TD_SECOND = timedelta(seconds=1)
TD_MINUS_SECOND = -TD_SECOND


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


class TooManyTransfersError(Exception):
    """Too many simultaneous transfers."""


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
    return debtor


@atomic
def get_or_create_debtor(debtor_id: int) -> Debtor:
    assert MIN_INT64 <= debtor_id <= MAX_INT64
    debtor = Debtor.get_instance(debtor_id)
    if debtor is None:
        debtor = Debtor(debtor_id=debtor_id)
        with db.retry_on_integrity_error():
            db.session.add(debtor)
    return debtor


@atomic
def update_debtor_policy(debtor_id: int,
                         interest_rate_target: Optional[float],
                         new_interest_rate_limits: LowerLimitSequence,
                         new_balance_limits: LowerLimitSequence) -> Debtor:
    debtor = Debtor.lock_instance(debtor_id)
    if debtor is None:
        raise DebtorDoesNotExistError()
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
def delete_initiated_transfer(debtor_id: int, transfer_uuid: UUID) -> int:
    return InitiatedTransfer.query.filter_by(debtor_id=debtor_id, transfer_uuid=transfer_uuid).delete()


@atomic
def initiate_transfer(debtor_id: int,
                      transfer_uuid: UUID,
                      recipient_creditor_id: int,
                      recipient_uri: str,
                      amount: int,
                      transfer_info: dict) -> InitiatedTransfer:
    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert recipient_creditor_id is None or MIN_INT64 <= recipient_creditor_id <= MAX_INT64
    assert 0 < amount <= MAX_INT64

    debtor = Debtor.get_instance(debtor_id)
    if debtor is None:
        raise DebtorDoesNotExistError()

    # TODO: Raise `TooManyTransfersError` if the debtor has initiated too many transfers.

    return _initiate_transfer(debtor, transfer_uuid, recipient_creditor_id, recipient_uri, amount, transfer_info)


@atomic
def process_account_change_signal(debtor_id: int,
                                  creditor_id: int,
                                  change_seqnum: int,
                                  change_ts: datetime,
                                  principal: int,
                                  interest: float,
                                  interest_rate: float,
                                  last_outgoing_transfer_date: date,
                                  status: int) -> None:
    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert MIN_INT32 <= change_seqnum <= MAX_INT32
    assert -MAX_INT64 <= principal <= MAX_INT64
    assert -100 < interest_rate <= 100.0
    assert MIN_INT16 <= status <= MAX_INT16

    account = Account.lock_instance((debtor_id, creditor_id))
    if account:
        this_event = (change_seqnum, change_ts)
        prev_event = (account.change_seqnum, account.change_ts)
        if not _is_later_event(this_event, prev_event):
            return
        account.change_seqnum = change_seqnum
        account.change_ts = change_ts
        account.principal = principal
        account.interest = interest
        account.interest_rate = interest_rate
        account.last_outgoing_transfer_date = last_outgoing_transfer_date
        account.status = status
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
            status=status,
        )
        with db.retry_on_integrity_error():
            db.session.add(account)

    # When the account does not have an interest rate set yet, we must
    # immediately calculate the interest rate currently applied by the
    # debtor, and send a `ChangeInterestRateSignal`.
    if not account.status & Account.STATUS_ESTABLISHED_INTEREST_RATE_FLAG:
        debtor = Debtor.get_instance(debtor_id)
        if debtor:
            _insert_change_interest_rate_signal(account, debtor.interest_rate)


def _is_later_event(event: Tuple[int, datetime], other_event: Tuple[Optional[int], Optional[datetime]]) -> bool:
    seqnum, ts = event
    other_seqnum, other_ts = other_event
    if other_ts:
        advance = ts - other_ts
    else:
        advance = TD_ZERO
    return advance >= TD_MINUS_SECOND and (
        advance > TD_SECOND
        or other_seqnum is None
        or 0 < (seqnum - other_seqnum) % 0x100000000 < 0x80000000
    )


def _insert_change_interest_rate_signal(account: Account, interest_rate: Optional[float]) -> None:
    if interest_rate is not None:
        current_ts = datetime.now(tz=timezone.utc)
        account.interest_rate_last_change_seqnum = increment_seqnum(account.interest_rate_last_change_seqnum)
        account.interest_rate_last_change_ts = max(account.interest_rate_last_change_ts, current_ts)
        db.session.add(ChangeInterestRateSignal(
            debtor_id=account.debtor_id,
            creditor_id=account.creditor_id,
            change_seqnum=account.interest_rate_last_change_seqnum,
            change_ts=account.interest_rate_last_change_ts,
            interest_rate=interest_rate,
        ))


def _are_same_transfers(first: InitiatedTransfer, second: InitiatedTransfer) -> bool:
    return all([
        first.debtor == second.debtor,
        first.transfer_uuid == second.transfer_uuid,
        first.recipient_uri == second.recipient_uri,
        first.amount == second.amount,
        first.transfer_info == second.transfer_info,
    ])


def _initiate_transfer(debtor: Debtor,
                       transfer_uuid: UUID,
                       recipient_creditor_id: Optional[int],
                       recipient_uri: str,
                       amount: int,
                       transfer_info: dict) -> InitiatedTransfer:
    finalized_at_ts: Optional[datetime] = None
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    if recipient_creditor_id is None:
        finalized_at_ts = datetime.now(tz=timezone.utc)
        error_code = 'DEB001'
        error_message = 'Unrecognized recipient URI.'
    new_transfer = InitiatedTransfer(
        debtor=debtor,
        transfer_uuid=transfer_uuid,
        recipient_uri=recipient_uri,
        amount=amount,
        transfer_info=transfer_info,
        finalized_at_ts=finalized_at_ts,
        error_code=error_code,
        error_message=error_message,
    )

    existing_transfer = InitiatedTransfer.get_instance((debtor.debtor_id, transfer_uuid))
    if existing_transfer is None:
        with db.retry_on_integrity_error():
            db.session.add(new_transfer)
    elif _are_same_transfers(new_transfer, existing_transfer):
        raise TransferExistsError(existing_transfer)
    else:
        raise TransfersConflictError()

    if not new_transfer.is_finalized and recipient_creditor_id is not None:
        _insert_running_transfer(new_transfer, recipient_creditor_id)
    return new_transfer


def _insert_running_transfer(t: InitiatedTransfer, recipient_creditor_id: int) -> RunningTransfer:
    running_transfer = RunningTransfer(
        debtor_id=t.debtor_id,
        transfer_uuid=t.transfer_uuid,
        recipient_creditor_id=recipient_creditor_id,
        amount=t.amount,
        transfer_info=t.transfer_info,
    )
    db.session.add(running_transfer)
    try:
        db.session.flush()
    except IntegrityError:
        raise TransfersConflictError()

    db.session.add(PrepareTransferSignal(
        debtor_id=t.debtor_id,
        coordinator_request_id=running_transfer.issuing_coordinator_request_id,
        min_amount=t.amount,
        max_amount=t.amount,
        sender_creditor_id=ROOT_CREDITOR_ID,
        recipient_creditor_id=recipient_creditor_id,
        minimum_account_balance=t.debtor.minimum_account_balance,
    ))
    return running_transfer
