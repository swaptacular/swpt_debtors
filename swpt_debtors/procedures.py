from datetime import datetime, date, timedelta, timezone
from typing import TypeVar, Optional, Callable, Tuple
from .extensions import db
from .models import Debtor, Account, ChangeInterestRateSignal, LowerLimitSequence, increment_seqnum, \
    MIN_INT16, MAX_INT16, MIN_INT32, MAX_INT32, MIN_INT64, MAX_INT64, INTEREST_RATE_FLOOR, INTEREST_RATE_CEIL

T = TypeVar('T')
atomic: Callable[[T], T] = db.atomic

TD_ZERO = timedelta(seconds=0)
TD_SECOND = timedelta(seconds=1)
TD_MINUS_SECOND = -TD_SECOND


@atomic
def get_debtor(debtor_id: int) -> Optional[Debtor]:
    return Debtor.get_instance(debtor_id)


@atomic
def get_current_interest_rate(debtor: Debtor) -> float:
    current_ts = datetime.now(tz=timezone.utc)
    interest_rate = _calc_interest_rate(current_ts.date(), debtor)
    assert interest_rate is not None
    return interest_rate


@atomic
def update_debtor_policy(
        debtor_id: int,
        interest_rate_target: float,
        new_interest_rate_limits: LowerLimitSequence,
        new_balance_limits: LowerLimitSequence):
    # TODO: This is probably not at all the function we need.

    debtor = Debtor.get_instance(debtor_id)
    if debtor is None:
        # TODO: define own exception type.
        raise Exception()

    interest_rate_lower_limits = debtor.interest_rate_lower_limits
    for l in new_interest_rate_limits:
        interest_rate_lower_limits.add_limit(l)
    balance_lower_limits = debtor.balance_lower_limits
    for l in new_balance_limits:
        balance_lower_limits.add_limit(l)
    debtor.interest_rate_target = interest_rate_target
    debtor.interest_rate_lower_limits = interest_rate_lower_limits
    debtor.balance_lower_limits = balance_lower_limits


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


def _calc_interest_rate(today: date, debtor: Optional[Debtor]) -> Optional[float]:
    if debtor is None:
        return None

    # Apply debtor's enforced interest rate limits.
    interest_rate = debtor.interest_rate_target
    interest_rate = debtor.interest_rate_lower_limits.current_limits(today).apply_to_value(interest_rate)

    # Apply the absolute interest rate limits.
    if interest_rate < INTEREST_RATE_FLOOR:
        interest_rate = INTEREST_RATE_FLOOR
    if interest_rate > INTEREST_RATE_CEIL:
        interest_rate = INTEREST_RATE_CEIL

    assert INTEREST_RATE_FLOOR <= interest_rate <= INTEREST_RATE_CEIL
    return interest_rate


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
def process_account_change_signal(
        debtor_id: int,
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
        today = datetime.now(tz=timezone.utc).date()
        debtor = Debtor.get_instance(debtor_id)
        _insert_change_interest_rate_signal(account, _calc_interest_rate(today, debtor))
