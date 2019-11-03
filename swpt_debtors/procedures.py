from datetime import datetime, date, timedelta, timezone
from typing import TypeVar, Optional, Callable, Tuple
from .extensions import db
from .models import Debtor, Account, ChangeInterestRateSignal, InterestRateConcession, ChangedDebtorInfoSignal, \
    increment_seqnum, MIN_INT16, MAX_INT16, MIN_INT32, MAX_INT32, MIN_INT64, MAX_INT64

T = TypeVar('T')
atomic: Callable[[T], T] = db.atomic

TD_ZERO = timedelta(seconds=0)
TD_SECOND = timedelta(seconds=1)
TD_MINUS_SECOND = -TD_SECOND


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


def _calc_interest_rate(
        account_principal: int,
        debtor: Optional[Debtor],
        concession: Optional[InterestRateConcession]) -> Optional[float]:
    if not debtor:
        return None
    today = datetime.now(tz=timezone.utc).date()

    # Apply debtor's standard interest rate limits.
    interest_rate = debtor.interest_rate_target
    interest_rate = debtor.interest_rate_upper_limits.current_limits(today).apply_to_value(interest_rate)
    interest_rate = debtor.interest_rate_lower_limits.current_limits(today).apply_to_value(interest_rate)

    # Apply concession interest rate limits.
    if concession:
        assert concession.debtor_id == debtor.debtor_id
        max_account_principal = concession.account_principal_limits.current_limits(today).apply_to_value(MIN_INT64)
        if account_principal <= max_account_principal:
            interest_rate = concession.interest_rate_lower_limits.current_limits(today).apply_to_value(interest_rate)

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


def _insert_changed_debtor_info_signal(debtor: Debtor, current_ts: datetime = None) -> None:
    current_ts = current_ts or datetime.now(tz=timezone.utc)
    debtor.last_change_seqnum = increment_seqnum(debtor.last_change_seqnum)
    debtor.last_change_ts = max(debtor.last_change_ts, current_ts)
    db.session.add(ChangedDebtorInfoSignal(
        debtor_id=debtor.debtor_id,
        change_seqnum=debtor.last_change_seqnum,
        change_ts=debtor.last_change_ts,
        status=debtor.status,
        balance=debtor.balance,
        interest_rate_target=debtor.interest_rate_target,
        bll_values=debtor.bll_values,
        bll_cutoffs=debtor.bll_cutoffs,
        irll_values=debtor.irll_values,
        irll_cutoffs=debtor.irll_cutoffs,
        irul_values=debtor.irul_values,
        irul_cutoffs=debtor.irul_cutoffs,
    ))


@atomic
def get_or_create_debtor(debtor_id: int) -> Debtor:
    assert MIN_INT64 <= debtor_id <= MAX_INT64
    debtor = Debtor.get_instance(debtor_id)
    if debtor is None:
        debtor = Debtor(debtor_id=debtor_id)
        with db.retry_on_integrity_error():
            db.session.add(debtor)
        _insert_changed_debtor_info_signal(debtor)
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

    this_event = (change_seqnum, change_ts)
    account_pk = (debtor_id, creditor_id)

    # TODO: Use caches for `Debtor`s and `InterestRateConcession`s.
    debtor = Debtor.get_instance(debtor_id)
    interest_rate_concession = InterestRateConcession.get_instance(account_pk)

    account = Account.lock_instance(account_pk)
    if account:
        if not _is_later_event(this_event, (account.change_seqnum, account.change_ts)):
            return
        old_interest_rate = _calc_interest_rate(account.principal, debtor, interest_rate_concession)
        account.change_seqnum = change_seqnum
        account.change_ts = change_ts
        account.principal = principal
        account.interest = interest
        account.interest_rate = interest_rate
        account.last_outgoing_transfer_date = last_outgoing_transfer_date
        account.status = status
    else:
        old_interest_rate = _calc_interest_rate(0, debtor, interest_rate_concession)
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

    # There are two cases when we must send `ChangeInterestRateSignal`
    # immediately: 1) The account does not have an interest rate set
    # yet; 2) A change in the account balance caused the interest rate
    # on the account to change.
    has_interest_rate_set = account.status & Account.STATUS_ESTABLISHED_INTEREST_RATE_FLAG
    new_interest_rate = _calc_interest_rate(account.principal, debtor, interest_rate_concession)
    if not has_interest_rate_set or new_interest_rate != old_interest_rate:
        _insert_change_interest_rate_signal(account, new_interest_rate)
