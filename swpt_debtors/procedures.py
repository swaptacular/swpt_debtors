import math
from datetime import datetime, date, timedelta, timezone
from numbers import Real
from typing import TypeVar, List, Optional, Callable, Tuple
from .extensions import db
from .models import Limit, Account, ChangeInterestRateSignal, increment_seqnum, \
    MIN_INT16, MAX_INT16, MIN_INT32, MAX_INT32, MIN_INT64, MAX_INT64

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


def _add_limit_to_list(l: List[Limit], new_limit: Limit, *, lower_limit=False, upper_limit=False) -> None:
    assert lower_limit or upper_limit, 'the limit type must be specified when calling _add_limit_to_list()'
    assert not (lower_limit and upper_limit)

    def get_restrictiveness(limit: Limit) -> Real:
        return limit.value if lower_limit else -limit.value

    def apply_eliminator(limits: List[Limit], eliminator: Limit) -> List[Limit]:
        """Remove the limits rendered ineffectual by the `eliminator`."""

        r = get_restrictiveness(eliminator)
        cutoff = eliminator.cutoff
        return [limit for limit in limits if get_restrictiveness(limit) > r or limit.cutoff > cutoff]

    def find_eliminator_in_sorted_limits(sorted_limits: List[Limit]) -> Optional[Limit]:
        """Try to find a limit that makes some of the other limits ineffectual."""

        restrictiveness = math.inf
        for eliminator in sorted_limits:
            r = get_restrictiveness(eliminator)
            if r >= restrictiveness:
                return eliminator
            restrictiveness = r
        return None

    limits = l
    while True:
        limits = apply_eliminator(limits, new_limit)
        limits.append(new_limit)
        limits.sort(key=lambda limit: limit.cutoff)
        new_limit = find_eliminator_in_sorted_limits(limits)
        if not new_limit:
            break
    l.clear()
    l.extend(limits)


def _calc_interest_rate(debtor_id: int, creditor_id: int) -> float:
    # TODO: Write a real implementation.
    return 0.0


def _insert_change_interest_rate_signal(account: Account, interest_rate: float, current_ts: datetime = None) -> None:
    current_ts = current_ts or datetime.now(tz=timezone.utc)
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
    account = Account.lock_instance((debtor_id, creditor_id))
    if account:
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
        prev_event = (None, None)
        assert _is_later_event(this_event, prev_event)
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

    if not account.status & Account.STATUS_ESTABLISHED_INTEREST_RATE_FLAG:
        _insert_change_interest_rate_signal(account, _calc_interest_rate(debtor_id, creditor_id))
