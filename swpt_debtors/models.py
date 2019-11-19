from __future__ import annotations
import math
from numbers import Real
from typing import NamedTuple, List, Tuple, Optional, Iterable
from datetime import datetime, date, timezone
from collections import abc
from marshmallow import Schema, fields
import dramatiq
from sqlalchemy.dialects import postgresql as pg
from sqlalchemy.sql.expression import func, null, false, or_, and_
from .extensions import db, broker, MAIN_EXCHANGE_NAME

MIN_INT16 = -1 << 15
MAX_INT16 = (1 << 15) - 1
MIN_INT32 = -1 << 31
MAX_INT32 = (1 << 31) - 1
MIN_INT64 = -1 << 63
MAX_INT64 = (1 << 63) - 1
INTEREST_RATE_FLOOR = -50.0
INTEREST_RATE_CEIL = 100.0


class Limit(NamedTuple):
    value: Real  # the limiting value
    cutoff: date  # the limit will stop to be enforced *after* this date


class LimitSequence(abc.Sequence):
    lower_limits: bool
    upper_limits: bool
    _limits: List[Limit]

    def __init__(self, limits: Iterable[Limit] = [], *, lower_limits: bool = False, upper_limits: bool = False):
        assert lower_limits or upper_limits, 'the limits type must be specified when calling LimitSequence()'
        assert not (lower_limits and upper_limits)
        self.lower_limits = bool(lower_limits)
        self.upper_limits = bool(upper_limits)
        self._limits = list(limits)

    def __getitem__(self, index):
        return self._limits[index]

    def __len__(self):
        return len(self._limits)

    def __eq__(self, other):
        return (isinstance(other, LimitSequence)
                and self._limits == other._limits
                and self.lower_limits == other.lower_limits
                and self.upper_limits == other.upper_limits)

    def sort(self):
        self._limits.sort(key=lambda l: l.cutoff)

    def insert_limit(self, new_limit: Limit) -> None:
        def find_eliminator_in_sorted_limit_sequence(sorted_limits: LimitSequence) -> Optional[Limit]:
            # Try to find a limit that makes some of the other limits
            # in the sequence redundant.
            restrictiveness: Real = math.inf
            for eliminator in sorted_limits:
                r = self._calc_limit_restrictiveness(eliminator)
                if r >= restrictiveness:
                    return eliminator
                restrictiveness = r
            return None

        eliminator: Optional[Limit] = new_limit
        while eliminator:
            self._apply_eliminator(eliminator)
            self.sort()
            eliminator = find_eliminator_in_sorted_limit_sequence(self)

    def current_limits(self, current_date: date) -> LimitSequence:
        return LimitSequence(
            (l for l in self._limits if l.cutoff >= current_date),
            lower_limits=self.lower_limits,
            upper_limits=self.upper_limits,
        )

    def apply_to_value(self, value: Real) -> Real:
        lower_limits = self.lower_limits
        upper_limits = self.upper_limits
        for limit in self._limits:
            limit_value = limit.value
            if lower_limits and value < limit_value or upper_limits and value > limit_value:
                value = limit_value
        return value

    def _calc_limit_restrictiveness(self, limit: Limit) -> Real:
        return limit.value if self.lower_limits else -limit.value

    def _apply_eliminator(self, eliminator: Limit) -> None:
        r = self._calc_limit_restrictiveness(eliminator)
        cutoff = eliminator.cutoff
        self._limits = [l for l in self._limits if self._calc_limit_restrictiveness(l) > r or l.cutoff > cutoff]
        self._limits.append(eliminator)


def _limits_property(values_attrname: str, cutoffs_attrname: str,
                     *, lower_limits: bool = False, upper_limits: bool = False):
    def unpack_limits(values: Optional[List], cutoffs: Optional[List]) -> LimitSequence:
        values = values or []
        cutoffs = cutoffs or []
        return LimitSequence(
            (Limit(*t) for t in zip(values, cutoffs) if all(x is not None for x in t)),
            lower_limits=lower_limits,
            upper_limits=upper_limits,
        )

    def pack_limits(limits: LimitSequence) -> Tuple[Optional[List], Optional[List]]:
        assert limits.lower_limits == lower_limits
        assert limits.upper_limits == upper_limits
        values = []
        cutoffs = []
        for limit in limits:
            assert isinstance(limit.value, Real)
            assert isinstance(limit.cutoff, date)
            values.append(limit.value)
            cutoffs.append(limit.cutoff)
        return values or None, cutoffs or None

    def getter(self) -> LimitSequence:
        values = getattr(self, values_attrname)
        cutoffs = getattr(self, cutoffs_attrname)
        return unpack_limits(values, cutoffs)

    def setter(self, value: LimitSequence) -> None:
        values, cutoffs = pack_limits(value)
        setattr(self, values_attrname, values)
        setattr(self, cutoffs_attrname, cutoffs)

    return property(getter, setter)


def increment_seqnum(n):
    return MIN_INT32 if n == MAX_INT32 else n + 1


def get_now_utc():
    return datetime.now(tz=timezone.utc)


class Signal(db.Model):
    __abstract__ = True

    # TODO: Define `send_signalbus_messages` class method, set
    #      `ModelClass.signalbus_autoflush = False` and
    #      `ModelClass.signalbus_burst_count = N` in models.

    queue_name: Optional[str] = None

    @property
    def event_name(self):  # pragma: no cover
        model = type(self)
        return f'on_{model.__tablename__}'

    def send_signalbus_message(self):  # pragma: no cover
        model = type(self)
        if model.queue_name is None:
            assert not hasattr(model, 'actor_name'), \
                'SignalModel.actor_name is set, but SignalModel.queue_name is not'
            actor_name = self.event_name
            routing_key = f'events.{actor_name}'
        else:
            actor_name = model.actor_name
            routing_key = model.queue_name
        data = model.__marshmallow_schema__.dump(self)
        message = dramatiq.Message(
            queue_name=model.queue_name,
            actor_name=actor_name,
            args=(),
            kwargs=data,
            options={},
        )
        broker.publish_message(message, exchange=MAIN_EXCHANGE_NAME, routing_key=routing_key)


class Debtor(db.Model):
    STATUS_TERMINATED_FLAG = 1

    debtor_id = db.Column(db.BigInteger, primary_key=True, autoincrement=False)
    status = db.Column(
        db.SmallInteger,
        nullable=False,
        default=0,
        comment='Debtor status flags.',
    )
    last_issuing_coordinator_request_id = db.Column(
        db.BigInteger,
        nullable=False,
        default=0,
        comment='Incremented when a `prepare_transfer` message is constructed for an '
                'issuing transfer. Must never decrease.',
    )
    balance = db.Column(
        db.BigInteger,
        nullable=False,
        default=0,
        comment="The total issued amount with a negative sign. Normally, it will be a "
                "negative number or a zero. A positive value, although theoretically "
                "possible, should be very rare.",
    )
    balance_ts = db.Column(
        db.TIMESTAMP(timezone=True),
        nullable=False,
        default=get_now_utc,
        comment='Updated on each change of the `balance` field.',
    )
    interest_rate_target = db.Column(
        db.REAL,
        nullable=False,
        default=0.0,
        comment="The desired annual rate (in percents) at which the interest should "
                "accumulate on creditors' accounts. The actual interest rate could be "
                "different if interest rate limits are enforced.",
    )

    # Ballance Lower Limits
    bll_values = db.Column(
        pg.ARRAY(db.BigInteger, dimensions=1),
        comment='Enforced lower limits for the `balance` field. Each element in  '
                'this array should have a corresponding element in the `bll_cutoffs` '
                'arrays (the cutoff dates for the limits). A `null` is the same as '
                'an empty array.',
    )
    bll_cutoffs = db.Column(pg.ARRAY(db.DATE, dimensions=1))

    # Interest Rate Lower Limits
    irll_values = db.Column(
        pg.ARRAY(db.BigInteger, dimensions=1),
        comment=(
            'Enforced interest rate lower limits. Each element in this array '
            'should have a corresponding element in the `irll_cutoffs` array '
            '(the cutoff dates for the limits). A `null` is the same as an '
            'empty array. If the array contains values bigger that {ceil}, '
            'they are treated as equal to {ceil}.'
        ).format(ceil=INTEREST_RATE_CEIL),
    )
    irll_cutoffs = db.Column(pg.ARRAY(db.DATE, dimensions=1))

    __table_args__ = (
        db.CheckConstraint(and_(
            interest_rate_target >= INTEREST_RATE_FLOOR,
            interest_rate_target <= INTEREST_RATE_CEIL,
        )),
        db.CheckConstraint(or_(bll_values == null(), func.array_ndims(bll_values) == 1)),
        db.CheckConstraint(or_(bll_cutoffs == null(), func.array_ndims(bll_cutoffs) == 1)),
        db.CheckConstraint(or_(irll_values == null(), func.array_ndims(irll_values) == 1)),
        db.CheckConstraint(or_(irll_cutoffs == null(), func.array_ndims(irll_cutoffs) == 1)),
        {
            'comment': "Represents debtor's principal information.",
        }
    )

    balance_lower_limits = _limits_property('bll_values', 'bll_cutoffs', lower_limits=True)
    interest_rate_lower_limits = _limits_property('irll_values', 'irll_cutoffs', lower_limits=True)

    # TODO: Add DOS-prevention fields.


class PendingTransfer(db.Model):
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    transfer_uuid = db.Column(pg.UUID(as_uuid=True), primary_key=True)
    amount = db.Column(
        db.BigInteger,
        nullable=False,
        comment='The amount to be transferred. Must be positive.',
    )
    transfer_info = db.Column(
        pg.JSON,
        nullable=False,
        default={},
        comment='A note from the debtor. Can be anything that the debtor wants the recipient to see.',
    )
    finalized_at_ts = db.Column(
        db.TIMESTAMP(timezone=True),
        comment='The moment at which the transfer was finalized. A `null` means that the '
                'transfer has not been finalized yet.',
    )
    is_successful = db.Column(
        db.BOOLEAN,
        nullable=False,
        default=False,
        comment='Whether the transfer has been successful or not.',
    )
    __table_args__ = (
        db.CheckConstraint(amount > 0),
        db.CheckConstraint(or_(finalized_at_ts != null(), is_successful == false())),
        {
            'comment': 'Represents a pending issuing transfer. A new row is inserted when '
                       'a debtor creates a new issuing transfer. The row is deleted when '
                       'the debtor acknowledges (purges) the transfer.',
        }
    )


class RecentTransfer(db.Model):
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    transfer_uuid = db.Column(pg.UUID(as_uuid=True), primary_key=True)
    amount = db.Column(
        db.BigInteger,
        nullable=False,
        comment='The amount to be transferred. Must be positive.',
    )
    transfer_info = db.Column(
        pg.JSON,
        nullable=False,
        default={},
        comment='A note from the debtor. Can be anything that the debtor wants the recipient to see.',
    )
    finalized_at_ts = db.Column(
        db.TIMESTAMP(timezone=True),
        comment='The moment at which the transfer was finalized. A `null` means that the '
                'transfer has not been finalized yet.',
    )
    issuing_coordinator_request_id = db.Column(
        db.BigInteger,
        nullable=False,
        comment='This is the value of the `coordinator_request_id` parameter, which has been '
                'sent with the `prepare_transfer` message for the transfer. The value of '
                '`debtor_id` is sent as the `coordinator_id` parameter. `coordinator_type` '
                'is "issuing".',
    )
    issuing_transfer_id = db.Column(
        db.BigInteger,
        comment="This value, along with `debtor_id` uniquely identifies the successfully prepared "
                "transfer. (The sender is always the debtor's account.)",
    )
    __table_args__ = (
        db.CheckConstraint(amount > 0),
        {
            'comment': 'Represents a recently initiated issuing transfer from a debtor. '
                       'Note that finalized issuing transfers (failed or successful) must not be '
                       'deleted right away. Instead, after they have been finalized, they should '
                       'stay in the database for at least few days. This is necessary in order '
                       'to prevent problems caused by message re-delivery.',
        }
    )


class Account(db.Model):
    STATUS_DELETED_FLAG = 1
    STATUS_ESTABLISHED_INTEREST_RATE_FLAG = 2
    STATUS_OVERFLOWN_FLAG = 4
    STATUS_SCHEDULED_FOR_DELETION_FLAG = 8

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    change_seqnum = db.Column(
        db.Integer,
        nullable=False,
        comment='Updated when a received `AccountChangeSignal` is applied.',
    )
    change_ts = db.Column(
        db.TIMESTAMP(timezone=True),
        nullable=False,
        comment='Updated when a received `AccountChangeSignal` is applied.',
    )
    principal = db.Column(
        db.BigInteger,
        nullable=False,
        comment='The total owed amount. Can be negative.',
    )
    interest = db.Column(
        db.FLOAT,
        nullable=False,
        comment='The amount of interest accumulated on the account before `change_ts`, '
                'but not added to the `principal` yet. Can be a negative number. `interest`'
                'gets zeroed and added to the principal once in a while (like once per week).',
    )
    interest_rate = db.Column(
        db.REAL,
        nullable=False,
        comment='Annual rate (in percents) at which interest accumulates on the account.',
    )
    last_outgoing_transfer_date = db.Column(
        db.DATE,
        comment='Updated on each transfer for which this account is the sender. This field is '
                'not updated on demurrage payments.',
    )
    status = db.Column(
        db.SmallInteger,
        nullable=False,
        comment='Additional account status flags.',
    )
    interest_rate_last_change_seqnum = db.Column(
        db.Integer,
        nullable=False,
        default=1,
        comment='Incremented (with wrapping) on each invocation of the `change_interest_rate` actor.',
    )
    interest_rate_last_change_ts = db.Column(
        db.TIMESTAMP(timezone=True),
        nullable=False,
        default=get_now_utc,
        comment='Updated on every increment of `interest_rate_last_change_seqnum`. Must never decrease.',
    )
    __table_args__ = (
        db.CheckConstraint((interest_rate >= INTEREST_RATE_FLOOR) & (interest_rate <= INTEREST_RATE_CEIL)),
        db.CheckConstraint(principal > MIN_INT64),
        {
            'comment': 'Tells who owes what to whom. This table is a replica of the table with the '
                       'same name in the `swpt_accounts` service. It is used to perform maintenance '
                       'routines like changing interest rates.',
        }
    )


class ChangeInterestRateSignal(Signal):
    queue_name = 'swpt_accounts'
    actor_name = 'change_interest_rate'

    class __marshmallow__(Schema):
        debtor_id = fields.Integer()
        creditor_id = fields.Integer()
        change_seqnum = fields.Integer()
        change_ts = fields.DateTime()
        interest_rate = fields.Float()

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    signal_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    creditor_id = db.Column(db.BigInteger, nullable=False)
    change_seqnum = db.Column(db.Integer, nullable=False)
    change_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    interest_rate = db.Column(db.REAL, nullable=False)
