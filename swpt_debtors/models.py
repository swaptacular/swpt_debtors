from __future__ import annotations
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
MAX_UINT64 = MAX_INT64 - MIN_INT64
INTEREST_RATE_FLOOR = -50.0
INTEREST_RATE_CEIL = 100.0
ROOT_CREDITOR_ID = 0


def increment_seqnum(n):
    return MIN_INT32 if n == MAX_INT32 else n + 1


def get_now_utc():
    return datetime.now(tz=timezone.utc)


class LowerLimit(NamedTuple):
    """A numerical lower limit that should be enforced until a given date."""

    value: Real  # the limiting value
    cutoff: date  # the limit will stop to be enforced *after* this date


class LowerLimitSequence(abc.Sequence):
    """A sequence of `LowerLimit`s."""

    _limits: List[LowerLimit]

    def __init__(self, limits: Iterable[LowerLimit] = []):
        self._limits = list(limits)

    def __getitem__(self, index):
        return self._limits[index]

    def __len__(self):
        return len(self._limits)

    def sort(self) -> None:
        """Sort the sequence by cutoff date."""

        self._limits.sort(key=lambda l: l.cutoff)

    def add_limit(self, new_limit: LowerLimit) -> None:
        """Add a limit, eliminate redundant limits, sort the sequence by cutoff date."""

        def find_eliminator_in_sorted_limit_sequence(sorted_limits: LowerLimitSequence) -> Optional[LowerLimit]:
            # Try to find a limit in the sequence that makes redundant
            # at least one of the other limits in the sequence.
            previous_value = None
            for eliminator in sorted_limits:
                value = eliminator.value
                if previous_value is not None and value >= previous_value:
                    return eliminator
                previous_value = value
            return None

        eliminator: Optional[LowerLimit] = new_limit
        while eliminator:
            self._apply_eliminator(eliminator)
            self.sort()
            eliminator = find_eliminator_in_sorted_limit_sequence(self)

    def current_limits(self, current_date: date) -> LowerLimitSequence:
        """Return a new sequence containing only the limits effectual to the `current_date`."""

        return LowerLimitSequence(l for l in self._limits if l.cutoff >= current_date)

    def apply_to_value(self, value: Real) -> Real:
        """Take a value, apply the limits, and return a possibly bigger value."""

        for limit in self._limits:
            limit_value = limit.value
            if value < limit_value:
                value = limit_value
        return value

    def _apply_eliminator(self, eliminator: LowerLimit) -> None:
        value = eliminator.value
        cutoff = eliminator.cutoff
        self._limits = [l for l in self._limits if l.value > value or l.cutoff > cutoff]
        self._limits.append(eliminator)


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


def _lower_limits_property(values_attrname: str, cutoffs_attrname: str):
    """Return a class property that treats two separate attributes (a list
    of values, and a list of cutoffs) as a sequence of lower limits.

    """

    def unpack_limits(values: Optional[List], cutoffs: Optional[List]) -> LowerLimitSequence:
        values = values or []
        cutoffs = cutoffs or []
        return LowerLimitSequence(LowerLimit(*t) for t in zip(values, cutoffs) if all(x is not None for x in t))

    def pack_limits(limits: LowerLimitSequence) -> Tuple[Optional[List], Optional[List]]:
        values = []
        cutoffs = []
        for limit in limits:
            assert isinstance(limit.value, Real)
            assert isinstance(limit.cutoff, date)
            values.append(limit.value)
            cutoffs.append(limit.cutoff)
        return values or None, cutoffs or None

    def getter(self) -> LowerLimitSequence:
        values = getattr(self, values_attrname)
        cutoffs = getattr(self, cutoffs_attrname)
        return unpack_limits(values, cutoffs)

    def setter(self, value: LowerLimitSequence) -> None:
        values, cutoffs = pack_limits(value)
        setattr(self, values_attrname, values)
        setattr(self, cutoffs_attrname, cutoffs)

    return property(getter, setter)


class Debtor(db.Model):
    STATUS_IS_ACTIVE_FLAG = 1

    debtor_id = db.Column(db.BigInteger, primary_key=True, autoincrement=False)
    status = db.Column(
        db.SmallInteger,
        nullable=False,
        default=0,
        comment=f"Debtor's status bits: {STATUS_IS_ACTIVE_FLAG} - is active.",
    )
    created_at_date = db.Column(
        db.DATE,
        nullable=False,
        default=get_now_utc,
        comment='The date on which the debtor was created.',
    )
    deactivated_at_date = db.Column(
        db.DATE,
        comment='The date on which the debtor was deactivated. A `null` means that the '
                'debtor has not been deactivated yet. Management operations (like policy '
                'updates and credit issuing) are not allowed on deactivated debtors. Once '
                'deactivated, a debtor stays deactivated until it is deleted. Important '
                'note: All debtors are created with their "is active" status bit set to `0`, '
                'and it gets set to `1` only after the first management operation has been '
                'performed.',
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
        comment="The annual rate (in percents) at which the debtor wants the interest "
                "to accumulate on creditors' accounts. The actual interest rate may be "
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
        pg.ARRAY(db.REAL, dimensions=1),
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
        db.CheckConstraint(or_(
            deactivated_at_date == null(),
            status.op('&')(STATUS_IS_ACTIVE_FLAG) == 0,
        )),
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

    balance_lower_limits = _lower_limits_property('bll_values', 'bll_cutoffs')
    interest_rate_lower_limits = _lower_limits_property('irll_values', 'irll_cutoffs')

    # TODO: Consider adding fields that prevent one debtor to make an
    #       enormous amount of issuing transfers for a short period of
    #       time. There is no good reason why a debtor would want to
    #       make more than say 1000 issuing transfers per week. The
    #       same is true for the number of policy updates that a
    #       debtor does.

    def calc_interest_rate(self, on_day: date) -> float:
        # Apply debtor's enforced interest rate limits.
        interest_rate = self.interest_rate_target
        interest_rate = self.interest_rate_lower_limits.current_limits(on_day).apply_to_value(interest_rate)

        # Apply the absolute interest rate limits.
        if interest_rate < INTEREST_RATE_FLOOR:
            interest_rate = INTEREST_RATE_FLOOR
        if interest_rate > INTEREST_RATE_CEIL:
            interest_rate = INTEREST_RATE_CEIL

        assert INTEREST_RATE_FLOOR <= interest_rate <= INTEREST_RATE_CEIL
        return interest_rate

    @property
    def interest_rate(self):
        current_ts = datetime.now(tz=timezone.utc)
        return self.calc_interest_rate(current_ts.date())

    @property
    def is_active(self):
        return bool(self.status & Debtor.STATUS_IS_ACTIVE_FLAG)


class InitiatedTransfer(db.Model):
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    transfer_uuid = db.Column(pg.UUID(as_uuid=True), primary_key=True)
    recipient_uri = db.Column(
        db.String,
        nullable=False,
        comment="The recipient's URI.",
    )
    amount = db.Column(
        db.BigInteger,
        nullable=False,
        comment='The amount to be transferred. Must be positive.',
    )
    transfer_info = db.Column(
        pg.JSON,
        nullable=False,
        default={},
        comment='Notes from the debtor. Can be any object that the debtor wants the recipient to see.',
    )
    initiated_at_ts = db.Column(
        db.TIMESTAMP(timezone=True),
        nullable=False,
        default=get_now_utc,
        comment='The moment at which the transfer was initiated.',
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
    error_code = db.Column(
        db.String,
        comment="The error code, in case the transfer has not been successful.",
    )
    error_message = db.Column(
        db.String,
        comment="The error message, in case the transfer has not been successful.",
    )
    __table_args__ = (
        db.ForeignKeyConstraint(['debtor_id'], ['debtor.debtor_id'], ondelete='CASCADE'),
        db.CheckConstraint(amount > 0),
        db.CheckConstraint(or_(finalized_at_ts != null(), is_successful == false())),
        db.CheckConstraint(or_(error_code == null(), error_message != null())),
        {
            'comment': 'Represents an initiated issuing transfer. A new row is inserted when '
                       'a debtor creates a new issuing transfer. The row is deleted when the '
                       'debtor acknowledges (purges) the transfer.',
        }
    )

    debtor = db.relationship(
        'Debtor',
        backref=db.backref('initiated_transfers', cascade="all, delete-orphan", passive_deletes=True),
    )

    @property
    def is_finalized(self):
        return bool(self.finalized_at_ts)

    @property
    def errors(self):
        if self.is_finalized and not self.is_successful and self.error_code is not None:
            return [{'error_code': self.error_code, 'message': self.error_message}]
        return []


class RunningTransfer(db.Model):
    _icr_seq = db.Sequence('issuing_coordinator_request_id_seq', metadata=db.Model.metadata)

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    transfer_uuid = db.Column(pg.UUID(as_uuid=True), primary_key=True)
    recipient_creditor_id = db.Column(
        db.BigInteger,
        nullable=False,
        comment='The recipient of the transfer.',
    )
    amount = db.Column(
        db.BigInteger,
        nullable=False,
        comment='The amount to be transferred. Must be positive.',
    )
    transfer_info = db.Column(
        pg.JSON,
        nullable=False,
        default={},
        comment='Notes from the debtor. Can be any object that the debtor wants the recipient to see.',
    )
    finalized_at_ts = db.Column(
        db.TIMESTAMP(timezone=True),
        comment='The moment at which the transfer was finalized. A `null` means that the '
                'transfer has not been finalized yet.',
    )
    issuing_coordinator_request_id = db.Column(
        db.BigInteger,
        nullable=False,
        server_default=_icr_seq.next_value(),
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
        db.Index(
            'idx_issuing_coordinator_request_id',
            debtor_id,
            issuing_coordinator_request_id,
            unique=True,
        ),
        db.CheckConstraint(amount > 0),
        {
            'comment': 'Represents a running issuing transfer. Important note: The records for the '
                       'finalized issuing transfers (failed or successful) must not be deleted '
                       'right away. Instead, after they have been finalized, they should stay in '
                       'the database for at least few days. This is necessary in order to prevent '
                       'problems caused by message re-delivery.',
        }
    )

    @property
    def is_finalized(self):
        return bool(self.finalized_at_ts)


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


class PrepareTransferSignal(Signal):
    queue_name = 'swpt_accounts'
    actor_name = 'prepare_transfer'

    class __marshmallow__(Schema):
        coordinator_type = fields.String(default='issuing')
        coordinator_id = fields.Integer(attribute='debtor_id', dump_only=True)
        coordinator_request_id = fields.Integer()
        min_amount = fields.Integer()
        max_amount = fields.Integer()
        debtor_id = fields.Integer()
        sender_creditor_id = fields.Integer()
        recipient_creditor_id = fields.Integer()

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    coordinator_request_id = db.Column(db.BigInteger, primary_key=True)
    min_amount = db.Column(db.BigInteger, nullable=False)
    max_amount = db.Column(db.BigInteger, nullable=False)
    sender_creditor_id = db.Column(db.BigInteger, nullable=False)
    recipient_creditor_id = db.Column(db.BigInteger, nullable=False)
    __table_args__ = (
        db.CheckConstraint(min_amount > 0),
        db.CheckConstraint(max_amount >= min_amount),
    )
