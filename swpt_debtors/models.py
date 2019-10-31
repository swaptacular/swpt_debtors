from numbers import Real
from typing import NamedTuple, List, Tuple, Optional
from datetime import datetime, date, timezone
from marshmallow import Schema, fields
import dramatiq
from sqlalchemy.dialects import postgresql as pg
from sqlalchemy.sql.expression import func, null, or_
from .extensions import db, broker, MAIN_EXCHANGE_NAME

MIN_INT16 = -1 << 15
MAX_INT16 = (1 << 15) - 1
MIN_INT32 = -1 << 31
MAX_INT32 = (1 << 31) - 1
MIN_INT64 = -1 << 63
MAX_INT64 = (1 << 63) - 1


class Limit(NamedTuple):
    value: Real  # the limiting value
    kickoff: date  # the limit has been created at this date
    cutoff: date  # the limit will stop to be enforced at this date


def _limit_property(values_attrname: str, kickoffs_attrname: str, cutoffs_attrname: str):
    def unpack_limits(values: Optional[List], kickoffs: Optional[List], cutoffs: Optional[List]) -> List[Limit]:
        values = values or []
        kickoffs = kickoffs or []
        cutoffs = cutoffs or []
        return [Limit(*t) for t in zip(values, kickoffs, cutoffs) if all(x is not None for x in t)]

    def pack_limits(limits: List[Limit]) -> Tuple[Optional[List], Optional[List], Optional[List]]:
        if len(limits) == 0:
            return None, None, None
        values = []
        kickoffs = []
        cutoffs = []
        for limit in limits:
            assert isinstance(limit.value, Real)
            assert isinstance(limit.kickoff, date)
            assert isinstance(limit.cutoff, date)
            values.append(limit.value)
            kickoffs.append(limit.kickoff)
            cutoffs.append(limit.cutoff)
        return values, kickoffs, cutoffs

    def getter(self):
        values = getattr(self, values_attrname)
        kickoffs = getattr(self, kickoffs_attrname)
        cutoffs = getattr(self, cutoffs_attrname)
        return unpack_limits(values, kickoffs, cutoffs)

    def setter(self, value):
        values, kickoffs, cutoffs = pack_limits(value)
        setattr(self, values_attrname, values)
        setattr(self, kickoffs_attrname, kickoffs)
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
    last_change_seqnum = db.Column(
        db.Integer,
        nullable=False,
        default=1,
        comment='Incremented (with wrapping) on every change.',
    )
    last_change_ts = db.Column(
        db.TIMESTAMP(timezone=True),
        nullable=False,
        default=get_now_utc,
        comment='Updated on every increment of `last_change_seqnum`. Must never decrease.',
    )
    status = db.Column(
        db.SmallInteger,
        nullable=False,
        default=0,
        comment='Debtor status flags.',
    )
    balance = db.Column(
        db.BigInteger,
        default=0,
        comment="The total issued amount with a negative sign. Normally, it will be a "
                "negative number or a zero. A positive value, although theoretically "
                "possible, should be very rare. A `NULL` means that the balance is unknown.",
    )
    balance_last_update_seqnum = db.Column(
        db.Integer,
        comment='Updated on each change of the `balance`.',
    )
    balance_last_update_ts = db.Column(
        db.TIMESTAMP(timezone=True),
        comment='Updated on each change of the `balance`.',
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
        comment='Enforced lower limits for the `balance` column. Each element in  '
                'this array should have a corresponding element in the `bll_kickoffs` '
                'and `bll_cutoffs` arrays (the kickoff and cutoff dates for the limits). '
                'A `NULL` is the same as an empty array.',
    )
    bll_kickoffs = db.Column(pg.ARRAY(db.DATE, dimensions=1))
    bll_cutoffs = db.Column(pg.ARRAY(db.DATE, dimensions=1))

    # Interest Rate Lower Limits
    irll_values = db.Column(
        pg.ARRAY(db.BigInteger, dimensions=1),
        comment='Enforced interest rate lower limits. Each element in this array '
                'should have a corresponding element in the `irll_kickoffs` and '
                '`irll_cutoffs` arrays (the kickoff and cutoff dates for the limits). '
                'A `NULL` is the same as an empty array.',
    )
    irll_kickoffs = db.Column(pg.ARRAY(db.DATE, dimensions=1))
    irll_cutoffs = db.Column(pg.ARRAY(db.DATE, dimensions=1))

    # Interest Rate Upper Limits
    irul_values = db.Column(
        pg.ARRAY(db.BigInteger, dimensions=1),
        comment='Enforced interest rate upper limits. Each element in this array '
                'should have a corresponding element in the `irul_kickoffs` and '
                '`irul_cutoffs` arrays (the kickoff and cutoff dates for the limits). '
                'A `NULL` is the same as an empty array.',
    )
    irul_kickoffs = db.Column(pg.ARRAY(db.DATE, dimensions=1))
    irul_cutoffs = db.Column(pg.ARRAY(db.DATE, dimensions=1))

    __table_args__ = (
        db.CheckConstraint((interest_rate_target > -100.0) & (interest_rate_target <= 100.0)),
        db.CheckConstraint(or_(bll_values == null(), func.array_ndims(bll_values) == 1)),
        db.CheckConstraint(or_(bll_kickoffs == null(), func.array_ndims(bll_kickoffs) == 1)),
        db.CheckConstraint(or_(bll_cutoffs == null(), func.array_ndims(bll_cutoffs) == 1)),
        db.CheckConstraint(or_(irll_values == null(), func.array_ndims(irll_values) == 1)),
        db.CheckConstraint(or_(irll_kickoffs == null(), func.array_ndims(irll_kickoffs) == 1)),
        db.CheckConstraint(or_(irll_cutoffs == null(), func.array_ndims(irll_cutoffs) == 1)),
        db.CheckConstraint(or_(irul_values == null(), func.array_ndims(irul_values) == 1)),
        db.CheckConstraint(or_(irul_kickoffs == null(), func.array_ndims(irul_kickoffs) == 1)),
        db.CheckConstraint(or_(irul_cutoffs == null(), func.array_ndims(irul_cutoffs) == 1)),
        {
            'comment': "Represents debtor's principal information.",
        }
    )

    balance_lower_limits = _limit_property('bll_values', 'bll_kickoffs', 'bll_cutoffs')
    interest_rate_lower_limits = _limit_property('irll_values', 'irll_kickoffs', 'irll_cutoffs')
    interest_rate_upper_limits = _limit_property('irul_values', 'irul_kickoffs', 'irul_cutoffs')


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
        db.CheckConstraint((interest_rate > -100.0) & (interest_rate <= 100.0)),
        db.CheckConstraint(principal > MIN_INT64),
        {
            'comment': 'Tells who owes what to whom. This table is a replica of the table with the '
                       'same name in the `swpt_accounts` service.',
        }
    )


class InterestRateConcession(db.Model):
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    last_change_seqnum = db.Column(
        db.Integer,
        nullable=False,
        default=1,
        comment='Incremented (with wrapping) on every change.',
    )
    last_change_ts = db.Column(
        db.TIMESTAMP(timezone=True),
        nullable=False,
        default=get_now_utc,
        comment='Updated on every increment of `last_change_seqnum`. Must never decrease.',
    )

    # Interest Rate Lower Limits
    irll_values = db.Column(
        pg.ARRAY(db.BigInteger, dimensions=1),
        comment='Enforced concession interest rate lower limits. Each element in this '
                'array should have a corresponding element in the `irll_kickoffs` and '
                '`irll_cutoffs` arrays (the kickoff and cutoff dates for the limits). '
                'A `NULL` is the same as an empty array.',
    )
    irll_kickoffs = db.Column(pg.ARRAY(db.DATE, dimensions=1))
    irll_cutoffs = db.Column(pg.ARRAY(db.DATE, dimensions=1))

    # Ballance Upper Limits
    bul_values = db.Column(
        pg.ARRAY(db.BigInteger, dimensions=1),
        comment="Upper limits for creditor's `account.principal` column. The concession interest "
                "rate lower limits will apply only when the `account.principal` is within the limits "
                "specified here. Each element in this array should have a corresponding "
                "element in the `bul_kickoffs` and `bul_cutoffs` arrays (the kickoff and "
                "cutoff dates for the limits). A `NULL` is the same as an empty array.",
    )
    bul_kickoffs = db.Column(pg.ARRAY(db.DATE, dimensions=1))
    bul_cutoffs = db.Column(pg.ARRAY(db.DATE, dimensions=1))

    __table_args__ = (
        db.CheckConstraint(or_(bul_values == null(), func.array_ndims(bul_values) == 1)),
        db.CheckConstraint(or_(bul_kickoffs == null(), func.array_ndims(bul_kickoffs) == 1)),
        db.CheckConstraint(or_(bul_cutoffs == null(), func.array_ndims(bul_cutoffs) == 1)),
        db.CheckConstraint(or_(irll_values == null(), func.array_ndims(irll_values) == 1)),
        db.CheckConstraint(or_(irll_kickoffs == null(), func.array_ndims(irll_kickoffs) == 1)),
        db.CheckConstraint(or_(irll_cutoffs == null(), func.array_ndims(irll_cutoffs) == 1)),
        {
            'comment': 'Represents an enforced concession interest rate, valid only for a specific '
                       'creditor, under specific conditions.',
        }
    )

    interest_rate_lower_limits = _limit_property('irll_values', 'irll_kickoffs', 'irll_cutoffs')
    balance_upper_limits = _limit_property('bul_values', 'bul_kickoffs', 'bul_cutoffs')


class ChangedDebtorInfoSignal(Signal):
    """Sent when debtor's principal information has changed."""

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    change_seqnum = db.Column(db.Integer, primary_key=True)
    change_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    status = db.Column(db.SmallInteger, nullable=False)
    balance = db.Column(db.BigInteger)
    interest_rate_target = db.Column(db.REAL, nullable=False)
    bll_values = db.Column(pg.ARRAY(db.BigInteger, dimensions=1))
    bll_kickoffs = db.Column(pg.ARRAY(db.DATE, dimensions=1))
    bll_cutoffs = db.Column(pg.ARRAY(db.DATE, dimensions=1))
    irll_values = db.Column(pg.ARRAY(db.BigInteger, dimensions=1))
    irll_kickoffs = db.Column(pg.ARRAY(db.DATE, dimensions=1))
    irll_cutoffs = db.Column(pg.ARRAY(db.DATE, dimensions=1))
    irul_values = db.Column(pg.ARRAY(db.BigInteger, dimensions=1))
    irul_kickoffs = db.Column(pg.ARRAY(db.DATE, dimensions=1))
    irul_cutoffs = db.Column(pg.ARRAY(db.DATE, dimensions=1))


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
