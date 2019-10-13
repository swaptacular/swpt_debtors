import datetime
import dramatiq
from sqlalchemy.dialects import postgresql as pg
from sqlalchemy.sql.expression import func
from .extensions import db, broker, MAIN_EXCHANGE_NAME

MIN_INT32 = -1 << 31
MAX_INT32 = (1 << 31) - 1
MIN_INT64 = -1 << 63
MAX_INT64 = (1 << 63) - 1


def get_now_utc():
    return datetime.datetime.now(tz=datetime.timezone.utc)


class Signal(db.Model):
    __abstract__ = True

    # TODO: Define `send_signalbus_messages` class method, set
    #      `ModelClass.signalbus_autoflush = False` and
    #      `ModelClass.signalbus_burst_count = N` in models.

    queue_name = None

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

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    status = db.Column(
        db.SmallInteger,
        nullable=False,
        comment='Debtor status flags.',
    )
    balance = db.Column(
        db.BigInteger,
        default=0,
        comment="The total issued amount with a negative sign. Normally, it will be a "
                "negative number or zero. A positive value, although theoretically "
                "possible, should be very rare. NULL means that the debtor's account "
                "has been overflown.",
    )
    balance_last_update_ts = db.Column(
        db.TIMESTAMP(timezone=True),
        comment='Updated on each change the `balance`.',
    )
    interest_rate_target = db.Column(
        db.REAL,
        nullable=False,
        default=0.0,
        comment="The desired annual rate (in percents) at which the interest should "
                "accumulate on creditors' accounts. The actual interest rate could be "
                "different if interest rate limits are enforced.",
    )
    __table_args__ = (
        db.CheckConstraint((interest_rate_target > -100.0) & (interest_rate_target <= 100.0)),
        {
            'comment': 'Represents a debtor.',
        }
    )


class BalanceLowerLimit(db.Model):
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    balance_lower_limit_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    value = db.Column(
        db.BigInteger,
        nullable=False,
        comment='The value under which the `debtor.balance` should not go.',
    )
    cutoff_ts = db.Column(
        db.TIMESTAMP(timezone=True),
        nullable=False,
        comment='The limit will not be enforced after this moment in time.'
    )
    created_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc)
    __table_args__ = (
        {
            'comment': 'Represents an enforced lower limit for `debtor.balance`.',
        }
    )


class InterestRateLowerLimit(db.Model):
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    interest_rate_lower_limit_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    value = db.Column(
        db.BigInteger,
        nullable=False,
        comment='The value under which the interest rate should not go.',
    )
    cutoff_ts = db.Column(
        db.TIMESTAMP(timezone=True),
        nullable=False,
        comment='The limit will not be enforced after this moment in time.'
    )
    created_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc)
    __table_args__ = (
        {
            'comment': 'Represents an enforced lower limit for the interest rate.',
        }
    )


class InterestRateUpperLimit(db.Model):
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    interest_rate_upper_limit_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    value = db.Column(
        db.BigInteger,
        nullable=False,
        comment='The value above which the interest rate should not go.',
    )
    cutoff_ts = db.Column(
        db.TIMESTAMP(timezone=True),
        nullable=False,
        comment='The limit will not be enforced after this moment in time.'
    )
    created_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc)
    __table_args__ = (
        {
            'comment': 'Represents an enforced upper limit for the interest rate.',
        }
    )


class ChangedDebtorInfoSignal(Signal):
    """Sent when debtor's principal information has changed."""

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    status = db.Column(db.SmallInteger, nullable=False)
    balance = db.Column(db.BigInteger)
    balance_ll_values = db.Column(pg.ARRAY(db.BigInteger, dimensions=1), nullable=False)
    balance_ll_cutoffs = db.Column(pg.ARRAY(db.TIMESTAMP(timezone=True), dimensions=1), nullable=False)
    interest_rate_target = db.Column(db.REAL, nullable=False)
    interest_rate_ll_values = db.Column(pg.ARRAY(db.BigInteger, dimensions=1), nullable=False)
    interest_rate_ll_cutoffs = db.Column(pg.ARRAY(db.TIMESTAMP(timezone=True), dimensions=1), nullable=False)
    interest_rate_ul_values = db.Column(pg.ARRAY(db.BigInteger, dimensions=1), nullable=False)
    interest_rate_ul_cutoffs = db.Column(pg.ARRAY(db.TIMESTAMP(timezone=True), dimensions=1), nullable=False)
    __table_args__ = (
        db.CheckConstraint(func.array_ndims(balance_ll_values) == 1),
        db.CheckConstraint(func.array_ndims(balance_ll_cutoffs) == 1),
        db.CheckConstraint(func.cardinality(balance_ll_values) == func.cardinality(balance_ll_cutoffs)),
        db.CheckConstraint(func.array_ndims(interest_rate_ll_values) == 1),
        db.CheckConstraint(func.array_ndims(interest_rate_ll_cutoffs) == 1),
        db.CheckConstraint(func.cardinality(interest_rate_ll_values) == func.cardinality(interest_rate_ll_cutoffs)),
        db.CheckConstraint(func.array_ndims(interest_rate_ul_values) == 1),
        db.CheckConstraint(func.array_ndims(interest_rate_ul_cutoffs) == 1),
        db.CheckConstraint(func.cardinality(interest_rate_ul_values) == func.cardinality(interest_rate_ul_cutoffs)),
    )
