from __future__ import annotations
from typing import Optional
from datetime import datetime, date, timezone, timedelta
from marshmallow import Schema, fields
from flask import current_app
import dramatiq
from sqlalchemy.dialects import postgresql as pg
from sqlalchemy.sql.expression import func, null, true, or_, and_
from swpt_lib.utils import i64_to_u64
from .lower_limits import lower_limits_property
from .extensions import db, broker, MAIN_EXCHANGE_NAME

MIN_INT16 = -1 << 15
MAX_INT16 = (1 << 15) - 1
MIN_INT32 = -1 << 31
MAX_INT32 = (1 << 31) - 1
MIN_INT64 = -1 << 63
MAX_INT64 = (1 << 63) - 1
MAX_UINT64 = (1 << 64) - 1
BEGINNING_OF_TIME = datetime(1970, 1, 1, tzinfo=timezone.utc)
VERY_DISTANT_DATE = date(9999, 12, 31)
INTEREST_RATE_FLOOR = -50.0
INTEREST_RATE_CEIL = 100.0
TRANSFER_NOTE_MAX_BYTES = 500
ROOT_CREDITOR_ID = 0

CT_ISSUING = 'issuing'

SC_OK = 'OK'
SC_UNEXPECTED_ERROR = 'UNEXPECTED_ERROR'
SC_INSUFFICIENT_AVAILABLE_AMOUNT = 'INSUFFICIENT_AVAILABLE_AMOUNT'
SC_CANCELED_BY_THE_SENDER = 'CANCELED_BY_THE_SENDER'


def get_now_utc():
    return datetime.now(tz=timezone.utc)


class Signal(db.Model):
    __abstract__ = True

    # TODO: Define `send_signalbus_messages` class method, set
    #      `ModelClass.signalbus_autoflush = False` and
    #      `ModelClass.signalbus_burst_count = N` in models. Make sure
    #      TTL is set properly for the messages.

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

    inserted_at = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc)


class NodeConfig(db.Model):
    is_effective = db.Column(db.BOOLEAN, primary_key=True, default=True)
    min_debtor_id = db.Column(db.BigInteger, nullable=False)
    max_debtor_id = db.Column(db.BigInteger, nullable=False)
    __table_args__ = (
        db.CheckConstraint(is_effective == true()),
        db.CheckConstraint(min_debtor_id <= max_debtor_id),
        {
            'comment': 'Represents the global node configuration (a singleton). The '
                       'node is responsible only for debtor IDs that are within the '
                       'interval [min_debtor_id, max_debtor_id].',
        }
    )


class Debtor(db.Model):
    STATUS_IS_ACTIVATED_FLAG = 1 << 0
    STATUS_IS_DEACTIVATED_FLAG = 1 << 1

    _ad_seq = db.Sequence('debtor_reservation_id_seq', metadata=db.Model.metadata)

    debtor_id = db.Column(db.BigInteger, nullable=False)
    status_flags = db.Column(
        db.SmallInteger,
        nullable=False,
        default=0,
        comment="Debtor's status bits: "
                f"{STATUS_IS_ACTIVATED_FLAG} - is activated, "
                f"{STATUS_IS_DEACTIVATED_FLAG} - is deactivated.",
    )
    reservation_id = db.Column(db.BigInteger, server_default=_ad_seq.next_value())
    created_at = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc)
    balance = db.Column(db.BigInteger, nullable=False, default=0)
    interest_rate_target = db.Column(db.REAL, nullable=False, default=0.0)
    debtor_info_iri = db.Column(db.String)
    debtor_info_content_type = db.Column(db.String)
    debtor_info_sha256 = db.Column(db.LargeBinary)
    running_transfers_count = db.Column(db.Integer, nullable=False, default=0)
    actions_count = db.Column(db.Integer, nullable=False, default=0)
    actions_count_reset_date = db.Column(db.DATE, nullable=False, default=get_now_utc)
    deactivated_at = db.Column(
        db.TIMESTAMP(timezone=True),
        comment='The moment at which the debtor was deactivated. When a debtor gets '
                'deactivated, all its belonging objects (transfers, etc.) are '
                'removed. To be deactivated, the debtor must be activated first. Once '
                'deactivated, a debtor stays deactivated until it is deleted.',
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
        default=[INTEREST_RATE_FLOOR],
        comment=(
            'Enforced interest rate lower limits. Each element in this array '
            'should have a corresponding element in the `irll_cutoffs` array '
            '(the cutoff dates for the limits). A `null` is the same as an '
            'empty array. If the array contains values bigger that {ceil}, '
            'they are treated as equal to {ceil}.'
        ).format(ceil=INTEREST_RATE_CEIL),
    )
    irll_cutoffs = db.Column(pg.ARRAY(db.DATE, dimensions=1), default=[VERY_DISTANT_DATE])

    __mapper_args__ = {
        'primary_key': [debtor_id],
        'eager_defaults': True,
    }
    __table_args__ = (
        db.CheckConstraint(and_(
            interest_rate_target >= INTEREST_RATE_FLOOR,
            interest_rate_target <= INTEREST_RATE_CEIL,
        )),
        db.CheckConstraint(or_(
            status_flags.op('&')(STATUS_IS_DEACTIVATED_FLAG) == 0,
            status_flags.op('&')(STATUS_IS_ACTIVATED_FLAG) != 0,
        )),
        db.CheckConstraint(or_(
            deactivated_at == null(),
            status_flags.op('&')(STATUS_IS_DEACTIVATED_FLAG) != 0,
        )),
        db.CheckConstraint(or_(
            debtor_info_sha256 == null(),
            func.octet_length(debtor_info_sha256) == 32
        )),
        db.CheckConstraint(actions_count >= 0),
        db.CheckConstraint(or_(bll_values == null(), func.array_ndims(bll_values) == 1)),
        db.CheckConstraint(or_(bll_cutoffs == null(), func.array_ndims(bll_cutoffs) == 1)),
        db.CheckConstraint(or_(irll_values == null(), func.array_ndims(irll_values) == 1)),
        db.CheckConstraint(or_(irll_cutoffs == null(), func.array_ndims(irll_cutoffs) == 1)),

        # TODO: The `status_flags` column is not be part of the
        #       primary key, but should be included in the primary key
        #       index to allow index-only scans. Because SQLAlchemy
        #       does not support this yet (2020-01-11), temporarily,
        #       there are no index-only scans.
        db.Index('idx_debtor_pk', debtor_id, unique=True),
    )

    balance_lower_limits = lower_limits_property('bll_values', 'bll_cutoffs')
    interest_rate_lower_limits = lower_limits_property('irll_values', 'irll_cutoffs')

    def calc_interest_rate(self, on_day: date) -> float:
        # Apply debtor's enforced interest rate limits.
        interest_rate = self.interest_rate_target
        interest_rate = self.interest_rate_lower_limits.current_limits(on_day).apply_to_value(interest_rate)

        # Apply the absolute interest rate limits.
        if interest_rate < INTEREST_RATE_FLOOR:  # pragma: no cover
            interest_rate = INTEREST_RATE_FLOOR
        if interest_rate > INTEREST_RATE_CEIL:
            interest_rate = INTEREST_RATE_CEIL

        assert INTEREST_RATE_FLOOR <= interest_rate <= INTEREST_RATE_CEIL
        return interest_rate

    def calc_min_account_balance(self, on_day: date) -> int:
        # Apply debtor's enforced balance limits.
        min_account_balance = self.balance_lower_limits.current_limits(on_day).apply_to_value(MIN_INT64)

        assert MIN_INT64 <= min_account_balance <= MAX_INT64
        return min_account_balance

    @property
    def interest_rate(self):
        current_ts = datetime.now(tz=timezone.utc)
        return self.calc_interest_rate(current_ts.date())

    @property
    def min_account_balance(self):
        current_ts = datetime.now(tz=timezone.utc)
        return self.calc_min_account_balance(current_ts.date())

    @property
    def is_activated(self):
        return bool(self.status_flags & Debtor.STATUS_IS_ACTIVATED_FLAG)

    @property
    def is_deactivated(self):
        return bool(self.status_flags & Debtor.STATUS_IS_DEACTIVATED_FLAG)

    def activate(self):
        self.status_flags |= Debtor.STATUS_IS_ACTIVATED_FLAG
        self.reservation_id = None

    def deactivate(self):
        # NOTE: We want to remove the limits to save disk space. But
        # before doing this, we update the interest rate target to
        # ensure that the interest rate will not change once the
        # limits are removed.
        self.interest_rate_target = self.interest_rate
        self.bll_values = None
        self.bll_cutoffs = None
        self.irll_values = None
        self.irll_cutoffs = None
        self.debtor_info_iri = None
        self.debtor_info_sha256 = None
        self.debtor_info_content_type = None

        self.status_flags |= Debtor.STATUS_IS_DEACTIVATED_FLAG
        self.deactivated_at = datetime.now(tz=timezone.utc)


class RunningTransfer(db.Model):
    _cr_seq = db.Sequence('coordinator_request_id_seq', metadata=db.Model.metadata)

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    transfer_uuid = db.Column(pg.UUID(as_uuid=True), primary_key=True)
    amount = db.Column(db.BigInteger, nullable=False)
    recipient_uri = db.Column(db.String, nullable=False)
    recipient = db.Column(db.String, nullable=False)
    transfer_note_format = db.Column(db.String, nullable=False)
    transfer_note = db.Column(db.String, nullable=False)
    initiated_at = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc)
    finalized_at = db.Column(db.TIMESTAMP(timezone=True))
    error_code = db.Column(db.String)
    total_locked_amount = db.Column(db.BigInteger)
    coordinator_request_id = db.Column(db.BigInteger, nullable=False, server_default=_cr_seq.next_value())
    transfer_id = db.Column(db.BigInteger)
    __mapper_args__ = {'eager_defaults': True}
    __table_args__ = (
        db.ForeignKeyConstraint(['debtor_id'], ['debtor.debtor_id'], ondelete='CASCADE'),
        db.CheckConstraint(amount >= 0),
        db.CheckConstraint(total_locked_amount >= 0),
        db.CheckConstraint(or_(error_code == null(), finalized_at != null())),
        db.Index('idx_coordinator_request_id', debtor_id, coordinator_request_id, unique=True),
        {
            'comment': 'Represents an initiated issuing transfer. A new row is inserted when '
                       'a debtor creates a new issuing transfer. The row is deleted when the '
                       'debtor acknowledges (purges) the transfer.',
        }
    )

    @property
    def is_settled(self):
        return self.transfer_id is not None

    @property
    def is_finalized(self):
        return bool(self.finalized_at)


class Account(db.Model):
    # TODO: To achieve better scalability, consider moving the `Account`
    #       table to a separate database. This will allow for it to be
    #       sharded independently from the debtor-related tables. If
    #       necessary, use signals to communicate between the two.

    CONFIG_SCHEDULED_FOR_DELETION_FLAG = 1 << 0

    STATUS_UNREACHABLE_FLAG = 1 << 0
    STATUS_OVERFLOWN_FLAG = 1 << 1
    STATUS_DELETED_FLAG = 1 << 16
    STATUS_ESTABLISHED_INTEREST_RATE_FLAG = 1 << 17

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    last_change_seqnum = db.Column(db.Integer, nullable=False)
    last_change_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    principal = db.Column(db.BigInteger, nullable=False)
    interest = db.Column(db.FLOAT, nullable=False)
    interest_rate = db.Column(db.REAL, nullable=False)
    last_interest_rate_change_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    creation_date = db.Column(db.DATE, nullable=False)
    negligible_amount = db.Column(db.REAL, nullable=False)
    config_flags = db.Column(db.Integer, nullable=False)
    status_flags = db.Column(db.Integer, nullable=False)
    is_muted = db.Column(
        db.BOOLEAN,
        nullable=False,
        default=False,
        comment='Whether the account is "muted" or not. Maintenance operation requests are '
                'not sent for muted accounts. This prevents flooding the signal bus with '
                'maintenance signals. It is set to `true` when a maintenance operation request '
                'is made, and set to back `false` when the matching `AccountMaintenanceSignal` '
                'is received. Important note: Accounts that have been muted a long time ago '
                '(this can be determined by checking the `last_maintenance_request_ts` column) '
                'are allowed to sent maintenance operation requests. (This is to avoid accounts '
                'staying muted forever when something went wrong with the awaited un-muting '
                '`AccountMaintenanceSignal`.'
    )
    last_heartbeat_ts = db.Column(
        db.TIMESTAMP(timezone=True),
        nullable=False,
        default=get_now_utc,
        comment='The moment at which the last `AccountChangeSignal` has been processed. It is '
                'used to detect "dead" accounts. A "dead" account is an account that have been '
                'removed from the `swpt_accounts` service, but still exist in this table.',
    )
    last_interest_capitalization_ts = db.Column(
        db.TIMESTAMP(timezone=True),
        nullable=False,
        default=BEGINNING_OF_TIME,
        comment='The moment at which the last interest capitalization was triggered. It is '
                'used to avoid capitalizing interest too often.',
    )
    last_deletion_attempt_ts = db.Column(
        db.TIMESTAMP(timezone=True),
        nullable=False,
        default=BEGINNING_OF_TIME,
        comment='The moment at which the last deletion attempt was made. It is used to '
                'avoid trying to delete the account too often.',
    )
    last_maintenance_request_ts = db.Column(
        db.TIMESTAMP(timezone=True),
        nullable=False,
        default=BEGINNING_OF_TIME,
        comment='The moment at which the last account maintenance operation request was made. It '
                'is used to avoid triggering account maintenance operations too often.',
    )
    __table_args__ = (
        db.CheckConstraint((interest_rate >= INTEREST_RATE_FLOOR) & (interest_rate <= INTEREST_RATE_CEIL)),
        db.CheckConstraint(principal > MIN_INT64),
        db.CheckConstraint(negligible_amount >= 0.0),
        {
            'comment': 'Tells who owes what to whom. This table is a replica of the table with the '
                       'same name in the `swpt_accounts` service. It is used to perform maintenance '
                       'routines like changing interest rates. Most of the columns get their values '
                       'from the corresponding fields in the last applied `AccountChangeSignal`.',
        }
    )

    @property
    def is_overflown(self):
        return bool(self.status_flags & Account.STATUS_OVERFLOWN_FLAG)

    @classmethod
    def get_interest_rate_change_min_interval(cls):
        return timedelta(days=current_app.config['APP_INTEREST_RATE_CHANGE_MIN_DAYS'] + 0.01)


class ConfigureAccountSignal(Signal):
    queue_name = 'swpt_accounts'
    actor_name = 'configure_account'

    class __marshmallow__(Schema):
        debtor_id = fields.Integer()
        creditor_id = fields.Constant(ROOT_CREDITOR_ID)
        ts = fields.DateTime()
        seqnum = fields.Constant(0)
        negligible_amount = fields.Constant(0.0)
        config_flags = fields.Constant(0)
        config = fields.Constant('')

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    signal_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)


class PrepareTransferSignal(Signal):
    queue_name = 'swpt_accounts'
    actor_name = 'prepare_transfer'

    class __marshmallow__(Schema):
        coordinator_type = fields.String(default='issuing')
        coordinator_id = fields.Integer(attribute='debtor_id', dump_only=True)
        coordinator_request_id = fields.Integer()
        min_locked_amount = fields.Integer(attribute='amount', dump_only=True)
        max_locked_amount = fields.Integer(attribute='amount', dump_only=True)
        debtor_id = fields.Integer()
        creditor_id = fields.Integer()
        recipient = fields.String()
        inserted_at = fields.DateTime(data_key='ts')
        max_commit_delay = fields.Constant(MAX_INT32)
        min_account_balance = fields.Integer()
        min_interest_rate = fields.Constant(-100.0)

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    coordinator_request_id = db.Column(db.BigInteger, primary_key=True)
    amount = db.Column(db.BigInteger, nullable=False)
    creditor_id = db.Column(db.BigInteger, nullable=False)
    recipient = db.Column(db.String, nullable=False)
    min_account_balance = db.Column(db.BigInteger, nullable=False)
    __table_args__ = (
        db.CheckConstraint(amount >= 0),
    )


class FinalizeTransferSignal(Signal):
    queue_name = 'swpt_accounts'
    actor_name = 'finalize_transfer'

    class __marshmallow__(Schema):
        debtor_id = fields.Integer()
        creditor_id = fields.Integer()
        transfer_id = fields.Integer()
        coordinator_type = fields.String(default='issuing')
        coordinator_id = fields.Integer()
        coordinator_request_id = fields.Integer()
        committed_amount = fields.Integer()
        finalization_flags = fields.Constant(0)
        transfer_note_format = fields.String()
        transfer_note = fields.String()
        inserted_at = fields.DateTime(data_key='ts')

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    signal_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    creditor_id = db.Column(db.BigInteger, nullable=False)
    coordinator_id = db.Column(db.BigInteger, nullable=False)
    coordinator_request_id = db.Column(db.BigInteger, nullable=False)
    transfer_id = db.Column(db.BigInteger, nullable=False)
    transfer_note_format = db.Column(db.String, nullable=False)
    transfer_note = db.Column(db.String, nullable=False)
    committed_amount = db.Column(db.BigInteger, nullable=False)
    __table_args__ = (
        db.CheckConstraint(committed_amount >= 0),
    )


class CapitalizeInterestSignal(Signal):
    queue_name = 'swpt_accounts'
    actor_name = 'capitalize_interest'

    class __marshmallow__(Schema):
        debtor_id = fields.Integer()
        creditor_id = fields.Integer()
        accumulated_interest_threshold = fields.Integer()
        request_ts = fields.DateTime()

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    signal_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    creditor_id = db.Column(db.BigInteger, nullable=False)
    accumulated_interest_threshold = db.Column(db.BigInteger, nullable=False)
    request_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)


class TryToDeleteAccountSignal(Signal):
    queue_name = 'swpt_accounts'
    actor_name = 'try_to_delete_account'

    class __marshmallow__(Schema):
        debtor_id = fields.Integer()
        creditor_id = fields.Integer()
        request_ts = fields.DateTime()

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    signal_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    creditor_id = db.Column(db.BigInteger, nullable=False)
    request_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)


class ChangeInterestRateSignal(Signal):
    queue_name = 'swpt_accounts'
    actor_name = 'try_to_change_interest_rate'

    class __marshmallow__(Schema):
        debtor_id = fields.Integer()
        creditor_id = fields.Integer()
        interest_rate = fields.Float()
        request_ts = fields.DateTime()

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    signal_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    creditor_id = db.Column(db.BigInteger, nullable=False)
    interest_rate = db.Column(db.REAL, nullable=False)
    request_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
