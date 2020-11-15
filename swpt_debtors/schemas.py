from datetime import datetime, timezone, timedelta
from typing import NamedTuple, List
from marshmallow import Schema, fields, validate, post_load, validates, ValidationError
from flask import url_for, current_app
from .lower_limits import LowerLimit
from .models import INTEREST_RATE_FLOOR, INTEREST_RATE_CEIL, MIN_INT64, MAX_INT64, MAX_UINT64, \
    TRANSFER_NOTE_MAX_BYTES, Debtor, InitiatedTransfer


class ValidateTypeMixin:
    @validates('type')
    def validate_type(self, value):
        if f'{value}Schema' != type(self).__name__:
            raise ValidationError('Invalid type.')


class TransfersCollection(NamedTuple):
    debtor_id: int
    items: List[str]


class InterestRateLowerLimitSchema(ValidateTypeMixin, Schema):
    type = fields.String(
        missing='InterestRateLowerLimit',
        default='InterestRateLowerLimit',
        description='The type of this object.',
        example='InterestRateLowerLimit',
    )
    value = fields.Float(
        required=True,
        validate=validate.Range(min=INTEREST_RATE_FLOOR, max=INTEREST_RATE_CEIL),
        description='The annual interest rate (in percents) should be no less than this value.',
    )
    cutoff = fields.Date(
        required=True,
        data_key='enforcedUntil',
        description='The limit will not be enforced after this date.',
    )

    @post_load
    def make_lower_limit(self, data, **kwargs):
        return LowerLimit(value=data['value'], cutoff=data['cutoff'])


class BalanceLowerLimitSchema(ValidateTypeMixin, Schema):
    type = fields.String(
        missing='BalanceLowerLimit',
        default='BalanceLowerLimit',
        description='The type of this object.',
        example='BalanceLowerLimit',
    )
    value = fields.Int(
        required=True,
        validate=validate.Range(min=MIN_INT64, max=MAX_INT64),
        format='int64',
        description='The balance should be no less than this value.',
    )
    cutoff = fields.Date(
        required=True,
        data_key='enforcedUntil',
        description='The limit will not be enforced after this date.',
    )

    @post_load
    def make_lower_limit(self, data, **kwargs):
        return LowerLimit(value=data['value'], cutoff=data['cutoff'])


class DebtorCreationOptionsSchema(ValidateTypeMixin, Schema):
    type = fields.String(
        missing='DebtorCreationOptions',
        default='DebtorCreationOptions',
        description='The type of this object.',
        example='DebtorCreationOptions',
    )


class DebtorSchema(Schema):
    uri = fields.Method(
        'get_uri',
        required=True,
        type='string',
        format='uri',
        description="The URI of this object.",
        example='https://example.com/debtors/1/',
    )
    type = fields.Function(
        lambda obj: 'Debtor',
        required=True,
        type='string',
        description='The type of this object.',
        example='Debtor',
    )
    accountingAuthorityUri = fields.Function(
        lambda obj: current_app.config['APP_AUTHORITY_URI'],
        required=True,
        type='string',
        format="uri-reference",
        description="The URI of the authority that manages creditors' accounts.",
        example='https://example.com/authority',
    )
    created_at_date = fields.Date(
        required=True,
        dump_only=True,
        data_key='createdOn',
        description=Debtor.created_at_date.comment,
        example='2019-11-30',
    )
    balance = fields.Int(
        required=True,
        dump_only=True,
        format="int64",
        description=Debtor.balance.comment,
    )
    balance_ts = fields.DateTime(
        required=True,
        dump_only=True,
        data_key='balanceTimestamp',
        description='The moment at which the last change in the `balance` field happened.',
    )
    balance_lower_limits = fields.Nested(
        BalanceLowerLimitSchema(many=True),
        required=True,
        dump_only=True,
        data_key='balanceLowerLimits',
        description='Enforced lower limits for the `balance` field.'
                    '\n\n'
                    '**Note:** Established limits can not be removed. They will '
                    'continue to be enforced until the specified expiration date.',
    )
    interest_rate_lower_limits = fields.Nested(
        InterestRateLowerLimitSchema(many=True),
        required=True,
        dump_only=True,
        data_key='interestRateLowerLimits',
        description='Enforced interest rate lower limits.'
                    '\n\n'
                    '**Note:** Established limits can not be removed. They will '
                    'continue to be enforced until the specified expiration date.',
    )
    interest_rate = fields.Float(
        required=True,
        dump_only=True,
        data_key='interestRate',
        description="The current annual interest rate (in percents) at which "
                    "interest accumulates on creditors' accounts.",
    )
    is_active = fields.Boolean(
        required=True,
        dump_only=True,
        data_key='isActive',
        description="Whether the debtor is currently active or not."
    )

    def get_uri(self, obj):
        return url_for(self.context['Debtor'], _external=True, debtorId=obj.debtor_id)


class DebtorPolicySchema(ValidateTypeMixin, Schema):
    uri = fields.Method(
        'get_uri',
        required=True,
        type='string',
        format='uri',
        description="The URI of this object.",
        example='https://example.com/debtors/1/policy',
    )
    type = fields.String(
        missing='DebtorPolicy',
        default='DebtorPolicy',
        description='The type of this object.',
        example='DebtorPolicy',
    )
    debtorUri = fields.Function(
        lambda obj: url_for('debtors.DebtorEndpoint', _external=True, debtorId=obj.debtor_id),
        required=True,
        type='string',
        format="uri",
        description="The debtor's URI.",
        example='https://example.com/debtors/1/',
    )
    balance_lower_limits = fields.Nested(
        BalanceLowerLimitSchema(many=True),
        missing=[],
        data_key='balanceLowerLimits',
        description='Enforced balance lower limits.'
                    '\n\n'
                    '**Note:** When the policy gets updated, this field should contain '
                    'only the additional limits that need to be added to the existing ones.',
    )
    interest_rate_lower_limits = fields.Nested(
        InterestRateLowerLimitSchema(many=True),
        missing=[],
        data_key='interestRateLowerLimits',
        description='Enforced interest rate lower limits.'
                    '\n\n'
                    '**Note:** When the policy gets updated, this field should contain '
                    'only the additional limits that need to be added to the existing ones.',
    )
    interest_rate_target = fields.Float(
        validate=validate.Range(min=INTEREST_RATE_FLOOR, max=INTEREST_RATE_CEIL),
        data_key='interestRateTarget',
        description=f'{Debtor.interest_rate_target.comment}'
                    '\n\n'
                    '**Note:** If this field is not present when the policy gets '
                    'updated, the current `interestRateTarget` will remain unchanged.',
        example=0,
    )

    def get_uri(self, obj):
        return url_for(self.context['DebtorPolicy'], _external=True, debtorId=obj.debtor_id)


class TransferErrorSchema(Schema):
    type = fields.Function(
        lambda obj: 'TransferError',
        required=True,
        type='string',
        description='The type of this object.',
        example='TransferError',
    )
    errorCode = fields.String(
        required=True,
        dump_only=True,
        description='The error code.',
        example='INSUFFICIENT_AVAILABLE_AMOUNT',
    )
    totalLockedAmount = fields.Integer(
        required=True,
        dump_only=True,
        format="int64",
        description='The total amount secured (locked) for prepared transfers on the account.',
        example=0,
    )


class IssuingTransferCreationRequestSchema(ValidateTypeMixin, Schema):
    type = fields.String(
        missing='IssuingTransferCreationRequest',
        default='IssuingTransferCreationRequest',
        description='The type of this object.',
        example='IssuingTransferCreationRequest',
    )
    transfer_uuid = fields.UUID(
        required=True,
        data_key='transferUuid',
        description="A client-generated UUID for the transfer.",
        example='123e4567-e89b-12d3-a456-426655440000',
    )
    recipient_creditor_id = fields.Integer(
        required=True,
        validate=validate.Range(min=0, max=MAX_UINT64),
        data_key='recipientCreditorId',
        description="The recipient's creditor ID.",
        example=1111,
    )
    amount = fields.Integer(
        required=True,
        validate=validate.Range(min=1, max=MAX_INT64),
        format="int64",
        description=InitiatedTransfer.amount.comment,
        example=1000,
    )
    transfer_note = fields.String(
        missing='',
        validate=validate.Length(max=TRANSFER_NOTE_MAX_BYTES),
        data_key='note',
        description=InitiatedTransfer.transfer_note.comment,
        example='Hello, World!',
    )

    @validates('transfer_note')
    def validate_transfer_note(self, value):
        if len(value.encode('utf8')) > TRANSFER_NOTE_MAX_BYTES:
            raise ValidationError(f'The total byte-length of the note exceeds {TRANSFER_NOTE_MAX_BYTES} bytes.')


class TransferSchema(Schema):
    uri = fields.Method(
        'get_uri',
        required=True,
        type='string',
        format='uri',
        description="The URI of this object.",
        example='https://example.com/debtors/1/transfers/123e4567-e89b-12d3-a456-426655440000',
    )
    type = fields.Function(
        lambda obj: 'Transfer',
        required=True,
        type='string',
        description='The type of this object.',
        example='Transfer',
    )
    debtorUri = fields.Function(
        lambda obj: url_for('debtors.DebtorEndpoint', _external=True, debtorId=obj.debtor_id),
        required=True,
        type='string',
        format="uri",
        description="The debtor's URI.",
        example='https://example.com/debtors/1/',
    )
    recipient_creditor_id = fields.Integer(
        required=True,
        dump_only=True,
        validate=validate.Range(min=0, max=MAX_UINT64),
        data_key='recipientCreditorId',
        description="The recipient's creditor ID.",
        example=1111,
    )
    amount = fields.Integer(
        required=True,
        dump_only=True,
        validate=validate.Range(min=1, max=MAX_INT64),
        format="int64",
        description=InitiatedTransfer.amount.comment,
        example=1000,
    )
    transfer_note = fields.String(
        required=True,
        dump_only=True,
        data_key='note',
        description=InitiatedTransfer.transfer_note.comment,
    )
    initiated_at_ts = fields.DateTime(
        required=True,
        dump_only=True,
        data_key='initiatedAt',
        description=InitiatedTransfer.initiated_at_ts.comment,
    )
    is_finalized = fields.Boolean(
        required=True,
        dump_only=True,
        data_key='isFinalized',
        description='Whether the transfer has been finalized or not.',
        example=True,
    )
    finalizedAt = fields.Method(
        'get_finalized_at_string',
        required=True,
        type='string',
        format='date-time',
        description='The moment at which the transfer has been finalized. If the transfer '
                    'has not been finalized yet, this field contains an estimation of when '
                    'the transfer should be finalized.',
    )
    is_successful = fields.Boolean(
        required=True,
        dump_only=True,
        data_key='isSuccessful',
        description=InitiatedTransfer.is_successful.comment,
        example=False,
    )
    errors = fields.Nested(
        TransferErrorSchema(many=True),
        dump_only=True,
        required=True,
        description='Errors that have occurred during the execution of the transfer. If '
                    'the transfer has been successful, this will be an empty array.',
    )

    def get_uri(self, obj):
        return url_for(self.context['Transfer'], _external=True, debtorId=obj.debtor_id, transferUuid=obj.transfer_uuid)

    def get_finalized_at_string(self, obj):
        if obj.is_finalized:
            finalized_at_ts = obj.finalized_at_ts
        else:
            current_ts = datetime.now(tz=timezone.utc)
            current_delay = current_ts - obj.initiated_at_ts
            average_delay = timedelta(seconds=current_app.config['APP_TRANSFERS_FINALIZATION_AVG_SECONDS'])
            finalized_at_ts = current_ts + max(current_delay, average_delay)
        return finalized_at_ts.isoformat()


class TransferUpdateRequestSchema(ValidateTypeMixin, Schema):
    type = fields.String(
        missing='TransferUpdateRequest',
        default='TransferUpdateRequest',
        description='The type of this object.',
        example='TransferUpdateRequest',
    )
    is_finalized = fields.Boolean(
        required=True,
        data_key='isFinalized',
        description='Should be `true`.',
        example=True,
    )
    is_successful = fields.Boolean(
        required=True,
        data_key='isSuccessful',
        description='Should be `false`.',
        example=False,
    )


class TransfersCollectionSchema(Schema):
    uri = fields.Method(
        'get_uri',
        required=True,
        type='string',
        format='uri',
        description="The URI of this object.",
        example='https://example.com/debtors/1/transfers/',
    )
    type = fields.Function(
        lambda obj: 'TransfersCollection',
        required=True,
        type='string',
        description='The type of this object.',
        example='TransfersCollection',
    )
    debtorUri = fields.Function(
        lambda obj: url_for('debtors.DebtorEndpoint', _external=True, debtorId=obj.debtor_id),
        required=True,
        type='string',
        format="uri",
        description="The debtor's URI.",
        example='https://example.com/debtors/1/',
    )
    totalItems = fields.Function(
        lambda obj: len(obj.items),
        required=True,
        type='integer',
        description="The total number of items in the collection.",
        example=2,
    )
    items = fields.List(
        fields.Str(format='uri-reference'),
        dump_only=True,
        description="When the total number of items in the collection is small enough, this field "
                    "will contain all of them (in an array), so that in such cases it would be "
                    "unnecessary to follow the `first` link.",
        example=['123e4567-e89b-12d3-a456-426655440000', '183ea7c7-7a96-4ed7-a50a-a2b069687d23'],
    )
    itemsType = fields.Function(
        lambda obj: 'string',
        required=True,
        type='string',
        description='The type of the items in the collection. In this particular case the items '
                    'are relative URIs, so the type will be `"string"`.',
        example='string',
    )
    first = fields.Function(
        lambda obj: '',
        required=True,
        type='string',
        format="uri-reference",
        description='The URI of the first page in the paginated collection. The object retrieved '
                    'from this URI will have: 1) An `items` property (an array), which will contain '
                    'the first items of the collection; 2) May have a `next` property (a string), '
                    'which would contain the URI of the next page in the collection. This can be '
                    'a relative URI.',
        example='',
    )

    def get_uri(self, obj):
        return url_for(self.context['TransfersCollection'], _external=True, debtorId=obj.debtor_id)
