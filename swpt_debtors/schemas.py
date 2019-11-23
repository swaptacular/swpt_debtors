from collections import abc
from marshmallow import Schema, fields, validate, pre_dump, missing
from .models import Debtor, PendingTransfer, INTEREST_RATE_FLOOR, INTEREST_RATE_CEIL, MIN_INT64, MAX_INT64
from . import procedures

SPEC_DEBTOR_ID = {
    'in': 'path',
    'name': 'debtorId',
    'required': True,
    'description': "The debtor's ID",
    'schema': {
        'type': 'integer',
        'format': 'int64',
    },
}
SPEC_TRANSFER_UUID = {
    'in': 'path',
    'name': 'transferUuid',
    'required': True,
    'description': "The transfer's UUID",
    'schema': {
        'type': 'string',
    },
}


class ResourceSchema(Schema):
    uri = fields.Method(
        'get_uri',
        type='string',
        format='uri',
        description="The URI of this object.",
        example='https://example.com/resources/123',
    )
    type = fields.Method(
        'get_type',
        type='string',
        description='The type of this object.',
        example='Resource',
    )

    def get_type(self, obj):
        raise NotImplementedError

    def get_uri(self, obj):
        raise NotImplementedError


class CollectionSchema(ResourceSchema):
    members = fields.List(
        fields.Str(format='uri-reference'),
        dump_only=True,
        description='A list of relative URIs for the contained items.',
        example=['111111', '222222', '333333'],
    )
    totalItems = fields.Function(
        lambda obj: len(obj['members']),
        type='number',
        format='int32',
        description='The total number of items in the collection.',
        example=3,
    )

    @pre_dump
    def _to_dict(self, obj, many):
        assert not many
        assert isinstance(obj, abc.Iterable)
        return {'members': obj}

    def get_type(self, obj):
        return 'Collection'

    def get_uri(self, obj):
        return missing


class InterestRateLowerLimitSchema(Schema):
    value = fields.Float(
        required=True,
        validate=validate.Range(min=INTEREST_RATE_FLOOR, max=INTEREST_RATE_CEIL),
        description='The annual interest rate (in percents) should be no less than this value.',
    )
    cutoff = fields.DateTime(
        required=True,
        data_key='enforcedUntil',
        description='The limit will not be enforced after this moment.',
    )


class BalanceLowerLimitSchema(Schema):
    value = fields.Int(
        format='int64',
        required=True,
        validate=validate.Range(min=MIN_INT64, max=MAX_INT64),
        description='The balance should be no less than this value.',
    )
    cutoff = fields.DateTime(
        required=True,
        data_key='enforcedUntil',
        description='The limit will not be enforced after this moment.',
    )


class CreateDebtorRequestSchema(Schema):
    debtor_id = fields.Int(
        required=True,
        data_key='debtorId',
        format="int64",
        description=SPEC_DEBTOR_ID['description'],
        example=1,
    )


class DebtorInfoSchema(ResourceSchema):
    debtor_id = fields.Int(
        dump_only=True,
        data_key='debtorId',
        format="int64",
        description=SPEC_DEBTOR_ID['description'],
        example=1,
    )
    created_at_date = fields.Date(
        dump_only=True,
        data_key='createdOn',
        description=Debtor.created_at_date.comment,
    )
    balance = fields.Int(
        dump_only=True,
        format="int64",
        description=Debtor.balance.comment,
    )
    balance_ts = fields.DateTime(
        dump_only=True,
        data_key='balanceTimestamp',
        description='The moment at which the last change in the `balance` field happened.',
    )
    balance_lower_limits = fields.Nested(
        BalanceLowerLimitSchema(many=True),
        data_key='balanceLowerLimits',
        description='Enforced lower limits for the `balance` field.',
    )
    interest_rate_target = fields.Float(
        validate=validate.Range(min=INTEREST_RATE_FLOOR, max=INTEREST_RATE_CEIL),
        data_key='interestRateTarget',
        description=Debtor.interest_rate_target.comment,
        example=0,
    )
    interest_rate_lower_limits = fields.Nested(
        InterestRateLowerLimitSchema(many=True),
        data_key='interestRateLowerLimits',
        description='Enforced interest rate lower limits.',
    )
    interestRate = fields.Method(
        'get_interest_rate',
        type='number',
        format='float',
        description="The current annual interest rate (in percents) at which "
                    "interest accumulates on creditors' accounts.",
    )
    isActive = fields.Method(
        'get_is_active',
        type='boolean',
        description="Whether the debtor is active or not."
    )

    def get_interest_rate(self, obj):
        assert isinstance(obj, Debtor)
        return procedures.get_current_interest_rate(obj)

    def get_is_active(self, obj):
        return bool(obj.status & Debtor.STATUS_IS_ACTIVE_FLAG)


class DebtorSchema(DebtorInfoSchema):
    def get_type(self, obj):
        return 'Debtor'

    def get_uri(self, obj):
        # TODO: Add schema and domain?
        return f'/debtors/{obj.debtor_id}'


class DebtorPolicySchema(DebtorInfoSchema):
    def get_type(self, obj):
        return 'DebtorPolicy'

    def get_uri(self, obj):
        # TODO: Add schema and domain?
        return f'/debtors/{obj.debtor_id}/policy'


class TransferErrorSchema(Schema):
    error_code = fields.String(
        dump_only=True,
        data_key='code',
        description='The error code.',
        example='ACC003',
    )
    message = fields.String(
        dump_only=True,
        description='The error message.',
        example='The recipient account does not exist.',
    )


class TransferSchema(ResourceSchema):
    debtor_id = fields.Int(
        dump_only=True,
        data_key='debtorId',
        format="int64",
        description=SPEC_DEBTOR_ID['description'],
        example=1,
    )
    transfer_uuid = fields.UUID(
        required=True,
        data_key='transferUuid',
        description="The client-generated UUID for the transfer.",
        example='123e4567-e89b-12d3-a456-426655440000',
    )
    recipient_creditor_id = fields.Integer(
        required=True,
        data_key='recipientCreditorId',
        format="int64",
        description=PendingTransfer.recipient_creditor_id.comment,
        example=54321,
    )
    amount = fields.Integer(
        required=True,
        validate=validate.Range(min=1, max=MAX_INT64),
        format="int64",
        description=PendingTransfer.amount.comment,
        example=1000,
    )
    transfer_info = fields.Dict(
        data_key='transferInfo',
        description=PendingTransfer.transfer_info.comment,
    )
    initiated_at_ts = fields.DateTime(
        dump_only=True,
        data_key='initiatedAt',
        description=PendingTransfer.initiated_at_ts.comment,
    )
    isFinalized = fields.Function(
        lambda obj: not obj.finalized_at_ts,
        type='boolean',
        data_key='isFinalized',
        description='Whether the transfer has been finalized or not.',
        example=True,
    )
    finalizedAt = fields.Function(
        lambda obj: obj.finalized_at_ts or missing,
        type='string',
        format='date-time',
        description='The moment at which the transfer has been finalized. If the transfer '
                    'has not been finalized yet, this field will not be present.',
    )
    is_successful = fields.Boolean(
        dump_only=True,
        data_key='isSuccessful',
        description=PendingTransfer.is_successful.comment,
        example=False,
    )
    errors = fields.Nested(
        TransferErrorSchema(many=True),
        dump_only=True,
        description='Errors that occurred during the transfer.'
    )

    def get_type(self, obj):
        return 'Transfer'

    def get_uri(self, obj):
        # TODO: Add schema and domain?
        return f'/debtors/{obj.debtor_id}/transfers/{obj.transfer_uuid}'


class TransfersCollectionSchema(CollectionSchema):
    def get_type(self, obj):
        return 'TransfersCollection'

    def get_uri(self, obj):
        # TODO: Add schema and domain?
        return 'transfers'
