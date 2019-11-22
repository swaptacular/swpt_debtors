from collections import abc
from flask.views import MethodView
from flask_smorest import Blueprint, abort
from marshmallow import Schema, fields, validate, pre_dump, missing
from .models import Debtor, INTEREST_RATE_FLOOR, INTEREST_RATE_CEIL, MIN_INT64, MAX_INT64
from . import procedures

admin_api = Blueprint(
    'admin',
    __name__,
    url_prefix='/debtors',
    description="Create new debtors.",
)
public_api = Blueprint(
    'public',
    __name__,
    url_prefix='/debtors',
    description="Obtain public information about debtors.",
)
policy_api = Blueprint(
    'policy',
    __name__,
    url_prefix='/debtors',
    description="Change individual debtor's policies.",
)
transfers_api = Blueprint(
    'transfers',
    __name__,
    url_prefix='/debtors',
    description="Make credit-issuing transfers.",
)


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


class DebtorIdSchema(Schema):
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
        description="A client-generated UUID for the transfer.",
        example='123e4567-e89b-12d3-a456-426655440000',
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


@admin_api.route('')
class DebtorsCollection(MethodView):
    @admin_api.arguments(DebtorIdSchema)
    @admin_api.response(DebtorSchema, code=201)
    def post(self, debtor_info):
        """Try to create a new debtor."""

        debtor_id = debtor_info['debtor_id']
        try:
            debtor = procedures.create_new_debtor(debtor_id)
            # debtor = procedures.get_or_create_debtor(debtor_id)
        except procedures.DebtorAlreadyExistsError:
            abort(409)
        # TODO: Add schema and domain?
        return debtor, {'Location': f'debtors/{debtor_id}'}


@public_api.route('/<int:debtorId>', parameters=[SPEC_DEBTOR_ID])
class DebtorInfo(MethodView):
    @public_api.response(DebtorSchema)
    def get(self, debtorId):
        """Return information about a debtor.

        ---
        Ignored
        """

        debtor = procedures.get_or_create_debtor(debtorId)
        return debtor


@policy_api.route('/<int:debtorId>/policy', parameters=[SPEC_DEBTOR_ID])
class DebtorPolicy(MethodView):
    @policy_api.response(DebtorPolicySchema)
    def get(self, debtorId):
        """Return information about debtor's policy."""

        return procedures.get_debtor(debtorId) or abort(404)

    @policy_api.arguments(DebtorPolicySchema)
    @policy_api.response(code=204)
    def patch(self, debtor_info, debtorId):
        """Update debtor's policy.

        This operation is **idempotent**!
        """

        # TODO: abort(409, message='fdfd', headers={'xxxyyy': 'zzz'})


@transfers_api.route('/<int:debtorId>/transfers', parameters=[SPEC_DEBTOR_ID])
class TransfersCollection(MethodView):
    @transfers_api.response(TransfersCollectionSchema)
    def get(self, debtorId):
        """Return all credit-issuing transfers for a given debtor."""

        return range(10)

    @policy_api.arguments(TransferSchema)
    @transfers_api.response(code=201)
    def post(self, debtorId):
        """Create a new credit-issuing transfer."""

        return ''


@transfers_api.route('/<int:debtorId>/transfers/<transferUuid>', parameters=[SPEC_DEBTOR_ID, SPEC_TRANSFER_UUID])
class Transfer(MethodView):
    @transfers_api.response(TransferSchema)
    def get(self, debtorId, transferUuid):
        """Return details about a credit-issuing transfer."""

        class Transfer:
            pass
        transfer = Transfer()
        transfer.debtor_id = debtorId
        transfer.transfer_uuid = '666'
        return transfer

    @transfers_api.response(code=204)
    def delete(self, debtorId, transferUuid):
        """Purge a finalized credit-issuing transfer."""
