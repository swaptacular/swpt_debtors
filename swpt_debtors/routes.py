from flask.views import MethodView
from flask_smorest import Blueprint, abort
from marshmallow import Schema, fields, validate
from .models import Debtor, INTEREST_RATE_FLOOR, INTEREST_RATE_CEIL, MIN_INT64, MAX_INT64
from . import procedures

public_api = Blueprint(
    'public',
    __name__,
    url_prefix='/debtors',
    description="Operations that everybody can perform",
)
private_api = Blueprint(
    'private',
    __name__,
    url_prefix='/debtors',
    description="Operations that only the respective debtor can perform",
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


class ResourceSchema(Schema):
    uri = fields.Method(
        'get_uri',
        type='string',
        format='uri-reference',
        description="The URI of the resource. Can be relative.",
        example='https://example.com/resources/1',
    )
    type = fields.Method(
        'get_type',
        type='string',
        description='The type of the resource.',
        example='Resource',
    )


class DebtorInfoSchema(ResourceSchema):
    debtor_id = fields.Int(
        dump_only=True,
        data_key='debtorId',
        format="int64",
        description=SPEC_DEBTOR_ID['description'],
        example=1,
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

    def get_interest_rate(self, obj):
        assert isinstance(obj, Debtor)
        return procedures.get_current_interest_rate(obj)


class DebtorSchema(DebtorInfoSchema):
    def get_type(self, obj):
        return 'Debtor'

    def get_uri(self, obj):
        assert isinstance(obj, Debtor)

        # TODO: Add schema and domain?
        return f'/debtors/{obj.debtor_id}'


class DebtorPolicySchema(DebtorInfoSchema):
    def get_type(self, obj):
        return 'DebtorPolicy'

    def get_uri(self, obj):
        assert isinstance(obj, Debtor)

        # TODO: Add schema and domain?
        return f'/debtors/{obj.debtor_id}/policy'


@public_api.route('/<int:debtorId>', parameters=[SPEC_DEBTOR_ID])
class DebtorInfo(MethodView):
    @public_api.response(DebtorSchema)
    def get(self, debtorId):
        """Return info about a debtor.

        ---
        Ignored
        """

        debtor = procedures.get_or_create_debtor(debtorId)
        return debtor


@private_api.route('/<int:debtorId>/policy', parameters=[SPEC_DEBTOR_ID])
class DebtorPolicy(MethodView):
    @private_api.response(DebtorPolicySchema)
    def get(self, debtorId):
        """Return info about debtor's policy."""

        return procedures.get_debtor(debtorId) or abort(404)

    @private_api.arguments(DebtorPolicySchema)
    @private_api.response(code=204)
    def patch(self, debtor_info, debtorId):
        """Update debtor's policy."""

        # abort(409, message='fdfd', headers={'xxxyyy': 'zzz'})
        # debtor = procedures.get_debtor(debtorId)
        # return debtor
