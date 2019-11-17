from flask.views import MethodView
from flask_smorest import Blueprint, abort
from marshmallow import Schema, fields, validate
from .models import Debtor, INTEREST_RATE_FLOOR, INTEREST_RATE_CEIL, MIN_INT64, MAX_INT64
from . import procedures

debtors_api = Blueprint('debtors_api', __name__, url_prefix='/debtors', description="Operations on debtors")


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


class DebtorSchema(Schema):
    uri = fields.Method(
        'get_uri',
        type='string',
        format='uri-reference',
        description="The URI of the object. Can be relative.",
        example='https://example.com/debtors/1',
    )
    type = fields.Function(
        lambda obj: 'Debtor',
        type='string',
        description='The type of the object ("Debtor").',
        example='Debtor',
    )
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
        description=Debtor.balance_ts.comment,
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

    def get_uri(self, obj):
        assert isinstance(obj, Debtor)

        # TODO: Add schema and domain?
        return f'/debtors/{obj.debtor_id}'


@debtors_api.route('/<int:debtorId>', parameters=[SPEC_DEBTOR_ID])
class DebtorInfo(MethodView):
    @debtors_api.response(DebtorSchema)
    def get(self, debtorId):
        """Return debtor's principal information.

        The content could be cached.

        ---
        Ignored
        """

        debtor = procedures.get_or_create_debtor(debtorId)
        return debtor


@debtors_api.route('/<int:debtorId>/policy', parameters=[SPEC_DEBTOR_ID])
class DebtorPolicy(MethodView):
    @debtors_api.response(DebtorSchema)
    def get(self, debtorId):
        """Return debtor's policy information."""

        return procedures.get_debtor(debtorId) or abort(404)

    @debtors_api.arguments(DebtorSchema)
    @debtors_api.response(DebtorSchema)
    def patch(self, debtor_info, debtorId):
        """Update debtor's policy."""

        # abort(409, message='fdfd', headers={'xxxyyy': 'zzz'})
        debtor = procedures.get_debtor(debtorId)
        return debtor
