from flask.views import MethodView
from flask_smorest import Blueprint, abort
from marshmallow import Schema, fields, validate, missing
from .models import Debtor, INTEREST_RATE_FLOOR, INTEREST_RATE_CEIL
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


class ResourceMixin:
    self = fields.Method(
        'get_self',
        type='string',
        format='uri-reference',
        example='https://foo.bar.com/resources/0',
        description="The canonical URL of the object. Can be relative.",
    )
    type = fields.Method(
        'get_type',
        type='string',
        example='Resource',
        description='The type of the object.',
    )

    def get_self(self, obj):
        return missing

    def get_type(self, obj):
        return missing


class InterestRateLowerLimitSchema(Schema):
    value = fields.Float(description='The annual interest rate (in percents) should be no less than this value.')
    cutoff = fields.DateTime(data_key='enforcedUntil', description='The limit will not be enforced after this moment.')


class BalanceLowerLimitSchema(Schema):
    value = fields.Int(format='int64', description='The balance should be no less than this value.')
    cutoff = fields.DateTime(data_key='enforcedUntil', description='The limit will not be enforced after this moment.')


class DebtorSchema(ResourceMixin, Schema):
    debtor_id = fields.Int(data_key='debtorId', format="int64", description=SPEC_DEBTOR_ID['description'])
    balance = fields.Int(format="int64", description=Debtor.balance.comment)
    balance_ts = fields.DateTime(data_key='balanceTimestamp', description=Debtor.balance_ts.comment)
    balance_lower_limits = fields.Nested(
        BalanceLowerLimitSchema(many=True),
        missing=[],
        data_key='balanceLowerLimits',
        description='Enforced lower limits for the `balance` field.',
    )
    interest_rate_target = fields.Float(
        validate=validate.Range(min=INTEREST_RATE_FLOOR, max=INTEREST_RATE_CEIL),
        data_key='interestRateTarget',
        description=Debtor.interest_rate_target.comment,
    )
    interest_rate_lower_limits = fields.Nested(
        InterestRateLowerLimitSchema(many=True),
        missing=[],
        data_key='interestRateLowerLimits',
        description='Enforced interest rate lower limits.',
    )


@debtors_api.route('/<int:debtorId>', parameters=[SPEC_DEBTOR_ID])
class DebtorInfo(MethodView):
    @debtors_api.response(DebtorSchema())
    def get(self, debtorId):
        """Return debtor's principal information.

        The content could be cached.

        ---
        Ignored
        """

        debtor = procedures.get_or_create_debtor(debtorId)
        return debtor


@debtors_api.route('/debtors/<int:debtorId>/balance-limits', parameters=[SPEC_DEBTOR_ID])
class BalanceLimits(MethodView):
    def get(self, debtorId):
        pass

    # @debtors_api.arguments(DebtorPathArgsSchema, location='path', as_kwargs=True)
    def patch(self, debtorId):
        pass


@debtors_api.route('/debtors/<int:debtorId>/interest-rate', parameters=[SPEC_DEBTOR_ID])
class InterestRate(MethodView):
    def get(self, debtorId):
        pass


@debtors_api.route('/debtors/<int:debtorId>/interest-rate-target', parameters=[SPEC_DEBTOR_ID])
class InterestRateTarget(MethodView):
    def get(self, debtorId):
        pass

    def put(self, debtorId):
        pass


@debtors_api.route('/debtors/<int:debtorId>/interest-rate-limits', parameters=[SPEC_DEBTOR_ID])
class InterestRateLimits(MethodView):
    def get(self, debtorId):
        pass

    def patch(self, debtorId):
        pass
