from flask.views import MethodView
from flask_smorest import Blueprint, abort
from marshmallow import Schema, fields
from .models import Debtor
from . import procedures

web_api = Blueprint('web_api', __name__, url_prefix='/debtors', description="Operations on debtors")


class DebtorPathArgsSchema(Schema):
    debtor_id = fields.Int(required=True, description="The debtor's ID", format="int64")


class DebtorSchema(Schema):
    debtor_id = fields.Int(data_key='debtorId', format="int64")


@web_api.route('/<int:debtor_id>', parameters=[DebtorPathArgsSchema])
class DebtorInfo(MethodView):
    # @web_api.arguments(DebtorPathArgsSchema, location='path', as_kwargs=True)
    @web_api.response(DebtorSchema)
    def get(self, debtor_id):
        """Return debtor's principal information.

        The content could be cached.
        """

        return Debtor(debtor_id=debtor_id)


@web_api.route('/debtors/<int:debtor_id>/balance-limits')
class BalanceLimits(MethodView):
    def get(self, debtor_id):
        pass

    def patch(self, debtor_id):
        pass


@web_api.route('/debtors/<int:debtor_id>/interest-rate')
class InterestRate(MethodView):
    def get(self, debtor_id):
        pass


@web_api.route('/debtors/<int:debtor_id>/interest-rate-target')
class InterestRateTarget(MethodView):
    def get(self, debtor_id):
        pass

    def put(self, debtor_id):
        pass


@web_api.route('/debtors/<int:debtor_id>/interest-rate-limits')
class InterestRateLimits(MethodView):
    def get(self, debtor_id):
        pass

    def patch(self, debtor_id):
        pass


# web_api.add_url_rule(
#     '/debtors/<int:debtor_id>',
#     view_func=DebtorInfo.as_view('debtor_info'),
# )
# web_api.add_url_rule(
#     '/debtors/<int:debtor_id>/interest-rate',
#     view_func=InterestRateTarget.as_view('interest_rate'),
# )
# web_api.add_url_rule(
#     '/debtors/<int:debtor_id>/interest-rate/target',
#     view_func=InterestRateTarget.as_view('interest_rate_target'),
# )
# web_api.add_url_rule(
#     '/debtors/<int:debtor_id>/interest-rate/lower-limits',
#     view_func=InterestRateLowerLimits.as_view('interest_rate_lower_limits'),
# )
