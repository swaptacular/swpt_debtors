from flask.views import MethodView
from flask_smorest import Blueprint, abort
from . import procedures

web_api = Blueprint('web_api', __name__)


@web_api.route('/debtors/<int:debtor_id>')
class DebtorInfo(MethodView):
    def get(self, debtor_id):
        pass


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
