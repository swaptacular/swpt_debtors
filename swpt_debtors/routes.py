import binascii
from base64 import urlsafe_b64decode, urlsafe_b64encode
from flask import Blueprint, abort
from flask.views import MethodView
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


@web_api.route('/debtors/<int:debtor_id>/concessions')
class ConcessionList(MethodView):
    def get(self, debtor_id):
        pass

    def post(self, debtor_id):
        pass


@web_api.route('/debtors/<int:debtor_id>/concessions/<int:creditor_id>')
class Concession(MethodView):
    def get(self, debtor_id, creditor_id):
        pass


@web_api.route('/debtors/<int:debtor_id>/concessions/<int:creditor_id>/interest-rate-limits')
class ConcessionLimits(MethodView):
    def get(self, debtor_id, creditor_id):
        pass

    def patch(self, debtor_id, creditor_id):
        pass


@web_api.route('/debtors/<int:debtor_id>/interest-rate/concessions/<int:creditor_id>/balance-limits')
class ConcessionBalanceLimits(MethodView):
    def get(self, debtor_id, creditor_id):
        pass

    def patch(self, debtor_id, creditor_id):
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
# web_api.add_url_rule(
#     '/debtors/<int:debtor_id>/interest-rate/concessions',
#     view_func=ConcessionList.as_view('interest_rate_concession_list'),
# )
# web_api.add_url_rule(
#     '/debtors/<int:debtor_id>/interest-rate/concessions/<int:creditor_id>',
#     view_func=Concession.as_view('interest_rate_concession'),
# )
# web_api.add_url_rule(
#     '/debtors/<int:debtor_id>/interest-rate/concessions/<int:creditor_id>/lower-limits',
#     view_func=ConcessionLowerLimits.as_view('concession_lower_limits'),
# )
# web_api.add_url_rule(
#     '/debtors/<int:debtor_id>/interest-rate/concessions/<int:creditor_id>/balance-limits',
#     view_func=ConcessionBalanceLimits.as_view('concession_balance_limits'),
# )
