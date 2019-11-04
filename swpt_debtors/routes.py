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


@web_api.route('/debtors/<int:debtor_id>/balance-upper-limits')
class BalanceUpperLimits(MethodView):
    def get(self, debtor_id):
        pass

    def patch(self, debtor_id):
        pass


@web_api.route('/debtors/<int:debtor_id>/interest-rate')
class InterestRate(MethodView):
    def get(self, debtor_id):
        pass


@web_api.route('/debtors/<int:debtor_id>/interest-rate/target')
class InterestRateTarget(MethodView):
    def get(self, debtor_id):
        pass

    def put(self, debtor_id):
        pass


@web_api.route('/debtors/<int:debtor_id>/interest-rate/upper-limits')
class InterestRateUpperLimits(MethodView):
    def get(self, debtor_id):
        pass

    def patch(self, debtor_id):
        pass


@web_api.route('/debtors/<int:debtor_id>/interest-rate/lower-limits')
class InterestRateLowerLimits(MethodView):
    def get(self, debtor_id):
        pass

    def patch(self, debtor_id):
        pass


@web_api.route('/debtors/<int:debtor_id>/interest-rate/concessions')
class InterestRateConcessionList(MethodView):
    def get(self, debtor_id):
        pass

    def post(self, debtor_id):
        pass


@web_api.route('/debtors/<int:debtor_id>/interest-rate/concessions/<int:creditor_id>')
class InterestRateConcession(MethodView):
    def get(self, debtor_id, creditor_id):
        pass


@web_api.route('/debtors/<int:debtor_id>/interest-rate/concessions/<int:creditor_id>/lower-limits')
class ConcessionLowerLimits(MethodView):
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
