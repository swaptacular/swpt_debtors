from flask import redirect
from flask.views import MethodView
from flask_smorest import Blueprint, abort
from .models import PendingTransfer
from .schemas import SPEC_DEBTOR_ID, SPEC_TRANSFER_UUID, SPEC_303_HEADERS, DebtorCreationRequestSchema, \
    DebtorPolicySchema, TransferSchema, TransfersCollectionSchema, TransferCreationRequestSchema, \
    DebtorPolicyUpdateRequestSchema, DebtorSchema
from . import procedures

CONFLICTING_DEBTOR_EXISTS = {'description': 'A debtor with the same ID already exists.'}
DEBTOR_DOES_NOT_EXIST = {'description': 'The debtor does not exist.'}
CONFLICTING_POLICY = {'description': 'The new policy is in conflict with the old one.'}
TRANSFER_DOES_NOT_EXIST = {'description': 'The transfer entry does not exist.'}
CONFLICTING_TRANSFER_EXISTS = {'description': 'A different transfer entry with the same UUID already exists.'}
TOO_MANY_TRANSFERS = {'description': 'Too many pending transfers.'}
IDENTICAL_TRANSFER_EXISTS = {'description': 'The same transfer entry already exists.', 'headers': SPEC_303_HEADERS}


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


@admin_api.route('')
class DebtorsCollection(MethodView):
    @admin_api.arguments(DebtorCreationRequestSchema)
    @admin_api.response(DebtorSchema, code=201)
    @admin_api.doc(responses={
        409: CONFLICTING_DEBTOR_EXISTS,
    })
    def post(self, debtor_info):
        """Try to create a new debtor."""

        debtor_id = debtor_info['debtor_id']
        try:
            debtor = procedures.create_new_debtor(debtor_id)
            # debtor = procedures.get_or_create_debtor(debtor_id)
        except procedures.DebtorExistsError:
            abort(409)
        # TODO: Add schema and domain?
        return debtor, {'Location': f'debtors/{debtor_id}'}


@public_api.route('/<int:debtorId>', parameters=[SPEC_DEBTOR_ID])
class DebtorInfo(MethodView):
    @public_api.response(DebtorSchema)
    @admin_api.doc(responses={
        404: DEBTOR_DOES_NOT_EXIST,
    })
    def get(self, debtorId):
        """Return information about a debtor.

        ---
        Ignored
        """

        debtor = procedures.get_or_create_debtor(debtorId)
        return debtor or abort(404)


@policy_api.route('/<int:debtorId>/policy', parameters=[SPEC_DEBTOR_ID])
class DebtorPolicy(MethodView):
    @policy_api.response(DebtorPolicySchema)
    @admin_api.doc(responses={
        404: DEBTOR_DOES_NOT_EXIST,
    })
    def get(self, debtorId):
        """Return information about debtor's policy."""

        debtor = procedures.get_debtor(debtorId)
        return debtor or abort(404)

    @policy_api.arguments(DebtorPolicyUpdateRequestSchema)
    @policy_api.response(code=204)
    @admin_api.doc(responses={
        404: DEBTOR_DOES_NOT_EXIST,
        409: CONFLICTING_POLICY,
    })
    def patch(self, debtor_info, debtorId):
        """Update debtor's policy.

        This operation is **idempotent**!
        """

        # TODO: abort(409, message='fdfd', headers={'xxxyyy': 'zzz'})
        abort(409)
        abort(404)


@transfers_api.route('/<int:debtorId>/transfers', parameters=[SPEC_DEBTOR_ID])
class TransfersCollection(MethodView):
    @transfers_api.response(TransfersCollectionSchema)
    @admin_api.doc(responses={
        404: DEBTOR_DOES_NOT_EXIST,
    })
    def get(self, debtorId):
        """Return all credit-issuing transfers for a given debtor."""

        return range(10)

    @policy_api.arguments(TransferCreationRequestSchema)
    @transfers_api.response(TransferSchema, code=201)
    @admin_api.doc(responses={
        303: IDENTICAL_TRANSFER_EXISTS,
        403: TOO_MANY_TRANSFERS,
        404: DEBTOR_DOES_NOT_EXIST,
        409: CONFLICTING_TRANSFER_EXISTS,
    })
    def post(self, transfer_info, debtorId):
        """Create a new credit-issuing transfer."""

        debtor = procedures.get_or_create_debtor(debtorId)
        transfer_uuid = transfer_info['transfer_uuid']
        try:
            transfer = procedures.create_pending_transfer(
                debtorId,
                transfer_uuid,
                transfer_info['recipient_creditor_id'],
                transfer_info['amount'],
                transfer_info['transfer_info'],
            )
        except procedures.TransferExistsError:
            # TODO: Add schema and domain?
            return redirect(f'/debtors/{debtorId}/transfers/{transfer_uuid}', code=303)
        except procedures.TransfersConflictError:
            abort(409)
        return transfer


@transfers_api.route('/<int:debtorId>/transfers/<transferUuid>', parameters=[SPEC_DEBTOR_ID, SPEC_TRANSFER_UUID])
class Transfer(MethodView):
    @transfers_api.response(TransferSchema)
    @admin_api.doc(responses={
        404: TRANSFER_DOES_NOT_EXIST,
    })
    def get(self, debtorId, transferUuid):
        """Return details about a credit-issuing transfer."""

        class Transfer:
            pass
        transfer = PendingTransfer.get_instance((debtorId, transferUuid))
        if transfer:
            return transfer
        abort(404)

    @transfers_api.response(code=204)
    def delete(self, debtorId, transferUuid):
        """Purge a finalized credit-issuing transfer."""
