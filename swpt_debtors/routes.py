from flask import redirect
from flask.views import MethodView
from flask_smorest import Blueprint, abort
from .models import PendingTransfer
from .schemas import DebtorCreationRequestSchema, DebtorSchema, DebtorPolicyUpdateRequestSchema, \
    DebtorPolicySchema, TransferSchema, TransfersCollectionSchema, TransferCreationRequestSchema
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
SPEC_LOCATION_HEADER = {
    'Location': {
        'description': 'The URI of the entry.',
        'schema': {
            'type': 'string',
            'format': 'uri',
        },
    },
}
SPEC_DEBTOR_DOES_NOT_EXIST = {
    'description': 'The debtor does not exist.',
}
SPEC_CONFLICTING_DEBTOR = {
    'description': 'A debtor with the same ID already exists.',
}
SPEC_CONFLICTING_POLICY = {
    'description': 'The new policy is in conflict with the old one.',
}
SPEC_TRANSFER_DOES_NOT_EXIST = {
    'description': 'The transfer entry does not exist.',
}
SPEC_CONFLICTING_TRANSFER = {
    'description': 'A different transfer entry with the same UUID already exists.',
}
SPEC_TOO_MANY_TRANSFERS = {
    'description': 'Too many pending transfers.',
}
SPEC_DUPLICATED_TRANSFER = {
    'description': 'The same transfer entry already exists.',
    'headers': SPEC_LOCATION_HEADER,
}

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
    @admin_api.response(DebtorSchema, code=201, headers=SPEC_LOCATION_HEADER)
    @admin_api.doc(responses={409: SPEC_CONFLICTING_DEBTOR})
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
    @public_api.doc(responses={404: SPEC_DEBTOR_DOES_NOT_EXIST})
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
    @policy_api.doc(responses={404: SPEC_DEBTOR_DOES_NOT_EXIST})
    def get(self, debtorId):
        """Return information about debtor's policy."""

        debtor = procedures.get_debtor(debtorId)
        return debtor or abort(404)

    @policy_api.arguments(DebtorPolicyUpdateRequestSchema)
    @policy_api.response(code=204)
    @policy_api.doc(responses={404: SPEC_DEBTOR_DOES_NOT_EXIST,
                               409: SPEC_CONFLICTING_POLICY})
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
    @transfers_api.doc(responses={404: SPEC_DEBTOR_DOES_NOT_EXIST})
    def get(self, debtorId):
        """Return all credit-issuing transfers for a given debtor."""

        return range(10)

    @transfers_api.arguments(TransferCreationRequestSchema)
    @transfers_api.response(TransferSchema, code=201, headers=SPEC_LOCATION_HEADER)
    @transfers_api.doc(responses={303: SPEC_DUPLICATED_TRANSFER,
                                  403: SPEC_TOO_MANY_TRANSFERS,
                                  404: SPEC_DEBTOR_DOES_NOT_EXIST,
                                  409: SPEC_CONFLICTING_TRANSFER})
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
    @transfers_api.doc(responses={404: SPEC_TRANSFER_DOES_NOT_EXIST})
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
