from typing import NamedTuple, List
from urllib.parse import urljoin
from flask import redirect, url_for, request
from flask.views import MethodView
from flask_smorest import Blueprint, abort
from swpt_lib import endpoints
from .schemas import DebtorCreationOptionsSchema, DebtorSchema, DebtorPolicyUpdateRequestSchema, \
    DebtorPolicySchema, TransferSchema, TransfersCollectionSchema, TransferCreationRequestSchema
from . import procedures

SPEC_DEBTOR_ID = {
    'in': 'path',
    'name': 'debtorId',
    'required': True,
    'description': "The debtor's ID",
    'schema': {
        'type': 'integer',
        'format': 'uint64',
        'minimum': 0,
        'maximum': (1 << 64) - 1,
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
    'description': 'Too many issuing transfers.',
}
SPEC_DUPLICATED_TRANSFER = {
    'description': 'The same transfer entry already exists.',
    'headers': SPEC_LOCATION_HEADER,
}

debtors_api = Blueprint(
    'debtors',
    __name__,
    url_prefix='/debtors',
    description="Obtain public information about debtors and create new debtors.",
)
policies_api = Blueprint(
    'policies',
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

context = {
    'Debtor': 'debtors.Debtor',
    'DebtorPolicy': 'policies.DebtorPolicy',
    'IssuingTransfers': 'transfers.IssuingTransfers',
    'Transfer': 'transfers.Transfer'
}


class TransfersCollection(NamedTuple):
    debtor_id: int
    members: List[str]


@debtors_api.route('/<i64:debtorId>', parameters=[SPEC_DEBTOR_ID], endpoint='Debtor')
class DebtorInfo(MethodView):
    @debtors_api.response(DebtorSchema(context=context))
    @debtors_api.doc(responses={404: SPEC_DEBTOR_DOES_NOT_EXIST})
    def get(self, debtorId):
        """Return public information about a debtor."""

        return procedures.get_debtor(debtorId) or abort(404)

    @debtors_api.arguments(DebtorCreationOptionsSchema)
    @debtors_api.response(DebtorSchema(context=context), code=201, headers=SPEC_LOCATION_HEADER)
    @debtors_api.doc(responses={409: SPEC_CONFLICTING_DEBTOR})
    def post(self, debtor_creation_options, debtorId):
        """Try to create a new debtor. Requires special privileges."""

        try:
            debtor = procedures.create_new_debtor(debtorId)
        except procedures.DebtorExistsError:
            abort(409)
        return debtor, {'Location': endpoints.build_url('debtor', debtorId=debtorId)}


@policies_api.route('/<i64:debtorId>/policy', parameters=[SPEC_DEBTOR_ID])
class DebtorPolicy(MethodView):
    @policies_api.response(DebtorPolicySchema(context=context))
    @policies_api.doc(responses={404: SPEC_DEBTOR_DOES_NOT_EXIST})
    def get(self, debtorId):
        """Return information about debtor's policy."""

        return procedures.get_debtor(debtorId) or abort(404)

    @policies_api.arguments(DebtorPolicyUpdateRequestSchema)
    @policies_api.response(DebtorPolicySchema(context=context))
    @policies_api.doc(responses={404: SPEC_DEBTOR_DOES_NOT_EXIST,
                                 409: SPEC_CONFLICTING_POLICY})
    def patch(self, policy_update_request, debtorId):
        """Update debtor's policy.

        This operation is **idempotent**!

        ---
        TODO:
        """

        debtor = procedures.get_debtor(debtorId)
        return debtor or abort(404)


@transfers_api.route('/<i64:debtorId>/transfers/', parameters=[SPEC_DEBTOR_ID])
class IssuingTransfers(MethodView):
    @transfers_api.response(TransfersCollectionSchema(context=context))
    @transfers_api.doc(responses={404: SPEC_DEBTOR_DOES_NOT_EXIST})
    def get(self, debtorId):
        """Return the debtor's collection of credit-issuing transfers."""

        if procedures.get_debtor(debtorId) is None:
            abort(404)
        return TransfersCollection(debtor_id=debtorId, members=procedures.get_transfer_uuids(debtorId))

    @transfers_api.arguments(TransferCreationRequestSchema)
    @transfers_api.response(TransferSchema(context=context), code=201, headers=SPEC_LOCATION_HEADER)
    @transfers_api.doc(responses={303: SPEC_DUPLICATED_TRANSFER,
                                  403: SPEC_TOO_MANY_TRANSFERS,
                                  404: SPEC_DEBTOR_DOES_NOT_EXIST,
                                  409: SPEC_CONFLICTING_TRANSFER})
    def post(self, transfer_request, debtorId):
        """Create a new credit-issuing transfer.

        ---
        TODO:
        """

        debtor = procedures.get_or_create_debtor(debtorId)
        transfer_uuid = transfer_request['transfer_uuid']
        location = url_for('transfers.Transfer', _external=True, debtorId=debtorId, transferUuid=transfer_uuid)
        recipient_uri = urljoin(request.base_url, transfer_request['recipient_uri'])
        try:
            try:
                recipient_creditor_id = endpoints.match_url('creditor', recipient_uri)['creditorId']
            except endpoints.MatchError:
                recipient_creditor_id = None
            transfer = procedures.initiate_transfer(
                debtorId,
                transfer_uuid,
                recipient_creditor_id,
                recipient_uri,
                transfer_request['amount'],
                transfer_request['transfer_info'],
            )
        except procedures.TransferExistsError:
            return redirect(location, code=303)
        except procedures.TransfersConflictError:
            abort(409)
        return transfer, {'Location': location}


@transfers_api.route('/<i64:debtorId>/transfers/<transferUuid>', parameters=[SPEC_DEBTOR_ID, SPEC_TRANSFER_UUID])
class Transfer(MethodView):
    @transfers_api.response(TransferSchema(context=context))
    @transfers_api.doc(responses={404: SPEC_TRANSFER_DOES_NOT_EXIST})
    def get(self, debtorId, transferUuid):
        """Return information about a credit-issuing transfer."""

        return procedures.get_initiated_transfer(debtorId, transferUuid) or abort(404)

    @transfers_api.response(code=204)
    def delete(self, debtorId, transferUuid):
        """Delete a credit-issuing transfer."""

        return procedures.delete_initiated_transfer(debtorId, transferUuid)
