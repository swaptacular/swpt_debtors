from flask import redirect, url_for
from flask.views import MethodView
from flask_smorest import Blueprint, abort
from swpt_lib import endpoints, utils
from .schemas import DebtorCreationOptionsSchema, DebtorSchema, DebtorPolicyUpdateRequestSchema, \
    DebtorPolicySchema, TransferSchema, TransfersCollectionSchema, IssuingTransferCreationRequestSchema, \
    TransfersCollection, TransferUpdateRequestSchema
from . import specs
from . import procedures

CONTEXT = {
    'Debtor': 'debtors.DebtorEndpoint',
    'DebtorPolicy': 'policies.DebtorPolicyEndpoint',
    'TransfersCollection': 'transfers.TransfersCollectionEndpoint',
    'Transfer': 'transfers.TransferEndpoint'
}


debtors_api = Blueprint(
    'debtors',
    __name__,
    url_prefix='/debtors',
    description="Obtain public information about debtors and create new debtors.",
)


@debtors_api.route('/<i64:debtorId>/', parameters=[specs.DEBTOR_ID])
class DebtorEndpoint(MethodView):
    @debtors_api.response(DebtorSchema(context=CONTEXT))
    @debtors_api.doc(responses={404: specs.DEBTOR_DOES_NOT_EXIST})
    def get(self, debtorId):
        """Return public information about a debtor."""

        debtor = procedures.get_debtor(debtorId)
        if not debtor:
            abort(404)
        return debtor, {'Cache-Control': 'max-age=86400'}

    @debtors_api.arguments(DebtorCreationOptionsSchema)
    @debtors_api.response(DebtorSchema(context=CONTEXT), code=201, headers=specs.LOCATION_HEADER)
    @debtors_api.doc(responses={409: specs.CONFLICTING_DEBTOR})
    def post(self, debtor_creation_options, debtorId):
        """Try to create a new debtor. Requires special privileges

        ---
        Must fail if the debtor already exists.

        """

        try:
            debtor = procedures.create_new_debtor(debtorId)
        except procedures.DebtorExistsError:
            abort(409)
        return debtor, {'Location': endpoints.build_url('debtor', debtorId=debtorId)}


policies_api = Blueprint(
    'policies',
    __name__,
    url_prefix='/debtors',
    description="Change individual debtor's policies.",
)


@policies_api.route('/<i64:debtorId>/policy', parameters=[specs.DEBTOR_ID])
class DebtorPolicyEndpoint(MethodView):
    @policies_api.response(DebtorPolicySchema(context=CONTEXT))
    @policies_api.doc(responses={404: specs.DEBTOR_DOES_NOT_EXIST})
    def get(self, debtorId):
        """Return information about debtor's policy."""

        return procedures.get_debtor(debtorId) or abort(404)

    @policies_api.arguments(DebtorPolicyUpdateRequestSchema)
    @policies_api.response(DebtorPolicySchema(context=CONTEXT))
    @policies_api.doc(responses={404: specs.DEBTOR_DOES_NOT_EXIST,
                                 403: specs.TOO_MANY_POLICY_CHANGES,
                                 409: specs.CONFLICTING_POLICY})
    def patch(self, policy_update_request, debtorId):
        """Update debtor's policy.

        This operation is **idempotent**!

        """

        try:
            debtor = procedures.update_debtor_policy(
                debtor_id=debtorId,
                interest_rate_target=policy_update_request['interest_rate_target'],
                new_interest_rate_limits=policy_update_request['interest_rate_lower_limits'],
                new_balance_limits=policy_update_request['balance_lower_limits'],
            )
        except procedures.TooManyManagementActionsError:
            abort(403)
        except procedures.DebtorDoesNotExistError:
            abort(404)
        except procedures.ConflictingPolicyError as e:
            abort(409, message=e.message)
        return debtor


transfers_api = Blueprint(
    'transfers',
    __name__,
    url_prefix='/debtors',
    description="Make credit-issuing transfers.",
)


@transfers_api.route('/<i64:debtorId>/transfers/', parameters=[specs.DEBTOR_ID])
class TransfersCollectionEndpoint(MethodView):
    # TODO: Consider implementing pagination. This might be needed in
    #       case the executed a query turns out to be too costly.

    @transfers_api.response(TransfersCollectionSchema(context=CONTEXT))
    @transfers_api.doc(responses={404: specs.DEBTOR_DOES_NOT_EXIST})
    def get(self, debtorId):
        """Return the debtor's collection of credit-issuing transfers."""

        try:
            transfer_uuids = procedures.get_debtor_transfer_uuids(debtorId)
        except procedures.DebtorDoesNotExistError:
            abort(404)
        return TransfersCollection(debtor_id=debtorId, items=transfer_uuids)

    @transfers_api.arguments(IssuingTransferCreationRequestSchema)
    @transfers_api.response(TransferSchema(context=CONTEXT), code=201, headers=specs.LOCATION_HEADER)
    @transfers_api.doc(responses={303: specs.TRANSFER_EXISTS,
                                  403: specs.TOO_MANY_TRANSFERS,
                                  404: specs.DEBTOR_DOES_NOT_EXIST,
                                  409: specs.TRANSFER_CONFLICT})
    def post(self, transfer_creation_request, debtorId):
        """Create a new credit-issuing transfer."""

        transfer_uuid = transfer_creation_request['transfer_uuid']
        recipient_creditor_id = utils.u64_to_i64(transfer_creation_request['recipient_creditor_id'])
        location = url_for('transfers.TransferEndpoint', _external=True, debtorId=debtorId, transferUuid=transfer_uuid)
        try:
            transfer = procedures.initiate_transfer(
                debtor_id=debtorId,
                transfer_uuid=transfer_uuid,
                recipient_creditor_id=recipient_creditor_id,
                amount=transfer_creation_request['amount'],
                transfer_notes=transfer_creation_request['transfer_notes'],
            )
        except procedures.TooManyManagementActionsError:
            abort(403)
        except procedures.DebtorDoesNotExistError:
            abort(404)
        except procedures.TransfersConflictError:
            abort(409)
        except procedures.TransferExistsError:
            return redirect(location, code=303)
        return transfer, {'Location': location}


@transfers_api.route('/<i64:debtorId>/transfers/<uuid:transferUuid>', parameters=[specs.DEBTOR_ID, specs.TRANSFER_UUID])
class TransferEndpoint(MethodView):
    @transfers_api.response(TransferSchema(context=CONTEXT))
    @transfers_api.doc(responses={404: specs.TRANSFER_DOES_NOT_EXIST})
    def get(self, debtorId, transferUuid):
        """Return information about a credit-issuing transfer."""

        return procedures.get_initiated_transfer(debtorId, transferUuid) or abort(404)

    @transfers_api.arguments(TransferUpdateRequestSchema)
    @transfers_api.response(TransferSchema(context=CONTEXT))
    @transfers_api.doc(responses={404: specs.TRANSFER_DOES_NOT_EXIST,
                                  409: specs.TRANSFER_UPDATE_CONFLICT})
    def patch(self, transfer_update_request, debtorId, transferUuid):
        """Cancel a credit-issuing transfer, if possible.

        This operation is **idempotent**!

        """

        try:
            if not (transfer_update_request['is_finalized'] and not transfer_update_request['is_successful']):
                raise procedures.TransferUpdateConflictError()
            transfer = procedures.cancel_transfer(debtorId, transferUuid)
        except procedures.TransferDoesNotExistError:
            abort(404)
        except procedures.TransferUpdateConflictError:
            abort(409)
        return transfer

    @transfers_api.response(code=204)
    def delete(self, debtorId, transferUuid):
        """Delete a credit-issuing transfer.

        Note that deleting a running (not finalized) transfer does not
        cancel it. To ensure that a running transfer has not been
        successful, it must be canceled before deletion.

        """

        procedures.delete_initiated_transfer(debtorId, transferUuid)


# TODO: Implement the endpoint
#       `public-transfers/<i64:debtorId>/<i64:creditorId>/<i64:transferSeqnum>`,
#       that shows all transfers having their `TRANSFER_FLAG_IS_PUBLIC`
#       flag set. Also, implement an `on_account_commit_signal` event
#       handler, which saves all public transfers in the database.
