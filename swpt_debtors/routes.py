from datetime import datetime, timedelta
from flask import redirect, url_for, request, current_app, g
from flask.views import MethodView
from flask_smorest import Blueprint, abort
from swpt_lib.utils import u64_to_i64
from .schemas import DebtorSchema, DebtorPolicySchema, TransferSchema, \
    TransfersListSchema, IssuingTransferCreationRequestSchema, \
    TransfersList, TransferCancelationRequestSchema, DebtorReservationRequestSchema, \
    DebtorReservationSchema, DebtorsListSchema, ObjectReferencesPageSchema, \
    DebtorActivationRequestSchema, DebtorDeactivationRequestSchema
from .models import MIN_INT64
from . import specs
from . import procedures


def calc_reservation_deadline(created_at: datetime) -> datetime:
    return created_at + timedelta(days=current_app.config['APP_INACTIVE_DEBTOR_RETENTION_DAYS'])


context = {
    'Debtor': 'debtors.DebtorEndpoint',
    'DebtorPolicy': 'policies.DebtorPolicyEndpoint',
    'TransfersList': 'transfers.TransfersListEndpoint',
    'Transfer': 'transfers.TransferEndpoint',
    'calc_reservation_deadline': calc_reservation_deadline,
}


admin_api = Blueprint(
    'admin',
    __name__,
    url_prefix='/debtors',
    description="View debtors list, create new debtors.",
)


@admin_api.route('/.debtor-reserve')
class RandomDebtorReserveEndpoint(MethodView):
    @admin_api.arguments(DebtorReservationRequestSchema)
    @admin_api.response(DebtorReservationSchema(context=context))
    @admin_api.doc(operationId='reserveRandomDebtor',
                   security=specs.SCOPE_ACTIVATE,
                   responses={409: specs.CONFLICTING_DEBTOR})
    def post(self, debtor_reservation_request):
        """Reserve an auto-generated debtor ID.

        **Note:** The reserved debtor ID will be a random valid
        debtor ID.

        """

        for _ in range(100):
            debtor_id = procedures.generate_new_debtor_id()
            try:
                debtor = procedures.reserve_debtor(debtor_id, verify_correctness=False)
                break
            except procedures.DebtorExists:  # pragma: no cover
                pass
        else:  # pragma: no cover
            abort(500, message='Can not generate a valid debtor ID.')

        return debtor


@admin_api.route('/.list')
class DebtorsListEndpoint(MethodView):
    @admin_api.response(DebtorsListSchema, example=specs.DEBTORS_LIST_EXAMPLE)
    @admin_api.doc(operationId='getDebtorsList', security=specs.SCOPE_ACCESS_READONLY)
    def get(self):
        """Return a paginated list of links to all active debtors."""

        return {
            'uri': url_for('admin.DebtorsListEndpoint'),
            'items_type': 'ObjectReference',
            'first': url_for('admin.DebtorEnumerateEndpoint', debtorId=MIN_INT64),
        }


@admin_api.route('/<i64:debtorId>/enumerate', parameters=[specs.DEBTOR_ID])
class DebtorEnumerateEndpoint(MethodView):
    @admin_api.response(ObjectReferencesPageSchema(context=context), example=specs.DEBTOR_LINKS_EXAMPLE)
    @admin_api.doc(operationId='getDebtorsPage', security=specs.SCOPE_ACCESS_READONLY)
    def get(self, debtorId):
        """Return a collection of active debtors.

        The returned object will be a fragment (a page) of a paginated
        list. The paginated list contains references to all active
        debtors on the server. The returned fragment, and all the
        subsequent fragments, will be sorted by debtor ID, starting
        from the `debtorID` specified in the path. The sorting order
        is implementation-specific.

        **Note:** To obtain references to all active debtors, the
        client should start with the debtor ID that precedes all other
        IDs in the sorting order.

        """

        n = int(current_app.config['APP_DEBTORS_PER_PAGE'])
        debtor_ids, next_debtor_id = procedures.get_debtor_ids(start_from=debtorId, count=n)
        debtor_uris = [{'uri': url_for('debtors.DebtorEndpoint', debtorId=debtor_id)} for debtor_id in debtor_ids]

        if next_debtor_id is None:
            # The last page does not have a 'next' link.
            return {
                'uri': request.full_path,
                'items': debtor_uris,
            }

        return {
            'uri': request.full_path,
            'items': debtor_uris,
            'next': url_for('admin.DebtorEnumerateEndpoint', debtorId=next_debtor_id),
        }


@admin_api.route('/<i64:debtorId>/reserve', parameters=[specs.DEBTOR_ID])
class DebtorReserveEndpoint(MethodView):
    @admin_api.arguments(DebtorReservationRequestSchema)
    @admin_api.response(DebtorReservationSchema(context=context))
    @admin_api.doc(operationId='reserveDebtor',
                   security=specs.SCOPE_ACTIVATE,
                   responses={409: specs.CONFLICTING_DEBTOR})
    def post(self, debtor_reservation_request, debtorId):
        """Try to reserve a specific debtor ID.

        **Note:** The reserved debtor ID will be the same as the
        `debtorId` specified in the path.

        ---
        Will fail if the debtor already exists.

        """

        try:
            debtor = procedures.reserve_debtor(debtorId)
        except procedures.DebtorExists:
            abort(409)
        except procedures.InvalidDebtorError:  # pragma: no cover
            abort(500, message='The agent is not responsible for this debtor.')

        return debtor


@admin_api.route('/<i64:debtorId>/activate', parameters=[specs.DEBTOR_ID])
class DebtorActivateEndpoint(MethodView):
    @admin_api.arguments(DebtorActivationRequestSchema)
    @admin_api.response(DebtorSchema(context=context))
    @admin_api.doc(operationId='activateDebtor',
                   security=specs.SCOPE_ACTIVATE,
                   responses={409: specs.CONFLICTING_DEBTOR})
    def post(self, debtor_activation_request, debtorId):
        """Activate a debtor."""

        reservation_id = debtor_activation_request.get('optional_reservation_id')
        try:
            if reservation_id is None:
                reservation_id = procedures.reserve_debtor(debtorId).reservation_id
                assert reservation_id is not None
            debtor = procedures.activate_debtor(debtorId, reservation_id)
        except procedures.DebtorExists:
            abort(409)
        except procedures.InvalidReservationId:
            abort(422, errors={'json': {'reservationId': ['Invalid ID.']}})
        except procedures.InvalidDebtorError:  # pragma: no cover
            abort(500, message='The agent is not responsible for this debtor.')

        return debtor


@admin_api.route('/<i64:debtorId>/deactivate', parameters=[specs.DEBTOR_ID])
class DebtorDeactivateEndpoint(MethodView):
    @admin_api.arguments(DebtorDeactivationRequestSchema)
    @admin_api.response(code=204)
    @admin_api.doc(operationId='deactivateDebtor', security=specs.SCOPE_DEACTIVATE)
    def post(self, debtor_deactivation_request, debtorId):
        """Deactivate a debtor."""

        if not g.superuser:
            abort(403)

        procedures.deactivate_debtor(debtorId)


debtors_api = Blueprint(
    'debtors',
    __name__,
    url_prefix='/debtors',
    description="View public information about debtors.",
)


@debtors_api.route('/<i64:debtorId>/', parameters=[specs.DEBTOR_ID])
class DebtorEndpoint(MethodView):
    @debtors_api.response(DebtorSchema(context=context))
    @debtors_api.doc(operationId='getDebtor')
    def get(self, debtorId):
        """Return public information about a debtor."""

        debtor = procedures.get_active_debtor(debtorId)
        if not debtor:
            abort(403)
        return debtor, {'Cache-Control': 'max-age=86400'}


policies_api = Blueprint(
    'policies',
    __name__,
    url_prefix='/debtors',
    description="Change individual debtor's policies.",
)


@policies_api.route('/<i64:debtorId>/policy', parameters=[specs.DEBTOR_ID])
class DebtorPolicyEndpoint(MethodView):
    @policies_api.response(DebtorPolicySchema(context=context))
    @policies_api.doc(operationId='getDebtorPolicy', security=specs.SCOPE_ACCESS_READONLY)
    def get(self, debtorId):
        """Return debtor's policy."""

        return procedures.get_active_debtor(debtorId) or abort(404)

    @policies_api.arguments(DebtorPolicySchema)
    @policies_api.response(DebtorPolicySchema(context=context))
    @policies_api.doc(operationId='updateDebtorPolicy',
                      security=specs.SCOPE_ACCESS_MODIFY,
                      responses={403: specs.FORBIDDEN_OPERATION,
                                 409: specs.CONFLICTING_POLICY})
    def patch(self, policy_update_request, debtorId):
        """Update debtor's policy."""

        try:
            debtor = procedures.update_debtor_policy(
                debtor_id=debtorId,
                interest_rate_target=policy_update_request.get('interest_rate_target'),
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
class TransfersListEndpoint(MethodView):
    # TODO: Consider implementing pagination. This might be needed in
    #       case the executed a query turns out to be too costly.

    @transfers_api.response(TransfersListSchema(context=context))
    @transfers_api.doc(operationId='getTransfersList', security=specs.SCOPE_ACCESS_READONLY)
    def get(self, debtorId):
        """Return the debtor's list of credit-issuing transfers."""

        try:
            transfer_uuids = procedures.get_debtor_transfer_uuids(debtorId)
        except procedures.DebtorDoesNotExistError:
            abort(404)
        return TransfersList(debtor_id=debtorId, items=transfer_uuids)

    @transfers_api.arguments(IssuingTransferCreationRequestSchema)
    @transfers_api.response(TransferSchema(context=context), code=201, headers=specs.LOCATION_HEADER)
    @transfers_api.doc(operationId='createTransfer',
                       security=specs.SCOPE_ACCESS_MODIFY,
                       responses={303: specs.TRANSFER_EXISTS,
                                  403: specs.FORBIDDEN_OPERATION,
                                  409: specs.TRANSFER_CONFLICT})
    def post(self, transfer_creation_request, debtorId):
        """Initiate a credit-issuing transfer."""

        transfer_uuid = transfer_creation_request['transfer_uuid']
        recipient_creditor_id = u64_to_i64(transfer_creation_request['recipient_creditor_id'])
        location = url_for('transfers.TransferEndpoint', _external=True, debtorId=debtorId, transferUuid=transfer_uuid)
        try:
            transfer = procedures.initiate_transfer(
                debtor_id=debtorId,
                transfer_uuid=transfer_uuid,
                recipient_creditor_id=recipient_creditor_id,
                amount=transfer_creation_request['amount'],
                transfer_note_format=transfer_creation_request['transfer_note_format'],
                transfer_note=transfer_creation_request['transfer_note'],
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
    @transfers_api.response(TransferSchema(context=context))
    @transfers_api.doc(operationId='getTransfer', security=specs.SCOPE_ACCESS_READONLY)
    def get(self, debtorId, transferUuid):
        """Return a credit-issuing transfer."""

        return procedures.get_initiated_transfer(debtorId, transferUuid) or abort(404)

    @transfers_api.arguments(TransferCancelationRequestSchema)
    @transfers_api.response(TransferSchema(context=context))
    @transfers_api.doc(operationId='cancelTransfer',
                       security=specs.SCOPE_ACCESS_MODIFY,
                       responses={403: specs.TRANSFER_CANCELLATION_FAILURE})
    def post(self, cancel_transfer_request, debtorId, transferUuid):
        """Try to cancel a credit-issuing transfer.

        **Note:** This is an idempotent operation.

        """

        try:
            transfer = procedures.cancel_transfer(debtorId, transferUuid)
        except procedures.ForbiddenTransferCancellation:  # pragma: no cover
            abort(403)
        except procedures.TransferDoesNotExistError:
            abort(404)

        return transfer

    @transfers_api.response(code=204)
    @transfers_api.doc(operationId='deleteTransfer', security=specs.SCOPE_ACCESS_MODIFY)
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
