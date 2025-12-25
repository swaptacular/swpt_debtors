import re
from random import randint
from enum import IntEnum
from typing import Tuple, Optional
from datetime import datetime, timedelta, timezone
from flask import redirect, url_for, request, current_app, g, make_response
from flask.views import MethodView
from flask_smorest import Blueprint, abort
from swpt_pythonlib.utils import u64_to_i64
from swpt_pythonlib.swpt_uris import parse_account_uri
from swpt_debtors.schemas import (
    DebtorSchema,
    TransferSchema,
    TransfersListSchema,
    TransferCreationRequestSchema,
    TransfersList,
    TransferCancelationRequestSchema,
    DebtorReservationRequestSchema,
    DebtorReservationSchema,
    DebtorsListSchema,
    ObjectReferencesPageSchema,
    DebtorActivationRequestSchema,
    DebtorDeactivationRequestSchema,
    DebtorRestrictionRequestSchema,
    DebtorConfigSchema,
)
from swpt_debtors.models import MIN_INT64, is_valid_debtor_id
from swpt_debtors import specs
from swpt_debtors import procedures

READ_ONLY_METHODS = ["GET", "HEAD", "OPTIONS"]


class UserType(IntEnum):
    SUPERUSER = 1
    SUPERVISOR = 2
    DEBTOR = 3


class UserIdPatternMatcher:
    PATTERN_CONFIG_KEYS = {
        UserType.SUPERUSER: "APP_SUPERUSER_SUBJECT_REGEX",
        UserType.SUPERVISOR: "APP_SUPERVISOR_SUBJECT_REGEX",
        UserType.DEBTOR: "APP_DEBTOR_SUBJECT_REGEX",
    }

    def __init__(self):
        self._regex_patterns = {}

    def get_pattern(self, user_type: UserType) -> re.Pattern:
        pattern_config_key = self.PATTERN_CONFIG_KEYS[user_type]
        regex = current_app.config[pattern_config_key]
        regex_patterns = self._regex_patterns
        regex_pattern = regex_patterns.get(regex)
        if regex_pattern is None:
            regex_pattern = regex_patterns[regex] = re.compile(regex)

        return regex_pattern

    def match(self, user_id: str) -> Tuple[UserType, Optional[int]]:
        for user_type in UserType:
            pattern = self.get_pattern(user_type)
            m = pattern.match(user_id)
            if m:
                debtor_id = (
                    u64_to_i64(int(m.group(1)))
                    if user_type == UserType.DEBTOR
                    else None
                )
                return user_type, debtor_id

        abort(403)


user_id_pattern_matcher = UserIdPatternMatcher()


def parse_swpt_user_id_header() -> Tuple[UserType, Optional[int]]:
    user_id = request.headers.get("X-Swpt-User-Id")
    if user_id is None:
        user_type = UserType.SUPERUSER
        debtor_id = None
    else:
        user_type, debtor_id = user_id_pattern_matcher.match(user_id)

    g.superuser = user_type == UserType.SUPERUSER
    return user_type, debtor_id


def ensure_admin():
    user_type, _ = parse_swpt_user_id_header()
    if user_type == UserType.DEBTOR:
        abort(403)


def ensure_debtor_permissions():
    # NOTE: Debtors can access and modify only their own resources.
    # Supervisors can activate new debtors, and have read-only access
    # to all debtors's resources. Superusers are allowed everything.

    user_type, debtor_id = parse_swpt_user_id_header()
    url_debtor_id = request.view_args.get("debtorId")
    if url_debtor_id is None:
        url_debtor_id = debtor_id
    else:
        assert isinstance(url_debtor_id, int)
        if not is_valid_debtor_id(url_debtor_id):
            abort(404)

    if user_type == UserType.DEBTOR and debtor_id != url_debtor_id:
        abort(403)

    if (
        user_type == UserType.SUPERVISOR
        and request.method not in READ_ONLY_METHODS
    ):
        abort(403)

    g.debtor_id = debtor_id


def calc_reservation_deadline(created_at: datetime) -> datetime:
    return created_at + timedelta(
        days=current_app.config["APP_INACTIVE_DEBTOR_RETENTION_DAYS"]
    )


def calc_checkup_datetime(debtor_id: int, initiated_at: datetime) -> datetime:
    current_ts = datetime.now(tz=timezone.utc)
    current_delay = current_ts - initiated_at
    average_delay = timedelta(
        seconds=current_app.config["APP_TRANSFERS_FINALIZATION_APPROX_SECONDS"]
    )
    return current_ts + max(current_delay, average_delay)


context = {
    "Debtor": "debtors.DebtorEndpoint",
    "DebtorConfig": "debtors.DebtorConfigEndpoint",
    "TransfersList": "transfers.TransfersListEndpoint",
    "Transfer": "transfers.TransferEndpoint",
    "SaveDocument": "documents.SaveDocumentEndpoint",
    "RedirectToDebtorsInfo": "documents.RedirectToDebtorsInfoEndpoint",
    "calc_reservation_deadline": calc_reservation_deadline,
    "calc_checkup_datetime": calc_checkup_datetime,
}


admin_api = Blueprint(
    "admin",
    __name__,
    url_prefix="/debtors",
    description="""**View debtors list, create new debtors, deactivate inactive
    debtors, restrict debtors' maximum issued amounts.** The creation of new
    debtors can optionally be done in two-phases: First a debtors ID can be
    *reserved*, and only then, the debtor can be *activated*. This is useful
    when the client wants to know the new debtor ID in advance. If this is
    not needed, the debtor can also be activated directly, by a single request.
    """,
)
admin_api.before_request(ensure_admin)


@admin_api.route("/.debtor-reserve")
class RandomDebtorReserveEndpoint(MethodView):
    @admin_api.arguments(DebtorReservationRequestSchema)
    @admin_api.response(200, DebtorReservationSchema(context=context))
    @admin_api.doc(
        operationId="reserveRandomDebtor",
        security=specs.SCOPE_ACTIVATE,
        responses={409: specs.CONFLICTING_DEBTOR},
    )
    def post(self, debtor_reservation_request):
        """Reserve an auto-generated debtor ID.

        **Note:** The reserved debtor ID will be a random valid
        debtor ID.

        """

        min_debtor_id = current_app.config["MIN_DEBTOR_ID"]
        max_debtor_id = current_app.config["MAX_DEBTOR_ID"]
        for _ in range(100):
            debtor_id = randint(min_debtor_id, max_debtor_id)
            if not is_valid_debtor_id(debtor_id):  # pragma: no cover
                abort(
                    500,
                    message=(
                        "The /.debtor-reserve endpoint does not support"
                        " shards."
                    ),
                )
            try:
                debtor = procedures.reserve_debtor(debtor_id)
                break
            except procedures.DebtorExists:  # pragma: no cover
                pass
        else:  # pragma: no cover
            abort(500, message="Can not generate a valid debtor ID.")

        return debtor


@admin_api.route("/.list")
class DebtorsListEndpoint(MethodView):
    @admin_api.response(
        200, DebtorsListSchema, example=specs.DEBTORS_LIST_EXAMPLE
    )
    @admin_api.doc(
        operationId="getDebtorsList", security=specs.SCOPE_ACCESS_READONLY
    )
    def get(self):
        """Return a paginated list of links to all activated debtors."""

        return {
            "uri": url_for("admin.DebtorsListEndpoint"),
            "items_type": "ObjectReference",
            "first": url_for(
                "admin.DebtorEnumerateEndpoint", debtorId=MIN_INT64
            ),
        }


@admin_api.route("/<i64:debtorId>/enumerate", parameters=[specs.DEBTOR_ID])
class DebtorEnumerateEndpoint(MethodView):
    @admin_api.response(
        200,
        ObjectReferencesPageSchema(context=context),
        example=specs.DEBTOR_LINKS_EXAMPLE,
    )
    @admin_api.doc(
        operationId="getDebtorsPage", security=specs.SCOPE_ACCESS_READONLY
    )
    def get(self, debtorId):
        """Return a collection of activated debtors.

        The returned object will be a fragment (a page) of a paginated
        list. The paginated list contains references to all activated
        debtors on the server. The returned fragment, and all the
        subsequent fragments, will be sorted by debtor ID, starting
        from the `debtorID` specified in the path. The sorting order
        is implementation-specific.

        **Note:** To obtain references to all activated debtors, the
        client should start with the debtor ID that precedes all other
        IDs in the sorting order.

        """

        n = int(current_app.config["APP_DEBTORS_PER_PAGE"])
        debtor_ids, next_debtor_id = procedures.get_debtor_ids(
            start_from=debtorId, count=n
        )
        debtor_uris = [
            {"uri": url_for("debtors.DebtorEndpoint", debtorId=debtor_id)}
            for debtor_id in debtor_ids
            if is_valid_debtor_id(debtor_id)
        ]

        if next_debtor_id is None:
            # The last page does not have a 'next' link.
            return {
                "uri": request.full_path,
                "items": debtor_uris,
            }

        return {
            "uri": request.full_path,
            "items": debtor_uris,
            "next": url_for(
                "admin.DebtorEnumerateEndpoint", debtorId=next_debtor_id
            ),
        }


@admin_api.route("/<i64:debtorId>/reserve", parameters=[specs.DEBTOR_ID])
class DebtorReserveEndpoint(MethodView):
    @admin_api.arguments(DebtorReservationRequestSchema)
    @admin_api.response(200, DebtorReservationSchema(context=context))
    @admin_api.doc(
        operationId="reserveDebtor",
        security=specs.SCOPE_ACTIVATE,
        responses={409: specs.CONFLICTING_DEBTOR},
    )
    def post(self, debtor_reservation_request, debtorId):
        """Try to reserve a specific debtor ID.

        **Note:** The reserved debtor ID will be the same as the
        `debtorId` specified in the path.

        ---
        Will fail if the debtor already exists.

        """

        if not is_valid_debtor_id(debtorId):  # pragma: no cover
            abort(404)

        try:
            debtor = procedures.reserve_debtor(debtorId)
        except procedures.DebtorExists:
            abort(409)

        return debtor


@admin_api.route("/<i64:debtorId>/activate", parameters=[specs.DEBTOR_ID])
class DebtorActivateEndpoint(MethodView):
    @admin_api.arguments(DebtorActivationRequestSchema)
    @admin_api.response(200, DebtorSchema(context=context))
    @admin_api.doc(
        operationId="activateDebtor",
        security=specs.SCOPE_ACTIVATE,
        responses={409: specs.CONFLICTING_DEBTOR},
    )
    def post(self, debtor_activation_request, debtorId):
        """Activate a debtor."""

        if not is_valid_debtor_id(debtorId):  # pragma: no cover
            abort(404)

        reservation_id = debtor_activation_request.get(
            "optional_reservation_id"
        )
        try:
            if reservation_id is None:
                reservation_id = str(
                    procedures.reserve_debtor(debtorId).reservation_id
                )
                assert reservation_id is not None
            debtor = procedures.activate_debtor(debtorId, reservation_id)
        except procedures.DebtorExists:
            abort(409)
        except procedures.InvalidReservationId:
            abort(422, errors={"json": {"reservationId": ["Invalid ID."]}})

        return debtor


@admin_api.route("/<i64:debtorId>/deactivate", parameters=[specs.DEBTOR_ID])
class DebtorDeactivateEndpoint(MethodView):
    @admin_api.arguments(DebtorDeactivationRequestSchema)
    @admin_api.response(204)
    @admin_api.doc(
        operationId="deactivateDebtor", security=specs.SCOPE_DEACTIVATE
    )
    def post(self, debtor_deactivation_request, debtorId):
        """Deactivate a debtor."""

        if not is_valid_debtor_id(debtorId):  # pragma: no cover
            abort(404)

        if not g.superuser:
            abort(403)

        procedures.deactivate_debtor(debtorId)


@admin_api.route("/<i64:debtorId>/restrict", parameters=[specs.DEBTOR_ID])
class DebtorRestrictEndpoint(MethodView):
    @admin_api.arguments(DebtorRestrictionRequestSchema)
    @admin_api.response(200, DebtorSchema(context=context))
    @admin_api.doc(
        operationId="restrictDebtor", security=specs.SCOPE_RESTRICT
    )
    def post(self, debtor_restriction_request, debtorId):
        """Restricts the maximum amount that a debtor is allowed to issue."""

        if not is_valid_debtor_id(debtorId):  # pragma: no cover
            abort(404)

        if not g.superuser:
            abort(403)

        min_balance = debtor_restriction_request["min_balance"]
        try:
            debtor = procedures.restrict_debtor(debtorId, min_balance)
        except procedures.DebtorDoesNotExist:
            abort(404)

        return debtor


debtors_api = Blueprint(
    "debtors",
    __name__,
    url_prefix="/debtors",
    description="""**Obtain information about existing debtors, update debtors'
    configuration.** Each debtor's record contains references to various kinds
    of information about the debtor (like debtor's list of transfers).
    """,
)
debtors_api.before_request(ensure_debtor_permissions)


@debtors_api.route("/.debtor")
class RedirectToDebtorEndpoint(MethodView):
    @debtors_api.response(204)
    @debtors_api.doc(
        operationId="redirectToDebtor",
        security=specs.SCOPE_ACCESS_READONLY,
        responses={204: specs.DEBTOR_DOES_NOT_EXIST, 303: specs.DEBTOR_EXISTS},
    )
    def get(self):
        """Redirect to the debtor's record."""

        debtorId = g.debtor_id
        if debtorId is not None:
            location = url_for(
                "debtors.DebtorEndpoint", _external=True, debtorId=debtorId
            )
            return redirect(location, code=303)


@debtors_api.route("/<i64:debtorId>/", parameters=[specs.DEBTOR_ID])
class DebtorEndpoint(MethodView):
    @debtors_api.response(200, DebtorSchema(context=context))
    @debtors_api.doc(
        operationId="getDebtor", security=specs.SCOPE_ACCESS_READONLY
    )
    def get(self, debtorId):
        """Return debtor."""

        return procedures.get_active_debtor(debtorId) or abort(403)


@debtors_api.route("/<i64:debtorId>/config", parameters=[specs.DEBTOR_ID])
class DebtorConfigEndpoint(MethodView):
    @debtors_api.response(200, DebtorConfigSchema(context=context))
    @debtors_api.doc(
        operationId="getDebtorConfig", security=specs.SCOPE_ACCESS_READONLY
    )
    def get(self, debtorId):
        """Return debtors's configuration."""

        return procedures.get_active_debtor(debtorId) or abort(404)

    @debtors_api.arguments(DebtorConfigSchema)
    @debtors_api.response(200, DebtorConfigSchema(context=context))
    @debtors_api.doc(
        operationId="updateDebtorConfig",
        security=specs.SCOPE_ACCESS_MODIFY,
        responses={403: specs.FORBIDDEN_OPERATION, 409: specs.UPDATE_CONFLICT},
    )
    def patch(self, debtor_config, debtorId):
        """Update debtor's configuration."""

        try:
            config = procedures.update_debtor_config(
                debtor_id=debtorId,
                config_data=debtor_config["config_data"],
                latest_update_id=debtor_config["latest_update_id"],
                max_actions_per_month=current_app.config[
                    "APP_MAX_TRANSFERS_PER_MONTH"
                ],
            )
        except procedures.TooManyManagementActions:
            abort(403)
        except procedures.DebtorDoesNotExist:
            abort(404)
        except procedures.UpdateConflict:
            abort(
                409, errors={"json": {"latestUpdateId": ["Incorrect value."]}}
            )

        return config


transfers_api = Blueprint(
    "transfers",
    __name__,
    url_prefix="/debtors",
    description="""**Make credit-issuing transfers.** A new transfer record
    will be created for every initiated credit-issuing transfer. The client
    itself is responsible for the deletion of each transfer record,
    once the client does not need it anymore. Sometime after the
    transfer has been initiated, it will be automatically finalized as
    either successful or unsuccessful. Note that the client may try to
    cancel an erroneously initiated transfer, but there are no
    guarantees for success.
    """,
)
transfers_api.before_request(ensure_debtor_permissions)


@transfers_api.route(
    "/<i64:debtorId>/transfers/", parameters=[specs.DEBTOR_ID]
)
class TransfersListEndpoint(MethodView):
    @transfers_api.response(200, TransfersListSchema(context=context))
    @transfers_api.doc(
        operationId="getTransfersList", security=specs.SCOPE_ACCESS_READONLY
    )
    def get(self, debtorId):
        """Return the debtor's list of initiated transfers."""

        try:
            transfer_uuids = procedures.get_debtor_transfer_uuids(debtorId)
        except procedures.DebtorDoesNotExist:
            abort(404)

        return TransfersList(debtor_id=debtorId, items=transfer_uuids)

    @transfers_api.arguments(TransferCreationRequestSchema)
    @transfers_api.response(
        201, TransferSchema(context=context), headers=specs.LOCATION_HEADER
    )
    @transfers_api.doc(
        operationId="createTransfer",
        security=specs.SCOPE_ACCESS_MODIFY,
        responses={
            303: specs.TRANSFER_EXISTS,
            403: specs.FORBIDDEN_OPERATION,
            409: specs.TRANSFER_CONFLICT,
        },
    )
    def post(self, transfer_creation_request, debtorId):
        """Initiate a credit-issuing transfer."""

        # Verify the recipient.
        recipient_uri = transfer_creation_request["recipient_identity"]["uri"]
        try:
            recipient_debtor_id, recipient = parse_account_uri(recipient_uri)
        except ValueError:
            abort(
                422,
                errors={
                    "json": {
                        "recipient": {
                            "uri": ["The URI can not be recognized."]
                        }
                    }
                },
            )
        if recipient_debtor_id != debtorId:
            abort(
                422,
                errors={
                    "json": {
                        "recipient": {"uri": ["Invalid recipient account."]}
                    }
                },
            )

        uuid = transfer_creation_request["transfer_uuid"]
        location = url_for(
            "transfers.TransferEndpoint",
            _external=True,
            debtorId=debtorId,
            transferUuid=uuid,
        )
        try:
            transfer = procedures.initiate_running_transfer(
                debtor_id=debtorId,
                transfer_uuid=uuid,
                amount=transfer_creation_request["amount"],
                recipient_uri=recipient_uri,
                recipient=recipient,
                transfer_note_format=transfer_creation_request[
                    "transfer_note_format"
                ],
                transfer_note=transfer_creation_request["transfer_note"],
                max_actions_per_month=current_app.config[
                    "APP_MAX_TRANSFERS_PER_MONTH"
                ],
            )
        except (
            procedures.TooManyManagementActions,
            procedures.TooManyRunningTransfers,
        ):
            abort(403)
        except procedures.DebtorDoesNotExist:
            abort(404)
        except procedures.TransfersConflict:
            abort(409)
        except procedures.TransferExists:
            return redirect(location, code=303)

        return transfer, {"Location": location}


@transfers_api.route(
    "/<i64:debtorId>/transfers/<uuid:transferUuid>",
    parameters=[specs.DEBTOR_ID, specs.TRANSFER_UUID],
)
class TransferEndpoint(MethodView):
    @transfers_api.response(200, TransferSchema(context=context))
    @transfers_api.doc(
        operationId="getTransfer", security=specs.SCOPE_ACCESS_READONLY
    )
    def get(self, debtorId, transferUuid):
        """Return a transfer."""

        return procedures.get_running_transfer(
            debtorId, transferUuid
        ) or abort(404)

    @transfers_api.arguments(TransferCancelationRequestSchema)
    @transfers_api.response(200, TransferSchema(context=context))
    @transfers_api.doc(
        operationId="cancelTransfer",
        security=specs.SCOPE_ACCESS_MODIFY,
        responses={403: specs.TRANSFER_CANCELLATION_FAILURE},
    )
    def post(self, cancel_transfer_request, debtorId, transferUuid):
        """Try to cancel a transfer.

        **Note:** This is an idempotent operation.

        """

        try:
            transfer = procedures.cancel_running_transfer(
                debtorId, transferUuid
            )
        except procedures.ForbiddenTransferCancellation:  # pragma: no cover
            abort(403)
        except procedures.TransferDoesNotExist:
            abort(404)

        return transfer

    @transfers_api.response(204)
    @transfers_api.doc(
        operationId="deleteTransfer", security=specs.SCOPE_ACCESS_MODIFY
    )
    def delete(self, debtorId, transferUuid):
        """Delete a transfer.

        Before deleting a transfer, client implementations should
        ensure that at least 5 days (120 hours) have passed since the
        transfer was initiated (see the `initiatedAt` field). Also, it
        is recommended successful transfers to stay on the server at
        least a few weeks after their finalization.

        Note that deleting a running (not finalized) transfer does not
        cancel it. To ensure that a running transfer has not been
        successful, it must be canceled before deletion.

        """

        try:
            procedures.delete_running_transfer(debtorId, transferUuid)
        except procedures.TransferDoesNotExist:
            pass


documents_api = Blueprint(
    "documents",
    __name__,
    url_prefix="/debtors",
    description="""**Maintains an ever-growing set of public documents.**
    Usually, the debtor's configuration includes a link to a document that
    describes the debtor's currency (a public info document).
    """,
)


@documents_api.route("/<i64:debtorId>/public", parameters=[specs.DEBTOR_ID])
class RedirectToDebtorsInfoEndpoint(MethodView):
    @documents_api.response(302)
    @documents_api.doc(
        operationId="redirectToDebtorsInfo",
        responses={302: specs.DEBTOR_INFO_EXISTS},
    )
    def get(self, debtorId):
        """Redirect to the debtor's public info document.

        The user will be redirected to the info URL specified in the
        debtor's configuration. If no URL is specified in the
        configuration, a `404` error code will be returned.

        """

        if not is_valid_debtor_id(debtorId):  # pragma: no cover
            abort(404)

        debtor = (
            procedures.get_active_debtor(debtorId, defer_toasted=True)
            or abort(410)
        )
        location = debtor.debtor_info_iri or abort(404)
        response = redirect(location, code=302)
        response.headers["Cache-Control"] = "max-age=86400"

        return response


@documents_api.route(
    "/<i64:debtorId>/documents/", parameters=[specs.DEBTOR_ID]
)
class SaveDocumentEndpoint(MethodView):
    @documents_api.response(201, headers=specs.LOCATION_HEADER)
    @documents_api.doc(
        operationId="saveDocument",
        security=specs.SCOPE_ACCESS_MODIFY,
        requestBody=specs.DOCUMENT_CONTENT,
        responses={
            201: specs.DOCUMENT_CONTENT,
            403: specs.FORBIDDEN_OPERATION,
            413: specs.DOCUMENT_IS_TOO_BIG,
        },
    )
    def post(self, debtorId):
        """Save a document.

        The body of the request should contain the document to be
        saved. The document can be of any type, as long as the type is
        correctly specified by the `Content-Type` header in the
        request.

        """

        ensure_debtor_permissions()

        if (
            request.content_length
            > current_app.config["APP_DOCUMENT_MAX_CONTENT_LENGTH"]
        ):
            abort(413)

        content_type = request.content_type or "text/html; charset=utf-8"
        content = request.get_data() or b""
        try:
            document = procedures.save_document(
                debtor_id=debtorId,
                content_type=content_type,
                content=content,
                max_saves_per_year=current_app.config[
                    "APP_DOCUMENT_MAX_SAVES_PER_YEAR"
                ],
            )
        except procedures.TooManySavedDocuments:
            abort(403)
        except procedures.DebtorDoesNotExist:
            abort(404)

        location = url_for(
            "documents.DocumentEndpoint",
            _external=True,
            debtorId=debtorId,
            documentId=document.document_id,
        )

        return make_response(
            content, 201, {"Content-Type": content_type, "Location": location}
        )


@documents_api.route(
    "/<i64:debtorId>/documents/<i64:documentId>/public",
    parameters=[specs.DEBTOR_ID, specs.DOC_ID],
)
class DocumentEndpoint(MethodView):
    @documents_api.response(200)
    @documents_api.doc(
        operationId="getDocument", responses={200: specs.DOCUMENT_CONTENT}
    )
    def get(self, debtorId, documentId):
        """Return a saved document.

        The returned document can be of any type. The document's type
        will be specified by the `Content-Type` header in the
        response.

        """

        if not is_valid_debtor_id(debtorId):  # pragma: no cover
            abort(404)

        document = procedures.get_document(debtorId, documentId) or abort(404)
        headers = {
            "Content-Type": document.content_type,
            "Cache-Control": "max-age=31536000",
        }

        return make_response(document.content, headers)


health_api = Blueprint(
    "health",
    __name__,
    url_prefix="/debtors/health",
    description="""**Check health.** These are public endpoints
    for checking server's health status.
    """,
)


@health_api.route("/check/public")
class HealthCheckEndpoint(MethodView):
    @health_api.response(200)
    @health_api.doc(operationId="checkHealth")
    def get(self):
        """Return HTTP status code 200 if the server is healthy.

        On success, the content type of the returned document will be
        `text/plain`.

        """

        message = "I am healthy."
        headers = {
            "Content-Type": "text/plain",
        }

        return make_response(message, headers)
