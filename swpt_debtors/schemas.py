from copy import copy
from marshmallow import (
    Schema,
    fields,
    validate,
    pre_dump,
    post_dump,
    validates,
    missing,
    ValidationError,
    EXCLUDE,
)
from flask import url_for
from swpt_pythonlib.utils import i64_to_u64
from swpt_pythonlib.swpt_uris import make_account_uri
from swpt_debtors.models import (
    MIN_INT64,
    MAX_INT64,
    TRANSFER_NOTE_MAX_BYTES,
    SC_INSUFFICIENT_AVAILABLE_AMOUNT,
    CONFIG_DATA_MAX_BYTES,
    Debtor,
    RunningTransfer,
)

TYPE_DESCRIPTION = (
    "The type of this object. Will always be present in the responses from the"
    " server."
)

URI_DESCRIPTION = "The URI of this object. Can be a relative URI."

PAGE_NEXT_DESCRIPTION = (
    "An URI of another `{type}` object which contains more items. When there"
    " are no remaining items, this field will not be present. If this field is"
    " present, there might be remaining items, even when the `items` array is"
    " empty. This can be a relative URI."
)

ESTABLISHED_LIMITS_NOTE = (
    "**Note:** Established limits can not be removed. They will continue to be"
    " enforced until the specified expiration date. Therefore, when the policy"
    " is being updated, this field should contain only the additional limits"
    " that have to be added to the existing ones."
)

TRANSFER_NOTE_FORMAT_REGEX = r"^[0-9A-Za-z.-]{0,8}$"

TRANSFER_NOTE_FORMAT_DESCRIPTION = (
    "The format used for the `note` field. An empty string signifies"
    " unstructured text."
)


class ValidateTypeMixin:
    @validates("type")
    def validate_type(self, value):
        if f"{value}Schema" != type(self).__name__:
            raise ValidationError("Invalid type.")


class TransfersList:
    def __init__(self, debtor_id, items):
        self.debtor_id = debtor_id
        self.items = items


class MutableResourceSchema(Schema):
    latest_update_id = fields.Integer(
        required=True,
        validate=validate.Range(min=1, max=MAX_INT64),
        data_key="latestUpdateId",
        metadata=dict(
            format="int64",
            description=(
                "The sequential number of the latest update in the object."
                " This will always be a positive number, which starts from `1`"
                " and gets incremented with each change in the"
                " object.\n\n**Note:** When the object is changed by the"
                " client, the value of this field must be incremented by the"
                " client. The server will use the value of the field to detect"
                " conflicts which can occur when two clients try to update the"
                " object simultaneously."
            ),
            example=123,
        ),
    )
    latest_update_ts = fields.DateTime(
        required=True,
        dump_only=True,
        data_key="latestUpdateAt",
        metadata=dict(
            description="The moment of the latest update on this object.",
        ),
    )


class ObjectReferenceSchema(Schema):
    uri = fields.String(
        required=True,
        metadata=dict(
            format="uri-reference",
            description="The URI of the object. Can be a relative URI.",
            example="https://example.com/objects/1",
        ),
    )

    @post_dump
    def assert_required_fields(self, obj, many):
        assert "uri" in obj
        return obj


class ObjectReferencesPageSchema(Schema):
    uri = fields.String(
        required=True,
        dump_only=True,
        metadata=dict(
            format="uri-reference",
            description=URI_DESCRIPTION,
            example="/debtors/2/enumerate",
        ),
    )
    type = fields.Function(
        lambda obj: "ObjectReferencesPage",
        required=True,
        metadata=dict(
            type="string",
            description=TYPE_DESCRIPTION,
            example="ObjectReferencesPage",
        ),
    )
    items = fields.Nested(
        ObjectReferenceSchema(many=True),
        required=True,
        dump_only=True,
        metadata=dict(
            description="An array of `ObjectReference`s. Can be empty.",
            example=[{"uri": f"{i}/"} for i in [1, 11, 111]],
        ),
    )
    next = fields.String(
        dump_only=True,
        metadata=dict(
            format="uri-reference",
            description=PAGE_NEXT_DESCRIPTION.format(
                type="ObjectReferencesPage"
            ),
            example="?prev=111",
        ),
    )

    @post_dump
    def assert_required_fields(self, obj, many):
        assert "uri" in obj
        assert "items" in obj
        return obj


class DebtorIdentitySchema(ValidateTypeMixin, Schema):
    type = fields.String(
        load_default="DebtorIdentity",
        dump_default="DebtorIdentity",
        metadata=dict(
            description=TYPE_DESCRIPTION,
            example="DebtorIdentity",
        ),
    )
    uri = fields.String(
        required=True,
        validate=validate.Length(max=100),
        metadata=dict(
            format="uri",
            description=(
                "The information contained in this field must be enough to"
                " uniquely and reliably identify the debtor. Note that a"
                " network request *should not be needed* to identify the"
                " debtor."
            ),
            example="swpt:1",
        ),
    )

    @post_dump
    def assert_required_fields(self, obj, many):
        assert "uri" in obj
        return obj


class AccountIdentitySchema(ValidateTypeMixin, Schema):
    type = fields.String(
        load_default="AccountIdentity",
        dump_default="AccountIdentity",
        metadata=dict(
            description=TYPE_DESCRIPTION,
            example="AccountIdentity",
        ),
    )
    uri = fields.String(
        required=True,
        validate=validate.Length(max=200),
        metadata=dict(
            format="uri",
            description=(
                "The information contained in this field must be enough to: 1)"
                " uniquely and reliably identify the debtor, 2) uniquely and"
                " reliably identify the creditor's account with the debtor."
                " Note that a network request *should not be needed* to"
                " identify the account.\n\nFor example, if the debtor happens"
                " to be a bank, the URI would reveal the type of the debtor (a"
                " bank), the ID of the bank, and the bank account number."
            ),
        ),
    )

    @post_dump
    def assert_required_fields(self, obj, many):
        assert "uri" in obj
        return obj


class DebtorsListSchema(Schema):
    uri = fields.String(
        required=True,
        dump_only=True,
        metadata=dict(
            format="uri-reference",
            description=URI_DESCRIPTION,
            example="/debtors/.list",
        ),
    )
    type = fields.Function(
        lambda obj: "DebtorsList",
        required=True,
        metadata=dict(
            type="string",
            description=TYPE_DESCRIPTION,
            example="DebtorsList",
        ),
    )
    items_type = fields.String(
        required=True,
        dump_only=True,
        data_key="itemsType",
        metadata=dict(
            description="The type of the items in the paginated list.",
            example="string",
        ),
    )
    first = fields.String(
        required=True,
        dump_only=True,
        metadata=dict(
            format="uri-reference",
            description=(
                "The URI of the first page in the paginated list. This can be"
                " a relative URI. The object retrieved from this URI will"
                " have: 1) An `items` field (an array), which will contain the"
                " first items of the paginated list; 2) May have a `next`"
                " field (a string), which would contain the URI of the next"
                " page in the list."
            ),
            example="/list?page=1",
        ),
    )

    @post_dump
    def assert_required_fields(self, obj, many):
        assert "uri" in obj
        assert "itemsType" in obj
        assert "first" in obj
        return obj


class DebtorReservationRequestSchema(ValidateTypeMixin, Schema):
    type = fields.String(
        load_default="DebtorReservationRequest",
        load_only=True,
        metadata=dict(
            description=TYPE_DESCRIPTION,
            example="DebtorReservationRequest",
        ),
    )


class DebtorReservationSchema(ValidateTypeMixin, Schema):
    type = fields.Function(
        lambda obj: "DebtorReservation",
        required=True,
        metadata=dict(
            type="string",
            description=TYPE_DESCRIPTION,
            example="DebtorReservation",
        ),
    )
    created_at = fields.DateTime(
        required=True,
        dump_only=True,
        data_key="createdAt",
        metadata=dict(
            description="The moment at which the reservation was created.",
        ),
    )
    reservation_id = fields.Function(
        lambda obj: str(obj.reservation_id or 0),
        required=True,
        data_key="reservationId",
        validate=validate.Length(max=100),
        metadata=dict(
            type="string",
            description=(
                "An opaque string that will be required in order to"
                " successfully activate the debtor."
            ),
            example="12345",
        ),
    )
    debtor_id = fields.Function(
        lambda obj: str(i64_to_u64(obj.debtor_id)),
        required=True,
        data_key="debtorId",
        metadata=dict(
            type="string",
            pattern="^[0-9A-Za-z_=-]{1,64}$",
            description="The reserved debtor ID.",
            example="1",
        ),
    )
    valid_until = fields.Method(
        "get_valid_until_string",
        required=True,
        data_key="validUntil",
        metadata=dict(
            type="string",
            format="date-time",
            description="The moment at which the reservation will expire.",
        ),
    )

    def get_valid_until_string(self, obj) -> str:
        calc_reservation_deadline = self.context["calc_reservation_deadline"]
        return calc_reservation_deadline(obj.created_at).isoformat()


class DebtorActivationRequestSchema(ValidateTypeMixin, Schema):
    type = fields.String(
        load_default="DebtorActivationRequest",
        load_only=True,
        metadata=dict(
            description=TYPE_DESCRIPTION,
            example="DebtorActivationRequest",
        ),
    )
    optional_reservation_id = fields.String(
        load_only=True,
        data_key="reservationId",
        metadata=dict(
            description=(
                "When this field is present, the server will try to activate"
                " an existing reservation with matching `debtorID` and"
                " `reservationID`.\n\nWhen this field is not present, the"
                " server will try to reserve the debtor ID specified in the"
                " path, and activate it at once."
            ),
            example="12345",
        ),
    )


class DebtorDeactivationRequestSchema(ValidateTypeMixin, Schema):
    type = fields.String(
        load_default="DebtorDeactivationRequest",
        load_only=True,
        metadata=dict(
            description=TYPE_DESCRIPTION,
            example="DebtorDeactivationRequest",
        ),
    )


class DebtorRestrictionRequestSchema(ValidateTypeMixin, Schema):
    type = fields.String(
        load_default="DebtorRestrictionRequest",
        load_only=True,
        metadata=dict(
            description=TYPE_DESCRIPTION,
            example="DebtorRestrictionRequest",
        ),
    )
    min_balance = fields.Integer(
        required=True,
        validate=validate.Range(min=MIN_INT64, max=0),
        data_key="minBalance",
        metadata=dict(
            format="int64",
            description=(
                "The maximum amount that the debtor is allowed to issue, with"
                " a negative sign. Must be a negative number or zero."
            ),
            example=-500000,
        ),
    )


class DebtorConfigSchema(ValidateTypeMixin, MutableResourceSchema):
    uri = fields.String(
        required=True,
        dump_only=True,
        metadata=dict(
            format="uri-reference",
            description=URI_DESCRIPTION,
            example="/debtors/1/config",
        ),
    )
    type = fields.String(
        load_default="DebtorConfig",
        dump_default="DebtorConfig",
        metadata=dict(
            description=TYPE_DESCRIPTION,
            example="DebtorConfig",
        ),
    )
    debtor = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        metadata=dict(
            description="The URI of the corresponding `Debtor`.",
            example={"uri": "/debtors/1/"},
        ),
    )
    config_data = fields.String(
        required=True,
        validate=validate.Length(max=CONFIG_DATA_MAX_BYTES),
        data_key="configData",
        metadata=dict(
            description=(
                "The debtor's configuration data. Different implementations"
                " may use different formats for this field."
            ),
            example="",
        ),
    )

    @validates("config_data")
    def validate_config_data(self, value):
        if len(value.encode("utf8")) > CONFIG_DATA_MAX_BYTES:
            raise ValidationError(
                "The total byte-length of the config exceeds"
                f" {CONFIG_DATA_MAX_BYTES} bytes."
            )

    @pre_dump
    def process_debtor_instance(self, obj, many):
        assert isinstance(obj, Debtor)
        obj = copy(obj)
        obj.uri = url_for(
            self.context["DebtorConfig"],
            _external=False,
            debtorId=obj.debtor_id,
        )
        obj.debtor = {
            "uri": url_for(
                self.context["Debtor"], _external=False, debtorId=obj.debtor_id
            )
        }
        obj.latest_update_id = obj.config_latest_update_id
        obj.latest_update_ts = obj.last_config_ts

        return obj


class DebtorSchema(ValidateTypeMixin, Schema):
    uri = fields.String(
        required=True,
        dump_only=True,
        metadata=dict(
            format="uri-reference",
            description=URI_DESCRIPTION,
            example="/debtors/1/",
        ),
    )
    type = fields.Function(
        lambda obj: "Debtor",
        required=True,
        metadata=dict(
            type="string",
            description=TYPE_DESCRIPTION,
            example="Debtor",
        ),
    )
    identity = fields.Nested(
        DebtorIdentitySchema,
        required=True,
        dump_only=True,
        data_key="identity",
        metadata=dict(
            description="The debtor's `DebtorIdentity`.",
            example={"type": "DebtorIdentity", "uri": "swpt:1"},
        ),
    )
    config = fields.Nested(
        DebtorConfigSchema,
        required=True,
        dump_only=True,
        data_key="config",
        metadata=dict(
            description="Debtor's `DebtorConfig` settings.",
        ),
    )
    transfers_list = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        data_key="transfersList",
        metadata=dict(
            description=(
                "The URI of the debtor's list of pending credit-issuing"
                " transfers (`TransfersList`)."
            ),
            example={"uri": "/debtors/1/transfers/"},
        ),
    )
    create_transfer = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        data_key="createTransfer",
        metadata=dict(
            description=(
                "A URI to which the debtor can POST `TransferCreationRequest`s"
                " to create new credit-issuing transfers."
            ),
            example={"uri": "/debtors/1/transfers/"},
        ),
    )
    save_document = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        data_key="saveDocument",
        metadata=dict(
            description=(
                "A URI to which the debtor can POST documents to be saved."
            ),
            example={"uri": "/debtors/1/documents/"},
        ),
    )
    public_info_document = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        data_key="publicInfoDocument",
        metadata=dict(
            description=(
                "A URI that redirects to the debtor's public info document."
            ),
            example={"uri": "/debtors/1/public"},
        ),
    )
    created_at = fields.DateTime(
        required=True,
        dump_only=True,
        data_key="createdAt",
        metadata=dict(
            description="The moment at which the debtor was created.",
        ),
    )
    balance = fields.Int(
        required=True,
        dump_only=True,
        metadata=dict(
            format="int64",
            description=(
                "The total issued amount with a negative sign. Normally, it"
                " will be a negative number or a zero. A positive value,"
                " although theoretically possible, should be very rare."
            ),
            example=-1000000,
        ),
    )
    min_balance = fields.Int(
        required=True,
        dump_only=True,
        data_key="minBalance",
        metadata=dict(
            format="int64",
            description=(
                "The maximum amount that the debtor is allowed to issue, with"
                " a negative sign. This will be a negative number or zero."
            ),
            example=-9223372036854775808,
        ),
    )
    transfer_note_max_bytes = fields.Integer(
        required=True,
        dump_only=True,
        data_key="noteMaxBytes",
        metadata=dict(
            format="int32",
            description=(
                "The maximal number of bytes that transfer notes are allowed"
                " to contain when UTF-8 encoded. This will be a non-negative"
                " number."
            ),
            example=500,
        ),
    )
    optional_config_error = fields.String(
        dump_only=True,
        data_key="configError",
        metadata=dict(
            description=(
                "When this field is present, this means that for some reason,"
                " the current `DebtorConfig` settings can not be applied, or"
                " are not effectual anymore. Usually this means that there has"
                " been a network communication problem, or a system"
                " configuration problem. The value alludes to the cause of the"
                " problem."
            ),
            example="CONFIGURATION_IS_NOT_EFFECTUAL",
        ),
    )
    optional_account = fields.Nested(
        AccountIdentitySchema,
        dump_only=True,
        data_key="account",
        metadata=dict(
            description=(
                "The `AccountIdentity` of the debtor's account. It uniquely"
                " and reliably identifies the debtor's account when it"
                " participates in transfers as sender or recipient. When this"
                " field is not present, this means that the debtor's account"
                " does not have an identity yet, and can not participate in"
                " transfers."
            ),
            example={"type": "AccountIdentity", "uri": "swpt:1/0"},
        ),
    )

    @pre_dump
    def process_debtor_instance(self, obj, many):
        assert isinstance(obj, Debtor)
        obj = copy(obj)
        obj.uri = url_for(
            self.context["Debtor"], _external=False, debtorId=obj.debtor_id
        )
        obj.identity = {"uri": f"swpt:{i64_to_u64(obj.debtor_id)}"}
        obj.config = obj
        obj.transfers_list = {
            "uri": url_for(
                self.context["TransfersList"],
                _external=False,
                debtorId=obj.debtor_id,
            )
        }
        obj.create_transfer = obj.transfers_list
        obj.save_document = {
            "uri": url_for(
                self.context["SaveDocument"],
                _external=False,
                debtorId=obj.debtor_id,
            )
        }
        obj.public_info_document = {
            "uri": url_for(
                self.context["RedirectToDebtorsInfo"],
                _external=False,
                debtorId=obj.debtor_id,
            )
        }

        if obj.config_error is not None:
            obj.optional_config_error = obj.config_error

        try:
            obj.optional_account = {
                "uri": make_account_uri(obj.debtor_id, obj.account_id)
            }
        except ValueError:
            pass

        return obj


class TransferErrorSchema(Schema):
    type = fields.Function(
        lambda obj: "TransferError",
        required=True,
        metadata=dict(
            type="string",
            description=TYPE_DESCRIPTION,
            example="TransferError",
        ),
    )
    error_code = fields.String(
        required=True,
        dump_only=True,
        data_key="errorCode",
        metadata=dict(
            description=(
                'The error code.\n\n* `"CANCELED_BY_THE_SENDER"` signifies'
                " that the transfer has been   canceled by the sender.\n*"
                ' `"SENDER_IS_UNREACHABLE"` signifies that the sender\'s'
                ' account does not exist, or can not make outgoing'
                ' transfers.\n* `"RECIPIENT_IS_UNREACHABLE"`'
                " signifies that the recipient's  account does not exist, or"
                " does not accept incoming transfers.\n*"
                ' `"TRANSFER_NOTE_IS_TOO_LONG"` signifies that the transfer'
                " has been   rejected because the byte-length of the transfer"
                " note is too big.\n*"
                ' `"INSUFFICIENT_AVAILABLE_AMOUNT"` signifies that the'
                " transfer   has been rejected due to insufficient amount"
                " available on the   sender\'s account.\n*"
                ' `"TIMEOUT"` signifies that the transfer has been terminated'
                " due to expired deadline.\n*"
                ' `"NEWER_INTEREST_RATE"` signifies that the transfer has'
                " been terminated because the current interest rate on the"
                " account is more recent than the specified final interest"
                " rate timestamp.\n"
            ),
            example="INSUFFICIENT_AVAILABLE_AMOUNT",
        ),
    )
    total_locked_amount = fields.Method(
        "get_total_locked_amount",
        data_key="totalLockedAmount",
        metadata=dict(
            type="integer",
            format="int64",
            description=(
                "This field will be present only when the transfer has been"
                " rejected due to insufficient available amount. In this case,"
                " it will contain the total sum secured (locked) for transfers"
                " on the account, *after* this transfer has been finalized."
            ),
            example=0,
        ),
    )

    @post_dump
    def assert_required_fields(self, obj, many):
        assert "errorCode" in obj
        return obj

    def get_total_locked_amount(self, obj):
        if obj["error_code"] != SC_INSUFFICIENT_AVAILABLE_AMOUNT:
            return missing
        return obj.get("total_locked_amount") or 0


class TransferResultSchema(Schema):
    type = fields.Function(
        lambda obj: "TransferResult",
        required=True,
        metadata=dict(
            type="string",
            description=TYPE_DESCRIPTION,
            example="TransferResult",
        ),
    )
    finalized_at = fields.DateTime(
        required=True,
        dump_only=True,
        data_key="finalizedAt",
        metadata=dict(
            description="The moment at which the transfer was finalized.",
        ),
    )
    committed_amount = fields.Integer(
        required=True,
        dump_only=True,
        data_key="committedAmount",
        metadata=dict(
            format="int64",
            description=(
                "The transferred amount. If the transfer has been successful,"
                " the value will be equal to the requested transfer amount"
                " (always a positive number). If the transfer has been"
                " unsuccessful, the value will be zero."
            ),
            example=0,
        ),
    )
    error = fields.Nested(
        TransferErrorSchema,
        dump_only=True,
        metadata=dict(
            description=(
                "An error that has occurred during the execution of the"
                " transfer. This field will be present if, and only if, the"
                " transfer has been unsuccessful."
            ),
        ),
    )

    @post_dump
    def assert_required_fields(self, obj, many):
        assert "finalizedAt" in obj
        assert "committedAmount" in obj
        return obj


class TransferCreationRequestSchema(ValidateTypeMixin, Schema):
    type = fields.String(
        load_default="TransferCreationRequest",
        dump_default="TransferCreationRequest",
        metadata=dict(
            description=TYPE_DESCRIPTION,
            example="TransferCreationRequest",
        ),
    )
    transfer_uuid = fields.UUID(
        required=True,
        data_key="transferUuid",
        metadata=dict(
            description="A client-generated UUID for the transfer.",
            example="123e4567-e89b-12d3-a456-426655440000",
        ),
    )
    recipient_identity = fields.Nested(
        AccountIdentitySchema,
        required=True,
        data_key="recipient",
        metadata=dict(
            description="The recipient's `AccountIdentity` information.",
            example={"type": "AccountIdentity", "uri": "swpt:1/2222"},
        ),
    )
    amount = fields.Integer(
        required=True,
        validate=validate.Range(min=0, max=MAX_INT64),
        metadata=dict(
            format="int64",
            description=(
                "The amount that has to be transferred. Must be a non-negative"
                " number. Setting this value to zero can be useful when the"
                " debtor wants to verify whether the recipient's account"
                " exists and accepts incoming transfers."
            ),
            example=1000,
        ),
    )
    transfer_note_format = fields.String(
        load_default="",
        validate=validate.Regexp(TRANSFER_NOTE_FORMAT_REGEX),
        data_key="noteFormat",
        metadata=dict(
            description=TRANSFER_NOTE_FORMAT_DESCRIPTION,
            example="",
        ),
    )
    transfer_note = fields.String(
        load_default="",
        validate=validate.Length(max=TRANSFER_NOTE_MAX_BYTES),
        data_key="note",
        metadata=dict(
            description=(
                "A note from the debtor. Can be any string that the debtor"
                " wants the recipient to see."
            ),
            example="Hello, World!",
        ),
    )

    @validates("transfer_note")
    def validate_transfer_note(self, value):
        if len(value.encode("utf8")) > TRANSFER_NOTE_MAX_BYTES:
            raise ValidationError(
                "The total byte-length of the note exceeds"
                f" {TRANSFER_NOTE_MAX_BYTES} bytes."
            )


class TransferSchema(TransferCreationRequestSchema):
    uri = fields.String(
        required=True,
        dump_only=True,
        metadata=dict(
            format="uri-reference",
            description=URI_DESCRIPTION,
            example=(
                "/debtors/1/transfers/123e4567-e89b-12d3-a456-426655440000"
            ),
        ),
    )
    type = fields.Function(
        lambda obj: "Transfer",
        required=True,
        metadata=dict(
            type="string",
            description=TYPE_DESCRIPTION,
            example="Transfer",
        ),
    )
    transfers_list = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        data_key="transfersList",
        metadata=dict(
            description="The URI of creditor's `TransfersList`.",
            example={"uri": "/debtors/1/transfers/"},
        ),
    )
    transfer_note_format = fields.String(
        required=True,
        dump_only=True,
        data_key="noteFormat",
        metadata=dict(
            pattern=TRANSFER_NOTE_FORMAT_REGEX,
            description=TRANSFER_NOTE_FORMAT_DESCRIPTION,
            example="",
        ),
    )
    transfer_note = fields.String(
        required=True,
        dump_only=True,
        data_key="note",
        metadata=dict(
            description=(
                "A note from the debtor. Can be any string that the debtor"
                " wants the recipient to see."
            ),
            example="Hello, World!",
        ),
    )
    initiated_at = fields.DateTime(
        required=True,
        dump_only=True,
        data_key="initiatedAt",
        metadata=dict(
            description="The moment at which the transfer was initiated.",
        ),
    )
    checkup_at = fields.Method(
        "get_checkup_at_string",
        data_key="checkupAt",
        metadata=dict(
            type="string",
            format="date-time",
            description=(
                "The moment at which the debtor is advised to look at the"
                " transfer again, to see if it's status has changed. If this"
                " field is not present, this means either that the status of"
                " the transfer is not expected to change, or that the moment"
                " of the expected change can not be predicted."
            ),
        ),
    )
    result = fields.Nested(
        TransferResultSchema,
        dump_only=True,
        metadata=dict(
            description=(
                "Contains information about the outcome of the transfer. This"
                " field will be preset if, and only if, the transfer has been"
                " finalized. Note that a finalized transfer can be either"
                " successful, or unsuccessful."
            ),
        ),
    )

    @pre_dump
    def process_initiated_transfer_instance(self, obj, many):
        assert isinstance(obj, RunningTransfer)
        obj = copy(obj)
        obj.uri = url_for(
            self.context["Transfer"],
            _external=False,
            debtorId=obj.debtor_id,
            transferUuid=obj.transfer_uuid,
        )
        obj.transfers_list = {
            "uri": url_for(
                self.context["TransfersList"],
                _external=False,
                debtorId=obj.debtor_id,
            )
        }
        obj.recipient_identity = {"uri": obj.recipient_uri}

        if obj.finalized_at:
            result = {"finalized_at": obj.finalized_at}

            error_code = obj.error_code
            if error_code is None:
                result["committed_amount"] = obj.amount
            else:
                result["committed_amount"] = 0
                result["error"] = {
                    "error_code": error_code,
                    "total_locked_amount": obj.total_locked_amount,
                }

            obj.result = result

        return obj

    def get_checkup_at_string(self, obj):
        if obj.finalized_at:
            return missing

        calc_checkup_datetime = self.context["calc_checkup_datetime"]
        return calc_checkup_datetime(
            obj.debtor_id, obj.initiated_at
        ).isoformat()


class TransferCancelationRequestSchema(ValidateTypeMixin, Schema):
    type = fields.String(
        load_default="TransferCancelationRequest",
        dump_default="TransferCancelationRequest",
        metadata=dict(
            description=TYPE_DESCRIPTION,
            example="TransferCancelationRequest",
        ),
    )


class TransfersListSchema(Schema):
    uri = fields.String(
        required=True,
        dump_only=True,
        metadata=dict(
            format="uri-reference",
            description=URI_DESCRIPTION,
            example="/debtors/1/transfers/",
        ),
    )
    type = fields.Function(
        lambda obj: "TransfersList",
        required=True,
        metadata=dict(
            type="string",
            description=TYPE_DESCRIPTION,
            example="TransfersList",
        ),
    )
    debtor = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        metadata=dict(
            description="The URI of the corresponding `Debtor`.",
            example={"uri": "/debtors/1/"},
        ),
    )
    items = fields.Nested(
        ObjectReferenceSchema(many=True),
        required=True,
        dump_only=True,
        metadata=dict(
            description=(
                "Contains links to all `Transfers` in an array of"
                " `ObjectReference`s."
            ),
            example=[
                {"uri": i}
                for i in [
                    "123e4567-e89b-12d3-a456-426655440000",
                    "183ea7c7-7a96-4ed7-a50a-a2b069687d23",
                ]
            ],
        ),
    )
    itemsType = fields.Function(
        lambda obj: "ObjectReference",
        required=True,
        metadata=dict(
            type="string",
            description="The type of the items in the list.",
            example="ObjectReference",
        ),
    )
    first = fields.Function(
        lambda obj: "",
        required=True,
        metadata=dict(
            type="string",
            format="uri-reference",
            description=(
                "This will always be an empty string, representing the"
                " relative URI of the first and only page in a paginated list."
            ),
            example="",
        ),
    )

    @pre_dump
    def process_transfers_collection_instance(self, obj, many):
        assert isinstance(obj, TransfersList)
        obj = copy(obj)
        obj.uri = url_for(
            self.context["TransfersList"],
            _external=False,
            debtorId=obj.debtor_id,
        )
        obj.debtor = {
            "uri": url_for(
                self.context["Debtor"], _external=False, debtorId=obj.debtor_id
            )
        }
        obj.items = [{"uri": uri} for uri in obj.items]

        return obj


class ActivateDebtorMessageSchema(Schema):
    """``ActivateDebtor`` message schema."""

    class Meta:
        unknown = EXCLUDE

    type = fields.String(required=True)
    debtor_id = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT64, max=MAX_INT64)
    )
    reservation_id = fields.String(
        required=True, validate=validate.Length(max=100)
    )
    ts = fields.DateTime(required=True)

    @validates("type")
    def validate_type(self, value):
        if f"{value}MessageSchema" != type(self).__name__:
            raise ValidationError("Invalid type.")
