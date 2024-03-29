import pytest
from datetime import datetime
from marshmallow import ValidationError
from swpt_debtors import schemas
from swpt_debtors.models import (
    Debtor,
    RunningTransfer,
    TRANSFER_NOTE_MAX_BYTES,
    TS0,
    SC_INSUFFICIENT_AVAILABLE_AMOUNT,
    CONFIG_DATA_MAX_BYTES,
)
from swpt_debtors.routes import context


def test_serialize_debtor_schema(db_session):
    debtor = Debtor(debtor_id=1)
    db_session.add(debtor)
    db_session.commit()
    debtor = Debtor.query.filter_by(debtor_id=1).one()
    with pytest.raises(KeyError):
        s = schemas.DebtorSchema()
        s.dump(debtor)
    s = schemas.DebtorSchema(context=context)
    obj = s.dump(debtor)
    assert obj["uri"] == "/debtors/1/"
    assert obj["type"] == "Debtor"
    assert datetime.fromisoformat(obj["createdAt"])
    assert obj["balance"] == 0
    assert obj["minBalance"] == -9223372036854775808
    assert obj["transfersList"] == {"uri": "/debtors/1/transfers/"}
    assert obj["saveDocument"] == {"uri": "/debtors/1/documents/"}
    assert obj["publicInfoDocument"] == {"uri": "/debtors/1/public"}
    assert obj["config"] == {
        "type": "DebtorConfig",
        "uri": "/debtors/1/config",
        "configData": "",
        "latestUpdateId": 1,
        "latestUpdateAt": "1970-01-01T00:00:00+00:00",
        "debtor": {"uri": "/debtors/1/"},
    }
    assert obj["noteMaxBytes"] == 0
    assert "account" not in obj
    assert "configError" not in obj

    debtor.config_error = "TEST_ERROR"
    debtor.account_id = "0"
    obj = s.dump(debtor)
    assert obj["configError"] == "TEST_ERROR"
    assert obj["account"] == {"type": "AccountIdentity", "uri": "swpt:1/0"}


def test_deserialize_debtor_config_schema(db_session):
    s = schemas.DebtorConfigSchema(context=context)
    with pytest.raises(ValidationError, match="Invalid type"):
        data = s.load(
            {"type": "INVALID_TYPE", "configData": "", "latestUpdateId": 1}
        )
    with pytest.raises(
        ValidationError, match="Missing data for required field."
    ):
        data = s.load({"type": "DebtorConfig", "latestUpdateId": 1})
    with pytest.raises(
        ValidationError, match="Missing data for required field."
    ):
        data = s.load({"type": "DebtorConfig", "configData": ""})
    with pytest.raises(
        ValidationError, match="Must be greater than or equal to 1"
    ):
        data = s.load(
            {"type": "DebtorConfig", "configData": "", "latestUpdateId": 0}
        )
    with pytest.raises(ValidationError, match="Longer than maximum length"):
        data = s.load(
            {
                "type": "DebtorConfig",
                "configData": (CONFIG_DATA_MAX_BYTES + 1) * "x",
                "latestUpdateId": 1,
            }
        )
    with pytest.raises(
        ValidationError, match="The total byte-length of the config exceeds"
    ):
        data = s.load(
            {
                "type": "DebtorConfig",
                "configData": int(CONFIG_DATA_MAX_BYTES * 0.7) * "Щ",
                "latestUpdateId": 1,
            }
        )

    data = s.load(
        {
            "configData": "",
            "latestUpdateId": 1,
        }
    )
    assert data["type"] == "DebtorConfig"
    assert data["config_data"] == ""
    assert data["latest_update_id"] == 1

    data = s.load(
        {
            "type": "DebtorConfig",
            "configData": CONFIG_DATA_MAX_BYTES * "x",
            "latestUpdateId": 667,
        }
    )
    assert data["type"] == "DebtorConfig"
    assert data["config_data"] == CONFIG_DATA_MAX_BYTES * "x"
    assert data["latest_update_id"] == 667


def test_deserialize_transfer_creation_request(db_session):
    s = schemas.TransferCreationRequestSchema()
    with pytest.raises(
        ValidationError, match="The total byte-length of the note exceeds"
    ):
        s.load(
            {
                "creditorId": 1,
                "transferUuid": "123e4567-e89b-12d3-a456-426655440000",
                "amount": 1000,
                "note": TRANSFER_NOTE_MAX_BYTES * "Щ",
            }
        )


def test_serialize_transfer(app):
    ts = schemas.TransferSchema(context=context)

    transfer_data = {
        "recipient_uri": "swpt:2/1111",
        "recipient": "1111",
        "transfer_uuid": "123e4567-e89b-12d3-a456-426655440000",
        "debtor_id": -1,
        "amount": 1000,
        "transfer_note_format": "json",
        "transfer_note": '{"note": "test"}',
        "initiated_at": TS0,
        "finalized_at": datetime(2020, 1, 4),
        "error_code": SC_INSUFFICIENT_AVAILABLE_AMOUNT,
        "total_locked_amount": 5,
    }
    it = RunningTransfer(**transfer_data)

    data = ts.dump(it)
    assert data == {
        "type": "Transfer",
        "uri": (
            "/debtors/18446744073709551615/transfers/"
            "123e4567-e89b-12d3-a456-426655440000"
        ),
        "transferUuid": "123e4567-e89b-12d3-a456-426655440000",
        "transfersList": {"uri": "/debtors/18446744073709551615/transfers/"},
        "initiatedAt": "1970-01-01T00:00:00+00:00",
        "recipient": {"type": "AccountIdentity", "uri": "swpt:2/1111"},
        "amount": 1000,
        "noteFormat": "json",
        "note": '{"note": "test"}',
        "result": {
            "type": "TransferResult",
            "finalizedAt": "2020-01-04T00:00:00",
            "committedAmount": 0,
            "error": {
                "type": "TransferError",
                "errorCode": SC_INSUFFICIENT_AVAILABLE_AMOUNT,
                "totalLockedAmount": 5,
            },
        },
    }

    it.error_code = None
    data = ts.dump(it)
    assert data == {
        "type": "Transfer",
        "uri": (
            "/debtors/18446744073709551615/transfers/"
            "123e4567-e89b-12d3-a456-426655440000"
        ),
        "transferUuid": "123e4567-e89b-12d3-a456-426655440000",
        "transfersList": {"uri": "/debtors/18446744073709551615/transfers/"},
        "initiatedAt": "1970-01-01T00:00:00+00:00",
        "recipient": {"type": "AccountIdentity", "uri": "swpt:2/1111"},
        "amount": 1000,
        "noteFormat": "json",
        "note": '{"note": "test"}',
        "result": {
            "type": "TransferResult",
            "finalizedAt": "2020-01-04T00:00:00",
            "committedAmount": 1000,
        },
    }

    it.finalized_at = None
    data = ts.dump(it)
    assert datetime.fromisoformat(data.pop("checkupAt"))
    assert data == {
        "type": "Transfer",
        "uri": (
            "/debtors/18446744073709551615/transfers/"
            "123e4567-e89b-12d3-a456-426655440000"
        ),
        "transferUuid": "123e4567-e89b-12d3-a456-426655440000",
        "transfersList": {"uri": "/debtors/18446744073709551615/transfers/"},
        "initiatedAt": "1970-01-01T00:00:00+00:00",
        "recipient": {"type": "AccountIdentity", "uri": "swpt:2/1111"},
        "amount": 1000,
        "noteFormat": "json",
        "note": '{"note": "test"}',
    }


def test_activate_debtor():
    s = schemas.ActivateDebtorMessageSchema()

    data = s.loads(
        """{
    "type": "ActivateDebtor",
    "debtor_id": -2000000000000000,
    "reservation_id": "test_id",
    "ts": "2022-01-01T00:00:00Z",
    "unknown": "ignored"
    }"""
    )

    assert data["type"] == "ActivateDebtor"
    assert data["debtor_id"] == -2000000000000000
    assert data["reservation_id"] == "test_id"
    assert data["ts"] == datetime.fromisoformat("2022-01-01T00:00:00+00:00")
    assert "unknown" not in data

    wrong_type = data.copy()
    wrong_type["type"] = "WrongType"
    wrong_type = s.dumps(wrong_type)
    with pytest.raises(ValidationError, match="Invalid type."):
        s.loads(wrong_type)

    wrong_reservation_id = data.copy()
    wrong_reservation_id["reservation_id"] = 1000 * "x"
    wrong_reservation_id = s.dumps(wrong_reservation_id)
    with pytest.raises(ValidationError, match="Longer than maximum length"):
        s.loads(wrong_reservation_id)

    try:
        s.loads("{}")
    except ValidationError as e:
        assert len(e.messages) == len(data)
        assert all(
            m == ["Missing data for required field."]
            for m in e.messages.values()
        )
