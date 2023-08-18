import re
from datetime import date, datetime, timezone
from urllib.parse import urljoin, urlparse
import pytest
from swpt_debtors.extensions import db
from swpt_debtors import procedures as p
from swpt_debtors import models as m


@pytest.fixture(scope="function")
def client(app, db_session):
    return app.test_client()


@pytest.fixture(scope="function")
def debtor(db_session):
    debtor = m.Debtor(debtor_id=4444444444, status_flags=0)
    debtor.activate()
    db.session.add(debtor)
    db.session.commit()

    return p.get_debtor(4444444444)


def _get_all_pages(client, url, page_type, streaming=False):
    r = client.get(url)
    assert r.status_code == 200

    data = r.get_json()
    assert data["type"] == page_type
    assert urlparse(data["uri"]) == urlparse(url)
    if streaming:
        assert "next" in data or "forthcoming" in data
        assert "next" not in data or "forthcoming" not in data
    else:
        assert "forthcoming" not in data

    items = data["items"]
    assert isinstance(items, list)

    if "next" in data:
        items.extend(
            _get_all_pages(
                client, urljoin(url, data["next"]), page_type, streaming
            )
        )

    return items


def test_auto_genereate_debtor_id(client):
    r = client.post("/debtors/.debtor-reserve", json={})
    assert r.status_code == 200
    data = r.get_json()
    assert data["type"] == "DebtorReservation"
    assert isinstance(data["debtorId"], str)
    assert isinstance(data["reservationId"], str)
    assert datetime.fromisoformat(data["validUntil"])
    assert datetime.fromisoformat(data["createdAt"])


def test_create_debtor(client):
    r = client.get("/debtors/4294967296/")
    assert r.status_code == 403

    r = client.post(
        "/debtors/4294967296/reserve",
        headers={"X-Swpt-User-Id": "INVALID_USER_ID"},
        json={},
    )
    assert r.status_code == 403

    r = client.post(
        "/debtors/2/reserve",
        headers={"X-Swpt-User-Id": "debtors:4294967296"},
        json={},
    )
    assert r.status_code == 403

    r = client.post("/debtors/4294967296/reserve", json={})
    assert r.status_code == 200
    data = r.get_json()
    assert data["type"] == "DebtorReservation"
    assert data["debtorId"] == "4294967296"
    assert isinstance(data["reservationId"], str)
    assert datetime.fromisoformat(data["validUntil"])
    assert datetime.fromisoformat(data["createdAt"])
    reservation_id = data["reservationId"]

    r = client.post("/debtors/4294967296/reserve", json={})
    assert r.status_code == 409

    r = client.get("/debtors/4294967296/")
    assert r.status_code == 403

    r = client.post(
        "/debtors/4294967296/activate",
        json={
            "reservationId": "123",
        },
    )
    assert r.status_code == 422
    assert "reservationId" in r.get_json()["errors"]["json"]

    r = client.post(
        "/debtors/4294967296/activate",
        json={
            "reservationId": reservation_id,
        },
    )
    assert r.status_code == 200
    data = r.get_json()
    assert data["type"] == "Debtor"
    assert data["uri"] == "/debtors/4294967296/"
    assert data["identity"] == {
        "type": "DebtorIdentity",
        "uri": "swpt:4294967296",
    }
    assert data["transfersList"] == {"uri": "/debtors/4294967296/transfers/"}
    assert data["createTransfer"] == {"uri": "/debtors/4294967296/transfers/"}
    assert datetime.fromisoformat(data["createdAt"])

    r = client.post(
        "/debtors/4294967296/activate",
        json={
            "reservationId": reservation_id,
        },
    )
    assert r.status_code == 200

    r = client.post(
        "/debtors/8589934591/activate",
        json={
            "reservationId": "123",
        },
    )
    assert r.status_code == 422
    assert "reservationId" in r.get_json()["errors"]["json"]

    r = client.post("/debtors/8589934591/activate", json={})
    assert r.status_code == 200
    data = r.get_json()
    assert data["type"] == "Debtor"
    assert data["uri"] == "/debtors/8589934591/"
    assert data["balance"] == 0
    assert data["minBalance"] == -9223372036854775808
    assert datetime.fromisoformat(data["createdAt"])
    assert "info" not in data

    r = client.post("/debtors/8589934591/activate", json={})
    assert r.status_code == 409

    r = client.get("/debtors/4294967296/")
    assert r.status_code == 200
    data = r.get_json()
    assert data["type"] == "Debtor"
    assert data["uri"] == "/debtors/4294967296/"
    assert data["balance"] == 0
    assert data["minBalance"] == -9223372036854775808
    assert datetime.fromisoformat(data["createdAt"])

    r = client.get("/debtors/8589934591/")
    assert r.status_code == 200

    r = client.post(
        "/debtors/8589934591/deactivate",
        headers={"X-Swpt-User-Id": "debtors:8589934591"},
        json={},
    )
    assert r.status_code == 403

    r = client.post(
        "/debtors/8589934591/deactivate",
        headers={"X-Swpt-User-Id": "debtors-supervisor"},
        json={},
    )
    assert r.status_code == 403

    r = client.post(
        "/debtors/8589934591/deactivate",
        headers={"X-Swpt-User-Id": "debtors-superuser"},
        json={},
    )
    assert r.status_code == 204

    r = client.post("/debtors/8589934591/deactivate", json={})
    assert r.status_code == 204

    r = client.get("/debtors/8589934591/")
    assert r.status_code == 403

    r = client.post("/debtors/8589934591/deactivate", json={})
    assert r.status_code == 204


def test_get_debtors_list(client):
    r = client.post("/debtors/4294967296/reserve", json={})
    assert r.status_code == 200
    r = client.post("/debtors/4294967297/activate", json={})
    assert r.status_code == 200
    r = client.post("/debtors/4294967298/activate", json={})
    assert r.status_code == 200
    r = client.post("/debtors/8589934591/activate", json={})
    assert r.status_code == 200

    r = client.get("/debtors/.list")
    assert r.status_code == 200
    data = r.get_json()
    assert data["type"] == "DebtorsList"
    assert data["uri"] == "/debtors/.list"
    assert data["itemsType"] == "ObjectReference"
    assert data["first"] == "/debtors/9223372036854775808/enumerate"

    entries = _get_all_pages(
        client, data["first"], page_type="ObjectReferencesPage"
    )
    assert entries == [
        {"uri": "/debtors/4294967297/"},
        {"uri": "/debtors/4294967298/"},
        {"uri": "/debtors/8589934591/"},
    ]


def test_get_debtor(client, debtor):
    r = client.get("/debtors/4444444444/")
    assert r.status_code == 200
    data = r.get_json()
    assert data["type"] == "Debtor"
    assert data["uri"] == "/debtors/4444444444/"
    assert data["config"] == {
        "type": "DebtorConfig",
        "uri": "/debtors/4444444444/config",
        "configData": "",
        "latestUpdateId": 1,
        "latestUpdateAt": "1970-01-01T00:00:00+00:00",
        "debtor": {"uri": "/debtors/4444444444/"},
    }
    assert data["transfersList"] == {"uri": "/debtors/4444444444/transfers/"}
    assert data["createTransfer"] == {"uri": "/debtors/4444444444/transfers/"}
    assert data["balance"] == 0
    assert data["minBalance"] == -9223372036854775808
    assert datetime.fromisoformat(data["createdAt"])
    assert data["identity"] == {
        "type": "DebtorIdentity",
        "uri": "swpt:4444444444",
    }
    assert data["noteMaxBytes"] == 0
    assert "configError" not in data
    assert "account" not in data


def test_restrict_debtor(client, debtor):
    request_body = {
        "type": "DebtorRestrictionRequest",
        "minBalance": -5500,
    }

    r = client.post(
        "/debtors/4444444445/restrict",
        headers={"X-Swpt-User-Id": "debtors-superuser"},
        json=request_body,
    )
    assert r.status_code == 404

    r = client.post(
        "/debtors/4444444444/restrict",
        headers={"X-Swpt-User-Id": "debtors:4444444444"},
        json=request_body,
    )
    assert r.status_code == 403

    r = client.post(
        "/debtors/4444444444/restrict",
        headers={"X-Swpt-User-Id": "debtors-supervisor"},
        json=request_body,
    )
    assert r.status_code == 403

    # Assert that there are no changes to minBalance.
    r = client.get("/debtors/4444444444/")
    assert r.status_code == 200
    data = r.get_json()
    assert data["minBalance"] == -9223372036854775808

    r = client.post(
        "/debtors/4444444444/restrict",
        headers={"X-Swpt-User-Id": "debtors-superuser"},
        json=request_body,
    )

    # Assert that minBalance has changed.
    assert r.status_code == 200
    r = client.get("/debtors/4444444444/")
    assert r.status_code == 200
    data = r.get_json()
    assert data["minBalance"] == -5500


def test_change_debtor_config(client, debtor):
    r = client.get("/debtors/4444444444/config")
    assert r.status_code == 200
    data = r.get_json()
    assert data["type"] == "DebtorConfig"
    assert data["uri"] == "/debtors/4444444444/config"
    assert data["configData"] == ""
    assert data["latestUpdateId"] == 1
    latest_update_at = data["latestUpdateAt"]
    assert datetime.fromisoformat(latest_update_at)
    assert data["debtor"] == {"uri": "/debtors/4444444444/"}

    request = {"configData": "TEST", "latestUpdateId": 2}
    r = client.patch("/debtors/4444444444/config", json=request)
    assert r.status_code == 200

    r = client.get("/debtors/4444444444/config")
    assert r.status_code == 200
    data = r.get_json()
    assert data["type"] == "DebtorConfig"
    assert data["uri"] == "/debtors/4444444444/config"
    assert data["configData"] == "TEST"
    assert data["latestUpdateId"] == 2
    assert datetime.fromisoformat(data["latestUpdateAt"])
    assert latest_update_at != data["latestUpdateAt"]
    assert data["debtor"] == {"uri": "/debtors/4444444444/"}

    empty_request = {
        "configData": "",
        "latestUpdateId": 2,
    }
    r = client.patch("/debtors/6666666666/config", json=empty_request)
    assert r.status_code == 404

    r = client.patch("/debtors/4444444444/config", json=empty_request)
    assert r.status_code == 409
    data = r.get_json()

    for _ in range(9):
        r = client.patch("/debtors/4444444444/config", json=request)
        assert r.status_code == 200
    r = client.patch("/debtors/4444444444/config", json=request)
    assert r.status_code == 403


def test_initiate_running_transfer(client, debtor):
    r = client.get("/debtors/6666666666/transfers/")
    assert r.status_code == 404

    r = client.get("/debtors/4444444444/transfers/")
    assert r.status_code == 200
    data = r.get_json()
    assert data["debtor"] == {"uri": "/debtors/4444444444/"}
    assert data["type"] == "TransfersList"
    assert data["uri"] == "/debtors/4444444444/transfers/"
    assert data["items"] == []

    json_request_body = {
        "amount": 1000,
        "noteFormat": "fmt",
        "note": "test",
        "recipient": {"uri": "swpt:4444444444/1111"},
        "transferUuid": "123e4567-e89b-12d3-a456-426655440000",
    }

    json_request_body2 = json_request_body.copy()
    json_request_body2["recipient"] = {"uri": "swpt:6666666666/1111"}
    r = client.post("/debtors/6666666666/transfers/", json=json_request_body2)
    assert r.status_code == 404

    r = client.post("/debtors/4444444444/transfers/", json=json_request_body)
    assert r.status_code == 201
    data = r.get_json()
    assert data["amount"] == 1000
    assert datetime.fromisoformat(data["initiatedAt"])
    assert "result" not in data
    assert data["recipient"] == {
        "type": "AccountIdentity",
        "uri": "swpt:4444444444/1111",
    }
    assert data["type"] == "Transfer"
    assert (
        data["uri"]
        == "/debtors/4444444444/transfers/123e4567-e89b-12d3-a456-426655440000"
    )
    assert data["noteFormat"] == "fmt"
    assert data["note"] == "test"
    assert data["transfersList"] == {"uri": "/debtors/4444444444/transfers/"}
    assert (
        r.headers["Location"]
        == "http://example.com/debtors/4444444444/transfers/"
        "123e4567-e89b-12d3-a456-426655440000"
    )

    r = client.get(
        "/debtors/4444444444/transfers/123e4567-e89b-12d3-a456-426655440000"
    )
    assert r.status_code == 200
    data = r.get_json()
    assert data["type"] == "Transfer"
    assert (
        data["uri"]
        == "/debtors/4444444444/transfers/123e4567-e89b-12d3-a456-426655440000"
    )
    assert data["amount"] == 1000

    r = client.post("/debtors/4444444444/transfers/", json=json_request_body)
    assert r.status_code == 303
    assert (
        r.headers["Location"]
        == "http://example.com/debtors/4444444444/transfers/"
        "123e4567-e89b-12d3-a456-426655440000"
    )

    json_request_body["amount"] += 1
    r = client.post("/debtors/4444444444/transfers/", json=json_request_body)
    assert r.status_code == 409

    r = client.post(
        "/debtors/4444444444/transfers/",
        json={**json_request_body, **{"recipient": {"uri": "INVALID"}}},
    )
    assert r.status_code == 422

    r = client.post(
        "/debtors/4444444444/transfers/",
        json={**json_request_body, **{"recipient": {"uri": "swpt:555/1111"}}},
    )
    assert r.status_code == 422

    r = client.post(
        "/debtors/555/transfers/",
        json={**json_request_body, **{"recipient": {"uri": "swpt:555/1111"}}},
    )
    assert r.status_code == 404

    r = client.get("/debtors/4444444444/transfers/")
    assert r.status_code == 200
    data = r.get_json()
    assert sorted(data["items"]) == [
        {"uri": "123e4567-e89b-12d3-a456-426655440000"},
    ]

    r = client.delete(
        "/debtors/4444444444/transfers/123e4567-e89b-12d3-a456-426655440001"
    )
    assert r.status_code == 204

    r = client.get("/debtors/4444444444/transfers/")
    assert r.status_code == 200
    data = r.get_json()
    assert sorted(data["items"]) == [
        {"uri": "123e4567-e89b-12d3-a456-426655440000"},
    ]

    for i in range(2, 12):
        suffix = "{:0>4}".format(i)
        json_request_body = {
            "amount": 1,
            "recipient": {"uri": "swpt:4444444444/1111"},
            "transferUuid": f"123e4567-e89b-12d3-a456-42665544{suffix}",
        }
        r = client.post(
            "/debtors/4444444444/transfers/", json=json_request_body
        )
        if i == 11:
            assert r.status_code == 403
        else:
            assert r.status_code == 201


def test_cancel_running_transfer(client, debtor):
    json_request_body = {
        "amount": 1000,
        "note": "test",
        "recipient": {"uri": "swpt:4444444444/1111"},
        "transferUuid": "123e4567-e89b-12d3-a456-426655440000",
    }
    r = client.post("/debtors/4444444444/transfers/", json=json_request_body)
    assert r.status_code == 201
    data = r.get_json()
    assert "result" not in data

    r = client.post(
        "/debtors/4444444444/transfers/123e4567-e89b-12d3-a456-426655440001",
        json={},
    )
    assert r.status_code == 404

    r = client.post(
        "/debtors/4444444444/transfers/123e4567-e89b-12d3-a456-426655440000",
        json={},
    )
    assert r.status_code == 200

    r = client.get(
        "/debtors/4444444444/transfers/123e4567-e89b-12d3-a456-426655440000"
    )
    assert r.status_code == 200
    data = r.get_json()
    result = data["result"]
    error = result["error"]
    assert error["errorCode"] == "CANCELED_BY_THE_SENDER"

    r = client.post(
        "/debtors/4444444444/transfers/123e4567-e89b-12d3-a456-426655440000",
        json={},
    )
    assert r.status_code == 200


def test_unauthorized_debtor_id(debtor, client):
    json_request_body = {
        "type": "DebtorConfig",
        "configData": "",
        "latestUpdateId": 2,
    }

    r = client.get("/debtors/4444444444/")
    assert r.status_code == 200

    r = client.get(
        "/debtors/4444444444/", headers={"X-Swpt-User-Id": "INVALID_USER_ID"}
    )
    assert r.status_code == 403

    r = client.patch(
        "/debtors/4444444444/config",
        json=json_request_body,
        headers={"X-Swpt-User-Id": "debtors-supervisor"},
    )
    assert r.status_code == 403

    r = client.patch(
        "/debtors/4444444444/config",
        json=json_request_body,
        headers={"X-Swpt-User-Id": "debtors:6666666666"},
    )
    assert r.status_code == 403

    r = client.patch(
        "/debtors/4444444444/config",
        json=json_request_body,
        headers={"X-Swpt-User-Id": "debtors:4444444444"},
    )
    assert r.status_code == 200

    with pytest.raises(ValueError):
        r = client.get(
            "/debtors/18446744073709551615/",
            json=json_request_body,
            headers={"X-Swpt-User-Id": "debtors:18446744073709551616"},
        )


def test_redirect_to_debtor(client, debtor):
    r = client.get("/debtors/.debtor")
    assert r.status_code == 204

    r = client.get("/debtors/.debtor", headers={"X-Swpt-User-Id": "debtors:2"})
    assert r.status_code == 303
    assert r.headers["Location"] == "http://example.com/debtors/2/"

    r = client.get(
        "/debtors/.debtor",
        headers={"X-Swpt-User-Id": "debtors:18446744073709551615"},
    )
    assert r.status_code == 303
    assert (
        r.headers["Location"]
        == "http://example.com/debtors/18446744073709551615/"
    )


def test_redirect_to_latest_info(client, debtor):
    r = client.get("/debtors/4444444444/public")
    assert r.status_code == 404

    request = {
        "configData": '{"info": {"iri": "https://example.com/"}}',
        "latestUpdateId": 2,
    }
    r = client.patch("/debtors/4444444444/config", json=request)
    assert r.status_code == 200

    debtor = p.get_debtor(4444444444)
    current_ts = datetime.now(tz=timezone.utc)

    p.process_account_update_signal(
        debtor_id=debtor.debtor_id,
        creditor_id=p.ROOT_CREDITOR_ID,
        creation_date=date(2020, 1, 1),
        last_change_ts=current_ts,
        last_change_seqnum=1,
        principal=0,
        interest_rate=0.0,
        last_config_ts=debtor.last_config_ts,
        last_config_seqnum=debtor.last_config_seqnum,
        negligible_amount=-float(debtor.min_balance),
        config_data="INCORRECT CONFIG DATA",
        config_flags=debtor.config_flags,
        account_id="",
        transfer_note_max_bytes=0,
        ts=current_ts,
        ttl=10000000,
    )
    r = client.get("/debtors/4444444444/public")
    assert r.status_code == 404

    p.process_account_update_signal(
        debtor_id=debtor.debtor_id,
        creditor_id=p.ROOT_CREDITOR_ID,
        creation_date=date(2020, 1, 1),
        last_change_ts=current_ts,
        last_change_seqnum=2,
        principal=0,
        interest_rate=0.0,
        last_config_ts=debtor.last_config_ts,
        last_config_seqnum=debtor.last_config_seqnum,
        negligible_amount=-float(debtor.min_balance),
        config_data=debtor.config_data,
        config_flags=debtor.config_flags,
        account_id="",
        transfer_note_max_bytes=0,
        ts=current_ts,
        ttl=10000000,
    )
    r = client.get("/debtors/4444444444/public")
    assert r.status_code == 302
    assert r.headers["Location"] == "https://example.com/"
    assert r.headers["Cache-Control"] == "max-age=86400"


def test_save_document(client, debtor):
    r = client.get("/debtors/4444444444/documents/0/public")
    assert r.status_code == 404

    r = client.post(
        "/debtors/4444444444/documents/",
        content_type="application/octet-stream",
        data=101 * b"1",
    )
    assert r.status_code == 413

    content = 100 * b"1"
    r = client.post(
        "/debtors/4444444444/documents/",
        content_type="application/octet-stream",
        data=content,
    )
    assert r.status_code == 201
    assert r.content_type == "application/octet-stream"
    assert r.get_data() == content
    location = r.headers["Location"]
    m = re.match(
        r"http://example.com/debtors/4444444444/documents/(\d)+/public",
        location,
    )
    assert m is not None
    document_id = int(m.group(1))
    assert document_id >= 0

    r = client.get(location)
    assert r.status_code == 200
    assert r.content_type == "application/octet-stream"
    assert r.get_data() == content

    r = client.post(
        "/debtors/4444444444/documents/",
        content_type="application/octet-stream",
        data=content,
    )
    assert r.status_code == 201
    assert r.content_type == "application/octet-stream"
    assert r.get_data() == content
    assert location != r.headers["Location"]

    r = client.post(
        "/debtors/4444444444/documents/",
        content_type="application/octet-stream",
        data=content,
    )
    assert r.status_code == 403

    r = client.post(
        "/debtors/6666666666/documents/",
        content_type="application/octet-stream",
        data=content,
    )
    assert r.status_code == 404
