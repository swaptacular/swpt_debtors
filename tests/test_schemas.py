import iso8601
import pytest
from datetime import date, datetime
from marshmallow import ValidationError
from swpt_debtors import schemas
from swpt_debtors.lower_limits import LowerLimit
from swpt_debtors.models import Debtor, RunningTransfer, TRANSFER_NOTE_MAX_BYTES, BEGINNING_OF_TIME, \
    SC_INSUFFICIENT_AVAILABLE_AMOUNT
from swpt_debtors.routes import context


def test_interest_rate_lower_limit_schema():
    s = schemas.InterestRateLowerLimitSchema()
    data = s.load({'value': 5.6, 'enforcedUntil': '2020-10-25'})
    assert isinstance(data, LowerLimit)
    assert data.value == 5.6
    assert data.cutoff == date(2020, 10, 25)
    assert s.dump(data) == {'type': 'InterestRateLowerLimit', 'value': 5.6, 'enforcedUntil': '2020-10-25'}


def test_balance_lower_limit_schema():
    s = schemas.BalanceLowerLimitSchema()
    data = s.load({'value': 1000, 'enforcedUntil': '2020-10-25'})
    assert isinstance(data, LowerLimit)
    assert data.value == 1000
    assert data.cutoff == date(2020, 10, 25)
    assert s.dump(data) == {'type': 'BalanceLowerLimit', 'value': 1000, 'enforcedUntil': '2020-10-25'}


def test_server_name(app):
    assert app.config['SERVER_NAME'] == app.config['SWPT_SERVER_NAME']


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
    assert obj['uri'] == '/debtors/1/'
    assert obj['type'] == 'Debtor'
    assert iso8601.parse_date(obj['createdAt'])
    assert obj['balance'] == 0
    assert obj['balanceLowerLimits'] == []
    assert obj['interestRateLowerLimits'] == [
        {'type': 'InterestRateLowerLimit', 'value': -50.0, 'enforcedUntil': '9999-12-31'},
    ]
    assert obj['interestRateTarget'] == 0.0
    assert obj['interestRate'] == 0.0
    assert obj['transfersList'] == {'uri': '/debtors/1/transfers/'}
    assert 'deactivatedAt' not in obj

    debtor.deactivate()
    obj = s.dump(debtor)
    assert iso8601.parse_date(obj['deactivatedAt'])


def test_deserialize_debtor_schema(db_session):
    s = schemas.DebtorSchema(context=context)
    with pytest.raises(ValidationError):
        data = s.load({'type': 'INVALID_TYPE'})

    data = s.load({
        'type': 'Debtor',
        'balanceLowerLimits': [],
        'interestRateLowerLimits': [],
        'interestRateTarget': 0.0
    })
    assert data['balance_lower_limits'] == []
    assert data['interest_rate_lower_limits'] == []
    assert data['interest_rate_target'] == 0.0
    data = s.load({
        'balanceLowerLimits': [{'value': 1000, 'enforcedUntil': '2020-10-25'}],
        'interestRateLowerLimits': [{'value': 5.6, 'enforcedUntil': '2020-10-25'}],
        'interestRateTarget': 6.1,
    })
    assert len(data['balance_lower_limits']) == 1
    assert data['balance_lower_limits'][0].value == 1000
    assert len(data['interest_rate_lower_limits']) == 1
    assert data['interest_rate_lower_limits'][0].value == 5.6
    assert data['interest_rate_target'] == 6.1


def test_deserialize_transfer_creation_request(db_session):
    s = schemas.TransferCreationRequestSchema()
    with pytest.raises(ValidationError, match='The total byte-length of the note exceeds'):
        s.load({
            'creditorId': 1,
            'transferUuid': '123e4567-e89b-12d3-a456-426655440000',
            'amount': 1000,
            'note': TRANSFER_NOTE_MAX_BYTES * 'Щ',
        })


def test_serialize_transfer(app):
    ts = schemas.TransferSchema(context=context)

    transfer_data = {
        'recipient_uri': 'swpt:2/1111',
        'recipient': '1111',
        'transfer_uuid': '123e4567-e89b-12d3-a456-426655440000',
        'debtor_id': -1,
        'amount': 1000,
        'transfer_note_format': 'json',
        'transfer_note': '{"note": "test"}',
        'initiated_at': BEGINNING_OF_TIME,
        'finalized_at': datetime(2020, 1, 4),
        'error_code': SC_INSUFFICIENT_AVAILABLE_AMOUNT,
        'total_locked_amount': 5,
    }
    it = RunningTransfer(**transfer_data)

    data = ts.dump(it)
    assert data == {
        "type": "Transfer",
        "uri": "/debtors/18446744073709551615/transfers/123e4567-e89b-12d3-a456-426655440000",
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
        "uri": "/debtors/18446744073709551615/transfers/123e4567-e89b-12d3-a456-426655440000",
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
    assert iso8601.parse_date(data.pop('checkupAt'))
    assert data == {
        "type": "Transfer",
        "uri": "/debtors/18446744073709551615/transfers/123e4567-e89b-12d3-a456-426655440000",
        "transferUuid": "123e4567-e89b-12d3-a456-426655440000",
        "transfersList": {"uri": "/debtors/18446744073709551615/transfers/"},
        "initiatedAt": "1970-01-01T00:00:00+00:00",
        "recipient": {"type": "AccountIdentity", "uri": "swpt:2/1111"},
        "amount": 1000,
        "noteFormat": "json",
        "note": '{"note": "test"}',
    }


def test_serialize_debtor_info(app):
    dis = schemas.DebtorInfoSchema()
    data = dis.dump({
        'iri': 'abc',
        'optional_content_type': 50 * 'x',
        'optional_sha256': 32 * 'AA',
    })
    assert data == {
        "type": "DebtorInfo",
        "iri": "abc",
        "contentType": 50 * 'x',
        "sha256": 32 * 'AA',
    }

    data = dis.dump({
        'iri': 'abc',
    })
    assert data == {
        "type": "DebtorInfo",
        "iri": "abc",
    }


def test_deserialize_debtor_info(app):
    dis = schemas.DebtorInfoSchema()

    assert dis.load({'iri': ''}) == {
        'type': 'DebtorInfo',
        'iri': '',
    }

    data = {
        'type': 'DebtorInfo',
        'iri': 'abc',
        'contentType': 50 * 'x',
        'sha256': 32 * 'AA',
    }

    assert dis.load(data) == {
        'type': 'DebtorInfo',
        'iri': 'abc',
        'optional_content_type': 50 * 'x',
        'optional_sha256': 32 * 'AA',
    }

    with pytest.raises(ValidationError, match='Includes non-ASCII characters'):
        dis.load({
            'type': 'DebtorInfo',
            'contentType': 100 * 'Щ',
            'sha256': 32 * 'AA',
        })
    with pytest.raises(ValidationError, match='Longer than maximum length'):
        dis.load({
            'type': 'DebtorInfo',
            'iri': 'abc',
            'contentType': 101 * 'Щ',
            'sha256': 32 * 'AA',
        })
