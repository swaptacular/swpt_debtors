import pytest
from datetime import date
from marshmallow import ValidationError
from swpt_debtors import schemas
from swpt_debtors.lower_limits import LowerLimit
from swpt_debtors.models import Debtor, TRANSFER_NOTE_MAX_BYTES


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


def test_debtor_policy_schema():
    s = schemas.DebtorPolicySchema()
    with pytest.raises(ValidationError):
        data = s.load({'type': 'INVALID_TYPE'})

    data = s.load({})
    assert data['balance_lower_limits'] == []
    assert data['interest_rate_lower_limits'] == []
    assert 'interest_rate_target' not in data
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


def test_server_name(app):
    assert app.config['SERVER_NAME'] == app.config['SWPT_SERVER_NAME']


def test_debtor_schema(db_session):
    debtor = Debtor(debtor_id=1)
    db_session.add(debtor)
    db_session.commit()
    with pytest.raises(KeyError):
        s = schemas.DebtorSchema()
        s.dump(debtor)
    s = schemas.DebtorSchema(context={'Debtor': 'debtors.DebtorEndpoint'})
    obj = s.dump(debtor)
    assert 'example.com' in obj['uri']
    assert obj['type'] == 'Debtor'
    assert 'example.com' in obj['accountingAuthorityUri']


def test_deserialize_transfer_creation_request(db_session):
    s = schemas.IssuingTransferCreationRequestSchema()
    with pytest.raises(ValidationError, match='The total byte-length of the note exceeds'):
        s.load({
            'recipientCreditorId': 1,
            'transferUuid': '123e4567-e89b-12d3-a456-426655440000',
            'amount': 1000,
            'note': TRANSFER_NOTE_MAX_BYTES * 'Щ',
        })
