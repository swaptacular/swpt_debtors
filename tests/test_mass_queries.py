import pytest
from datetime import datetime, timezone
from swpt_debtors.extensions import db
from sqlalchemy.inspection import inspect
from sqlalchemy.sql.expression import null, true, false, or_, and_, tuple_
from swpt_debtors.models import Debtor, ConfigureAccountSignal


@pytest.fixture(scope="function")
def current_ts():
    return datetime.now(tz=timezone.utc)


@pytest.mark.skip
def test_debtor_mass_update(db_session, current_ts):
    n = 7500
    table = Debtor.__table__
    pk = table.c.debtor_id
    pks_to_set = [
        i for i in range(n)
    ]
    for debtor_id in pks_to_set:
        db.session.add(
            Debtor(
                debtor_id=debtor_id,
                status_flags=Debtor.STATUS_IS_ACTIVATED_FLAG,
            )
        )
    db.session.commit()
    status_flags_mask = (
        Debtor.STATUS_IS_ACTIVATED_FLAG | Debtor.STATUS_IS_DEACTIVATED_FLAG
    )
    to_update = (
        db.session.query(Debtor.debtor_id)
        .filter(
            pk.in_(pks_to_set),
            or_(
                Debtor.is_config_effectual == false(),
                and_(
                    Debtor.has_server_account == true(),
                    Debtor.account_last_heartbeat_ts
                    < current_ts,
                ),
            ),
            Debtor.config_error == null(),
            Debtor.last_config_ts < current_ts,
            Debtor.status_flags.op("&")(status_flags_mask)
            == Debtor.STATUS_IS_ACTIVATED_FLAG,
        )
        .with_for_update(skip_locked=True, key_share=True)
        .all()
    )
    pks_to_update = [row[0] for row in to_update]
    Debtor.query.filter(pk.in_(pks_to_update)).update(
        {Debtor.config_error: "CONFIGURATION_IS_NOT_EFFECTUAL"},
        synchronize_session=False,
    )
    db.session.commit()
    assert len(
        Debtor.query
        .filter(Debtor.config_error == null())
        .all()
    ) == 0


@pytest.mark.skip
def test_configure_account_signal_mass_select(db_session, current_ts):
    n = 7500
    model_cls = ConfigureAccountSignal
    mapper = inspect(model_cls)
    pk_attrs = [
        mapper.get_property_by_column(c).class_attribute
        for c in mapper.primary_key
    ]
    pk = tuple_(*pk_attrs)
    session = db.session
    query = (
        session.query(model_cls)
        .with_for_update(skip_locked=True)
    )
    primary_keys = [
        (-i, current_ts, i) for i in range(n)
    ]
    signals = query.filter(pk.in_(primary_keys)).all()
    db.session.commit()
    assert len(signals) == 0
