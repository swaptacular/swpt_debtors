import pytest
from datetime import datetime, timezone
from swpt_debtors.extensions import db
from sqlalchemy import select, update
from sqlalchemy.inspection import inspect
from sqlalchemy.sql.expression import null, true, false, or_, and_, tuple_
from swpt_debtors.models import Debtor, ConfigureAccountSignal


@pytest.fixture(scope="function")
def current_ts():
    return datetime.now(tz=timezone.utc)


@pytest.mark.skip
def test_debtor_mass_update(db_session, current_ts):
    n = 7500
    pk = tuple_(Debtor.debtor_id)
    pks_to_set = [
        (i,) for i in range(n)
    ]
    for (debtor_id,) in pks_to_set:
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
    pks_to_set.pop()
    chosen = Debtor.choose_rows(pks_to_set)
    pks_to_update = [
        (row.debtor_id,)
        for row in db.session.execute(
            select(Debtor.debtor_id)
            .join(chosen, pk == tuple_(*chosen.c))
            .where(
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
        ).all()
    ]
    to_update = Debtor.choose_rows(pks_to_update)
    db.session.execute(
        update(Debtor)
        .execution_options(synchronize_session=False)
        .where(pk == tuple_(*to_update.c))
        .values(config_error="CONFIGURATION_IS_NOT_EFFECTUAL")
    )
    db.session.commit()
    assert len(
        Debtor.query
        .filter(Debtor.config_error == null())
        .all()
    ) == 1


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
    q = session.query(model_cls).with_for_update(skip_locked=True)

    def _query_signals(pks):
        chosen = model_cls.choose_rows([tuple(x) for x in pks])
        return q.join(chosen, pk == tuple_(*chosen.c)).all()

    primary_keys = [
        (-i, current_ts, i) for i in range(n)
    ]
    signals = _query_signals(primary_keys)
    db.session.commit()
    assert len(signals) == 0
