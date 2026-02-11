from datetime import datetime, timedelta, timezone
from swpt_pythonlib.scan_table import TableScanner
from sqlalchemy import select, update
from sqlalchemy.orm import load_only
from sqlalchemy.sql.expression import and_, or_, null, true, false, tuple_
from flask import current_app
from swpt_debtors.extensions import db
from swpt_debtors.models import (
    Debtor,
    is_valid_debtor_id,
    SET_INDEXSCAN_ON,
    SET_INDEXSCAN_OFF,
)

SECONDS_IN_YEAR = 365.25 * 24 * 60 * 60


class DebtorScanner(TableScanner):
    """Garbage-collects inactive debtors."""

    table = Debtor.__table__
    columns = [
        Debtor.debtor_id,
        Debtor.created_at,
        Debtor.status_flags,
        Debtor.has_server_account,
        Debtor.account_last_heartbeat_ts,
        Debtor.is_config_effectual,
        Debtor.last_config_ts,
        Debtor.config_error,
        Debtor.deactivation_date,
    ]
    pk = tuple_(Debtor.debtor_id)

    def __init__(self):
        super().__init__()
        self.inactive_interval = timedelta(
            days=current_app.config["APP_INACTIVE_DEBTOR_RETENTION_DAYS"]
        )
        self.max_heartbeat_delay = timedelta(
            days=current_app.config["APP_MAX_HEARTBEAT_DELAY_DAYS"]
        )
        self.max_config_delay = timedelta(
            hours=current_app.config["APP_MAX_CONFIG_DELAY_HOURS"]
        )

    @property
    def blocks_per_query(self) -> int:
        return int(current_app.config["APP_DEBTORS_SCAN_BLOCKS_PER_QUERY"])

    @property
    def target_beat_duration(self) -> int:
        return int(current_app.config["APP_DEBTORS_SCAN_BEAT_MILLISECS"])

    def process_rows(self, rows):
        current_ts = datetime.now(tz=timezone.utc)
        if current_app.config["DELETE_PARENT_SHARD_RECORDS"]:
            self._delete_parent_shard_debtors(rows, current_ts)
        self._delete_debtors_not_activated_for_long_time(rows, current_ts)
        self._set_config_errors_if_necessary(rows, current_ts)
        db.session.close()

    def _delete_debtors_not_activated_for_long_time(self, rows, current_ts):
        c = self.table.c
        c_debtor_id = c.debtor_id
        c_status_flags = c.status_flags
        c_created_at = c.created_at
        activated_flag = Debtor.STATUS_IS_ACTIVATED_FLAG
        inactive_cutoff_ts = current_ts - self.inactive_interval

        def not_activated_for_long_time(row) -> bool:
            return (
                row[c_status_flags] & activated_flag == 0
                and row[c_created_at] < inactive_cutoff_ts
            )

        pks_to_delete = [
            (row[c_debtor_id],)
            for row in rows
            if not_activated_for_long_time(row)
        ]
        if pks_to_delete:
            db.session.execute(SET_INDEXSCAN_OFF)
            chosen = Debtor.choose_rows(pks_to_delete)
            to_delete = (
                Debtor.query
                .options(load_only(Debtor.debtor_id))
                .join(chosen, self.pk == tuple_(*chosen.c))
                .filter(
                    Debtor.status_flags.op("&")(activated_flag) == 0,
                    Debtor.created_at < inactive_cutoff_ts,
                )
                .with_for_update(skip_locked=True)
                .all()
            )
            db.session.execute(SET_INDEXSCAN_ON)

            for debtor in to_delete:
                db.session.delete(debtor)

            db.session.commit()

    def _set_config_errors_if_necessary(self, rows, current_ts):
        c = self.table.c
        c_debtor_id = c.debtor_id
        c_is_config_effectual = c.is_config_effectual
        c_has_server_account = c.has_server_account
        c_account_last_heartbeat_ts = c.account_last_heartbeat_ts
        c_config_error = c.config_error
        c_last_config_ts = c.last_config_ts
        c_status_flags = c.status_flags
        account_last_heartbeat_ts_cutoff = (
            current_ts - self.max_heartbeat_delay
        )
        last_config_ts_cutoff = current_ts - self.max_config_delay
        status_flags_mask = (
            Debtor.STATUS_IS_ACTIVATED_FLAG | Debtor.STATUS_IS_DEACTIVATED_FLAG
        )

        def has_unreported_config_problem(row) -> bool:
            return (
                (
                    not row[c_is_config_effectual]
                    or (
                        row[c_has_server_account]
                        and row[c_account_last_heartbeat_ts]
                        < account_last_heartbeat_ts_cutoff
                    )
                )
                and row[c_config_error] is None
                and row[c_last_config_ts] < last_config_ts_cutoff
                and row[c_status_flags] & status_flags_mask
                == Debtor.STATUS_IS_ACTIVATED_FLAG
            )

        pks_to_lock = [
            (row[c_debtor_id],)
            for row in rows
            if has_unreported_config_problem(row)
        ]
        if pks_to_lock:
            db.session.execute(SET_INDEXSCAN_OFF)
            chosen = Debtor.choose_rows(pks_to_lock)
            pks_to_update = [
                (row.debtor_id,)
                for row in db.session.execute(
                        select(Debtor.debtor_id)
                        .join(chosen, self.pk == tuple_(*chosen.c))
                        .where(
                            or_(
                                Debtor.is_config_effectual == false(),
                                and_(
                                    Debtor.has_server_account == true(),
                                    Debtor.account_last_heartbeat_ts
                                    < account_last_heartbeat_ts_cutoff,
                                ),
                            ),
                            Debtor.config_error == null(),
                            Debtor.last_config_ts < last_config_ts_cutoff,
                            Debtor.status_flags.op("&")(status_flags_mask)
                            == Debtor.STATUS_IS_ACTIVATED_FLAG,
                        )
                        .with_for_update(skip_locked=True, key_share=True)
                ).all()
            ]
            if pks_to_update:
                to_update = Debtor.choose_rows(pks_to_update)
                db.session.execute(
                    update(Debtor)
                    .execution_options(synchronize_session=False)
                    .where(self.pk == tuple_(*to_update.c))
                    .values(config_error="CONFIGURATION_IS_NOT_EFFECTUAL")
                )

            db.session.commit()

    def _delete_parent_shard_debtors(self, rows, current_ts):
        c = self.table.c
        c_debtor_id = c.debtor_id

        def belongs_to_parent_shard(row) -> bool:
            return not is_valid_debtor_id(
                row[c_debtor_id]
            ) and is_valid_debtor_id(row[c_debtor_id], match_parent=True)

        pks_to_delete = [
            (row[c_debtor_id],)
            for row in rows
            if belongs_to_parent_shard(row)
        ]
        if pks_to_delete:
            db.session.execute(SET_INDEXSCAN_OFF)
            chosen = Debtor.choose_rows(pks_to_delete)
            to_delete = (
                Debtor.query
                .options(load_only(Debtor.debtor_id))
                .join(chosen, self.pk == tuple_(*chosen.c))
                .with_for_update(skip_locked=True)
                .all()
            )
            db.session.execute(SET_INDEXSCAN_ON)

            for debtor in to_delete:
                db.session.delete(debtor)

            db.session.commit()
