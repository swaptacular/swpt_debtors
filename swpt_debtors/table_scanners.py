from typing import TypeVar, Callable
from datetime import datetime, timedelta, timezone
from swpt_pythonlib.scan_table import TableScanner
from sqlalchemy.sql.expression import and_, or_, null, true, false
from flask import current_app
from swpt_debtors.extensions import db
from swpt_debtors.models import Debtor, is_valid_debtor_id

T = TypeVar("T")
atomic: Callable[[T], T] = db.atomic
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
    pk = table.c.debtor_id

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
        self.deactivated_interval = timedelta(
            days=current_app.config["APP_DEACTIVATED_DEBTOR_RETENTION_DAYS"]
        )

    @property
    def blocks_per_query(self) -> int:
        return int(current_app.config["APP_DEBTORS_SCAN_BLOCKS_PER_QUERY"])

    @property
    def target_beat_duration(self) -> int:
        return int(current_app.config["APP_DEBTORS_SCAN_BEAT_MILLISECS"])

    @atomic
    def process_rows(self, rows):
        current_ts = datetime.now(tz=timezone.utc)
        if current_app.config["DELETE_PARENT_SHARD_RECORDS"]:
            self._delete_parent_shard_debtors(rows, current_ts)
        self._delete_debtors_not_activated_for_long_time(rows, current_ts)
        self._delete_dead_debtors(rows, current_ts)
        self._set_config_errors_if_necessary(rows, current_ts)

    def _delete_debtors_not_activated_for_long_time(self, rows, current_ts):
        c = self.table.c
        activated_flag = Debtor.STATUS_IS_ACTIVATED_FLAG
        inactive_cutoff_ts = current_ts - self.inactive_interval

        def not_activated_for_long_time(row) -> bool:
            return (
                row[c.status_flags] & activated_flag == 0
                and row[c.created_at] < inactive_cutoff_ts
            )

        ids_to_delete = [
            row[c.debtor_id]
            for row in rows
            if not_activated_for_long_time(row)
        ]
        if ids_to_delete:
            to_delete = (
                Debtor.query.filter(Debtor.debtor_id.in_(ids_to_delete))
                .filter(Debtor.status_flags.op("&")(activated_flag) == 0)
                .filter(Debtor.created_at < inactive_cutoff_ts)
                .with_for_update(skip_locked=True)
                .all()
            )

            for debtor in to_delete:
                db.session.delete(debtor)

            db.session.commit()

    def _set_config_errors_if_necessary(self, rows, current_ts):
        c = self.table.c
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
                    not row[c.is_config_effectual]
                    or (
                        row[c.has_server_account]
                        and row[c.account_last_heartbeat_ts]
                        < account_last_heartbeat_ts_cutoff
                    )
                )
                and row[c.config_error] is None
                and row[c.last_config_ts] < last_config_ts_cutoff
                and row[c.status_flags] & status_flags_mask
                == Debtor.STATUS_IS_ACTIVATED_FLAG
            )

        pks_to_set = [
            row[c.debtor_id]
            for row in rows
            if has_unreported_config_problem(row)
        ]
        if pks_to_set:
            to_update = (
                db.session.query(Debtor.debtor_id)
                .filter(self.pk.in_(pks_to_set))
                .filter(
                    or_(
                        Debtor.is_config_effectual == false(),
                        and_(
                            Debtor.has_server_account == true(),
                            Debtor.account_last_heartbeat_ts
                            < account_last_heartbeat_ts_cutoff,
                        ),
                    )
                )
                .filter(Debtor.config_error == null())
                .filter(Debtor.last_config_ts < last_config_ts_cutoff)
                .filter(
                    Debtor.status_flags.op("&")(status_flags_mask)
                    == Debtor.STATUS_IS_ACTIVATED_FLAG
                )
                .with_for_update(skip_locked=True)
                .all()
            )

            if to_update:
                pks_to_update = [row[0] for row in to_update]
                Debtor.query.filter(self.pk.in_(pks_to_update)).update(
                    {Debtor.config_error: "CONFIGURATION_IS_NOT_EFFECTUAL"},
                    synchronize_session=False,
                )

            db.session.commit()

    def _delete_dead_debtors(self, rows, current_ts):
        c = self.table.c
        deactivated_flag = Debtor.STATUS_IS_DEACTIVATED_FLAG
        deactivated_cutoff_date = (
            current_ts - self.deactivated_interval
        ).date()

        def is_dead_debtor(row) -> bool:
            return (
                row[c.status_flags] & deactivated_flag != 0
                and not row[c.has_server_account]
                and (
                    row[c.deactivation_date] is None
                    or row[c.deactivation_date] < deactivated_cutoff_date
                )
            )

        ids_to_delete = [
            row[c.debtor_id] for row in rows if is_dead_debtor(row)
        ]
        if ids_to_delete:
            to_delete = (
                Debtor.query.filter(Debtor.debtor_id.in_(ids_to_delete))
                .filter(Debtor.status_flags.op("&")(deactivated_flag) != 0)
                .filter(Debtor.has_server_account == false())
                .filter(
                    or_(
                        Debtor.deactivation_date == null(),
                        Debtor.deactivation_date < deactivated_cutoff_date,
                    ),
                )
                .with_for_update(skip_locked=True)
                .all()
            )

            for debtor in to_delete:
                db.session.delete(debtor)

            db.session.commit()

    def _delete_parent_shard_debtors(self, rows, current_ts):
        c = self.table.c

        def belongs_to_parent_shard(row) -> bool:
            return not is_valid_debtor_id(
                row[c.debtor_id]
            ) and is_valid_debtor_id(row[c.debtor_id], match_parent=True)

        ids_to_delete = [
            row[c.debtor_id] for row in rows if belongs_to_parent_shard(row)
        ]
        if ids_to_delete:
            to_delete = (
                Debtor.query.filter(Debtor.debtor_id.in_(ids_to_delete))
                .with_for_update(skip_locked=True)
                .all()
            )

            for debtor in to_delete:
                db.session.delete(debtor)

            db.session.commit()
