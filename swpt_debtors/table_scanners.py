from typing import NamedTuple, TypeVar, Callable
from datetime import datetime, timedelta, timezone
from swpt_lib.scan_table import TableScanner
from sqlalchemy.sql.expression import tuple_
from flask import current_app
from swpt_debtors.extensions import db
from swpt_debtors.models import Debtor

T = TypeVar('T')
atomic: Callable[[T], T] = db.atomic
SECONDS_IN_YEAR = 365.25 * 24 * 60 * 60

# TODO: Make `TableScanner.blocks_per_query` and
#       `TableScanner.target_beat_duration` configurable.


class CachedInterestRate(NamedTuple):
    interest_rate: float
    timestamp: datetime


class DebtorScanner(TableScanner):
    """Garbage-collects inactive debtors."""

    table = Debtor.__table__
    columns = [Debtor.debtor_id, Debtor.created_at, Debtor.status_flags]
    pk = tuple_(table.c.debtor_id,)

    def __init__(self):
        super().__init__()
        self.inactive_interval = timedelta(days=current_app.config['APP_INACTIVE_DEBTOR_RETENTION_DAYS'])

    @property
    def blocks_per_query(self) -> int:
        return int(current_app.config['APP_DEBTORS_SCAN_BLOCKS_PER_QUERY'])

    @property
    def target_beat_duration(self) -> int:
        return int(current_app.config['APP_DEBTORS_SCAN_BEAT_MILLISECS'])

    @atomic
    def process_rows(self, rows):
        current_ts = datetime.now(tz=timezone.utc)
        self._delete_debtors_not_activated_for_long_time(rows, current_ts)

    def _delete_debtors_not_activated_for_long_time(self, rows, current_ts):
        c = self.table.c
        activated_flag = Debtor.STATUS_IS_ACTIVATED_FLAG
        inactive_cutoff_ts = current_ts - self.inactive_interval

        def not_activated_for_long_time(row) -> bool:
            return (
                row[c.status_flags] & activated_flag == 0
                and row[c.created_at] < inactive_cutoff_ts
            )

        ids_to_delete = [row[c.debtor_id] for row in rows if not_activated_for_long_time(row)]
        if ids_to_delete:
            to_delete = Debtor.query.\
                filter(Debtor.debtor_id.in_(ids_to_delete)).\
                filter(Debtor.status_flags.op('&')(activated_flag) == 0).\
                filter(Debtor.created_at < inactive_cutoff_ts).\
                with_for_update(skip_locked=True).\
                all()

            for debtor in to_delete:
                db.session.delete(debtor)
