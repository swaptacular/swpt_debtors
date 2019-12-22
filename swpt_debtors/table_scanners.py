from typing import NamedTuple, Dict, List, TypeVar, Callable
from datetime import datetime, timedelta, timezone
from swpt_lib.scan_table import TableScanner
from sqlalchemy.sql.expression import tuple_
from flask import current_app
from .extensions import db
from .models import Debtor, RunningTransfer, Account, PurgeDeletedAccountSignal

T = TypeVar('T')
atomic: Callable[[T], T] = db.atomic


class CachedInterestRate(NamedTuple):
    interest_rate: float
    timestamp: datetime


class RunningTransfersCollector(TableScanner):
    table = RunningTransfer.__table__
    columns = [RunningTransfer.debtor_id, RunningTransfer.transfer_uuid, RunningTransfer.finalized_at_ts]

    def __init__(self):
        super().__init__()
        self.signalbus_max_delay = timedelta(days=current_app.config['APP_SIGNALBUS_MAX_DELAY_DAYS'])
        self.pk = tuple_(self.table.c.debtor_id, self.table.c.transfer_uuid)

    def process_rows(self, rows):
        cutoff_ts = datetime.now(tz=timezone.utc) - self.signalbus_max_delay
        pks_to_delete = [(row[0], row[1]) for row in rows if row[2] < cutoff_ts]
        if pks_to_delete:
            db.engine.execute(self.table.delete().where(self.pk.in_(pks_to_delete)))


class AccountsScanner(TableScanner):
    table = Account.__table__
    old_interest_rate = CachedInterestRate(0.0, datetime(1900, 1, 1))
    pk = tuple_(Account.debtor_id, Account.creditor_id)

    def __init__(self, days: float):
        super().__init__()
        self.interval = timedelta(days=days)
        self.signalbus_max_delay = timedelta(days=current_app.config['APP_SIGNALBUS_MAX_DELAY_DAYS'])
        self.debtor_interest_rates: Dict[int, CachedInterestRate] = {}

    def _get_debtor_interest_rates(self, debtor_ids: List[int], current_ts) -> List[float]:
        cutoff_ts = current_ts - self.interval
        rates = self.debtor_interest_rates
        old_rate = self.old_interest_rate
        old_rate_debtor_ids = [x for x in debtor_ids if rates.get(x, old_rate).timestamp < cutoff_ts]
        if old_rate_debtor_ids:
            for debtor in Debtor.query.filter(Debtor.debtor_id.in_(old_rate_debtor_ids)):
                rates[debtor.debtor_id] = CachedInterestRate(debtor.interest_rate, current_ts)
        return [rates.get(x, old_rate).interest_rate for x in debtor_ids]

    @atomic
    def check_interest_rate(self, rows, current_ts):
        pass

    @atomic
    def check_accumulated_interest(self, rows, current_ts):
        pass

    @atomic
    def check_negative_balance(self, rows, current_ts):
        pass

    @atomic
    def check_if_deleted(self, rows, current_ts):
        c = self.table.c
        cutoff_ts = current_ts - self.signalbus_max_delay
        deleted_flag = Account.STATUS_DELETED_FLAG
        pks_to_purge = [
            (row[c.debtor_id], row[c.creditor_id])
            for row in rows
            if row[c.status] & deleted_flag and row[c.change_ts] < cutoff_ts
        ]
        if pks_to_purge:
            for pk in pks_to_purge:
                db.session.add(PurgeDeletedAccountSignal(
                    debtor_id=pk[0],
                    creditor_id=pk[1],
                    if_deleted_before=cutoff_ts,
                ))
            Account.query.filter(self.pk.in_(pks_to_purge)).delete(synchronize_session=False)

    def process_rows(self, rows):
        current_ts = datetime.now(tz=timezone.utc)
        self.check_interest_rate(rows, current_ts)
        self.check_accumulated_interest(rows, current_ts)
        self.check_negative_balance(rows, current_ts)
        self.check_if_deleted(rows, current_ts)
