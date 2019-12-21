from datetime import datetime, timedelta, timezone
from swpt_lib.scan_table import TableScanner
from sqlalchemy.sql.expression import tuple_
from .extensions import db
from .models import RunningTransfer, Account


class RunningTransfersCollector(TableScanner):
    table = RunningTransfer.__table__
    columns = [RunningTransfer.debtor_id, RunningTransfer.transfer_uuid, RunningTransfer.finalized_at_ts]

    def __init__(self, days):
        super().__init__()
        self.days = days
        self.pk = tuple_(self.table.c.debtor_id, self.table.c.transfer_uuid)

    def process_rows(self, rows):
        cutoff_ts = datetime.now(tz=timezone.utc) - timedelta(days=self.days)
        pks_to_delete = [(row[0], row[1]) for row in rows if row[2] < cutoff_ts]
        db.engine.execute(self.table.delete().where(self.pk.in_(pks_to_delete)))


class AccountsScanner(TableScanner):
    table = Account.__table__

    def __init__(self, days):
        super().__init__()
        self.days = days

    def process_rows(self, rows):
        # TODO: Put a real implementation.
        pass
