import math
from decimal import Decimal
from typing import NamedTuple, Dict, List, TypeVar, Callable
from datetime import datetime, timedelta, timezone
from swpt_lib.scan_table import TableScanner
from sqlalchemy.sql.expression import tuple_
from flask import current_app
from .extensions import db
from .models import Debtor, RunningTransfer, Account, PurgeDeletedAccountSignal, CapitalizeInterestSignal, \
    ZeroOutNegativeBalanceSignal, MAX_INT64, ROOT_CREDITOR_ID
from .procedures import insert_change_interest_rate_signal

T = TypeVar('T')
atomic: Callable[[T], T] = db.atomic
SECONDS_IN_YEAR = 365.25 * 24 * 60 * 60
TD_ONE_WEEK = timedelta(days=7)


class CachedInterestRate(NamedTuple):
    interest_rate: float
    timestamp: datetime


class RunningTransfersCollector(TableScanner):
    table = RunningTransfer.__table__
    columns = [RunningTransfer.debtor_id, RunningTransfer.transfer_uuid, RunningTransfer.finalized_at_ts]
    pk = tuple_(table.c.debtor_id, table.c.transfer_uuid)

    def __init__(self):
        super().__init__()
        self.signalbus_max_delay = timedelta(days=current_app.config['APP_SIGNALBUS_MAX_DELAY_DAYS'])

    def process_rows(self, rows):
        cutoff_ts = datetime.now(tz=timezone.utc) - self.signalbus_max_delay
        pks_to_delete = [(row[0], row[1]) for row in rows if row[2] < cutoff_ts]
        if pks_to_delete:
            db.engine.execute(self.table.delete().where(self.pk.in_(pks_to_delete)))


class AccountsScanner(TableScanner):
    table = Account.__table__
    old_interest_rate = CachedInterestRate(0.0, datetime(1900, 1, 1, tzinfo=timezone.utc))
    pk = tuple_(Account.debtor_id, Account.creditor_id)

    def __init__(self, days: float):
        super().__init__()
        self.interval = timedelta(days=days)
        self.signalbus_max_delay = timedelta(days=current_app.config['APP_SIGNALBUS_MAX_DELAY_DAYS'])
        self.zero_out_negative_balance_delay = timedelta(days=current_app.config['APP_ZERO_OUT_NEGATIVE_BALANCE_DAYS'])
        self.debtor_interest_rates: Dict[int, CachedInterestRate] = {}

    def _separate_accounts_by_type(self, rows):
        c = self.table.c
        regular_account_rows = []
        deleted_account_rows = []
        debtor_account_rows = []
        deleted_flag = Account.STATUS_DELETED_FLAG
        for row in rows:
            if row[c.status] & deleted_flag:
                deleted_account_rows.append(row)
            elif row[c.creditor_id] == ROOT_CREDITOR_ID:
                debtor_account_rows.append(row)
            else:
                regular_account_rows.append(row)
        return regular_account_rows, deleted_account_rows, debtor_account_rows

    def _remove_muted_accounts(self, rows, current_ts):
        muted_until_ts = self.table.c.do_not_send_signals_until_ts
        return [row for row in rows if row[muted_until_ts] is None or row[muted_until_ts] <= current_ts]

    def _get_debtor_interest_rates(self, debtor_ids: List[int], current_ts: datetime) -> List[float]:
        cutoff_ts = current_ts - self.interval
        rates = self.debtor_interest_rates
        old_rate = self.old_interest_rate
        old_rate_debtor_ids = [x for x in debtor_ids if rates.get(x, old_rate).timestamp < cutoff_ts]
        if old_rate_debtor_ids:
            for debtor in Debtor.query.filter(Debtor.debtor_id.in_(old_rate_debtor_ids)):
                rates[debtor.debtor_id] = CachedInterestRate(debtor.interest_rate, current_ts)
        return [rates.get(x, old_rate).interest_rate for x in debtor_ids]

    def _calc_current_balance(self, row, current_ts) -> Decimal:
        c = self.table.c
        assert row[c.creditor_id] != ROOT_CREDITOR_ID
        current_balance = row[c.principal] + Decimal.from_float(row[c.interest])
        if current_balance > 0:
            k = math.log(1.0 + row[c.interest_rate] / 100.0) / SECONDS_IN_YEAR
            passed_seconds = max(0.0, (current_ts - row[c.change_ts]).total_seconds())
            current_balance *= Decimal.from_float(math.exp(k * passed_seconds))
        return current_balance

    def _calc_accumulated_interest(self, row, current_ts) -> int:
        c = self.table.c
        current_balance = self._calc_current_balance(row, current_ts)
        accumulated_interest = math.floor(current_balance - row[c.principal])
        accumulated_interest = min(accumulated_interest, MAX_INT64)
        accumulated_interest = max(-MAX_INT64, accumulated_interest)
        return accumulated_interest

    def _check_interest_rates(self, rows, current_ts):
        pks = []
        c = self.table.c
        debtor_ids = [row[c.debtor_id] for row in rows]
        interest_rates = self._get_debtor_interest_rates(debtor_ids, current_ts)
        established_rate_flag = Account.STATUS_ESTABLISHED_INTEREST_RATE_FLAG
        for row, interest_rate in zip(rows, interest_rates):
            has_correct_interest_rate = row[c.status] & established_rate_flag and row[c.interest_rate] == interest_rate
            if not has_correct_interest_rate:
                pk = (row[c.debtor_id], row[c.creditor_id])
                pks.append(pk)
                insert_change_interest_rate_signal(pk[0], pk[1], interest_rate)
        return pks

    def _check_accumulated_interests(self, rows, current_ts):
        pks = []
        c = self.table.c
        max_ratio = current_app.config['APP_MAX_INTEREST_TO_PRINCIPAL_RATIO']
        for row in rows:
            accumulated_interest = self._calc_accumulated_interest(row, current_ts)
            ratio = abs(accumulated_interest) / (1 + abs(row[c.principal]))
            if ratio > max_ratio:
                pk = (row[c.debtor_id], row[c.creditor_id])
                pks.append(pk)
                db.session.add(CapitalizeInterestSignal(
                    debtor_id=pk[0],
                    creditor_id=pk[1],
                    accumulated_interest_threshold=accumulated_interest // 2,
                ))
        return pks

    def _check_negative_balances(self, rows, current_ts):
        pks = []
        c = self.table.c
        cutoff_date = (current_ts - self.zero_out_negative_balance_delay).date()
        for row in rows:
            balance = math.floor(self._calc_current_balance(row, current_ts))
            transfer_date = row[c.last_outgoing_transfer_date]
            transfer_date_is_old = transfer_date is None or transfer_date <= cutoff_date
            if balance < 0 and transfer_date_is_old:
                pk = (row[c.debtor_id], row[c.creditor_id])
                pks.append(pk)
                db.session.add(ZeroOutNegativeBalanceSignal(
                    debtor_id=pk[0],
                    creditor_id=pk[1],
                    last_outgoing_transfer_date=cutoff_date,
                ))
        return pks

    def _mute_accounts(self, pks_to_mute, current_ts):
        if pks_to_mute:
            Account.query.filter(self.pk.in_(pks_to_mute)).update({
                Account.do_not_send_signals_until_ts: current_ts + self.signalbus_max_delay,
            }, synchronize_session=False)

    def _purge_not_recently_changed(self, rows, current_ts):
        c = self.table.c
        cutoff_ts = current_ts - max(self.signalbus_max_delay, TD_ONE_WEEK)
        pks_to_purge = [(row[c.debtor_id], row[c.creditor_id]) for row in rows if row[c.change_ts] < cutoff_ts]
        if pks_to_purge:
            for pk in pks_to_purge:
                db.session.add(PurgeDeletedAccountSignal(
                    debtor_id=pk[0],
                    creditor_id=pk[1],
                    if_deleted_before=cutoff_ts,
                ))
            Account.query.filter(self.pk.in_(pks_to_purge)).delete(synchronize_session=False)

    @atomic
    def process_rows(self, rows):
        current_ts = datetime.now(tz=timezone.utc)
        regular_account_rows, deleted_account_rows, _ = self._separate_accounts_by_type(rows)
        regular_account_rows = self._remove_muted_accounts(regular_account_rows, current_ts)
        pks_to_mute = []
        pks_to_mute.extend(self._check_interest_rates(regular_account_rows, current_ts))
        pks_to_mute.extend(self._check_accumulated_interests(regular_account_rows, current_ts))
        pks_to_mute.extend(self._check_negative_balances(regular_account_rows, current_ts))
        self._mute_accounts(pks_to_mute, current_ts)
        self._purge_not_recently_changed(deleted_account_rows, current_ts)
