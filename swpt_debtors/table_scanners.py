import math
from decimal import Decimal
from typing import NamedTuple, Dict, List, TypeVar, Callable
from datetime import datetime, timedelta, timezone
from swpt_lib.scan_table import TableScanner
from sqlalchemy.sql.expression import tuple_
from flask import current_app
from .extensions import db
from .models import Debtor, RunningTransfer, Account, CapitalizeInterestSignal, ChangeInterestRateSignal, \
    ZeroOutNegativeBalanceSignal, TryToDeleteAccountSignal, MAX_INT64, ROOT_CREDITOR_ID

T = TypeVar('T')
atomic: Callable[[T], T] = db.atomic
SECONDS_IN_YEAR = 365.25 * 24 * 60 * 60


class CachedInterestRate(NamedTuple):
    interest_rate: float
    timestamp: datetime


class RunningTransfersCollector(TableScanner):
    table = RunningTransfer.__table__
    columns = [RunningTransfer.debtor_id, RunningTransfer.transfer_uuid, RunningTransfer.started_at_ts]
    pk = tuple_(table.c.debtor_id, table.c.transfer_uuid)

    def __init__(self):
        super().__init__()
        self.abandon_interval = timedelta(days=current_app.config['APP_RUNNING_TRANSFERS_ABANDON_DAYS'])

    def process_rows(self, rows):
        cutoff_ts = datetime.now(tz=timezone.utc) - self.abandon_interval
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
        self.pending_transfers_max_delay = timedelta(days=current_app.config['APP_PENDING_TRANSFERS_MAX_DELAY_DAYS'])
        self.account_purge_delay = 2 * self.signalbus_max_delay + self.pending_transfers_max_delay
        self.zero_out_negative_balance_delay = timedelta(days=current_app.config['APP_ZERO_OUT_NEGATIVE_BALANCE_DAYS'])
        self.dead_accounts_abandon_delay = timedelta(days=current_app.config['APP_DEAD_ACCOUNTS_ABANDON_DAYS'])
        self.min_interest_cap_interval = max(
            timedelta(days=current_app.config['APP_MIN_INTEREST_CAPITALIZATION_DAYS']),
            self.signalbus_max_delay,
        )
        self.max_interest_to_principal_ratio = current_app.config['APP_MAX_INTEREST_TO_PRINCIPAL_RATIO']
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
        pks = set()
        c = self.table.c
        debtor_ids = [row[c.debtor_id] for row in rows]
        interest_rates = self._get_debtor_interest_rates(debtor_ids, current_ts)
        established_rate_flag = Account.STATUS_ESTABLISHED_INTEREST_RATE_FLAG
        for row, interest_rate in zip(rows, interest_rates):
            has_correct_interest_rate = row[c.status] & established_rate_flag and row[c.interest_rate] == interest_rate
            if not has_correct_interest_rate:
                pk = (row[c.debtor_id], row[c.creditor_id])
                pks.add(pk)
                db.session.add(ChangeInterestRateSignal(
                    debtor_id=pk[0],
                    creditor_id=pk[1],
                    interest_rate=interest_rate,
                ))
        return pks

    def _check_accumulated_interests(self, rows, current_ts):
        pks = set()
        c = self.table.c
        max_ratio = self.max_interest_to_principal_ratio
        cutoff_ts = current_ts - self.min_interest_cap_interval
        for row in rows:
            if row[c.last_interest_capitalization_ts] > cutoff_ts:
                continue
            accumulated_interest = self._calc_accumulated_interest(row, current_ts)
            ratio = abs(accumulated_interest) / (1 + abs(row[c.principal]))
            if abs(accumulated_interest) > 1 and ratio > max_ratio:
                pk = (row[c.debtor_id], row[c.creditor_id])
                pks.add(pk)
                db.session.add(CapitalizeInterestSignal(
                    debtor_id=pk[0],
                    creditor_id=pk[1],
                    accumulated_interest_threshold=accumulated_interest // 2,
                ))
        return pks

    def _check_negative_balances(self, rows, current_ts):
        pks = set()
        c = self.table.c
        cutoff_date = (current_ts - self.zero_out_negative_balance_delay).date()
        for row in rows:
            balance = math.floor(self._calc_current_balance(row, current_ts))
            transfer_date = row[c.last_outgoing_transfer_date]
            transfer_date_is_old = transfer_date is None or transfer_date <= cutoff_date
            if balance < 0 and transfer_date_is_old:
                pk = (row[c.debtor_id], row[c.creditor_id])
                pks.add(pk)
                db.session.add(ZeroOutNegativeBalanceSignal(
                    debtor_id=pk[0],
                    creditor_id=pk[1],
                    last_outgoing_transfer_date=cutoff_date,
                ))
        return pks

    def _check_scheduled_for_deletion(self, rows, current_ts):
        pks = set()
        c = self.table.c
        scheduled_for_deletion_flag = Account.STATUS_SCHEDULED_FOR_DELETION_FLAG
        for row in rows:
            if (row[c.status] & scheduled_for_deletion_flag
                    and 0 <= self._calc_current_balance(row, current_ts) <= row[c.negligible_amount]):
                pk = (row[c.debtor_id], row[c.creditor_id])
                pks.add(pk)
                db.session.add(TryToDeleteAccountSignal(
                    debtor_id=pk[0],
                    creditor_id=pk[1],
                ))
        return pks

    def _mute_accounts(self, pks_to_mute, current_ts, capitalized=False):
        if pks_to_mute:
            new_values = {Account.do_not_send_signals_until_ts: current_ts + self.signalbus_max_delay}
            if capitalized:
                new_values[Account.last_interest_capitalization_ts] = current_ts
            Account.query.filter(self.pk.in_(pks_to_mute)).update(new_values, synchronize_session=False)

    def _purge_dead_accounts(self, rows, current_ts):
        c = self.table.c
        cutoff_ts = current_ts - self.dead_accounts_abandon_delay
        c_debtor_id, c_creditor_id, c_last_heartbeat_ts = c.debtor_id, c.creditor_id, c.last_heartbeat_ts
        pks_to_purge = [(row[c_debtor_id], row[c_creditor_id]) for row in rows if row[c_last_heartbeat_ts] < cutoff_ts]
        if pks_to_purge:
            pks_to_purge = db.session.\
                query(Account.debtor_id, Account.creditor_id).\
                filter(self.pk.in_(pks_to_purge)).\
                filter(Account.last_heartbeat_ts < cutoff_ts).\
                with_for_update().\
                all()
            Account.query.filter(self.pk.in_(pks_to_purge)).delete(synchronize_session=False)
            self._delete_debtors_with_purged_debtor_accounts(pks_to_purge)
            pks_to_purge = set(pks_to_purge)
            rows = [row for row in rows if ((row[c_debtor_id], row[c_creditor_id]) not in pks_to_purge)]
        return rows

    def _delete_debtors_with_purged_debtor_accounts(self, purged_account_pks):
        debtors_ids = [debtor_id for debtor_id, creditor_id in purged_account_pks if creditor_id == ROOT_CREDITOR_ID]
        if debtors_ids:
            Debtor.query.filter(Debtor.debtor_id.in_(debtors_ids)).delete(synchronize_session=False)

    @atomic
    def process_rows(self, rows):
        current_ts = datetime.now(tz=timezone.utc)
        alive_rows = self._purge_dead_accounts(rows, current_ts)
        regular_rows, _, _ = self._separate_accounts_by_type(alive_rows)
        nonmuted_regular_rows = self._remove_muted_accounts(regular_rows, current_ts)
        pks_to_mute = set()
        pks_to_mute |= self._check_interest_rates(nonmuted_regular_rows, current_ts)
        pks_to_mute |= self._check_negative_balances(nonmuted_regular_rows, current_ts)
        pks_to_mute |= self._check_scheduled_for_deletion(nonmuted_regular_rows, current_ts)
        capitalized_pks = self._check_accumulated_interests(nonmuted_regular_rows, current_ts)

        # All muted accounts will be un-muted when the triggered
        # `AccountChangeSignal` is processed.
        self._mute_accounts(capitalized_pks, current_ts, capitalized=True)
        self._mute_accounts(pks_to_mute - capitalized_pks, current_ts, capitalized=False)
