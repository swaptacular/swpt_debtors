import math
from decimal import Decimal
from typing import NamedTuple, Dict, List, TypeVar, Callable
from datetime import datetime, timedelta, timezone
from swpt_lib.scan_table import TableScanner
from sqlalchemy.sql.expression import tuple_
from flask import current_app
from swpt_debtors.extensions import db
from swpt_debtors.models import Debtor, Account, CapitalizeInterestSignal, ChangeInterestRateSignal, \
    TryToDeleteAccountSignal, MAX_INT64, ROOT_CREDITOR_ID, TS0

T = TypeVar('T')
atomic: Callable[[T], T] = db.atomic
SECONDS_IN_YEAR = 365.25 * 24 * 60 * 60

# TODO: Make `TableScanner.blocks_per_query` and
#       `TableScanner.target_beat_duration` configurable.


class CachedInterestRate(NamedTuple):
    interest_rate: float
    timestamp: datetime


class AccountsScanner(TableScanner):
    """Executes accounts maintenance operations."""

    table = Account.__table__
    old_interest_rate = CachedInterestRate(0.0, TS0)
    pk = tuple_(Account.debtor_id, Account.creditor_id)

    def __init__(self, hours: float):
        super().__init__()
        self.interval = timedelta(hours=hours)
        self.signalbus_max_delay = timedelta(days=current_app.config['APP_SIGNALBUS_MAX_DELAY_DAYS'])
        self.interest_rate_change_min_interval = Account.get_interest_rate_change_min_interval()
        self.dead_accounts_abandon_delay = timedelta(days=current_app.config['APP_DEAD_ACCOUNTS_ABANDON_DAYS'])
        self.max_interest_to_principal_ratio = current_app.config['APP_MAX_INTEREST_TO_PRINCIPAL_RATIO']
        self.account_unmute_interval = 2 * self.signalbus_max_delay
        self.deletion_attempts_min_interval = max(
            timedelta(days=current_app.config['APP_DELETION_ATTEMPTS_MIN_DAYS']),
            self.signalbus_max_delay,
        )
        self.min_interest_cap_interval = max(
            timedelta(days=current_app.config['APP_MIN_INTEREST_CAPITALIZATION_DAYS']),

            # We want to avoid capitalizing interests on each table
            # scan, because this could prevent other important
            # maintenance actions from taking place.
            4 * self.interval,
        )
        self.debtor_interest_rates: Dict[int, CachedInterestRate] = {}

    def _calc_current_balance(self, row, current_ts) -> Decimal:
        c = self.table.c
        assert row[c.creditor_id] != ROOT_CREDITOR_ID
        current_balance = row[c.principal] + Decimal.from_float(row[c.interest])
        if current_balance > 0:
            k = math.log(1.0 + row[c.interest_rate] / 100.0) / SECONDS_IN_YEAR
            passed_seconds = max(0.0, (current_ts - row[c.last_change_ts]).total_seconds())
            current_balance *= Decimal.from_float(math.exp(k * passed_seconds))
        return current_balance

    def _calc_accumulated_interest(self, row, current_ts) -> int:
        c = self.table.c
        current_balance = self._calc_current_balance(row, current_ts)
        accumulated_interest = math.floor(current_balance - row[c.principal])
        accumulated_interest = min(accumulated_interest, MAX_INT64)
        accumulated_interest = max(-MAX_INT64, accumulated_interest)
        return accumulated_interest

    def _separate_accounts_by_type(self, rows):
        """Separate `rows` in three categories: regular accounts, deleted
        accounts, and debtors' accounts.

        """

        c = self.table.c
        regular_account_rows = []
        deleted_account_rows = []
        debtor_account_rows = []
        deleted_flag = Account.STATUS_DELETED_FLAG
        for row in rows:
            if row[c.status_flags] & deleted_flag:
                deleted_account_rows.append(row)
            elif row[c.creditor_id] == ROOT_CREDITOR_ID:
                debtor_account_rows.append(row)
            else:
                regular_account_rows.append(row)
        return regular_account_rows, deleted_account_rows, debtor_account_rows

    def _remove_muted_accounts(self, rows, current_ts):
        """Return the list of passed `rows`, but with the rows referring to
        "muted" accounts removed.

        """

        c = self.table.c
        c_is_muted, c_last_maintenance_request_ts = c.is_muted, c.last_maintenance_request_ts
        mute_cutoff_ts = current_ts - self.account_unmute_interval
        last_request_cutoff_ts = current_ts - self.interval / 10
        return [
            row for row in rows if (
                (not row[c_is_muted] or row[c_last_maintenance_request_ts] < mute_cutoff_ts)

                # To avoid sending more than one maintenance signal
                # for a given account during a single table scan, we
                # ensure that the last maintenance request was far
                # enough in the past.
                and row[c_last_maintenance_request_ts] < last_request_cutoff_ts
            )
        ]

    def _remove_pks(self, rows, pks):
        """Return the list of passed `rows`, but with the rows referred in
        `pks` removed. (`pks` is a set of primary keys.)

        """

        c = self.table.c
        c_debtor_id, c_creditor_id = c.debtor_id, c.creditor_id
        return [row for row in rows if (row[c_debtor_id], row[c_creditor_id]) not in pks]

    def _get_debtor_interest_rates(self, debtor_ids: List[int], current_ts: datetime) -> List[float]:
        """Return a list of interest rates, corresponding to the passed list
        of debtor IDs. Try to minimize database access by caching the
        interest rates.

        """

        cutoff_ts = current_ts - self.interval
        rates = self.debtor_interest_rates
        old_rate = self.old_interest_rate
        old_rate_debtor_ids = [x for x in debtor_ids if rates.get(x, old_rate).timestamp < cutoff_ts]
        if old_rate_debtor_ids:
            for debtor in Debtor.query.filter(Debtor.debtor_id.in_(old_rate_debtor_ids)):
                rates[debtor.debtor_id] = CachedInterestRate(debtor.interest_rate, current_ts)
        return [rates.get(x, old_rate).interest_rate for x in debtor_ids]

    def _check_interest_rates(self, rows, current_ts):
        """Send `ChangeInterestRateSignal` if necessary. Return a set of
        primary keys, for the accounts for which a signal has been
        sent.

        """

        pks = set()
        c = self.table.c
        debtor_ids = [row[c.debtor_id] for row in rows]
        interest_rates = self._get_debtor_interest_rates(debtor_ids, current_ts)
        established_rate_flag = Account.STATUS_ESTABLISHED_INTEREST_RATE_FLAG
        cutoff_ts = current_ts - self.interest_rate_change_min_interval
        for row, interest_rate in zip(rows, interest_rates):
            has_established_interest_rate = row[c.status_flags] & established_rate_flag
            has_incorrect_interest_rate = not has_established_interest_rate or row[c.interest_rate] != interest_rate
            if row[c.last_interest_rate_change_ts] < cutoff_ts and has_incorrect_interest_rate:
                pk = (row[c.debtor_id], row[c.creditor_id])
                pks.add(pk)
                db.session.add(ChangeInterestRateSignal(
                    debtor_id=pk[0],
                    creditor_id=pk[1],
                    interest_rate=interest_rate,
                    request_ts=current_ts,
                ))
        return pks

    def _check_accumulated_interests(self, rows, current_ts):
        """Send `CapitalizeInterestSignal` if necessary. Return a set of
        primary keys, for the accounts for which a signal has been
        sent.

        """

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
                    request_ts=current_ts,
                ))
        return pks

    def _check_scheduled_for_deletion(self, rows, current_ts):
        """Send `TryToDeleteAccountSignal` if necessary. Return a set of
        primary keys, for the accounts for which a signal has been
        sent.

        """

        pks = set()
        c = self.table.c
        scheduled_for_deletion_flag = Account.CONFIG_SCHEDULED_FOR_DELETION_FLAG
        cutoff_ts = current_ts - self.deletion_attempts_min_interval
        for row in rows:
            if row[c.last_deletion_attempt_ts] > cutoff_ts:
                continue
            if (row[c.config_flags] & scheduled_for_deletion_flag
                    and self._calc_current_balance(row, current_ts) <= max(2.0, row[c.negligible_amount])):
                pk = (row[c.debtor_id], row[c.creditor_id])
                pks.add(pk)
                db.session.add(TryToDeleteAccountSignal(
                    debtor_id=pk[0],
                    creditor_id=pk[1],
                    request_ts=current_ts,
                ))
        return pks

    def _mute_accounts(self, pks_to_mute, current_ts, capitalized=False, for_deletion=False):
        """Mute the accounts refered by `pks_to_mute`."""

        if pks_to_mute:
            new_values = {
                Account.is_muted: True,
                Account.last_maintenance_request_ts: current_ts,
            }
            if capitalized:
                new_values[Account.last_interest_capitalization_ts] = current_ts
            if for_deletion:
                new_values[Account.last_deletion_attempt_ts] = current_ts
            Account.query.filter(self.pk.in_(pks_to_mute)).update(new_values, synchronize_session=False)

    def _purge_dead_accounts(self, rows, current_ts):
        """Delete accounts which have not received a heartbeat for a very long
        time. Returns the list of passed `rows`, but with the rows for
        the purged accounts removed.

        """

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
            pks_to_purge = set(pks_to_purge)
            rows = [row for row in rows if ((row[c_debtor_id], row[c_creditor_id]) not in pks_to_purge)]
        return rows

    @atomic
    def process_rows(self, rows):
        """Send account maintenance operation requests if necessary.

        We must send maintenance operation requests only for alive,
        regular accounts, which are not "muted". Also, we want to send
        at most one maintenance operation request per account.

        NOTE: We want the `ChangeInterestRateSignal` to have the
              lowest priority, so that it does not prevent other more
              important maintenance actions from taking place.

        """

        current_ts = datetime.now(tz=timezone.utc)
        alive_rows = self._purge_dead_accounts(rows, current_ts)
        regular_rows, _, _ = self._separate_accounts_by_type(alive_rows)
        nonmuted_regular_rows = self._remove_muted_accounts(regular_rows, current_ts)

        # 1) Send `TryToDeleteAccountSignal`s:
        for_deletion_pks = self._check_scheduled_for_deletion(nonmuted_regular_rows, current_ts)
        nonmuted_regular_rows = self._remove_pks(nonmuted_regular_rows, for_deletion_pks)

        # 2) Send `CapitalizeInterestSignal`s:
        capitalized_pks = self._check_accumulated_interests(nonmuted_regular_rows, current_ts)
        nonmuted_regular_rows = self._remove_pks(nonmuted_regular_rows, capitalized_pks)

        # 3) Send `ChangeInterestRateSignal`s:
        changed_rate_pks = self._check_interest_rates(nonmuted_regular_rows, current_ts)

        # TODO: Try to execute the three updates in one database
        #       round-trip. Also, use bulk-inserts for the maintenance
        #       operation requests.
        self._mute_accounts(for_deletion_pks, current_ts, for_deletion=True)
        self._mute_accounts(capitalized_pks, current_ts, capitalized=True)
        self._mute_accounts(changed_rate_pks, current_ts)


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
