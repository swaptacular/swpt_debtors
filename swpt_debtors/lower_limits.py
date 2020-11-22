from __future__ import annotations
from numbers import Real
from typing import NamedTuple, List, Tuple, Optional, Iterable, Sequence
from datetime import date
from collections import abc
from flask import current_app


class TooLongLimitSequence(Exception):
    """Exceeded the maximum allowed length for limit sequences."""


class LowerLimit(NamedTuple):
    """A numerical lower limit that should be enforced until a given date."""

    value: Real  # the limiting value
    cutoff: date  # the limit will stop to be enforced *after* this date


class LowerLimitSequence(abc.Sequence):
    """A sequence of `LowerLimit`s."""

    _limits: List[LowerLimit]

    def __init__(self, limits: Iterable[LowerLimit] = []):
        self._limits = list(limits)

    def __getitem__(self, index):
        return self._limits[index]

    def __len__(self):
        return len(self._limits)

    def __repr__(self):
        return f'LowerLimitSequence({self._limits})'

    def sort(self) -> None:
        """Sort the sequence by cutoff date."""

        self._limits.sort(key=lambda l: l.cutoff)

    def add_limit(self, new_limit: LowerLimit, check_limits_count: bool = True) -> None:
        """Add a limit, eliminate redundant limits, sort the sequence by cutoff date.

        Raises `TooLongLimitSequence` if `check_limits_count` is
        `True` and the resulting sequence is too long.

        """

        def find_eliminator_in_sorted_limit_sequence(sorted_limits: LowerLimitSequence) -> Optional[LowerLimit]:
            # Try to find a limit in the sequence that makes redundant
            # at least one of the other limits in the sequence.
            previous_value = None
            for eliminator in sorted_limits:
                value = eliminator.value
                if previous_value is not None and value >= previous_value:
                    return eliminator
                previous_value = value
            return None

        eliminator: Optional[LowerLimit] = new_limit
        while eliminator:
            self._apply_eliminator(eliminator)
            self.sort()
            eliminator = find_eliminator_in_sorted_limit_sequence(self)
        if check_limits_count and len(self) > self._get_max_limits_count():
            raise TooLongLimitSequence()

    def add_limits(self, limits: Sequence[LowerLimit]) -> None:
        """Safely add several limits at once.

        Raises `TooLongLimitSequence` if the resulting number of
        limits is too big.

        """

        max_limits_count = self._get_max_limits_count()

        # If we presume that the passed `limits` do not eliminate each
        # other, we can be certain that the resulting number of limits
        # will be no less than `len(limits)`.
        if len(limits) > max_limits_count:
            raise TooLongLimitSequence()

        # We check the number of limits only at the end (not on each
        # added limit) because each added limit can eliminate any
        # number of already existing limits.
        for limit in limits:
            self.add_limit(limit, check_limits_count=False)
        if len(self) > max_limits_count:
            raise TooLongLimitSequence()

    def current_limits(self, current_date: date) -> LowerLimitSequence:
        """Return a new sequence containing only the limits effectual to the `current_date`."""

        return LowerLimitSequence(limit for limit in self._limits if limit.cutoff >= current_date)

    def apply_to_value(self, value: Real) -> Real:
        """Take a value, apply the limits, and return a possibly bigger value."""

        for limit in self._limits:
            limit_value = limit.value
            if value < limit_value:
                value = limit_value
        return value

    def _apply_eliminator(self, eliminator: LowerLimit) -> None:
        value = eliminator.value
        cutoff = eliminator.cutoff
        self._limits = [limit for limit in self._limits if limit.value > value or limit.cutoff > cutoff]
        self._limits.append(eliminator)

    def _get_max_limits_count(self) -> int:
        return current_app.config['APP_MAX_LIMITS_COUNT']


def lower_limits_property(values_attrname: str, cutoffs_attrname: str):
    """Return a class property that treats two separate attributes (a list
    of values, and a list of cutoffs) as a sequence of lower limits.

    """

    def unpack_limits(values: Optional[List], cutoffs: Optional[List]) -> LowerLimitSequence:
        values = values or []
        cutoffs = cutoffs or []
        return LowerLimitSequence(LowerLimit(*t) for t in zip(values, cutoffs) if all(x is not None for x in t))

    def pack_limits(limits: LowerLimitSequence) -> Tuple[Optional[List], Optional[List]]:
        values = []
        cutoffs = []
        for limit in limits:
            assert isinstance(limit.value, Real)
            assert isinstance(limit.cutoff, date)
            values.append(limit.value)
            cutoffs.append(limit.cutoff)
        return values or None, cutoffs or None

    def getter(self) -> LowerLimitSequence:
        values = getattr(self, values_attrname)
        cutoffs = getattr(self, cutoffs_attrname)
        return unpack_limits(values, cutoffs)

    def setter(self, value: LowerLimitSequence) -> None:
        values, cutoffs = pack_limits(value)
        setattr(self, values_attrname, values)
        setattr(self, cutoffs_attrname, cutoffs)

    return property(getter, setter)
