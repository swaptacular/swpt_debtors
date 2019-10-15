import math
from datetime import date
from numbers import Real
from typing import TypeVar, List, Optional, Callable
from .extensions import db
from .models import Limit

T = TypeVar('T')
atomic: Callable[[T], T] = db.atomic

BEGINNING_OF_TIME = date(1900, 1, 1)


def _add_limit_to_list(limits: List[Limit], new_limit: Limit, *, lower_limit=False, upper_limit=False) -> List[Limit]:
    assert lower_limit or upper_limit
    assert not (lower_limit and upper_limit)

    def get_restrictiveness(l: Limit) -> Real:
        return l.value if lower_limit else -l.value

    def filter_by_champion(champion: Limit) -> List[Limit]:
        r = get_restrictiveness(champion)
        c = champion.cutoff
        return [l for l in limits if get_restrictiveness(l) > r or l.cutoff > c]

    def find_champion_in_sorted_limits(sorted_limits: List[Limit]) -> Optional[Limit]:
        restrictiveness = math.inf
        for champion in sorted_limits:
            r = get_restrictiveness(champion)
            if r >= restrictiveness:
                return champion
            restrictiveness = r
        return None

    while True:
        limits = filter_by_champion(new_limit)
        limits.append(new_limit)
        limits.sort(key=lambda l: l.cutoff)
        new_limit = find_champion_in_sorted_limits(limits)
        if not new_limit:
            break
    return limits
