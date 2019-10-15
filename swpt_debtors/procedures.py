import math
from numbers import Real
from typing import TypeVar, List, Optional, Callable
from .extensions import db
from .models import Limit

T = TypeVar('T')
atomic: Callable[[T], T] = db.atomic


def _add_limit_to_list(l: List[Limit], new_limit: Limit, *, lower_limit=False, upper_limit=False) -> None:
    assert lower_limit or upper_limit, 'the limit type must be specified when calling _add_limit_to_list()'
    assert not (lower_limit and upper_limit)

    def get_restrictiveness(limit: Limit) -> Real:
        return limit.value if lower_limit else -limit.value

    def apply_eliminator(limits: List[Limit], eliminator: Limit) -> List[Limit]:
        """Remove the limits rendered ineffectual by the `eliminator`."""

        r = get_restrictiveness(eliminator)
        cutoff = eliminator.cutoff
        return [limit for limit in limits if get_restrictiveness(limit) > r or limit.cutoff > cutoff]

    def find_eliminator_in_sorted_limits(sorted_limits: List[Limit]) -> Optional[Limit]:
        """Try to find a limit that makes some of the other limits ineffectual."""

        restrictiveness = math.inf
        for eliminator in sorted_limits:
            r = get_restrictiveness(eliminator)
            if r >= restrictiveness:
                return eliminator
            restrictiveness = r
        return None

    limits = l
    while True:
        limits = apply_eliminator(limits, new_limit)
        limits.append(new_limit)
        limits.sort(key=lambda limit: limit.cutoff)
        new_limit = find_eliminator_in_sorted_limits(limits)
        if not new_limit:
            break
    l.clear()
    l.extend(limits)
