import typing as tp
from itertools import groupby


def sorted_groupby(
    iterable: tp.Iterable[tp.Any], key: tp.Callable[..., tp.Any] | None = None
) -> tp.Iterator[tp.Tuple[tp.Any, tp.Iterator[tp.Any]]]:
    """
    Group elements of iterable by key.
    Assert error if keys are not sorted.
    :param iterable: iterable of elements
    :param key: function to get key from element
    """
    last_key = None
    for key, group in groupby(iterable, key):
        # mypy saying
        # Right operand of "and" is never evaluated
        # which is not true
        if last_key is not None and key < last_key:  # type: ignore
            raise AssertionError(f"Keys are not sorted: {key} < {last_key}")
        yield key, group
        last_key = key
