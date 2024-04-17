import typing as tp

from compgraph.operations.operations_base import TRowsIterable, TRow, TRowsGenerator, Joiner

from .utils import PeekableIterator

TRowsIterator = tp.Iterator[TRow]


def _plug_iter() -> TRowsIterator:
    empty_dict: TRow = {}
    return iter((empty_dict,))


class InnerJoiner(Joiner):
    """Join with inner strategy"""

    def _merge_matching_rows(
        self,
        keys: tp.Sequence[str],
        rows_a: TRowsIterator,
        rows_b: TRowsIterator
    ) -> TRowsGenerator:
        rows_b_list = list(rows_b)
        for row_left in rows_a:
            for row_right in rows_b_list:
                yield self._merge_rows_with_suffixes(keys, row_left, row_right)

    def _handle_empty_iterators(
        self,
        keys: tp.Sequence[str],
        rows_a: PeekableIterator[TRow],
        rows_b: PeekableIterator[TRow]
    ) -> TRowsGenerator:
        yield from self._merge_matching_rows(keys, rows_a, rows_b)

    def __call__(
        self,
        keys: tp.Sequence[str],
        rows_a: TRowsIterable,
        rows_b: TRowsIterable,
    ) -> TRowsGenerator:
        yield from self._handle_empty_iterators(
            keys, PeekableIterator(iter(rows_a)), PeekableIterator(iter(rows_b))
        )


class OuterJoiner(InnerJoiner):
    """Join with outer strategy"""

    def _handle_empty_iterators(
        self,
        keys: tp.Sequence[str],
        rows_a: PeekableIterator[TRow],
        rows_b: PeekableIterator[TRow]
    ) -> TRowsGenerator:
        yield from super()._merge_matching_rows(
            keys,
            rows_a if rows_a else _plug_iter(),
            rows_b if rows_b else _plug_iter()
        )


class LeftJoiner(InnerJoiner):
    """Join with left strategy"""

    def _handle_empty_iterators(
        self,
        keys: tp.Sequence[str],
        rows_a: PeekableIterator[TRow],
        rows_b: PeekableIterator[TRow]
    ) -> TRowsGenerator:
        yield from super()._merge_matching_rows(
            keys,
            rows_a,
            rows_b if rows_b else _plug_iter()
        )


class RightJoiner(InnerJoiner):
    """Join with right strategy"""

    def _handle_empty_iterators(
        self,
        keys: tp.Sequence[str],
        rows_a: PeekableIterator[TRow],
        rows_b: PeekableIterator[TRow]
    ) -> TRowsGenerator:
        yield from super()._merge_matching_rows(
            keys,
            rows_a if rows_a else _plug_iter(),
            rows_b
        )
