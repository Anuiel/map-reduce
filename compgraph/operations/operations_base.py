from abc import abstractmethod, ABC
import typing as tp

from .utils import sorted_groupby

TRow = dict[str, tp.Any]
TRowsIterable = tp.Iterable[TRow]
TRowsGenerator = tp.Generator[TRow, None, None]
TRowGroup = tuple[tuple[str, ...], TRowsIterable]


def _get_subdict_values(row: TRow, keys: tp.Sequence[str]) -> tuple[tp.Any, ...]:
    return tuple(row[k] for k in keys)


class Operation(ABC):
    @abstractmethod
    def __call__(
        self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any
    ) -> TRowsGenerator:
        pass


class Read(Operation):
    @abstractmethod
    def __call__(self, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        pass


class ReadIterFile(Read):
    def __init__(
        self, filename: str, parser: tp.Callable[[str], TRow]
    ) -> None:
        self.filename = filename
        self.parser = parser

    def __call__(self, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        with open(self.filename) as f:
            for line in f:
                yield self.parser(line)


class ReadIterFactory(Read):
    def __init__(self, name: str) -> None:
        self.name = name

    def __call__(self, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        for row in kwargs[self.name]():
            yield row


# Map

class Mapper(ABC):
    """Base class for mappers"""

    @abstractmethod
    def __call__(self, row: TRow) -> TRowsGenerator:
        """
        :param row: one table row
        """
        pass


class Map(Operation):
    def __init__(self, mapper: Mapper) -> None:
        self.mapper = mapper

    def __call__(
        self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any
    ) -> TRowsGenerator:
        for row in rows:
            yield from self.mapper(row)

# Reduce


class Reducer(ABC):
    """Base class for reducers"""

    @abstractmethod
    def __call__(
        self, group_key: tuple[str, ...], rows: TRowsIterable
    ) -> TRowsGenerator:
        """
        :param rows: table rows
        """
        pass


class Reduce(Operation):
    def __init__(self, reducer: Reducer, keys: tp.Sequence[str]) -> None:
        self.reducer = reducer
        self.keys = keys

    def __call__(
        self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any
    ) -> TRowsGenerator:
        for _, group in sorted_groupby(
            rows, key=lambda row: _get_subdict_values(row, self.keys)
        ):
            yield from self.reducer(tuple(self.keys), group)

# Join


class Joiner(ABC):
    """Base class for joiners"""

    def __init__(self, suffix_a: str = "_1", suffix_b: str = "_2") -> None:
        self._a_suffix = suffix_a
        self._b_suffix = suffix_b

    def _merge_rows_with_suffixes(
        self, keys: tp.Sequence[str], row_a: TRow, row_b: TRow
    ) -> TRow:
        to_yield = {}
        for suffix, row in zip(
            (self._a_suffix, self._b_suffix), (row_a, row_b)
        ):
            for field, value in row.items():
                if field in keys or not (field in row_a and field in row_b):
                    to_yield[field] = value
                else:
                    to_yield[field + suffix] = value
        return to_yield

    @abstractmethod
    def __call__(
        self,
        keys: tp.Sequence[str],
        rows_a: TRowsIterable,
        rows_b: TRowsIterable,
    ) -> TRowsGenerator:
        """
        :param keys: join keys
        :param rows_a: left table rows
        :param rows_b: right table rows
        """
        pass


class Join(Operation):
    def __init__(self, joiner: Joiner, keys: tp.Sequence[str]):
        self.keys = keys
        self.joiner = joiner

    def _group_rows_by_keys(self, rows: TRowsIterable) -> tp.Iterator[TRowGroup]:
        return sorted_groupby(
            rows, key=lambda row: _get_subdict_values(row, self.keys)
        )

    @staticmethod
    def next_or_none(iter: tp.Iterator[TRowGroup]) -> TRowGroup | tuple[None, None]:
        return next(iter, (None, None))

    def __call__(
        self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any
    ) -> TRowsGenerator:
        assert len(args) > 0
        assert hasattr(args[0], "__iter__")
        rows_left = rows
        rows_right = args[0]

        iter_left = self._group_rows_by_keys(rows_left)
        iter_right = self._group_rows_by_keys(rows_right)

        key_left, group_left = self.next_or_none(iter_left)
        key_right, group_right = self.next_or_none(iter_right)

        while key_left is not None or key_right is not None:
            if key_right is None or (key_left is not None and key_left < key_right):
                assert group_left is not None  # mypy incident
                yield from self.joiner(self.keys, group_left, [])
                key_left, group_left = self.next_or_none(iter_left)
            elif key_left is None or (key_right is not None and key_left > key_right):
                assert group_right is not None  # mypy incident
                yield from self.joiner(self.keys, [], group_right)
                key_right, group_right = self.next_or_none(iter_right)
            else:  # key_left == key_right
                assert group_left is not None
                assert group_right is not None
                yield from self.joiner(self.keys, group_left, group_right)
                key_left, group_left = self.next_or_none(iter_left)
                key_right, group_right = self.next_or_none(iter_right)
