import typing as tp
import heapq
from collections import Counter, defaultdict

from compgraph.operations.operations_base import Reducer, TRowsGenerator, TRowsIterable, TRow

from .utils import PeekableIterator


class FirstReducer(Reducer):
    """Yield only first row from passed ones"""

    def __call__(
        self, group_key: tuple[str, ...], rows: TRowsIterable
    ) -> TRowsGenerator:
        for row in rows:
            yield row
            break


class TopN(Reducer):
    """Calculate top N by value"""

    def __init__(self, column: str, n: int) -> None:
        """
        :param column: column name to get top by
        :param n: number of top values to extract
        """
        self.column_max = column
        self.n = n

    def __call__(
        self, group_key: tuple[str, ...], rows: TRowsIterable
    ) -> TRowsGenerator:
        result: list[tp.Any] = []
        hash_table: defaultdict[tp.Any, list[TRow]] = defaultdict(list)
        for row in rows:
            value = row[self.column_max]
            if len(result) < self.n:
                heapq.heappush(result, value)
                hash_table[value].append(row)
            elif result[0] < value:
                hash_table[result[0]].pop()
                if hash_table[result[0]]:
                    hash_table.pop(result[0])
                heapq.heappushpop(result, value)
                hash_table[value].append(row)

        for top_rows in hash_table.values():
            yield from top_rows


class TermFrequency(Reducer):
    """Calculate frequency of values in column"""

    def __init__(self, words_column: str, result_column: str = "tf") -> None:
        """
        :param words_column: name for column with words
        :param result_column: name for result column
        """
        self.words_column = words_column
        self.result_column = result_column

    def __call__(
        self, group_key: tuple[str, ...], rows: TRowsIterable
    ) -> TRowsGenerator:
        rows = PeekableIterator(iter(rows))
        first_row = rows.peek()
        assert first_row is not None
        to_yield_template = {key: first_row[key] for key in group_key}
        counter = Counter((row[self.words_column] for row in rows))
        overall = sum(counter.values())
        for word, count in counter.items():
            to_yield = to_yield_template.copy()
            to_yield[self.words_column] = word
            to_yield[self.result_column] = count / overall
            yield to_yield


class Count(Reducer):
    """
    Count records by key
    Example for group_key=('a',) and column='d'
        {'a': 1, 'b': 5, 'c': 2}c∆
        {'a': 1, 'b': 6, 'c': 1}
        =>
        {'a': 1, 'd': 2}
    """

    def __init__(self, column: str) -> None:
        """
        :param column: name for result column
        """
        self.column = column

    def __call__(
        self, group_key: tuple[str, ...], rows: TRowsIterable
    ) -> TRowsGenerator:
        rows = iter(rows)
        first_row = next(rows)
        to_yield: TRow = {key: first_row[key] for key in group_key}
        to_yield[self.column] = 1
        for _ in rows:
            to_yield[self.column] += 1
        yield to_yield


class Sum(Reducer):
    """
    Sum values aggregated by key
    Example for key=('a',) and column='b'
        {'a': 1, 'b': 2, 'c': 4}
        {'a': 1, 'b': 3, 'c': 5}
        =>
        {'a': 1, 'b': 5}
    """

    def __init__(self, column: str) -> None:
        """
        :param column: name for sum column
        """
        self.column = column

    def __call__(
        self, group_key: tuple[str, ...], rows: TRowsIterable
    ) -> TRowsGenerator:
        rows = iter(rows)
        first_row = next(rows)
        to_yield: TRow = {key: first_row[key] for key in group_key}
        to_yield[self.column] = first_row[self.column]
        for row in rows:
            to_yield[self.column] += row[self.column]
        yield to_yield
