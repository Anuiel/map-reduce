import math
import re
import string
import typing as tp
import datetime
import ast
import operator
import zoneinfo

from compgraph.operations.operations_base import Mapper, TRow, TRowsGenerator


class DummyMapper(Mapper):
    """Yield exactly the row passed"""

    def __call__(self, row: TRow) -> TRowsGenerator:
        yield row


class FilterPunctuation(Mapper):
    """Left only non-punctuation symbols"""

    maping_filer = str.maketrans('', '', string.punctuation)

    def __init__(self, column: str):
        """
        :param column: name of column to process
        """
        self.column = column

    def __call__(self, row: TRow) -> TRowsGenerator:
        row[self.column] = row[self.column].translate(self.maping_filer)
        yield row


class LowerCase(Mapper):
    """Replace column value with value in lower case"""

    def __init__(self, column: str):
        """
        :param column: name of column to process
        """
        self.column = column

    @staticmethod
    def _lower_case(txt: str) -> str:
        return txt.lower()

    def __call__(self, row: TRow) -> TRowsGenerator:
        row[self.column] = self._lower_case(row[self.column])
        yield row


class Split(Mapper):
    """Split row on multiple rows by separator"""

    def __init__(self, column: str, separator: str | None = None) -> None:
        """
        :param column: name of column to split
        :param separator: string to separate by
        """
        self.column = column
        self.separator = separator

    @staticmethod
    def _word_generator(
        text: str, separator: str
    ) -> tp.Generator[str, None, None]:
        start = 0
        for pattern in re.finditer(separator, text):
            yield text[start:pattern.start()]
            start = pattern.end()
        yield text[start:]

    def __call__(self, row: TRow) -> TRowsGenerator:
        for word in self._word_generator(
            row[self.column], self.separator or "\\s+"
        ):
            to_yield = row.copy()
            to_yield[self.column] = word
            yield to_yield


class Product(Mapper):
    """Calculates product of multiple columns"""

    def __init__(
        self, columns: tp.Sequence[str], result_column: str = "product"
    ) -> None:
        """
        :param columns: column names to product
        :param result_column: column name to save product in
        """
        self.columns = columns
        self.result_column = result_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        row[self.result_column] = math.prod(
            row[column] for column in self.columns
        )
        yield row


class Filter(Mapper):
    """Remove records that don't satisfy some condition"""

    def __init__(self, condition: tp.Callable[[TRow], bool]) -> None:
        """
        :param condition: if condition is not true - remove record
        """
        self.condition = condition

    def __call__(self, row: TRow) -> TRowsGenerator:
        if self.condition(row):
            yield row


class Project(Mapper):
    """Leave only mentioned columns"""

    def __init__(self, columns: tp.Sequence[str]) -> None:
        """
        :param columns: names of columns
        """
        self.columns = columns

    def __call__(self, row: TRow) -> TRowsGenerator:
        yield {column: row[column] for column in self.columns}


class LogarithmMap(Mapper):
    """Replace columns by its logarithm"""

    def __init__(self, column: str, base: float | None = None) -> None:
        """
        :param columns: columns with logarithm argument
        """
        self.column = column
        self._args = [] if base is None else [base]

    def __call__(self, row: TRow) -> TRowsGenerator:
        row[self.column] = math.log(row[self.column], *self._args)
        yield row


class Rename(Mapper):
    """Rename column"""

    def __init__(self, column_from: str, column_to: str) -> None:
        """
        :param column_from: column to rename
        :param column_to: new column name
        """
        self.column_from = column_from
        self.column_to = column_to

    def __call__(self, row: TRow) -> TRowsGenerator:
        row[self.column_to] = row[self.column_from]
        row.pop(self.column_from)
        yield row


class Haversine(Mapper):
    """Calculate haversine distance"""

    EARTH_RADIUS = 6373

    def __init__(self, start_column: str, end_column: str, result_columns: str) -> None:
        self.start_columns = start_column
        self.end_column = end_column
        self.result_columns = result_columns

    def __call__(self, row: TRow) -> TRowsGenerator:
        to_yield = row.copy()
        to_yield[self.result_columns] = self.haversine_distance(
            *row[self.start_columns],
            *row[self.end_column]
        )
        yield to_yield

    @staticmethod
    def haversine_distance(
        longitude_start: float,
        latitude_start: float,
        longitude_end: float,
        latitude_end: float,
    ) -> float:
        """
        Calculate the great circle distance between two points
        on the earth (specified in decimal degrees)
        """
        (
            latitude_start, latitude_end,
            longitude_start, longitude_end
        ) = [
            math.radians(arr) for arr in
            [latitude_start, latitude_end, longitude_start, longitude_end]
        ]

        dlon = longitude_start - longitude_end
        dlat = latitude_start - latitude_end

        x = math.sin(dlat / 2)**2 + (
            math.cos(latitude_start) * math.cos(latitude_end) * math.sin(dlon / 2)**2
        )
        return 2 * Haversine.EARTH_RADIUS * math.asin(math.sqrt(x))


class ToDatetime(Mapper):
    """Convert column to datetime"""

    def __init__(
        self,
        column: str,
        timezone: str | None = None,
        year_column: str | None = None,
        month_column: str | None = None,
        weekday_column: str | None = None,
        hour_column: str | None = None,
        minute_column: str | None = None,
        second_column: str | None = None
    ) -> None:
        self.column = column
        self.kwargs = {}
        self.timezone = zoneinfo.ZoneInfo(timezone if timezone is not None else 'UTC')
        for symbol, date_column in zip(
            'YmaHMS',  # symbols for year, month, etc.
            [
                year_column, month_column, weekday_column,
                hour_column, minute_column, second_column
            ]
        ):
            if date_column is not None:
                self.kwargs[date_column] = symbol

    def __call__(self, row: TRow) -> TRowsGenerator:
        date = datetime.datetime.fromisoformat(row[self.column] + '+00:00').astimezone(self.timezone)
        to_yield = row.copy()
        for date_column, symbol in self.kwargs.items():
            x: str | int = date.strftime(f'%{symbol}')
            if symbol != 'a':
                x = int(x)
            to_yield[date_column] = x
        yield to_yield


class TimestampDiff(Mapper):
    """Convert column to datetime"""

    def __init__(
        self,
        left_timestamp_column: str,
        right_timestamp_column: str,
        result_column: str,
    ) -> None:
        self.result_column = result_column
        self.left_timestamp_column = left_timestamp_column
        self.right_timestamp_column = right_timestamp_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        to_yield = row.copy()
        to_yield[self.result_column] = (
            datetime.datetime.fromisoformat(row[self.left_timestamp_column]) -
            datetime.datetime.fromisoformat(row[self.right_timestamp_column])
        ).total_seconds()
        yield to_yield


class MathMapper(Mapper):
    """Evaluates simple math opeartions over columns"""

    operators: dict[type, tp.Any] = {
        ast.Add: operator.add,
        ast.Sub: operator.sub,
        ast.Mult: operator.mul,
        ast.Div: operator.truediv,
        ast.Pow: operator.pow,
        ast.BitXor: operator.xor,
        ast.USub: operator.neg
    }

    @staticmethod
    def math_eval(node: ast.expr) -> int | float | complex:
        if isinstance(node, ast.Num):  # <number>
            return node.n
        elif isinstance(node, ast.BinOp):  # <left> <operator> <right>
            return MathMapper.operators[type(node.op)](
                MathMapper.math_eval(node.left), MathMapper.math_eval(node.right)
            )
        elif isinstance(node, ast.UnaryOp):  # <operator> <operand> e.g., -1
            return MathMapper.operators[type(node.op)](MathMapper.math_eval(node.operand))
        else:
            raise TypeError(node)

    @staticmethod
    def replace_names(equation: str, row: TRow) -> str:
        for column in re.findall('[a-zA-Z_]+', equation):
            equation = re.sub(column, '{}', equation, count=1).format(row[column])
        return equation

    def __init__(self, result_column: str, equation: str) -> None:
        self.result_column = result_column
        self.equation = equation

    def __call__(self, row: TRow) -> TRowsGenerator:
        equation = self.replace_names(self.equation, row)
        value = self.math_eval(ast.parse(equation, mode='eval').body)
        to_yield = row.copy()
        to_yield[self.result_column] = value
        yield to_yield
