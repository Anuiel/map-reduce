import dataclasses
import typing as tp
import math
import datetime

import pytest
from pytest import approx

from compgraph import operations as ops
from .utils import _Key


@dataclasses.dataclass
class MapCase:
    mapper: ops.Mapper
    data: list[ops.TRow]
    ground_truth: list[ops.TRow]
    cmp_keys: tuple[str, ...]


MAP_CASES = [
    MapCase(
        mapper=ops.Rename('id', 'group_id'),
        data=[{'id': 0}, {'id': 1}],
        ground_truth=[{'group_id': 0}, {'group_id': 1}],
        cmp_keys=('group_id',)
    ),
    MapCase(
        mapper=ops.LogarithmMap('score'),
        data=[
            {'score': 1},
            {'score': math.e},
            {'score': math.e**2},
            {'score': 1. / math.e},
            {'score': 2},
        ],
        ground_truth=[
            {'score': approx(0, 0.001)},
            {'score': approx(1, 0.001)},
            {'score': approx(2, 0.001)},
            {'score': approx(-1, 0.001)},
            {'score': approx(0.693, 0.001)},
        ],
        cmp_keys=('score',)
    ),
    MapCase(
        mapper=ops.LogarithmMap('score', base=10),
        data=[
            {'score': 1},
            {'score': 100},
            {'score': 0.1},
            {'score': 3},
        ],
        ground_truth=[
            {'score': approx(0, 0.001)},
            {'score': approx(2, 0.001)},
            {'score': approx(-1, 0.001)},
            {'score': approx(0.477, 0.001)},
        ],
        cmp_keys=('score',)
    ),
    MapCase(
        mapper=ops.ToDatetime(
            'datetime',
            weekday_column='weekday',
            hour_column='hour',
            timezone='UTC'
        ),
        data=[
            {'datetime': '20171020T112238.723000'},
            {'datetime': '20171011T145553.040000'},
            {'datetime': '20171020T090548.939000'},
            {'datetime': '20171024T144101.879000'},
        ],
        ground_truth=[
            {'datetime': '20171020T112238.723000', 'weekday': 'Fri', 'hour': 11},
            {'datetime': '20171011T145553.040000', 'weekday': 'Wed', 'hour': 14},
            {'datetime': '20171020T090548.939000', 'weekday': 'Fri', 'hour': 9},
            {'datetime': '20171024T144101.879000', 'weekday': 'Tue', 'hour': 14},
        ],
        cmp_keys=('weekday', 'hour')
    ),
    MapCase(
        mapper=ops.MathMapper('result', '(left_edge**2 + right_edge**2)**(0.5)'),
        data=[
            {'left_edge': 1, 'right_edge': 1},
            {'left_edge': 1, 'right_edge': -1},
            {'left_edge': 3, 'right_edge': 4},
            {'left_edge': 12, 'right_edge': 5},
        ],
        ground_truth=[
            {'left_edge': 1, 'right_edge': 1, 'result': approx(1.4142, abs=0.0001)},
            {'left_edge': 1, 'right_edge': -1, 'result': 0},
            {'left_edge': 3, 'right_edge': 4, 'result': approx(5, abs=0.0001)},
            {'left_edge': 12, 'right_edge': 5, 'result': approx(13, abs=0.0001)},
        ],
        cmp_keys=('left_edge', 'right_edge', 'result')
    ),
    MapCase(
        mapper=ops.MathMapper('y', 'x**3 + x**2 + x + 1'),
        data=[
            {'x': x} for x in range(5)
        ],
        ground_truth=[
            {'x': x, 'y': x**3 + x**2 + x + 1} for x in range(5)
        ],
        cmp_keys=('x', 'y')
    ),
    MapCase(
        mapper=ops.ToDatetime(
            'datetime',
            hour_column='hour',
            timezone='Europe/Moscow'
        ),
        data=[
            {'datetime': datetime.datetime(2023, 1, 1, tzinfo=datetime.UTC).strftime('%Y%m%dT%H%M%S.%f')},
        ],
        ground_truth=[
            {'datetime': datetime.datetime(2023, 1, 1, tzinfo=datetime.UTC).strftime('%Y%m%dT%H%M%S.%f'), 'hour': 3},
        ],
        cmp_keys=('datetime', 'hour')
    )
]


@pytest.mark.parametrize('case', MAP_CASES)
def test_mapper(case: MapCase) -> None:
    key_func = _Key(*case.cmp_keys)

    result = ops.Map(case.mapper)(iter(case.data))
    assert sorted(case.ground_truth, key=key_func) == sorted(result, key=key_func)


@dataclasses.dataclass
class ReduceCase:
    reducer: ops.Reducer
    reducer_keys: tuple[str, ...]
    data: list[ops.TRow]
    ground_truth: list[ops.TRow]
    cmp_keys: tuple[str, ...]


REDUCE_CASES = [
    # check for correct work with non-unique
    # values in topN column
    ReduceCase(
        reducer=ops.TopN('score', n=2),
        reducer_keys=('group_id',),
        data=[
            {'id': 0, 'group_id': 0, 'score': 0},
            {'id': 1, 'group_id': 0, 'score': 0},
            {'id': 2, 'group_id': 0, 'score': 0},
            {'id': 3, 'group_id': 0, 'score': 0},

            {'id': 4, 'group_id': 1, 'score': 0},
            {'id': 5, 'group_id': 1, 'score': 0},
            {'id': 6, 'group_id': 1, 'score': 0},
            {'id': 7, 'group_id': 1, 'score': 0},
        ],
        ground_truth=[
            {'id': 0, 'group_id': 0, 'score': 0},
            {'id': 1, 'group_id': 0, 'score': 0},

            {'id': 4, 'group_id': 1, 'score': 0},
            {'id': 5, 'group_id': 1, 'score': 0},
        ],
        cmp_keys=('id', 'group_id', 'score'),
    )
]


@pytest.mark.parametrize('case', REDUCE_CASES)
def test_reducer(case: ReduceCase) -> None:

    key_func = _Key(*case.cmp_keys)

    result = ops.Reduce(case.reducer, case.reducer_keys)(iter(case.data))
    assert isinstance(result, tp.Iterator)
    assert sorted(case.ground_truth, key=key_func) == sorted(result, key=key_func)
