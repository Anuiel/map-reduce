import dataclasses
import typing as tp

from compgraph import operations as ops


@dataclasses.dataclass
class MapReduceOperation:
    operation_type: str
    operation: ops.Mapper | ops.Reducer | ops.Sort | ops.Joiner
    kwargs: tuple[tuple[str, tp.Any], ...] = tuple()


@dataclasses.dataclass
class GraphCase:
    operations: list[MapReduceOperation]
    data: list[ops.TRow]
    ground_truth: list[ops.TRow]
    cmp_keys: tuple[str, ...]
    check_sorted: bool = True


GPAPH_CASES = [
    GraphCase(
        operations=[
            MapReduceOperation(
                operation_type='map',
                operation=ops.DummyMapper()
            )
            for _ in range(5)
        ],
        data=[
            {'id': 0},
            {'id': 1},
            {'id': 2}
        ],
        ground_truth=[
            {'id': 0},
            {'id': 1},
            {'id': 2}
        ],
        cmp_keys=('id',)
    ),
    GraphCase(
        operations=[
            MapReduceOperation(
                operation_type='map',
                operation=ops.FilterPunctuation(column='text')
            ),
            MapReduceOperation(
                operation_type='map',
                operation=ops.LowerCase(column='text')
            )
        ],
        data=[
            {'id': 0, 'text': "GOD I LOVE ANIME!"},
            {'id': 1, 'text': "I HATE ANIME SO MUCH!!!!!!"},
            {'id': 2, 'text': "This, is, correct? It isn't"}
        ],
        ground_truth=[
            {'id': 0, 'text': "god i love anime"},
            {'id': 1, 'text': "i hate anime so much"},
            {'id': 2, 'text': "this is correct it isnt"}
        ],
        cmp_keys=('id', 'text')
    ),
    GraphCase(
        operations=[
            MapReduceOperation(
                operation_type='map',
                operation=ops.FilterPunctuation(column='text')
            ),
            MapReduceOperation(
                operation_type='map',
                operation=ops.LowerCase(column='text')
            ),
            MapReduceOperation(
                operation_type='map',
                operation=ops.Split('text', separator=' ')
            )
        ],
        data=[
            {'id': 0, 'text': "GOD I LOVE ANIME!"},
            {'id': 1, 'text': "I HATE ANIME SO MUCH!!!!!!"},
        ],
        ground_truth=[
            {'id': 0, 'text': "god"},
            {'id': 0, 'text': "i"},
            {'id': 0, 'text': "love"},
            {'id': 0, 'text': "anime"},

            {'id': 1, 'text': "i"},
            {'id': 1, 'text': "hate"},
            {'id': 1, 'text': "anime"},
            {'id': 1, 'text': "so"},
            {'id': 1, 'text': "much"},
        ],
        cmp_keys=('id', 'text')
    ),
    GraphCase(
        operations=[
            MapReduceOperation(
                operation_type='reduce',
                operation=ops.FirstReducer(),
                kwargs=tuple({
                    'keys': ('id_B',)
                }.items())
            ),
            MapReduceOperation(
                operation_type='reduce',
                operation=ops.FirstReducer(),
                kwargs=tuple({
                    'keys': ('id_C',)
                }.items())
            ),
            MapReduceOperation(
                operation_type='map',
                operation=ops.Product(columns=('id_A', 'id_B', 'id_C'))
            )
        ],
        data=[
            {'id_A': 1, 'id_B': 1, 'id_C': 1},
            {'id_A': 2, 'id_B': 1, 'id_C': 1},

            {'id_A': 3, 'id_B': 2, 'id_C': 1},
            {'id_A': 4, 'id_B': 2, 'id_C': 1},

            {'id_A': 5, 'id_B': 3, 'id_C': 2},
            {'id_A': 6, 'id_B': 3, 'id_C': 2},

            {'id_A': 7, 'id_B': 4, 'id_C': 2},
            {'id_A': 8, 'id_B': 4, 'id_C': 2},
        ],
        ground_truth=[
            {'id_A': 1, 'id_B': 1, 'id_C': 1, 'product': 1},
            {'id_A': 5, 'id_B': 3, 'id_C': 2, 'product': 30},
        ],
        cmp_keys=('id_A', 'id_B', 'id_C', 'product')
    ),
    GraphCase(
        operations=[
            MapReduceOperation(
                operation_type='sort',
                operation=ops.Sort(keys=('score',))
            ),
        ],
        data=[
            {'id': 0, 'score': 111},
            {'id': 2, 'score': 11},
            {'id': 3, 'score': 1},
            {'id': 4, 'score': 12},
            {'id': 5, 'score': 112},
        ],
        ground_truth=[
            {'id': 3, 'score': 1},
            {'id': 2, 'score': 11},
            {'id': 4, 'score': 12},
            {'id': 0, 'score': 111},
            {'id': 5, 'score': 112},
        ],
        cmp_keys=('group_id', 'score'),
        check_sorted=False
    ),
    GraphCase(
        operations=[
            MapReduceOperation(
                operation_type='reduce',
                operation=ops.TopN(column='score', n=3),
                kwargs=tuple({
                    'keys': ('group_id',)
                }.items())
            ),
            MapReduceOperation(
                operation_type='sort',
                operation=ops.Sort(keys=['group_id', 'score'])
            )
        ],
        data=[
            {'group_id': 0, 'score': 111},
            {'group_id': 0, 'score': 11},
            {'group_id': 0, 'score': 1},
            {'group_id': 0, 'score': 12},
            {'group_id': 0, 'score': 112},

            {'group_id': 1, 'score': -111},
            {'group_id': 1, 'score': -11},
            {'group_id': 1, 'score': -1},
            {'group_id': 1, 'score': -12},
            {'group_id': 1, 'score': -112},
        ],
        ground_truth=[
            {'group_id': 0, 'score': 12},
            {'group_id': 0, 'score': 111},
            {'group_id': 0, 'score': 112},

            {'group_id': 1, 'score': -12},
            {'group_id': 1, 'score': -11},
            {'group_id': 1, 'score': -1},
        ],
        cmp_keys=('group_id', 'score'),
        check_sorted=False
    )
]
