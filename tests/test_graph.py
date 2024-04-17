import typing as tp
import tempfile
import ast

import pytest

from compgraph import operations as ops
from compgraph.graph import Graph
from .graph_cases import GPAPH_CASES, GraphCase
from .utils import _Key


@pytest.mark.parametrize('case', GPAPH_CASES)
def test_mapper(case: GraphCase) -> None:
    graph = Graph.graph_from_iter('data')
    for op in case.operations:
        graph = graph.__getattribute__(op.operation_type)(op.operation, **dict(op.kwargs))

    graph_result = graph.run(data=lambda: iter(case.data))
    if case.check_sorted:
        assert check_sorted(graph_result, case.ground_truth, case.cmp_keys)
    else:
        assert list(graph_result) == case.ground_truth


def check_sorted(
    graph_result: tp.Iterable[tp.Any], ground_truth: tp.Iterable[tp.Any], cmp_keys: tp.Sequence[str]
) -> bool:
    cmp_func = _Key(*cmp_keys)
    return sorted(graph_result, key=cmp_func) == sorted(ground_truth, key=cmp_func)


def test_inner_join() -> None:
    data = [
        {'id_A': i, 'id_B': 2 * i, 'id_C': 3 * i}
        for i in range(5)
    ]
    graph_base = Graph.graph_from_iter('data')

    graph_no_B = graph_base.map(ops.Project(columns=('id_A', 'id_C')))
    graph_no_C = graph_base.map(ops.Project(columns=('id_A', 'id_B')))
    graph = graph_no_B.join(ops.InnerJoiner(), graph_no_C, keys=('id_A',))

    graph_output = graph.run(data=lambda: iter(data))
    assert check_sorted(graph_output, data, ('id_A', 'id_B', 'id_C'))


def test_left_join() -> None:
    data = [
        {'group_id': 0},
        {'group_id': 0},
        {'group_id': 0},
        {'group_id': 0},

        {'group_id': 1},
        {'group_id': 1},

        {'group_id': 2},
        {'group_id': 2},
        {'group_id': 2},
    ]
    ground_truth = [
        {'group_id': 1, 'count': 2},
        {'group_id': 1, 'count': 2},

        {'group_id': 2, 'count': 3},
        {'group_id': 2, 'count': 3},
        {'group_id': 2, 'count': 3},

        {'group_id': 0, 'count': 4},
        {'group_id': 0, 'count': 4},
        {'group_id': 0, 'count': 4},
        {'group_id': 0, 'count': 4},
    ]
    graph_base = Graph.graph_from_iter('data')

    graph_max_score = graph_base.reduce(ops.Count('count'), ('group_id',))

    graph = graph_base \
        .join(ops.LeftJoiner(), graph_max_score, ('group_id',)) \
        .sort(ops.Sort(('count',)))
    graph_output = graph.run(data=lambda: iter(data))

    assert list(graph_output) == ground_truth


def dump_to_tmp_file(data: list[tp.Any]) -> tp.IO[str]:
    file = tempfile.NamedTemporaryFile("w")
    for row in data:
        file.write(str(row).replace('\'', '\"') + '\n')
    file.seek(0)
    return file


def test_read_from_file() -> None:
    data = [{"id": i} for i in range(5)]
    file = dump_to_tmp_file(data)
    graph = Graph.graph_from_file(file.name, ast.literal_eval)

    # check that multiple read works
    for _ in range(3):
        graph_output = graph.run()
        assert list(graph_output) == data


def test_read_from_multiple_files() -> None:
    id_value_table = [
        {"id": 0, "group_id": 0},
        {"id": 1, "group_id": 0},
        {"id": 2, "group_id": 1},
        {"id": 3, "group_id": 1},
        {"id": 5, "group_id": 2},
        {"id": 8, "group_id": 2},
        {"id": 12, "group_id": 3},
    ]
    group_name_table = [
        {"group_id": 0, "name": "Aboba gaming"},
        {"group_id": 1, "name": "Sussy baka gaming"},
        {"group_id": 2, "name": "Clown9"},
    ]

    ground_truth = [
        {"id": 0, "group_id": 0, "name": "Aboba gaming"},
        {"id": 1, "group_id": 0, "name": "Aboba gaming"},
        {"id": 2, "group_id": 1, "name": "Sussy baka gaming"},
        {"id": 3, "group_id": 1, "name": "Sussy baka gaming"},
        {"id": 5, "group_id": 2, "name": "Clown9"},
        {"id": 8, "group_id": 2, "name": "Clown9"},
    ]

    id_value_file = dump_to_tmp_file(id_value_table)
    group_name_file = dump_to_tmp_file(group_name_table)

    id_value_graph = Graph.graph_from_file(id_value_file.name, ast.literal_eval)
    group_name_graph = Graph.graph_from_file(group_name_file.name, ast.literal_eval)

    graph = id_value_graph.join(ops.InnerJoiner(), group_name_graph, ['group_id'])
    graph_output = graph.run()

    assert check_sorted(graph_output, ground_truth, ('id', 'group_id', 'name'))
