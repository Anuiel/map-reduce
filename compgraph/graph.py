import typing as tp

from . import operations as ops


class Graph:
    """Computational graph implementation"""

    def __init__(self) -> None:
        self.previos_graphs: list['Graph'] = []
        self.operation: ops.Operation | None = None

    @staticmethod
    def _private_init(operation: ops.Operation, previos_graphs: list['Graph']) -> 'Graph':
        new_graph = Graph()
        new_graph.operation = operation
        new_graph.previos_graphs = previos_graphs
        return new_graph

    @staticmethod
    def graph_from_iter(name: str) -> 'Graph':
        """Construct new graph which reads data
        from row iterator (in form of sequence of Rows
        from 'kwargs' passed to 'run' method) into graph data-flow

        Use ops.ReadIterFactory
        :param name: name of kwarg to use as data source
        """
        return Graph._private_init(ops.ReadIterFactory(name), [])

    @staticmethod
    def graph_from_file(filename: str, parser: tp.Callable[[str], ops.TRow]) -> 'Graph':
        """Construct new graph extended with operation
        for reading rows from file

        Use ops.Read
        :param filename: filename to read from
        :param parser: parser from string to Row
        """
        return Graph._private_init(ops.ReadIterFile(filename, parser), [])

    def _add_operation(self, operation: ops.Operation) -> 'Graph':
        """Extend current graph with map-reduce operation
        :param mapper: mapper to use
        """
        return self._private_init(operation, [self])

    def map(self, mapper: ops.Mapper) -> 'Graph':
        """Construct new graph extended with
        map operation with particular mapper
        :param mapper: mapper to use
        """
        return self._add_operation(ops.Map(mapper))

    def reduce(self, reducer: ops.Reducer, keys: tp.Sequence[str]) -> 'Graph':
        """Construct new graph extended with
        reduce operation with particular reducer
        :param reducer: reducer to use
        :param keys: keys for grouping
        """
        return self._add_operation(ops.Reduce(reducer, keys))

    def sort(self, sort: ops.Sort) -> 'Graph':
        """Construct new graph extended with sort operation
        :param keys: sorting keys (typical is tuple of strings)
        """
        return self._add_operation(sort)

    def join(self, joiner: ops.Joiner, join_graph: 'Graph', keys: tp.Sequence[str]) -> 'Graph':
        """Construct new graph extended with join operation with another graph
        :param joiner: join strategy to use
        :param join_graph: other graph to join with
        :param keys: keys for grouping
        """
        return self._private_init(ops.Join(joiner, keys), [self, join_graph])

    def run(self, **kwargs: tp.Any) -> ops.TRowsIterable:
        """Single method to start execution; data sources passed as kwargs"""
        if self.operation is None:
            raise ValueError('No operation to perform.')
        if not self.previos_graphs:
            yield from self.operation(**kwargs)
        else:
            yield from self.operation(*[
                prev_graphs.run(**kwargs) for prev_graphs in self.previos_graphs
            ])
