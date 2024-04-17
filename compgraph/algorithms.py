from compgraph import operations
from compgraph.graph import Graph


def word_count_graph(
    input_stream_name: str,
    text_column: str = 'text',
    count_column: str = 'count',
    reversed: bool = False
) -> Graph:
    """Constructs graph which counts words in text_column of all rows passed"""
    return Graph \
        .graph_from_iter(input_stream_name) \
        .map(operations.FilterPunctuation(text_column)) \
        .map(operations.LowerCase(text_column)) \
        .map(operations.Split(text_column)) \
        .sort(operations.Sort([text_column])) \
        .reduce(operations.Count(count_column), [text_column]) \
        .sort(operations.Sort([count_column, text_column], reverse=reversed))


def inverted_index_graph(
    input_stream_name: str,
    doc_column: str = 'doc_id',
    text_column: str = 'text',
    result_column: str = 'tf_idf',
) -> Graph:
    """Constructs graph which calculates td-idf for every word/document pair"""
    graph_base = Graph.graph_from_iter(input_stream_name)

    splited_words = graph_base \
        .map(operations.FilterPunctuation(text_column)) \
        .map(operations.LowerCase(text_column)) \
        .map(operations.Split(text_column))

    count_docs = graph_base.reduce(operations.Count('count'), tuple())

    tf = splited_words \
        .sort(operations.Sort([doc_column])) \
        .reduce(operations.TermFrequency(text_column, 'tf'), [doc_column]) \
        .sort(operations.Sort([text_column]))

    idf = tf \
        .reduce(operations.Count('count'), (text_column,)) \
        .join(
            operations.InnerJoiner(suffix_a='_word', suffix_b='_doc'),
            count_docs,
            tuple()
        ) \
        .map(operations.MathMapper('idf', 'count_doc / count_word')) \
        .map(operations.LogarithmMap('idf')) \
        .map(operations.Project(['idf', text_column]))

    return tf \
        .join(operations.InnerJoiner(), idf, (text_column,)) \
        .map(operations.Product(['idf', 'tf'], result_column)) \
        .map(operations.Project([doc_column, text_column, result_column])) \
        .reduce(operations.TopN(result_column, 3), (text_column,))


def pmi_graph(
    input_stream_name: str,
    doc_column: str = 'doc_id',
    text_column: str = 'text',
    result_column: str = 'pmi',
) -> Graph:
    """Constructs graph which gives for every document
    the top 10 words ranked by pointwise mutual information"""
    graph_base = Graph.graph_from_iter(input_stream_name)

    words = graph_base \
        .map(operations.FilterPunctuation(text_column)) \
        .map(operations.LowerCase(text_column)) \
        .map(operations.Split(text_column)) \
        .map(operations.Filter(
            lambda row: len(row[text_column]) > 4
        )) \
        .sort(operations.Sort([doc_column, text_column])) \
        .reduce(operations.Count('doc_count'), [doc_column, text_column]) \
        .map(operations.Filter(
            lambda row: row['doc_count'] > 1
        ))

    tf = words \
        .join(
            operations.InnerJoiner('', '_overall'),
            words.reduce(operations.Sum('doc_count'), [doc_column]),
            [doc_column]
        ) \
        .map(operations.MathMapper('tf', 'doc_count / doc_count_overall')) \
        .map(operations.Project([doc_column, text_column, 'tf'])) \
        .sort(operations.Sort([text_column]))

    count_words = words.reduce(operations.Sum('doc_count'), tuple())

    idf = words \
        .sort(operations.Sort([text_column])) \
        .reduce(operations.Sum('doc_count'), (text_column,)) \
        .map(operations.Rename('doc_count', 'overall_count')) \
        .join(
            operations.InnerJoiner(),
            count_words,
            tuple()
        ) \
        .map(operations.MathMapper('idf', 'overall_count / doc_count')) \
        .map(operations.Project(['idf', text_column]))

    return tf \
        .join(operations.InnerJoiner(), idf, [text_column]) \
        .map(operations.MathMapper(result_column, 'tf / idf')) \
        .map(operations.LogarithmMap(result_column)) \
        .map(operations.Project([doc_column, text_column, result_column])) \
        .sort(operations.Sort([result_column], reverse=True)) \
        .sort(operations.Sort([doc_column])) \
        .reduce(operations.TopN(result_column, 10), [doc_column])


def yandex_maps_graph(
    input_stream_name_time: str,
    input_stream_name_length: str,
    enter_time_column: str = 'enter_time',
    leave_time_column: str = 'leave_time',
    edge_id_column: str = 'edge_id',
    start_coord_column: str = 'start',
    end_coord_column: str = 'end',
    weekday_result_column: str = 'weekday',
    hour_result_column: str = 'hour',
    speed_result_column: str = 'speed',
    timezone: str = 'UTC'
) -> Graph:
    """Constructs graph which measures average
    speed in km/h depending on the weekday and hour"""

    edges_len = Graph.graph_from_iter(input_stream_name_length) \
        .map(operations.Haversine(start_coord_column, end_coord_column, 'distance')) \
        .map(operations.Project(['distance', edge_id_column])) \
        .sort(operations.Sort([edge_id_column]))

    times = Graph.graph_from_iter(input_stream_name_time) \
        .map(operations.ToDatetime(
            enter_time_column,
            timezone=timezone,
            weekday_column=weekday_result_column,
            hour_column=hour_result_column
        )) \
        .map(operations.TimestampDiff(
            leave_time_column, enter_time_column, 'total_time'
        )) \
        .map(operations.MathMapper('total_time', 'total_time / 3600')) \
        .sort(operations.Sort([edge_id_column])) \
        .join(operations.InnerJoiner(), edges_len, [edge_id_column]) \
        .sort(operations.Sort([weekday_result_column, hour_result_column]))

    total_time = times \
        .reduce(operations.Sum('total_time'), [weekday_result_column, hour_result_column])
    total_distance = times \
        .reduce(operations.Sum('distance'), [weekday_result_column, hour_result_column])

    return total_time \
        .join(operations.InnerJoiner(), total_distance, [weekday_result_column, hour_result_column]) \
        .map(operations.MathMapper(speed_result_column, 'distance / total_time')) \
        .map(operations.Project([weekday_result_column, hour_result_column, speed_result_column]))
