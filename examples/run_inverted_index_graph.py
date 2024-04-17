import json

import click

from compgraph.algorithms import inverted_index_graph


@click.command()
@click.option('--input', required=True)
@click.option('--output', default=None)
def run_inverted_index_graph(input: str, output: str | None) -> None:
    graph = inverted_index_graph(
        input_stream_name='input',
        doc_column='doc_id',
        text_column='text',
        result_column='tf_idf',
    )
    result = graph.run(input=lambda: map(json.loads, open(input)))
    if output is None:
        for row in result:
            print(row)
    else:
        with open(output, 'w') as out:
            for row in result:
                print(row, file=out)


if __name__ == '__main__':
    run_inverted_index_graph()
