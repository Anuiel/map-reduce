import json

import click

from compgraph.algorithms import pmi_graph


@click.command()
@click.option('--input', required=True)
@click.option('--output', default=None)
def run_pmi_graph(input: str, output: str | None) -> None:
    graph = pmi_graph(
        input_stream_name='input',
        doc_column='doc_id',
        text_column='text',
        result_column='pmi',
    )
    result = graph.run(input=lambda: iter(map(json.loads, open(input))))
    if output is None:
        for row in result:
            print(row)
    else:
        with open(output, 'w') as out:
            for row in result:
                print(row, file=out)


if __name__ == '__main__':
    run_pmi_graph()
