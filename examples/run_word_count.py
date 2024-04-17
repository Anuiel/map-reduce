import json

import click

from compgraph.algorithms import word_count_graph


@click.command()
@click.option('--input', required=True)
@click.option('--output', default=None)
def run_word_count(input: str, output: str | None) -> None:
    graph = word_count_graph(
        input_stream_name='input',
        text_column='text',
        count_column='count',
        reversed=True
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
    run_word_count()
