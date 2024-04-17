import json

import click

from compgraph.algorithms import yandex_maps_graph


@click.command()
@click.option('--input-time', required=True)
@click.option('--input-road-lenght', required=True)
@click.option('--output', default=None)
def run_yandex_maps_graph(input_time: str, input_road_lenght: str, output: str | None) -> None:
    graph = yandex_maps_graph(
        input_stream_name_time='time',
        input_stream_name_length='road_meta',
        enter_time_column='enter_time',
        leave_time_column='leave_time',
        edge_id_column='edge_id',
        start_coord_column='start',
        end_coord_column='end',
        weekday_result_column='weekday',
        hour_result_column='hour',
        speed_result_column='speed',
    )
    result = graph.run(
        time=lambda: map(json.loads, open(input_time)),
        road_meta=lambda: map(json.loads, open(input_road_lenght)),
    )
    if output is None:
        for row in result:
            print(row)
    else:
        with open(output, 'w') as out:
            for row in result:
                print(row, file=out)


if __name__ == '__main__':
    run_yandex_maps_graph()
