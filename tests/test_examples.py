from pytest import approx

from compgraph.operations.mappers import Haversine


def test_haversine() -> None:
    coords = [
        [37.6056, 55.7422],
        [37.6256, 55.7422],
        [37.6056, 55.7622],
        [37.6256, 55.7622]
    ]

    pairwise = [
        [
            Haversine.haversine_distance(
                latitude_start, longitude_start,
                latitude_end, longitude_end
            )
            for latitude_start, longitude_start in coords
        ] for latitude_end, longitude_end in coords
    ]
    ground_truth = [  # got it from sklearn
        [0.  , 1.25, 2.22, 2.55], # noqa E203
        [1.25, 0.  , 2.55, 2.22], # noqa E203
        [2.22, 2.55, 0.  , 1.25], # noqa E203
        [2.55, 2.22, 1.25, 0.  ], # noqa E203
    ]
    ground_truth_approx = [
        [approx(dist, abs=0.01) for dist in row]
        for row in ground_truth
    ]

    assert pairwise == ground_truth_approx
