import math
import unittest

from typing import List, Union

from parameterized import parameterized
from opentelemetry.sdk.metrics.export import HistogramDataPoint

from dynatrace.opentelemetry.metrics.export._histogram_utils import (
    _get_histogram_min,
    _get_histogram_max,
)


def create_histogram(explicit_bounds: List[int], bucket_counts: List[int],
                     histogram_sum: Union[float, int]):
    start_time = 1619687639000000000
    end_time = 1619687639000000000
    return HistogramDataPoint(bucket_counts=bucket_counts,
                              explicit_bounds=explicit_bounds,
                              sum=histogram_sum,
                              min=+math.inf,
                              max=-math.inf,
                              time_unix_nano=end_time,
                              start_time_unix_nano=start_time,
                              attributes=dict(),
                              count=sum(bucket_counts))


class TestMin(unittest.TestCase):
    @parameterized.expand([
        # Values between the first two boundaries.
        ([1, 2, 3, 4, 5], [0, 1, 0, 3, 2, 0], 21.2, 1),
        # First bucket has value, use the first boundary
        # as estimation instead of Inf.
        ([1, 2, 3, 4, 5], [1, 0, 0, 3, 0, 4], 34.5, 1),
        # Only the first bucket has values, use the mean
        # (0.25) Otherwise, the min would be estimated as
        # 1, and min <= avg would be violated.
        ([1, 2, 3, 4, 5], [3, 0, 0, 0, 0, 0], 0.75, 0.25),
        # Just one bucket from -Inf to Inf, calculate the
        # mean as min value.
        ([], [4], 8.8, 2.2),
        # Just one bucket from -Inf to Inf, calculate the
        # mean as min value.
        ([], [1], 1.2, 1.2),
        # Only the last bucket has a value, use the lower
        # bound.
        ([1, 2, 3, 4, 5], [0, 0, 0, 0, 0, 3], 15.6, 5),
    ])
    def test_get_min(self, boundaries, buckets, histogram_sum, expected_min):
        # Values between the first two boundaries.
        self.assertEqual(expected_min,
                         _get_histogram_min(
                             create_histogram(boundaries,
                                              buckets,
                                              histogram_sum)))


class TestMax(unittest.TestCase):
    @parameterized.expand([
        # Values between the first two boundaries.
        ([1, 2, 3, 4, 5], [0, 1, 0, 3, 2, 0], 21.2, 5),
        # Last bucket has value, use the last boundary as
        # estimation instead of Inf.
        ([1, 2, 3, 4, 5], [1, 0, 0, 3, 0, 4], 34.5, 5),
        # Only the last bucket has values, use the
        # mean (10.1) Otherwise, the max would be
        # estimated as 5, and max >= avg would be
        # violated.
        ([1, 2, 3, 4, 5], [0, 0, 0, 0, 0, 2], 20.2, 10.1),
        # Just one bucket from -Inf to Inf, calculate
        # the mean as max value.
        ([], [4], 8.8, 2.2),
        # Just one bucket from -Inf to Inf, calculate
        # the mean as max value.
        ([], [1], 1.2, 1.2),
        # Max is larger than the sum, use the
        # estimated boundary.
        ([0, 5], [0, 2, 0], 2.3, 5),
        # Only the last bucket has a value, use the lower
        # bound.
        ([1, 2, 3, 4, 5], [3, 0, 0, 0, 0, 0], 1.5, 1),
    ])
    def test_get_max(self, boundaries, buckets, histogram_sum, expected_max):
        # Values between the first two boundaries.
        self.assertEqual(expected_max,
                         _get_histogram_max(
                             create_histogram(boundaries,
                                              buckets,
                                              histogram_sum)))
