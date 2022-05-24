import math
import unittest

from typing import List
from opentelemetry.sdk._metrics.point import Histogram, AggregationTemporality, Union

from dynatrace.opentelemetry.metrics.export import _get_histogram_min, _get_histogram_max


def create_histogram(explicit_bounds: List[int], bucket_counts: List[int], sum: Union[float, int]):
    start_time = 1619687639000000000
    end_time = 1619687639000000000
    return Histogram(bucket_counts=bucket_counts,
                     explicit_bounds=explicit_bounds,
                     sum=sum,
                     min=+math.inf,
                     max=-math.inf,
                     aggregation_temporality=AggregationTemporality.DELTA,
                     time_unix_nano=end_time,
                     start_time_unix_nano=start_time)


class TestMin(unittest.TestCase):
    def test_get_min(self):
        # A value between the first two boundaries.
        self.assertEqual(1,
                         _get_histogram_min(create_histogram([1, 2, 3, 4, 5], [0, 1, 0, 3, 0, 4], 10.234)))
        # lowest bucket has value, use the first boundary as estimation instead of -Inf
        self.assertEqual(1,
                         _get_histogram_min(create_histogram([1, 2, 3, 4, 5], [1, 0, 0, 3, 0, 4], 10.234)))
        # lowest bucket (-Inf, 1) has values, mean is lower than the lowest bucket bound and smaller than the sum
        self.assertAlmostEqual(0.234 / 3,
                               _get_histogram_min(create_histogram([1, 2, 3, 4, 5], [3, 0, 0, 0, 0, 0], 0.234)),
                               delta=0.001)
        # lowest bucket (-Inf, 0) has values, sum is lower than the lowest bucket bound
        self.assertAlmostEqual(-25.3,
                               _get_histogram_min(create_histogram([0, 5], [3, 0, 0], -25.3)),
                               delta=0.001)
        # no bucket has a value
        self.assertAlmostEqual(10.234,
                               _get_histogram_min(create_histogram([1, 2, 3, 4, 5], [0, 0, 0, 0, 0, 0], 10.234)),
                               delta=0.001)
        # just one bucket from -Inf to Inf, calc the mean as min value.
        self.assertAlmostEqual(2.2,
                               _get_histogram_min(create_histogram([], [4], 8.8)),
                               delta=0.001)
        # just one bucket from -Inf to Inf, with a count of 1
        self.assertAlmostEqual(1.2,
                               _get_histogram_min(create_histogram([], [1], 1.2)),
                               delta=0.001)
        # only the last bucket has a value (5, +Inf)
        self.assertAlmostEqual(5,
                               _get_histogram_min(create_histogram([1, 2, 3, 4, 5], [0, 0, 0, 0, 0, 1], 10.234)),
                               delta=0.001)


class TestMax(unittest.TestCase):
    def test_get_max(self):
        #  A value between the last two boundaries.
        self.assertEqual(5,
                         _get_histogram_max(create_histogram([1, 2, 3, 4, 5], [0, 1, 0, 3, 2, 0], 10.234)))
        # last bucket has value, use the last boundary as estimation instead of Inf
        self.assertEqual(5,
                         _get_histogram_max(create_histogram([1, 2, 3, 4, 5], [1, 0, 0, 3, 0, 4], 10.234)))
        # no bucket has a value
        self.assertAlmostEqual(10.234,
                               _get_histogram_max(create_histogram([1, 2, 3, 4, 5], [0, 0, 0, 0, 0, 0], 10.234)),
                               delta=0.001)
        # just one bucket from -Inf to Inf, calc the mean as max value.
        self.assertAlmostEqual(2.2,
                               _get_histogram_max(create_histogram([], [4], 8.8)),
                               delta=0.001)
        #  just one bucket from -Inf to Inf, with a count of 1
        self.assertAlmostEqual(1.2,
                               _get_histogram_max(create_histogram([], [1], 1.2)),
                               delta=0.001)
        # only the last bucket has a value (5, +Inf)
        self.assertAlmostEqual(5,
                               _get_histogram_max(create_histogram([1, 2, 3, 4, 5], [0, 0, 0, 0, 0, 1], 10.234)),
                               delta=0.001)
        # the max is greater than the sum
        self.assertAlmostEqual(2.3,
                               _get_histogram_max(create_histogram([-5, 0, 5], [0, 0, 2, 0], 2.3)),
                               delta=0.001)

