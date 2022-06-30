# Copyright 2022 Dynatrace LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import math

from opentelemetry.sdk.metrics.export import (
    HistogramDataPoint
)


def _get_histogram_max(histogram: HistogramDataPoint):
    if histogram.max is not None and math.isfinite(histogram.max):
        return histogram.max

    if len(histogram.bucket_counts) == 1:
        # In this case, only one bucket exists: (-Inf, Inf). If there were
        # any boundaries, there would be more counts.
        if histogram.bucket_counts[0] > 0:
            # in case the single bucket contains something, use the mean as
            # max.
            return histogram.sum / histogram.count
        # otherwise the histogram has no data. Use the sum as the min and
        # max, respectively.
        return histogram.sum

    # loop over bucket_counts in reverse
    last_element_index = len(histogram.bucket_counts) - 1
    for index in range(last_element_index, -1, -1):
        if histogram.bucket_counts[index] > 0:
            if index == last_element_index:
                # use the last bound in the bounds array. This can only be the
                # case if there is a count >  0 in the last bucket (lastBound,
                # Inf). In some cases, the mean of the histogram is larger than
                # this bound, thus use the maximum of the estimated bound and
                # the mean.
                return max(histogram.explicit_bounds[index - 1],
                           histogram.sum / histogram.count)
            # In any other bucket (lowerBound, upperBound], use the upperBound.
            return histogram.explicit_bounds[index]

    # there are no counts > 0, so calculating a mean would result in a
    # division by 0. By returning the sum, we can let the backend decide what
    # to do with the value (with a count of 0)
    return histogram.sum


def _get_histogram_min(histogram: HistogramDataPoint):
    if histogram.min is not None and math.isfinite(histogram.min):
        return histogram.min

    if len(histogram.bucket_counts) == 1:
        # In this case, only one bucket exists: (-Inf, Inf). If there were
        # any boundaries, there would be more counts.
        if histogram.bucket_counts[0] > 0:
            # in case the single bucket contains something, use the mean as
            # min.
            return histogram.sum / histogram.count
        # otherwise the histogram has no data. Use the sum as the min and
        # max, respectively.
        return histogram.sum

    # iterate all buckets to find the first bucket with count > 0
    for index in range(0, len(histogram.bucket_counts)):
        # the current bucket contains something.
        if histogram.bucket_counts[index] > 0:
            if index == 0:
                # In the first bucket, (-Inf, firstBound], use firstBound
                # (this is the lowest specified bound overall). This is not
                # quite correct but the best approximation we can get at
                # this point. However, this might lead to a min bigger than
                # the mean, thus choose the minimum of the following:
                # - The lowest boundary
                # - The histogram's average (histogram sum / sum of counts)
                return min(histogram.explicit_bounds[index],
                           histogram.sum / histogram.count)
            # In all other buckets (lowerBound, upperBound] use the
            # lowerBound to estimate min.
            return histogram.explicit_bounds[index - 1]

    # there are no counts > 0, so calculating a mean would result in a
    # division by 0. By returning the sum, we can let the backend decide what
    # to do with the value (with a count of 0)
    return histogram.sum
