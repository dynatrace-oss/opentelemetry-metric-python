# Copyright 2020 Dynatrace LLC
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

import logging
import math

import requests
from typing import Mapping, Optional, Sequence

from opentelemetry.sdk._metrics.export import (
    MetricExporter,
    Metric,
    MetricExportResult,
)

from dynatrace.metric.utils import (
    DynatraceMetricsSerializer,
    DynatraceMetricsApiConstants,
    DynatraceMetricsFactory,
    MetricError
)
from opentelemetry.sdk._metrics.point import (
    Sum,
    AggregationTemporality,
    Gauge,
    Histogram)

VERSION = "0.2.0b0"


def _get_histogram_max(histogram: Histogram):
    if histogram.max is not None and math.isfinite(histogram.max):
        return histogram.max

    histogram_sum = histogram.sum
    histogram_count = sum(histogram.bucket_counts)
    if len(histogram.bucket_counts) == 1:
        # In this case, only one bucket exists: (-Inf, Inf). If there were
        # any boundaries, there would be more counts.
        if histogram.bucket_counts[0] > 0:
            # in case the single bucket contains something, use the mean as
            # max.
            return histogram_sum / histogram_count
        # otherwise the histogram has no data. Use the sum as the min and
        # max, respectively.
        return histogram_sum

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
                           histogram_sum / histogram_count)
            # In any other bucket (lowerBound, upperBound], use the upperBound.
            return histogram.explicit_bounds[index]

    # there are no counts > 0, so calculating a mean would result in a
    # division by 0. By returning the sum, we can let the backend decide what
    # to do with the value (with a count of 0)
    return histogram_sum


def _get_histogram_min(histogram: Histogram):
    if histogram.min is not None and math.isfinite(histogram.min):
        return histogram.min

    histogram_sum = histogram.sum
    histogram_count = sum(histogram.bucket_counts)
    if len(histogram.bucket_counts) == 1:
        # In this case, only one bucket exists: (-Inf, Inf). If there were
        # any boundaries, there would be more counts.
        if histogram.bucket_counts[0] > 0:
            # in case the single bucket contains something, use the mean as
            # min.
            return histogram_sum / histogram_count
        # otherwise the histogram has no data. Use the sum as the min and
        # max, respectively.
        return histogram_sum

    # iterate all buckets to find the first bucket with count > 0
    for index in range(0, len(histogram.bucket_counts)):
        # the current bucket contains something.
        if histogram.bucket_counts[index] > 0:
            if index == 0:
                # In the first bucket, (-Inf, firstBound], use firstBound (
                # this is the lowest specified bound overall). This is not
                # quite correct but the best approximation we can get at
                # this point. However, this might lead to a min bigger than
                # the mean, thus choose the minimum of the following:
                # - The lowest boundary
                # - The histogram's average (histogram sum / sum of counts)
                return min(histogram.explicit_bounds[index],
                           histogram_sum / histogram_count)
            # In all other buckets (lowerBound, upperBound] use the
            # lowerBound to estimate min.
            return histogram.explicit_bounds[index - 1]

    # there are no counts > 0, so calculating a mean would result in a
    # division by 0. By returning the sum, we can let the backend decide what
    # to do with the value (with a count of 0)
    return histogram_sum


class DynatraceMetricsExporter(MetricExporter):
    """
    A class which implements the OpenTelemetry MetricsExporter interface

    Methods
    -------
    export(metric_records: Sequence[MetricRecord])
    """

    def __init__(
            self,
            endpoint_url: Optional[str] = None,
            api_token: Optional[str] = None,
            prefix: Optional[str] = None,
            default_dimensions: Optional[Mapping[str, str]] = None,
            export_dynatrace_metadata: Optional[bool] = False,
    ):
        self.__logger = logging.getLogger(__name__)

        if endpoint_url:
            self._endpoint_url = endpoint_url
        else:
            self.__logger.info("No Dynatrace endpoint specified, exporting "
                               "to default local OneAgent ingest endpoint.")
            self._endpoint_url = "http://localhost:14499/metrics/ingest"

        self._metric_factory = DynatraceMetricsFactory()
        self._serializer = DynatraceMetricsSerializer(
            self.__logger.getChild(DynatraceMetricsSerializer.__name__),
            prefix,
            default_dimensions,
            export_dynatrace_metadata,
            "opentelemetry")

        self._session = requests.Session()
        self._headers = {
            "Accept": "*/*; q=0",
            "Content-Type": "text/plain; charset=utf-8",
            "User-Agent": "opentelemetry-metric-python",
        }
        if api_token:
            if not endpoint_url:
                self.__logger.warning("Just API token but no endpoint passed. "
                                      "Skipping token authentication for local"
                                      " OneAgent endpoint")
            else:
                self._headers["Authorization"] = "Api-Token " + api_token

    @property
    def preferred_temporality(self) -> AggregationTemporality:
        return AggregationTemporality.DELTA

    def export(
            self, metric_records: Sequence[Metric]
    ) -> MetricExportResult:
        """
        Export a batch of metric records to Dynatrace

        Parameters
        ----------
        metric_records : Sequence[Metric], required
            A sequence of metrics to be exported

        Raises
        ------
        HTTPError
            If one occurred

        Returns
        -------
        MetricExportResult
            Indicates SUCCESS or FAILURE
        """
        if not metric_records:
            return MetricExportResult.SUCCESS

        # split all metrics into batches of
        # DynatraceMetricApiConstants.PayloadLinesLimit lines
        chunk_size = DynatraceMetricsApiConstants.payload_lines_limit()
        chunks = [metric_records[i:i + chunk_size] for i in
                  range(0, len(metric_records), chunk_size)]

        for chunk in chunks:
            string_buffer = []
            for metric in chunk:
                dt_metric = self._to_dynatrace_metric(metric)
                if dt_metric is None:
                    continue
                try:
                    string_buffer.append(self._serializer.serialize(dt_metric))
                    string_buffer.append("\n")
                except MetricError as ex:
                    self.__logger.warning(
                        "Failed to serialize metric. Skipping: %s", ex)

            serialized_records = "".join(string_buffer)
            self.__logger.debug("sending lines:\n" + serialized_records)

            if not serialized_records:
                return MetricExportResult.SUCCESS

            try:
                with self._session.post(self._endpoint_url,
                                        data=serialized_records,
                                        headers=self._headers) as resp:
                    resp.raise_for_status()
                    self.__logger.debug("got response: {}".format(
                        resp.content.decode("utf-8")))
            except Exception as ex:
                self.__logger.warning("Failed to export metrics: %s", ex)
                return MetricExportResult.FAILURE

        return MetricExportResult.SUCCESS

    def _to_dynatrace_metric(self, metric: Metric):
        try:
            attrs = dict(metric.attributes)
            if isinstance(metric.point, Sum):
                if isinstance(metric.point.value, float):
                    return self._metric_factory.create_float_counter_delta(
                        metric.name,
                        metric.point.value,
                        attrs,
                        int(metric.point.time_unix_nano / 1000000))
                if isinstance(metric.point.value, int):
                    return self._metric_factory.create_int_counter_delta(
                        metric.name,
                        metric.point.value,
                        attrs,
                        int(metric.point.time_unix_nano / 1000000))

            if isinstance(metric.point, Gauge):
                if isinstance(metric.point.value, float):
                    return self._metric_factory.create_float_gauge(
                        metric.name,
                        metric.point.value,
                        attrs,
                        int(metric.point.time_unix_nano / 1000000))
                if isinstance(metric.point.value, int):
                    return self._metric_factory.create_int_gauge(
                        metric.name,
                        metric.point.value,
                        attrs,
                        int(metric.point.time_unix_nano / 1000000))

            if isinstance(metric.point, Histogram):
                return self._metric_factory.create_float_summary(
                    metric.name,
                    _get_histogram_min(metric.point),
                    _get_histogram_max(metric.point),
                    metric.point.sum,
                    sum(metric.point.bucket_counts),
                    attrs,
                    int(metric.point.time_unix_nano / 1000000))

            self.__logger.warning("Failed to create a Dynatrace metric, "
                                  "unsupported metric point type: %s",
                                  type(metric.point).__name__)

        except MetricError as ex:
            self.__logger.warning("Failed to create the Dynatrace metric: %s",
                                  ex)
            return None

    def shutdown(self) -> None:
        return
