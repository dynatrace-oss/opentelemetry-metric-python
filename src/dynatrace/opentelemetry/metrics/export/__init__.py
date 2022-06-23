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
from typing import Mapping, Optional

import opentelemetry.sdk.metrics as metrics

from dynatrace.metric.utils import (
    DynatraceMetricsSerializer,
    DynatraceMetricsApiConstants,
    DynatraceMetricsFactory,
    MetricError
)

from opentelemetry.sdk.metrics.export import (
    MetricExporter,
    MetricExportResult,
    Sum,
    AggregationTemporality,
    Gauge,
    Histogram,
    MetricsData,
    DataPointT,
    NumberDataPoint,
    Metric,
    HistogramDataPoint
)

VERSION = "0.3.0-rc1"

DYNATRACE_TEMPORALITY_PREFERENCE = {
    metrics.Counter: AggregationTemporality.DELTA,
    metrics.UpDownCounter: AggregationTemporality.CUMULATIVE,
    metrics.Histogram: AggregationTemporality.DELTA,
    metrics.ObservableCounter: AggregationTemporality.DELTA,
    metrics.ObservableUpDownCounter: AggregationTemporality.CUMULATIVE,
    metrics.ObservableGauge: AggregationTemporality.CUMULATIVE,
}


def _get_histogram_max(histogram: HistogramDataPoint):
    if histogram.max is not None and math.isfinite(histogram.max):
        return histogram.max

    histogram_sum = histogram.sum
    histogram_count = histogram.count
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


def _get_histogram_min(histogram: HistogramDataPoint):
    if histogram.min is not None and math.isfinite(histogram.min):
        return histogram.min

    histogram_sum = histogram.sum
    histogram_count = histogram.count
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
                # In the first bucket, (-Inf, firstBound], use firstBound
                # (this is the lowest specified bound overall). This is not
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
    export(metric_records: MetricsData)
    """

    def shutdown(self, timeout_millis: float = 30_000, **kwargs) -> None:
        # nothing to do.
        pass

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

    def export(self,
               metrics_data: MetricsData,
               **kwargs) -> MetricExportResult:
        """
                Export Metrics to Dynatrace

                Parameters
                ----------
                metrics_data : MetricsData, required
                    The Metrics to be exported

                Returns
                -------
                MetricExportResult
                    Indicates SUCCESS (all metrics exported successfully)
                    or FAILURE (otherwise)
                """
        if len(metrics_data.resource_metrics) == 0:
            return MetricExportResult.SUCCESS

        string_buffer = []
        for resource_metric in metrics_data.resource_metrics:
            for scope_metric in resource_metric.scope_metrics:
                for metric in scope_metric.metrics:
                    for data_point in metric.data.data_points:
                        dt_metric = self._to_dynatrace_metric(metric,
                                                              data_point)
                        if dt_metric is None:
                            continue
                        try:
                            string_buffer.append(
                                self._serializer.serialize(dt_metric))
                        except MetricError as ex:
                            self.__logger.warning(
                                "Failed to serialize metric. Skipping: %s", ex)
        try:
            self._send_lines(string_buffer)
        except Exception as ex:
            self.__logger.warning(
                "Failed to export metrics: %s", ex)
            return MetricExportResult.FAILURE
        return MetricExportResult.SUCCESS

    def _send_lines(self, metric_lines):
        # split all metrics into batches of
        # DynatraceMetricApiConstants.PayloadLinesLimit lines
        chunk_size = DynatraceMetricsApiConstants.payload_lines_limit()

        for index in range(0, len(metric_lines), chunk_size):
            metric_lines_chunk = metric_lines[index:index + chunk_size]
            serialized_records = "\n".join(metric_lines_chunk) + "\n"
            self.__logger.debug(
                "sending lines:\n" + serialized_records)
            with self._session.post(self._endpoint_url,
                                    data=serialized_records,
                                    headers=self._headers) as resp:
                resp.raise_for_status()
                self.__logger.debug(
                    "got response: {}".format(
                        resp.content.decode("utf-8")))

    def _log_temporality_mismatch(
            self,
            metric_type:
            str, metric: Metric,
            supported_temporality: AggregationTemporality):
        self.__logger.warning("Failed to create Dynatrace metric: "
                              "exporter received %s '%s' with "
                              "AggregationTemporality.%s, but only "
                              "AggregationTemporality.%s is supported.",
                              metric_type,
                              metric.name,
                              metric.data.aggregation_temporality.name,
                              supported_temporality.name)

    def _monotonic_to_dynatrace_metric(self, metric: Metric,
                                       point: NumberDataPoint):
        if isinstance(point.value, float):
            return self._metric_factory.create_float_counter_delta(
                metric.name,
                point.value,
                dict(point.attributes),
                int(point.time_unix_nano / 1000000))
        if isinstance(point.value, int):
            return self._metric_factory.create_int_counter_delta(
                metric.name,
                point.value,
                dict(point.attributes),
                int(point.time_unix_nano / 1000000))

    def _non_monotonic_to_dynatrace_metric(self, metric: Metric,
                                           point: NumberDataPoint):
        if isinstance(point.value, float):
            return self._metric_factory.create_float_gauge(
                metric.name,
                point.value,
                dict(point.attributes),
                int(point.time_unix_nano / 1000000))
        if isinstance(point.value, int):
            return self._metric_factory.create_int_gauge(
                metric.name,
                point.value,
                dict(point.attributes),
                int(point.time_unix_nano / 1000000))

    def _histogram_to_dynatrace_metric(self, metric: Metric,
                                       point: HistogramDataPoint):
        # only allow AggregationTemporality.DELTA
        if metric.data.aggregation_temporality != AggregationTemporality.DELTA:
            self._log_temporality_mismatch("Histogram",
                                           metric,
                                           AggregationTemporality.DELTA)
            return None

        return self._metric_factory.create_float_summary(
            metric.name,
            _get_histogram_min(point),
            _get_histogram_max(point),
            point.sum,
            sum(point.bucket_counts),
            dict(point.attributes),
            int(point.time_unix_nano / 1000000))

    def _sum_to_dynatrace_metric(self, metric: Metric, point: NumberDataPoint):
        if metric.data.is_monotonic:
            if metric.data.aggregation_temporality != \
                    AggregationTemporality.DELTA:
                self._log_temporality_mismatch(
                    "monotonic Sum",
                    metric,
                    supported_temporality=AggregationTemporality.DELTA)
                return None
            return self._monotonic_to_dynatrace_metric(metric, point)
        else:
            if metric.data.aggregation_temporality != \
                    AggregationTemporality.CUMULATIVE:
                self._log_temporality_mismatch(
                    "non-monotonic Sum",
                    metric,
                    supported_temporality=AggregationTemporality.CUMULATIVE)
                return None
            return self._non_monotonic_to_dynatrace_metric(metric, point)

    def _to_dynatrace_metric(self, metric: Metric, point: DataPointT):
        try:
            if isinstance(metric.data, Sum):
                return self._sum_to_dynatrace_metric(metric, point)
            if isinstance(metric.data, Histogram):
                return self._histogram_to_dynatrace_metric(metric, point)
            if isinstance(metric.data, Gauge):
                # allow any temporality.
                return self._non_monotonic_to_dynatrace_metric(metric, point)

            self.__logger.warning("Failed to create a Dynatrace metric, "
                                  "unsupported metric point type: %s",
                                  type(metric.data).__name__)

        except MetricError as ex:
            self.__logger.warning("Failed to create the Dynatrace metric: %s",
                                  ex)
            return None
