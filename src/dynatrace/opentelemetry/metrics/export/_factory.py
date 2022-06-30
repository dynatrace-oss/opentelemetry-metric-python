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

import logging

from dynatrace.metric.utils import (
    DynatraceMetricsFactory,
    MetricError
)
from opentelemetry.sdk.metrics.export import (
    Sum,
    AggregationTemporality,
    Gauge,
    Histogram,
    DataPointT,
    NumberDataPoint,
    Metric,
    HistogramDataPoint
)

from dynatrace.opentelemetry.metrics.export._histogram_utils import (
    _get_histogram_min,
    _get_histogram_max
)


class _OTelDynatraceMetricsFactory:
    """
    A class which implements the OpenTelemetry MetricsExporter interface

    Methods
    -------
    export(metric_records: MetricsData)
    """

    def __init__(
            self,
    ):
        self.__logger = logging.getLogger(__name__)
        self._metric_factory = DynatraceMetricsFactory()

    def create_metric(self, metric: Metric, point: DataPointT):
        try:
            if isinstance(metric.data, Sum):
                return self._sum_to_dynatrace_metric(metric, point)
            if isinstance(metric.data, Histogram):
                return self._histogram_to_dynatrace_metric(metric, point)
            if isinstance(metric.data, Gauge):
                # gauge does not support or require temporality.
                return self._to_dynatrace_gauge(metric, point)

            self.__logger.warning("Failed to create a Dynatrace metric, "
                                  "unsupported metric point type: %s",
                                  type(metric.data).__name__)

        except MetricError as ex:
            self.__logger.warning("Failed to create the Dynatrace metric: %s",
                                  ex)
            return None

    def _sum_to_dynatrace_metric(self, metric: Metric, point: NumberDataPoint):
        if metric.data.is_monotonic:
            if metric.data.aggregation_temporality != \
                    AggregationTemporality.DELTA:
                self._log_temporality_mismatch(
                    "monotonic Sum",
                    metric,
                    supported_temporality=AggregationTemporality.DELTA)
                return None
            return self._to_dynatrace_counter(metric, point)
        else:
            if metric.data.aggregation_temporality != \
                    AggregationTemporality.CUMULATIVE:
                self._log_temporality_mismatch(
                    "non-monotonic Sum",
                    metric,
                    supported_temporality=AggregationTemporality.CUMULATIVE)
                return None
            return self._to_dynatrace_gauge(metric, point)

    def _to_dynatrace_counter(self, metric: Metric,
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

    def _to_dynatrace_gauge(self, metric: Metric,
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
            self._log_temporality_mismatch(
                "Histogram",
                metric,
                supported_temporality=AggregationTemporality.DELTA)
            return None

        return self._metric_factory.create_float_summary(
            metric.name,
            _get_histogram_min(point),
            _get_histogram_max(point),
            point.sum,
            sum(point.bucket_counts),
            dict(point.attributes),
            int(point.time_unix_nano / 1000000))

    def _log_temporality_mismatch(
            self,
            metric_type:
            str, metric: Metric,
            supported_temporality: AggregationTemporality):
        self.__logger.warning("Failed to create Dynatrace metric: "
                              "exporter received %s '%s' with "
                              "AggregationTemporality.%s, but only "
                              "AggregationTemporality.%s is currently "
                              "supported.",
                              metric_type,
                              metric.name,
                              metric.data.aggregation_temporality.name,
                              supported_temporality.name)
