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
from typing import Mapping, Optional

import requests
from dynatrace.metric.utils import (
    DynatraceMetricsSerializer,
    MetricError,
    DynatraceMetricsApiConstants,
)
from dynatrace.opentelemetry.metrics.export._factory import (
    _OTelDynatraceMetricsFactory,
)
from opentelemetry.sdk.metrics.export import (
    MetricExporter,
    MetricExportResult,
    MetricsData,
)

import opentelemetry.sdk.metrics as metrics
from opentelemetry.sdk.metrics.export import (
    AggregationTemporality,
)

_DYNATRACE_TEMPORALITY_PREFERENCE = {
    metrics.Counter: AggregationTemporality.DELTA,
    metrics.UpDownCounter: AggregationTemporality.CUMULATIVE,
    metrics.Histogram: AggregationTemporality.DELTA,
    metrics.ObservableCounter: AggregationTemporality.DELTA,
    metrics.ObservableUpDownCounter: AggregationTemporality.CUMULATIVE,
    metrics.ObservableGauge: AggregationTemporality.CUMULATIVE,
}


class _DynatraceMetricsExporter(MetricExporter):
    """
    A class which implements the OpenTelemetry MetricsExporter interface

    Methods
    -------
    export(metric_records: MetricsData)
    """

    def __init__(
        self,
        endpoint_url: Optional[str] = None,
        api_token: Optional[str] = None,
        prefix: Optional[str] = None,
        default_dimensions: Optional[Mapping[str, str]] = None,
        export_dynatrace_metadata: Optional[bool] = False,
    ):
        super().__init__(
            preferred_temporality=_DYNATRACE_TEMPORALITY_PREFERENCE,
            preferred_aggregation=None
        )
        self.__logger = logging.getLogger(__name__)

        if endpoint_url:
            self._endpoint_url = endpoint_url
        else:
            self.__logger.info("No Dynatrace endpoint specified, exporting "
                               "to default local OneAgent ingest endpoint.")
            self._endpoint_url = "http://localhost:14499/metrics/ingest"

        self._metric_factory = _OTelDynatraceMetricsFactory()
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
                        dt_metric = self._metric_factory.create_metric(
                            metric,
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

    def force_flush(self, timeout_millis: float = 10_000) -> bool:
        # nothing to do.
        pass

    def shutdown(self, timeout_millis: float = 30_000, **kwargs) -> None:
        # nothing to do.
        pass

    def _send_lines(self, metric_lines):
        # split all metrics into batches of
        # DynatraceMetricApiConstants.PayloadLinesLimit lines
        chunk_size = DynatraceMetricsApiConstants.payload_lines_limit()

        for index in range(0, len(metric_lines), chunk_size):
            metric_lines_chunk = metric_lines[index:index + chunk_size]
            serialized_records = "\n".join(metric_lines_chunk)
            self.__logger.debug(
                "sending lines:\n" + serialized_records)
            with self._session.post(self._endpoint_url,
                                    data=serialized_records,
                                    headers=self._headers) as resp:
                resp.raise_for_status()
                self.__logger.debug(
                    "got response: {}".format(
                        resp.content.decode("utf-8")))
