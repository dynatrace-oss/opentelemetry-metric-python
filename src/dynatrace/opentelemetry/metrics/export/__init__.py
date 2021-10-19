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
import requests
from typing import Mapping, Optional, Sequence

from opentelemetry.metrics import get_meter_provider
from opentelemetry.sdk.metrics.export import (
    MetricsExporter,
    MetricRecord,
    MetricsExportResult, aggregate,
)

from dynatrace.metric.utils import (
    DynatraceMetricsSerializer,
    DynatraceMetricsApiConstants,
    DynatraceMetricsFactory,
    MetricError
)

VERSION = "0.1.0b1"


class DynatraceMetricsExporter(MetricsExporter):
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

        self._is_delta_export = None
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

    def export(
        self, metric_records: Sequence[MetricRecord]
    ) -> MetricsExportResult:
        """
        Export a batch of metric records to Dynatrace

        Parameters
        ----------
        metric_records : Sequence[MetricRecord], required
            A sequence of metric records to be exported

        Raises
        ------
        HTTPError
            If one occurred

        Returns
        -------
        MetricsExportResult
            Indicates SUCCESS or FAILURE
        """
        if not metric_records:
            return MetricsExportResult.SUCCESS

        if self._is_delta_export is None:
            self._is_delta_export = self._determine_is_delta_export()

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
                return MetricsExportResult.SUCCESS

            try:
                with self._session.post(self._endpoint_url,
                                        data=serialized_records,
                                        headers=self._headers) as resp:
                    resp.raise_for_status()
                    self.__logger.debug("got response: {}".format(
                        resp.content.decode("utf-8")))
            except Exception as ex:
                self.__logger.warning("Failed to export metrics: %s", ex)
                return MetricsExportResult.FAILURE

        return MetricsExportResult.SUCCESS

    def _to_dynatrace_metric(self, metric: MetricRecord):
        try:
            attrs = dict(metric.labels)
            if isinstance(metric.aggregator, aggregate.SumAggregator):
                if not self._is_delta_export:
                    self.__logger.info("Received cumulative value which is currently not supported, using delta instead.")
                    # TODO: implement and use a Cumulative-to-Delta converter
                    return self._metric_factory.create_float_counter_delta(
                        metric.instrument.name,
                        metric.aggregator.checkpoint,
                        attrs,
                        metric.aggregator.last_update_timestamp)

                return self._metric_factory.create_float_counter_delta(
                    metric.instrument.name,
                    metric.aggregator.checkpoint,
                    attrs,
                    metric.aggregator.last_update_timestamp)
            if isinstance(metric.aggregator,
                          aggregate.MinMaxSumCountAggregator):
                cp = metric.aggregator.checkpoint
                return self._metric_factory.create_float_summary(
                    metric.instrument.name,
                    cp.min,
                    cp.max,
                    cp.sum,
                    cp.count,
                    attrs,
                    metric.aggregator.last_update_timestamp)
            if isinstance(metric.aggregator,
                          aggregate.ValueObserverAggregator):
                return self._metric_factory.create_float_gauge(
                    metric.instrument.name,
                    metric.aggregator.checkpoint,
                    attrs,
                    metric.aggregator.last_update_timestamp)
            if isinstance(metric.aggregator, aggregate.LastValueAggregator):
                return self._metric_factory.create_float_gauge(
                    metric.instrument.name,
                    metric.aggregator.checkpoint,
                    attrs,
                    metric.aggregator.last_update_timestamp)
            if isinstance(metric.aggregator, aggregate.HistogramAggregator):
                cp = metric.aggregator.checkpoint
                # TODO: remove this hack which pretends
                #  all data points had the same value
                avg = cp.sum / cp.count
                return self._metric_factory.create_float_summary(
                    metric.instrument.name,
                    avg,
                    avg,
                    cp.sum,
                    cp.count,
                    attrs,
                    metric.aggregator.last_update_timestamp)
            return None
        except MetricError as ex:
            self.__logger.warning("Failed to create the Dynatrace metric: %s",
                                  ex)
            return None

    @staticmethod
    def _determine_is_delta_export():
        meter_provider = get_meter_provider()
        return hasattr(meter_provider,
                       "stateful") and not meter_provider.stateful
