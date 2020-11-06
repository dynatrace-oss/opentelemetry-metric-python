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
from typing import Callable, List, Optional, Sequence

import requests

from opentelemetry.sdk.metrics.export import (
    aggregate,
    MetricsExporter,
    MetricRecord,
    MetricsExportResult,
)

logger = logging.Logger(__name__)


class DynatraceMetricsExporter(MetricsExporter):

    def __init__(self, endpoint_url: str, api_token: Optional[str] = None):
        self._endpoint_url = endpoint_url
        self._session = requests.Session()
        self._headers = {
            "Accept": "*/*; q=0",
            "Content-Type": "text/plain; charset=utf-8",
        }
        if api_token:
            self._headers["Authorization"] = "Api-Token " + api_token

    def export(
        self, metric_records: Sequence[MetricRecord]
    ) -> MetricsExportResult:

        string_buffer = []
        for record in metric_records:
            self._write_record(string_buffer, record)

        export_data = "".join(string_buffer)
        if not export_data:
            return MetricsExportResult.SUCCESS

        try:
            with self._session.post(
                self._endpoint_url,
                data= export_data,
                headers=self._headers,
            ) as resp:
                resp.raise_for_status()
        except Exception as ex:
            logger.warning("Failed to export metrics: %s", ex)
        return MetricsExportResult.SUCCESS

    @staticmethod
    def _write_metric_key(string_buffer: List, record: MetricRecord):
        string_buffer.append(record.instrument.name)
        pass

    @staticmethod
    def _write_dimensions(string_buffer: List, record: MetricRecord):
        for key, value in record.labels:
            string_buffer.append(",")
            string_buffer.append(key)
            string_buffer.append("=")
            string_buffer.append(value)

    def _write_record(self, string_buffer: List, record: MetricRecord):
        aggregator = record.aggregator
        serialize_func = self._get_serialize_func(aggregator)

        if serialize_func is None:
            return

        self._write_metric_key(string_buffer, record)
        self._write_dimensions(string_buffer, record)

        serialize_func(string_buffer, aggregator)

        self._write_timestamp(string_buffer, aggregator)
        string_buffer.append("\n")

    def _get_serialize_func(
        self, aggregator: aggregate.Aggregator
    ) -> Optional[Callable]:
        if isinstance(aggregator, aggregate.SumAggregator):
            return self._write_delta_value
        if isinstance(aggregator, aggregate.MinMaxSumCountAggregator):
            return self._write_summary_value
        if isinstance(aggregator, aggregate.ValueObserverAggregator):
            return self._write_summary_value
        if isinstance(aggregator, aggregate.LastValueAggregator):
            return None  # Not supported
        if isinstance(aggregator, aggregate.HistogramAggregator):
            return None  # Not supported
        return None

    @staticmethod
    def _write_delta_value(
        string_buffer: List, aggregator: aggregate.SumAggregator
    ):
        string_buffer.append(" count,delta=")
        string_buffer.append(str(aggregator.checkpoint))

    @staticmethod
    def _write_absolute_value(string_buffer: List, aggregator):
        string_buffer.append(" count,")
        string_buffer.append(str(aggregator.checkpoint))

    @staticmethod
    def _write_summary_value(
        string_buffer: List, aggregator: aggregate.MinMaxSumCountAggregator
    ):
        checkpoint = aggregator.checkpoint
        string_buffer.append(" gauge,min=")
        string_buffer.append(str(checkpoint.min))
        string_buffer.append(",max=")
        string_buffer.append(str(checkpoint.max))
        string_buffer.append(",sum=")
        string_buffer.append(str(checkpoint.sum))
        string_buffer.append(",count=")
        string_buffer.append(str(checkpoint.count))

    @staticmethod
    def _write_timestamp(sb: List, aggregator: aggregate.Aggregator):
        sb.append(" ")
        sb.append(str(aggregator.last_update_timestamp // 1000000))
