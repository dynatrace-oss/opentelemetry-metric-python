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
from typing import Mapping, Optional, Sequence

import requests

from dynatrace.opentelemetry.metric import serializer
from opentelemetry.sdk.metrics.export import (
    MetricsExporter,
    MetricRecord,
    MetricsExportResult,
)

logger = logging.Logger(__name__)


class DynatraceMetricsExporter(MetricsExporter):

    def __init__(
        self,
        endpoint_url: str,
        api_token: Optional[str] = None,
        prefix: Optional[str] = None,
        tags: Optional[Mapping[str, str]] = None,
    ):
        self._endpoint_url = endpoint_url
        self._serializer = serializer.DynatraceMetricsSerializer(prefix, tags)
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
        serialized_records = self._serializer.serialize_records(metric_records)
        if not serialized_records:
            return MetricsExportResult.SUCCESS

        try:
            with self._session.post(
                self._endpoint_url,
                data = serialized_records,
                headers = self._headers,
            ) as resp:
                resp.raise_for_status()
        except Exception as ex:
            logger.warning("Failed to export metrics: %s", ex)
            return MetricsExportResult.FAILURE
        return MetricsExportResult.SUCCESS
