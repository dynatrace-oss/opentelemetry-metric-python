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

from opentelemetry.sdk.metrics.export import (
    MetricsExporter,
    MetricRecord,
    MetricsExportResult,
)

from .serializer import DynatraceMetricsSerializer
from .oneagentmetadataenricher import OneAgentMetadataEnricher

VERSION = "0.1.0-beta"


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
        export_oneagent_metadata: Optional[bool] = False,
    ):
        if endpoint_url:
            self._endpoint_url = endpoint_url
        else:
            logging.info("No Dynatrace endpoint specified, exporting "
                         "to default local OneAgent ingest endpoint.")
            self._endpoint_url = "http://localhost:14499/metrics/ingest"

        all_dimensions = default_dimensions or {}

        if export_oneagent_metadata:
            enricher = OneAgentMetadataEnricher()
            enricher.add_oneagent_metadata_to_dimensions(all_dimensions)

        self._serializer = DynatraceMetricsSerializer(prefix, all_dimensions)
        self._session = requests.Session()
        self._headers = {
            "Accept": "*/*; q=0",
            "Content-Type": "text/plain; charset=utf-8",
        }
        if api_token:
            if not endpoint_url:
                logging.warning("Just API token but no endpoint passed. "
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
        serialized_records = self._serializer.serialize_records(metric_records)
        logging.debug(serialized_records)
        if not serialized_records:
            return MetricsExportResult.SUCCESS

        try:
            with self._session.post(
                self._endpoint_url,
                data=serialized_records,
                headers=self._headers,
            ) as resp:
                resp.raise_for_status()
        except Exception as ex:
            logging.warning("Failed to export metrics: %s", ex)
            return MetricsExportResult.FAILURE
        return MetricsExportResult.SUCCESS
