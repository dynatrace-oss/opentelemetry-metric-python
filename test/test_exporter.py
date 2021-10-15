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

import unittest
from typing import Union
from unittest.mock import patch

import requests
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import aggregate, MetricRecord, \
    MetricsExportResult
from opentelemetry.sdk.resources import Resource

from dynatrace.opentelemetry.metrics.export import DynatraceMetricsExporter


class DummyMetric:
    def __init__(self, name: str):
        self.name = name


class TestExporterCreation(unittest.TestCase):

    def setUp(self) -> None:
        self._meter_provider = MeterProvider()
        self._metric = DummyMetric("my.instr")
        self._labels = (("l1", "v1"), ("l2", "v2"))
        self._headers = {
            "Accept": "*/*; q=0",
            "Content-Type": "text/plain; charset=utf-8",
            "User-Agent": "opentelemetry-metric-python",
        }
        # 01/01/2021 00:00:00
        self._test_timestamp = 1609455600000

    @patch.object(requests.Session, 'post')
    def test_empty_records(self, mock_post):
        mock_post.return_value = self._get_session_response()

        exporter = DynatraceMetricsExporter()
        result = exporter.export([])
        self.assertEqual(MetricsExportResult.SUCCESS, result)

        mock_post.assert_not_called()

    @patch.object(requests.Session, 'post')
    def test_all_optional(self, mock_post):
        mock_post.return_value = self._get_session_response()

        aggregator = aggregate.SumAggregator()
        self._update_agg_value(aggregator, 10)
        aggregator.last_update_timestamp = self._test_timestamp
        record = self._create_record(aggregator)

        exporter = DynatraceMetricsExporter()
        result = exporter.export([record])

        self.assertEqual(MetricsExportResult.SUCCESS, result)
        mock_post.assert_called_once_with(
            "http://localhost:14499/metrics/ingest",
            data="my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry "
                 "count,delta=10 " + str(self._test_timestamp),
            headers=self._headers)

    @patch.object(requests.Session, 'post')
    def test_with_endpoint(self, mock_post):
        mock_post.return_value = self._get_session_response()

        endpoint = "https://abc1234.dynatrace.com/metrics/ingest"

        aggregator = aggregate.SumAggregator()
        self._update_agg_value(aggregator, 10)
        aggregator.last_update_timestamp = self._test_timestamp
        record = self._create_record(aggregator)

        exporter = DynatraceMetricsExporter(endpoint_url=endpoint)
        result = exporter.export([record])

        self.assertEqual(MetricsExportResult.SUCCESS, result)
        mock_post.assert_called_once_with(
            endpoint,
            data="my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry "
                 "count,delta=10 " + str(self._test_timestamp),
            headers=self._headers)

    @patch.object(requests.Session, 'post')
    def test_with_endpoint_and_token(self, mock_post):
        mock_post.return_value = self._get_session_response()

        endpoint = "https://abc1234.dynatrace.com/metrics/ingest"
        token = "my.secret.token"
        # add the token to the expected headers
        self._headers["Authorization"] = "Api-Token {}".format(token)

        aggregator = aggregate.SumAggregator()
        self._update_agg_value(aggregator, 10)
        aggregator.last_update_timestamp = self._test_timestamp
        record = self._create_record(aggregator)

        exporter = DynatraceMetricsExporter(
            endpoint_url=endpoint, api_token=token)
        result = exporter.export([record])

        self.assertEqual(MetricsExportResult.SUCCESS, result)
        mock_post.assert_called_once_with(
            endpoint,
            data="my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry "
                 "count,delta=10 " + str(self._test_timestamp),
            headers=self._headers)

    @patch.object(requests.Session, 'post')
    def test_with_only_token(self, mock_post):
        mock_post.return_value = self._get_session_response()

        # token is not added in the expected headers
        token = "my.secret.token"

        aggregator = aggregate.SumAggregator()
        self._update_agg_value(aggregator, 10)
        aggregator.last_update_timestamp = self._test_timestamp
        record = self._create_record(aggregator)

        exporter = DynatraceMetricsExporter(api_token=token)
        result = exporter.export([record])

        self.assertEqual(MetricsExportResult.SUCCESS, result)
        mock_post.assert_called_once_with(
            "http://localhost:14499/metrics/ingest",
            data="my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry "
                 "count,delta=10 " + str(self._test_timestamp),
            headers=self._headers)

    @patch.object(requests.Session, 'post')
    def test_with_only_token(self, mock_post):
        mock_post.return_value = self._get_session_response()

        aggregator = aggregate.SumAggregator()
        self._update_agg_value(aggregator, 10)
        record = self._create_record(aggregator)

        prefix = "test_prefix"
        exporter = DynatraceMetricsExporter(prefix=prefix)
        result = exporter.export([record])

        self.assertEqual(MetricsExportResult.SUCCESS, result)
        mock_post.assert_called_once_with(
            "http://localhost:14499/metrics/ingest",
            data="{}.my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry "
                 "count,delta=10".format(prefix),
            headers=self._headers)

    @patch.object(requests.Session, 'post')
    def test_with_tags(self, mock_post):
        mock_post.return_value = self._get_session_response()

        aggregator = aggregate.SumAggregator()
        self._update_agg_value(aggregator, 10)
        record = self._create_record(aggregator)

        tags = {"tag1": "tv1", "tag2": "tv2"}
        exporter = DynatraceMetricsExporter(default_dimensions=tags)
        result = exporter.export([record])

        self.assertEqual(MetricsExportResult.SUCCESS, result)
        mock_post.assert_called_once_with(
            "http://localhost:14499/metrics/ingest",
            data="my.instr,tag1=tv1,tag2=tv2,l1=v1,l2=v2,"
                 "dt.metrics.source=opentelemetry count,delta=10",
            headers=self._headers)

    @patch.object(requests.Session, 'post')
    @patch('dynatrace.metric.utils._dynatrace_metadata_enricher'
           '.DynatraceMetadataEnricher._get_metadata_file_content')
    def test_dynatrace_metadata_enrichment_with_default_tags(
            self, mock_enricher, mock_post):
        mock_post.return_value = self._get_session_response()

        # tags coming from the Dynatrace metadata enricher
        mock_enricher.return_value = [
            "dt_mtag1=value1",
            "dt_mtag2=value2"
        ]

        default_tags = {"tag1": "tv1", "tag2": "tv2"}

        aggregator = aggregate.SumAggregator()
        self._update_agg_value(aggregator, 10)
        record = self._create_record(aggregator)

        exporter = DynatraceMetricsExporter(
            default_dimensions=default_tags,
            export_dynatrace_metadata=True)
        result = exporter.export([record])

        self.assertEqual(MetricsExportResult.SUCCESS, result)
        mock_post.assert_called_once_with(
            "http://localhost:14499/metrics/ingest",
            data="my.instr,tag1=tv1,tag2=tv2,l1=v1,l2=v2,"
                 "dt_mtag1=value1,dt_mtag2=value2,"
                 "dt.metrics.source=opentelemetry count,delta=10",
            headers=self._headers)

    def _create_record(self, aggregator: aggregate.Aggregator):
        return MetricRecord(
            self._metric, self._labels, aggregator, Resource({})
        )

    @staticmethod
    def _update_agg_value(
        aggregator: aggregate.Aggregator,
        value: Union[int, float],
    ):
        aggregator.update(value)
        aggregator.take_checkpoint()
        # can be overwritten later
        aggregator.last_update_timestamp = 0

    @staticmethod
    def _get_session_response(error: bool = False) -> requests.Response:
        r = requests.Response()
        if error:
            r.status_code = 500
        else:
            r.status_code = 200
            r._content = str.encode('{}')
        return r


if __name__ == '__main__':
    unittest.main()
