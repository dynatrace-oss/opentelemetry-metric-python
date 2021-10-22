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
from unittest.mock import patch, MagicMock

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
        self._update_value(aggregator, 10)
        record = self._create_record(aggregator)

        exporter = DynatraceMetricsExporter()
        exporter._is_delta_export = True
        result = exporter.export([record])

        self.assertEqual(MetricsExportResult.SUCCESS, result)
        mock_post.assert_called_once_with(
            "http://localhost:14499/metrics/ingest",
            data="my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry count,delta=10\n",
            headers=self._headers)

    @patch.object(requests.Session, 'post')
    def test_with_endpoint(self, mock_post):
        mock_post.return_value = self._get_session_response()

        endpoint = "https://abc1234.dynatrace.com/metrics/ingest"

        aggregator = aggregate.SumAggregator()
        self._update_value(aggregator, 10)
        record = self._create_record(aggregator)

        exporter = DynatraceMetricsExporter(endpoint_url=endpoint)
        exporter._is_delta_export = True
        result = exporter.export([record])

        self.assertEqual(MetricsExportResult.SUCCESS, result)
        mock_post.assert_called_once_with(
            endpoint,
            data="my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry count,delta=10\n",
            headers=self._headers)

    @patch.object(requests.Session, 'post')
    def test_with_endpoint_and_token(self, mock_post):
        mock_post.return_value = self._get_session_response()

        endpoint = "https://abc1234.dynatrace.com/metrics/ingest"
        token = "my.secret.token"
        # add the token to the expected headers
        self._headers["Authorization"] = "Api-Token {}".format(token)

        aggregator = aggregate.SumAggregator()
        self._update_value(aggregator, 10)
        record = self._create_record(aggregator)

        exporter = DynatraceMetricsExporter(
            endpoint_url=endpoint, api_token=token)
        exporter._is_delta_export = True
        result = exporter.export([record])

        self.assertEqual(MetricsExportResult.SUCCESS, result)
        mock_post.assert_called_once_with(
            endpoint,
            data="my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry count,delta=10\n",
            headers=self._headers)

    @patch.object(requests.Session, 'post')
    def test_with_only_token(self, mock_post):
        mock_post.return_value = self._get_session_response()

        # token is not added in the expected headers
        token = "my.secret.token"

        aggregator = aggregate.SumAggregator()
        self._update_value(aggregator, 10)
        record = self._create_record(aggregator)

        exporter = DynatraceMetricsExporter(api_token=token)
        exporter._is_delta_export = True
        result = exporter.export([record])

        self.assertEqual(MetricsExportResult.SUCCESS, result)
        mock_post.assert_called_once_with(
            "http://localhost:14499/metrics/ingest",
            data="my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry count,delta=10\n",
            headers=self._headers)

    @patch.object(requests.Session, 'post')
    def test_with_prefix(self, mock_post):
        mock_post.return_value = self._get_session_response()

        aggregator = aggregate.SumAggregator()
        self._update_value(aggregator, 10)
        record = self._create_record(aggregator)

        prefix = "test_prefix"
        exporter = DynatraceMetricsExporter(prefix=prefix)
        exporter._is_delta_export = True
        result = exporter.export([record])

        self.assertEqual(MetricsExportResult.SUCCESS, result)
        mock_post.assert_called_once_with(
            "http://localhost:14499/metrics/ingest",
            data="{}.my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry count,delta=10\n".format(prefix),
            headers=self._headers)

    @patch.object(requests.Session, 'post')
    def test_with_tags(self, mock_post):
        mock_post.return_value = self._get_session_response()

        aggregator = aggregate.SumAggregator()
        self._update_value(aggregator, 10)
        record = self._create_record(aggregator)

        tags = {"tag1": "tv1", "tag2": "tv2"}
        exporter = DynatraceMetricsExporter(default_dimensions=tags)
        exporter._is_delta_export = True
        result = exporter.export([record])

        self.assertEqual(MetricsExportResult.SUCCESS, result)
        mock_post.assert_called_once_with(
            "http://localhost:14499/metrics/ingest",
            data="my.instr,tag1=tv1,tag2=tv2,l1=v1,l2=v2,dt.metrics.source=opentelemetry count,delta=10\n",
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
        self._update_value(aggregator, 10)
        record = self._create_record(aggregator)

        exporter = DynatraceMetricsExporter(
            default_dimensions=default_tags,
            export_dynatrace_metadata=True)
        exporter._is_delta_export = True
        result = exporter.export([record])

        self.assertEqual(MetricsExportResult.SUCCESS, result)
        mock_post.assert_called_once_with(
            "http://localhost:14499/metrics/ingest",
            data="my.instr,tag1=tv1,tag2=tv2,l1=v1,l2=v2,"
                 "dt_mtag1=value1,dt_mtag2=value2,"
                 "dt.metrics.source=opentelemetry count,delta=10\n",
            headers=self._headers)


    @patch.object(requests.Session, 'post')
    @patch('dynatrace.metric.utils.dynatrace_metrics_api_constants'
           '.DynatraceMetricsApiConstants.payload_lines_limit')
    def test_invalid_metricname_skipped(self, mock_const, mock_post):
        mock_post.return_value = self._get_session_response()
        mock_const.return_value = 2

        records = []
        for n in range(4):
            aggregator = aggregate.SumAggregator()
            self._update_value(aggregator, n)

            if n == 3:
                # for the last metric, we create one with an invalid name
                records.append(MetricRecord(
                    DummyMetric(""), self._labels, aggregator, Resource({})
                ))
            else:
                records.append(self._create_record(aggregator))

        first_expected = "my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry count,delta=0\nmy.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry count,delta=1\n"

        # the second export misses the metric with delta=3 because the name was invalid
        second_expected = "my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry count,delta=2\n"

        exporter = DynatraceMetricsExporter()
        exporter._is_delta_export = True
        result = exporter.export(records)

        self.assertEqual(MetricsExportResult.SUCCESS, result)

        mock_post.assert_any_call(
            "http://localhost:14499/metrics/ingest",
            data=second_expected,
            headers=self._headers)

        mock_post.assert_any_call(
            "http://localhost:14499/metrics/ingest",
            data=first_expected,
            headers=self._headers)

    @patch.object(requests.Session, 'post')
    @patch('dynatrace.metric.utils.dynatrace_metrics_api_constants'
           '.DynatraceMetricsApiConstants.payload_lines_limit')
    def test_fail_to_serialize_skipped(self, mock_const, mock_post):
        mock_post.return_value = self._get_session_response()
        mock_const.return_value = 2

        records = []
        for n in range(4):
            aggregator = aggregate.SumAggregator()
            self._update_value(aggregator, n)

            if n == 3:
                # for the last metric, we create one with an invalid name
                # this will only fail when calling the serializer
                records.append(MetricRecord(
                    DummyMetric("."), self._labels, aggregator, Resource({})
                ))
            else:
                records.append(self._create_record(aggregator))

        first_expected = "my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry count,delta=0\nmy.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry count,delta=1\n"

        # the second export misses the metric with delta=3 because the name was invalid
        second_expected = "my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry count,delta=2\n"

        exporter = DynatraceMetricsExporter()
        exporter._is_delta_export = True
        result = exporter.export(records)

        self.assertEqual(MetricsExportResult.SUCCESS, result)

        mock_post.assert_any_call(
            "http://localhost:14499/metrics/ingest",
            data=second_expected,
            headers=self._headers)

        mock_post.assert_any_call(
            "http://localhost:14499/metrics/ingest",
            data=first_expected,
            headers=self._headers)

    @patch.object(requests.Session, 'post')
    @patch('dynatrace.metric.utils.dynatrace_metrics_api_constants'
           '.DynatraceMetricsApiConstants.payload_lines_limit')
    def test_entire_batch_fail(self, mock_const, mock_post):
        mock_post.side_effect = [self._get_session_response(),
                                 self._get_session_response(error=True)]
        mock_const.return_value = 2

        records = []
        for n in range(4):
            aggregator = aggregate.SumAggregator()
            self._update_value(aggregator, n)
            records.append(self._create_record(aggregator))

        first_expected = "my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry count,delta=0\nmy.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry count,delta=1\n"
        second_expected = "my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry count,delta=2\nmy.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry count,delta=3\n"

        exporter = DynatraceMetricsExporter()
        exporter._is_delta_export = True
        result = exporter.export(records)

        # should have failed the whole batch as the second POST request failed
        self.assertEqual(MetricsExportResult.FAILURE, result)

        mock_post.assert_any_call(
            "http://localhost:14499/metrics/ingest",
            data=first_expected,
            headers=self._headers)

        mock_post.assert_any_call(
            "http://localhost:14499/metrics/ingest",
            data=second_expected,
            headers=self._headers)

    @patch.object(requests.Session, 'post')
    def test_sum_aggregator_delta(self, mock_post):
        aggregator = aggregate.SumAggregator()
        self._update_value(aggregator, 10)
        aggregator.last_update_timestamp = self._test_timestamp

        record = self._create_record(aggregator)

        exporter = DynatraceMetricsExporter()
        exporter._is_delta_export = True

        result = exporter.export([record])

        self.assertEqual(MetricsExportResult.SUCCESS, result)
        mock_post.assert_called_once_with(
            "http://localhost:14499/metrics/ingest",
            data="my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry count,delta=10 " + str(self._test_timestamp) + "\n",
            headers=self._headers)

    @patch.object(requests.Session, 'post')
    def test_sum_aggregator_total_reported_as_gauge(self, mock_post):
        aggregator = aggregate.SumAggregator()
        self._update_value(aggregator, 10)
        aggregator.last_update_timestamp = self._test_timestamp

        record = self._create_record(aggregator)

        exporter = DynatraceMetricsExporter()
        exporter._is_delta_export = False
        result = exporter.export([record])

        self.assertEqual(MetricsExportResult.SUCCESS, result)
        mock_post.assert_called_once_with(
            "http://localhost:14499/metrics/ingest",
            data="my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry gauge,10 " + str(self._test_timestamp) + "\n",
            headers=self._headers)

    @patch.object(requests.Session, 'post')
    def test_min_max_sum_count_aggregator(self, mock_post):
        mock_post.return_value = self._get_session_response()

        aggregator = aggregate.MinMaxSumCountAggregator()
        aggregator.update(100)
        aggregator.update(1)
        self._update_value(aggregator, 10)
        aggregator.last_update_timestamp = self._test_timestamp
        record = self._create_record(aggregator)

        exporter = DynatraceMetricsExporter()
        result = exporter.export([record])

        self.assertEqual(MetricsExportResult.SUCCESS, result)
        mock_post.assert_called_once_with(
            "http://localhost:14499/metrics/ingest",
            data="my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry gauge,min=1,max=100,sum=111,count=3 " + str(self._test_timestamp) + "\n",
            headers=self._headers)

    @patch.object(requests.Session, 'post')
    def test_last_value_aggregator(self, mock_post):
        mock_post.return_value = self._get_session_response()

        aggregator = aggregate.LastValueAggregator()
        self._update_value(aggregator, 20)

        record = self._create_record(aggregator)

        exporter = DynatraceMetricsExporter()
        result = exporter.export([record])

        self.assertEqual(MetricsExportResult.SUCCESS, result)
        mock_post.assert_called_once_with(
            "http://localhost:14499/metrics/ingest",
            data="my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry gauge,20\n",
            headers=self._headers)

    @patch.object(requests.Session, 'post')
    def test_multiple_records(self, mock_post):
        mock_post.return_value = self._get_session_response()

        records = []
        aggregator = aggregate.SumAggregator()
        self._update_value(aggregator, 10)
        records.append(self._create_record(aggregator))

        aggregator = aggregate.MinMaxSumCountAggregator()
        aggregator.update(100)
        aggregator.update(1)
        self._update_value(aggregator, 10)
        records.append(self._create_record(aggregator))

        aggregator = aggregate.LastValueAggregator()
        self._update_value(aggregator, 20)
        records.append(self._create_record(aggregator))

        exporter = DynatraceMetricsExporter()
        exporter._is_delta_export = True
        result = exporter.export(records)

        expected = """my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry count,delta=10
my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry gauge,min=1,max=100,sum=111,count=3
my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry gauge,20\n"""

        self.assertEqual(MetricsExportResult.SUCCESS, result)
        mock_post.assert_called_once_with(
            "http://localhost:14499/metrics/ingest",
            data=expected,
            headers=self._headers)

    def _create_record(self, aggregator: aggregate.Aggregator):
        return MetricRecord(
            self._metric, self._labels, aggregator, Resource({})
        )

    @staticmethod
    def _update_value(
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
